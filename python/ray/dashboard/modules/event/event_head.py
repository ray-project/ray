import asyncio
import logging
import os
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import islice
from typing import Dict, List, Union

import aiohttp.web

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import env_integer
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.dashboard.consts import (
    RAY_STATE_SERVER_MAX_HTTP_REQUEST,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME,
)
from ray.dashboard.modules.event.event_utils import monitor_events, parse_event_strings
from ray.dashboard.state_api_utils import do_filter, handle_list_api
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.util.state.common import ClusterEventState, ListApiOptions, ListApiResponse

logger = logging.getLogger(__name__)

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)

MAX_EVENTS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_EVENTS_TO_CACHE", 10000))

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS", 1
)


async def _list_cluster_events_impl(
    *, all_events, executor: ThreadPoolExecutor, option: ListApiOptions
) -> ListApiResponse:
    """
    List all cluster events from the cluster. Made a free function to allow unit tests.

    Returns:
        A list of cluster events in the cluster.
        The schema of returned "dict" is equivalent to the
        `ClusterEventState` protobuf message.
    """

    def transform(all_events) -> ListApiResponse:
        result = []
        for _, events in all_events.items():
            for _, event in events.items():
                event["time"] = str(datetime.fromtimestamp(int(event["timestamp"])))
                result.append(event)

        num_after_truncation = len(result)
        result.sort(key=lambda entry: entry["timestamp"])
        total = len(result)
        result = do_filter(result, option.filters, ClusterEventState, option.detail)
        num_filtered = len(result)
        # Sort to make the output deterministic.
        result = list(islice(result, option.limit))
        return ListApiResponse(
            result=result,
            total=total,
            num_after_truncation=num_after_truncation,
            num_filtered=num_filtered,
        )

    return await get_or_create_event_loop().run_in_executor(
        executor, transform, all_events
    )


class EventHead(
    SubprocessModule,
    dashboard_utils.RateLimitedModule,
):
    def __init__(self, *args, **kwargs):
        SubprocessModule.__init__(self, *args, **kwargs)
        dashboard_utils.RateLimitedModule.__init__(
            self,
            min(
                RAY_STATE_SERVER_MAX_HTTP_REQUEST,
                RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
            ),
        )
        self._event_dir = os.path.join(self.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        self.total_report_events_count = 0
        self.total_events_received = 0
        self.module_started = time.monotonic()
        # {job_id hex(str): {event_id (str): event (dict)}}
        self.events: Dict[str, JobEvents] = defaultdict(JobEvents)

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="event_head_executor",
        )

        # To init gcs_client in internal_kv for record_extra_usage_tag.
        assert self.gcs_client is not None
        assert ray.experimental.internal_kv._internal_kv_initialized()

    async def limit_handler_(self):
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
            error_message=(
                "Max number of in-progress requests="
                f"{self.max_num_call_} reached. "
                "To set a higher limit, set environment variable: "
                f"export {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME}='xxx'. "
                f"Max allowed = {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED}"
            ),
            result=None,
        )

    def _update_events(self, event_list):
        # {job_id: {event_id: event}}
        all_job_events = defaultdict(JobEvents)
        for event in event_list:
            event_id = event["event_id"]
            custom_fields = event.get("custom_fields")
            system_event = False
            if custom_fields:
                job_id = custom_fields.get("job_id", "global") or "global"
            else:
                job_id = "global"
            if system_event is False:
                all_job_events[job_id][event_id] = event

        for job_id, new_job_events in all_job_events.items():
            job_events = self.events[job_id]
            job_events.update(new_job_events)

            # Limit the # of events cached if it exceeds the threshold.
            if len(job_events) > MAX_EVENTS_TO_CACHE * 1.1:
                while len(job_events) > MAX_EVENTS_TO_CACHE:
                    job_events.popitem(last=False)

    @routes.post("/report_events")
    async def report_events(self, request):
        """
        Report events to the dashboard.
        The request body is a JSON array of event strings in type string.
        Response should contain {"success": true}.
        """
        try:
            request_body: List[str] = await request.json()
        except Exception as e:
            logger.warning(f"Failed to parse request body: {request=}, {e=}")
            raise aiohttp.web.HTTPBadRequest()
        if not isinstance(request_body, list):
            logger.warning(f"Request body is not a list, {request_body=}")
            raise aiohttp.web.HTTPBadRequest()
        events = parse_event_strings(request_body)
        logger.debug("Received %d events", len(events))
        self._update_events(events)
        self.total_report_events_count += 1
        self.total_events_received += len(events)
        return dashboard_optional_utils.rest_response(
            success=True,
            message="",
            status_code=dashboard_utils.HTTPStatusCode.OK,
        )

    async def _periodic_state_print(self):
        if self.total_events_received <= 0 or self.total_report_events_count <= 0:
            return

        elapsed = time.monotonic() - self.module_started
        return {
            "total_events_received": self.total_events_received,
            "Total_requests_received": self.total_report_events_count,
            "total_uptime": elapsed,
        }

    @routes.get("/events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_event(self, req) -> aiohttp.web.Response:
        job_id = req.query.get("job_id")
        if job_id is None:
            all_events = {
                job_id: list(job_events.values())
                for job_id, job_events in self.events.items()
            }
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="All events fetched.",
                events=all_events,
            )

        job_events = self.events[job_id]
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Job events fetched.",
            job_id=job_id,
            events=list(job_events.values()),
        )

    @routes.get("/api/v0/cluster_events")
    @dashboard_utils.RateLimitedModule.enforce_max_concurrent_calls
    async def list_cluster_events(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_CLUSTER_EVENTS, "1")

        async def list_api_fn(option: ListApiOptions):
            return await _list_cluster_events_impl(
                all_events=self.events, executor=self._executor, option=option
            )

        return await handle_list_api(list_api_fn, req)

    async def run(self):
        await super().run()
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: self._update_events(parse_event_strings(data)),
            self._executor,
        )
