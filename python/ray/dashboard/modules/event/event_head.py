import asyncio
import json
import logging
import os
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import islice
from typing import Dict, List, Union

import aiohttp.web
from google.protobuf.json_format import ParseDict, ParseError as ProtobufParseError

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._common.network_utils import build_address
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray._common.utils import get_or_create_event_loop
from ray._private import ray_constants
from ray._private.grpc_utils import init_grpc_channel
from ray._private.ray_constants import env_integer
from ray.core.generated import (
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
    events_event_aggregator_service_pb2_grpc,
)
from ray.dashboard import consts as dashboard_consts
from ray.dashboard.consts import (
    RAY_STATE_SERVER_MAX_HTTP_REQUEST,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME,
)
from ray.dashboard.modules.event.autoscaler_events_storage import (
    AutoscalerEventCacheInput,
    AutoscalerEventsStorage,
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
MAX_AUTOSCALER_EVENTS_CACHE_SIZE_BYTES = int(
    os.environ.get(
        "RAY_DASHBOARD_MAX_AUTOSCALER_EVENTS_CACHE_SIZE_BYTES", 10 * 1024 * 1024
    )  # 10MB
)

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS", 1
)

_AUTOSCALER_EVENT_TYPES = {
    events_base_event_pb2.RayEvent.EventType.AUTOSCALER_CONFIG_DEFINITION_EVENT,
    events_base_event_pb2.RayEvent.EventType.AUTOSCALER_SCALING_DECISION_EVENT,
    events_base_event_pb2.RayEvent.EventType.AUTOSCALER_NODE_PROVISIONING_EVENT,
}


async def _list_cluster_events_impl(
    *,
    all_events,
    autoscaler_events,
    executor: ThreadPoolExecutor,
    option: ListApiOptions,
) -> ListApiResponse:
    """
    List all cluster events from the cluster. Made a free function to allow unit tests.

    Args:
        all_events: The job or actor events to include (typically an OrderedDict).
        autoscaler_events: List of autoscaler event dictionaries.
        executor (ThreadPoolExecutor): The executor to use for running the transformation.
        option (ListApiOptions): Options for list API (filtering, limits, etc.).

    Returns:
        A list of cluster events in the cluster.
        The schema of returned "dict" is equivalent to the
        `ClusterEventState` protobuf message.
    """

    def transform(all_events) -> ListApiResponse:
        result = []
        for _, events in all_events.items():
            for _, event in events.items():
                row = dict(event)
                row["time"] = str(datetime.fromtimestamp(int(row["timestamp"])))
                result.append(row)
        for event in autoscaler_events:
            row = dict(event)
            row["time"] = str(datetime.fromtimestamp(int(row["timestamp"])))
            result.append(row)

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
        self._autoscaler_events_storage = AutoscalerEventsStorage(
            MAX_AUTOSCALER_EVENTS_CACHE_SIZE_BYTES
        )

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="event_head_executor",
        )
        self._head_aggregator_stub = None
        self._head_node_id = None

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

    @staticmethod
    def _parse_external_ray_events(
        request_body,
    ) -> List[events_base_event_pb2.RayEvent]:
        if not isinstance(request_body, list):
            raise ValueError("Request body must be a list of RayEvent JSON objects.")

        events = []
        for index, event_json in enumerate(request_body):
            if not isinstance(event_json, dict):
                raise ValueError(
                    f"Event at index {index} must be a JSON object, got {type(event_json).__name__}."
                )
            event = events_base_event_pb2.RayEvent()
            ParseDict(event_json, event)
            events.append(event)
        return events

    @staticmethod
    def _get_external_ray_event_allowlist() -> set[str]:
        return {
            event_type
            for event_type in ray._config.external_ray_event_allowlist()
            if event_type
        }

    @staticmethod
    def _is_autoscaler_event_type(event_type: int) -> bool:
        return event_type in _AUTOSCALER_EVENT_TYPES

    def _cache_supported_external_ray_events(
        self, request_body, events: List[events_base_event_pb2.RayEvent]
    ) -> None:
        cache_inputs = []
        for event_json, event in zip(request_body, events):
            if not self._is_autoscaler_event_type(event.event_type):
                # we currently only cache autoscaler events
                continue
            cache_inputs.append(
                AutoscalerEventCacheInput(
                    event_json=event_json,
                    event_id=event.event_id.hex(),
                    timestamp_seconds=event.timestamp.seconds,
                    size_bytes=event.ByteSize(),
                )
            )
        if cache_inputs:
            self._autoscaler_events_storage.add_events(cache_inputs)

    @classmethod
    def _validate_external_ray_events(
        cls, events: List[events_base_event_pb2.RayEvent]
    ) -> None:
        allowlist = cls._get_external_ray_event_allowlist()
        for event in events:
            event_type_name = events_base_event_pb2.RayEvent.EventType.Name(
                event.event_type
            )
            if event_type_name not in allowlist:
                raise ValueError(
                    f"Event type {event_type_name} is not allowed on /api/v0/external/ray_events."
                )

    async def _get_head_node_id(self) -> Union[str, None]:
        if self._head_node_id is not None:
            return self._head_node_id

        head_node_id = await self.gcs_client.async_internal_kv_get(
            ray_constants.KV_HEAD_NODE_ID_KEY,
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )
        if head_node_id is None:
            return None

        self._head_node_id = head_node_id.decode()
        return self._head_node_id

    async def _get_head_aggregator_stub(self):
        if self._head_aggregator_stub is not None:
            return self._head_aggregator_stub

        head_node_id = await self._get_head_node_id()
        if head_node_id is None:
            return None

        agent_addr_json = await self.gcs_client.async_internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{head_node_id}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )
        if agent_addr_json is None:
            return None

        ip, _, grpc_port = json.loads(agent_addr_json)
        channel = init_grpc_channel(
            build_address(ip, grpc_port),
            options=ray_constants.GLOBAL_GRPC_OPTIONS,
            asynchronous=True,
        )
        self._head_aggregator_stub = (
            events_event_aggregator_service_pb2_grpc.EventAggregatorServiceStub(channel)
        )
        return self._head_aggregator_stub

    async def _forward_external_ray_events(
        self, events: List[events_base_event_pb2.RayEvent]
    ) -> None:
        stub = await self._get_head_aggregator_stub()
        if stub is None:
            raise RuntimeError("Head node aggregator agent is not available.")

        request = events_event_aggregator_service_pb2.AddEventsRequest(
            events_data=events_event_aggregator_service_pb2.RayEventsData(events=events)
        )
        await stub.AddEvents(request)

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

    @routes.post("/api/v0/external/ray_events")
    async def report_external_ray_events(self, request):
        try:
            request_body = await request.json()
            events = self._parse_external_ray_events(request_body)
            self._validate_external_ray_events(events)
        except (ValueError, ProtobufParseError) as e:
            logger.warning("Invalid external Ray event payload: %s", e)
            raise aiohttp.web.HTTPBadRequest(reason=str(e))
        except Exception as e:
            logger.warning("Failed to parse external Ray event payload: %s", e)
            raise aiohttp.web.HTTPBadRequest()

        try:
            await self._forward_external_ray_events(events)
        except Exception:
            logger.exception("Failed to forward external Ray events to aggregator.")
            raise aiohttp.web.HTTPInternalServerError()

        self._cache_supported_external_ray_events(request_body, events)

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
            event_lists = {
                job_id: list(job_events.values())
                for job_id, job_events in self.events.items()
            }
            autoscaler_events = list(self._autoscaler_events_storage.get_event_values())
            if autoscaler_events:
                event_lists.setdefault("global", []).extend(autoscaler_events)
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="All events fetched.",
                events=event_lists,
            )

        job_events = list(self.events[job_id].values())
        if job_id == "global":
            job_events.extend(self._autoscaler_events_storage.get_event_values())
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Job events fetched.",
            job_id=job_id,
            events=job_events,
        )

    @routes.get("/api/v0/cluster_events")
    @dashboard_utils.RateLimitedModule.enforce_max_concurrent_calls
    async def list_cluster_events(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_CLUSTER_EVENTS, "1")

        async def list_api_fn(option: ListApiOptions):
            return await _list_cluster_events_impl(
                all_events=self.events,
                autoscaler_events=self._autoscaler_events_storage.get_event_values(),
                executor=self._executor,
                option=option,
            )

        return await handle_list_api(list_api_fn, req)

    async def run(self):
        await super().run()
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: self._update_events(parse_event_strings(data)),
            self._executor,
        )
