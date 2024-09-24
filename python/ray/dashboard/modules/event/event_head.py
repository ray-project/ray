import asyncio
import logging
import os
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Union

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import env_integer
from ray.core.generated import event_pb2, event_pb2_grpc
from ray.dashboard.datacenter import DataSource
from ray.dashboard.modules.event.event_utils import monitor_events, parse_event_strings

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)

MAX_EVENTS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_EVENTS_TO_CACHE", 10000))

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS", 1
)


class EventHead(
    dashboard_utils.DashboardHeadModule, event_pb2_grpc.ReportEventServiceServicer
):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._event_dir = os.path.join(self._dashboard_head.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        self.total_report_events_count = 0
        self.total_events_received = 0
        self.module_started = time.monotonic()

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="event_head_executor",
        )

    @staticmethod
    def _update_events(event_list):
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
            job_events = DataSource.events.get(job_id, JobEvents())
            job_events.update(new_job_events)
            DataSource.events[job_id] = job_events

            # Limit the # of events cached if it exceeds the threshold.
            events = DataSource.events[job_id]
            if len(events) > MAX_EVENTS_TO_CACHE * 1.1:
                while len(events) > MAX_EVENTS_TO_CACHE:
                    events.popitem(last=False)

    async def ReportEvents(self, request, context):
        received_events = []
        if request.event_strings:
            received_events.extend(parse_event_strings(request.event_strings))
        logger.debug("Received %d events", len(received_events))
        self._update_events(received_events)
        self.total_report_events_count += 1
        self.total_events_received += len(received_events)
        return event_pb2.ReportEventsReply(send_success=True)

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
                for job_id, job_events in DataSource.events.items()
            }
            return dashboard_optional_utils.rest_response(
                success=True, message="All events fetched.", events=all_events
            )

        job_events = DataSource.events.get(job_id, {})
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Job events fetched.",
            job_id=job_id,
            events=list(job_events.values()),
        )

    async def run(self, server):
        event_pb2_grpc.add_ReportEventServiceServicer_to_server(self, server)
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: self._update_events(parse_event_strings(data)),
            self._executor,
        )

    @staticmethod
    def is_minimal_module():
        return False
