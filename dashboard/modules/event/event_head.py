import os
import asyncio
import logging
import time
from typing import Union
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor

import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.modules.event.event_utils import (
    parse_event_strings,
    monitor_events,
)
from ray.core.generated import event_pb2
from ray.dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)

MAX_EVENTS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_EVENTS_TO_CACHE", 10000))


class EventHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._event_dir = os.path.join(self._dashboard_head.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        # Not setting max_workers to 1 because this thread pool may be busy with
        # thousands of concurrent events from each worker.
        self.receive_event_thread_pool_executor = ThreadPoolExecutor(
            thread_name_prefix="event_receiver"
        )
        self.monitor_thread_pool_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="event_monitor"
        )
        self.total_report_events_count = 0
        self.total_events_received = 0
        self.module_started = time.monotonic()

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

    def report_events_raw(self, binary_proto: bytes):
        """
        Receives a binary protobuf ReportEventsRequest and updates the event cache.
        """
        proto = event_pb2.ReportEventsRequest()
        proto.ParseFromString(binary_proto)
        received_events = []
        if proto.event_strings:
            received_events.extend(parse_event_strings(proto.event_strings))
        logger.debug("Received %d events", len(received_events))
        self._update_events(received_events)
        self.total_report_events_count += 1
        self.total_events_received += len(received_events)

    @routes.post("/events")
    async def report_events(self, request):
        """
        POST /events with payload is a serialized protobuf ReportEventsRequest.

        On success: Replies 200 OK with payload = JSON {"result":true}.
        On error: Replies non-OK with payload = JSON
            {"result":false, "message": "error message"}.

        Note: Parsing protobuf and inner JSON can be CPU intensive so we use a thread
        pool.
        """
        try:
            data = await request.read()
            await self.receive_event_thread_pool_executor.submit(
                self.report_events_raw, data
            )
            return dashboard_optional_utils.rest_response(success=True)
        except Exception as e:
            logger.exception("Error processing event report")
            return dashboard_optional_utils.rest_response(success=False, message=str(e))

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

    async def run(self):
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: self._update_events(parse_event_strings(data)),
            self.monitor_thread_pool_executor,
        )

    @staticmethod
    def is_minimal_module():
        return False
