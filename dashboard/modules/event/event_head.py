import os
import asyncio
import logging
from typing import Union
from collections import OrderedDict, defaultdict

import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.modules.event.event_utils import (
    parse_event_strings,
    monitor_events,
)
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc
from ray.dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)


class EventHead(
    dashboard_utils.DashboardHeadModule, event_pb2_grpc.ReportEventServiceServicer
):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._event_dir = os.path.join(self._dashboard_head.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None

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
        # TODO(fyrestone): Limit the event count per job.
        for job_id, new_job_events in all_job_events.items():
            job_events = DataSource.events.get(job_id, JobEvents())
            job_events.update(new_job_events)
            DataSource.events[job_id] = job_events

    async def ReportEvents(self, request, context):
        received_events = []
        if request.event_strings:
            received_events.extend(parse_event_strings(request.event_strings))
        logger.info("Received %d events", len(received_events))
        self._update_events(received_events)
        return event_pb2.ReportEventsReply(send_success=True)

    @routes.get("/events")
    @dashboard_optional_utils.aiohttp_cache(2)
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
            self._event_dir, lambda data: self._update_events(parse_event_strings(data))
        )

    @staticmethod
    def is_minimal_module():
        return False
