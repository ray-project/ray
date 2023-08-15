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
from ray.core.generated import event_pb2_grpc
from ray.dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable

JobEvents = OrderedDict
dashboard_utils._json_compatible_types.add(JobEvents)

MAX_EVENTS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_EVENTS_TO_CACHE", 10000))
MOCK_EVENTS = {
    "01000": [
        {
            "eventId": "event1",
            "sourceType": "GCS",
            "hostName": "host1",
            "pid": 12345,
            "label": "",
            "message": "Message 1",
            "timestamp": 1691979376.490715,
            "severity": "INFO",
            "customFields": {"jobId": "64000000", "nodeId": "node1", "taskId": "task1"},
        },
        {
            "eventId": "event2",
            "sourceType": "RAYLET",
            "hostName": "host2",
            "pid": 67890,
            "label": "",
            "message": "Message 2",
            "timestamp": 1691979376.4938798,
            "severity": "ERROR",
            "customFields": {"jobId": "64000000", "nodeId": "node2", "taskId": "task2"},
        },
        {
            "eventId": "event3",
            "sourceType": "GCS",
            "hostName": "host3",
            "pid": 54321,
            "label": "",
            "message": "Message 3",
            "timestamp": 1691979376.4941854,
            "severity": "DEBUG",
            "customFields": {"jobId": "64000000", "nodeId": "node3", "taskId": "task3"},
        },
        {
            "eventId": "event4",
            "sourceType": "RAYLET",
            "hostName": "host4",
            "pid": 23456,
            "label": "",
            "message": "Message 4",
            "timestamp": 1691979376.490715,
            "severity": "INFO",
            "customFields": {"jobId": "64000000", "nodeId": "node4", "taskId": "task4"},
        },
        {
            "eventId": "event5",
            "sourceType": "GCS",
            "hostName": "host5",
            "pid": 78901,
            "label": "",
            "message": "Message 5",
            "timestamp": 1691979376.4938798,
            "severity": "ERROR",
            "customFields": {"jobId": "64000000", "nodeId": "node5", "taskId": "task5"},
        },
        {
            "eventId": "event6",
            "sourceType": "RAYLET",
            "hostName": "host6",
            "pid": 43210,
            "label": "",
            "message": "Message 6",
            "timestamp": 1691979376.4941854,
            "severity": "DEBUG",
            "customFields": {"jobId": "64000000", "nodeId": "node6", "taskId": "task6"},
        },
        {
            "eventId": "event7",
            "sourceType": "GCS",
            "hostName": "host7",
            "pid": 98765,
            "label": "",
            "message": "Message 7",
            "timestamp": 1691979376.490715,
            "severity": "INFO",
            "customFields": {"jobId": "64000000", "nodeId": "node7", "taskId": "task7"},
        },
        {
            "eventId": "event8",
            "sourceType": "RAYLET",
            "hostName": "host8",
            "pid": 56789,
            "label": "",
            "message": "Message 8",
            "timestamp": 1691979376.4938798,
            "severity": "ERROR",
            "customFields": {"jobId": "64000000", "nodeId": "node8", "taskId": "task8"},
        },
        {
            "eventId": "event9",
            "sourceType": "GCS",
            "hostName": "host9",
            "pid": 10987,
            "label": "",
            "message": "Message 9",
            "timestamp": 1691979376.4941854,
            "severity": "DEBUG",
            "customFields": {"jobId": "64000000", "nodeId": "node9", "taskId": "task9"},
        },
        {
            "eventId": "event10",
            "sourceType": "RAYLET",
            "hostName": "host10",
            "pid": 54321,
            "label": "",
            "message": "Message 10",
            "timestamp": 1691979376.490715,
            "severity": "INFO",
            "customFields": {
                "jobId": "64000000",
                "nodeId": "node10",
                "taskId": "task10",
            },
        },
    ]
}


class EventHead(
    dashboard_utils.DashboardHeadModule, event_pb2_grpc.ReportEventServiceServicer
):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._event_dir = os.path.join(self._dashboard_head.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        self.monitor_thread_pool_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="event_monitor"
        )
        self.total_report_events_count = 0
        self.total_events_received = 0
        self.module_started = time.monotonic()

    @staticmethod
    def _update_events(event_list):
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

    def filter_events(
        events: Dict[str, Dict[str, dict]],
        severity_levels: list,
        source_type: str,
        custom_field: dict,
        count: int,
    ) -> Dict[str, dict]:
        filtered_events = {}
        for job_id, job_events in events.items():
            filtered_job_events = []
            for event_id, event in job_events.items():
                if (
                    event["severity"] in severity_levels
                    and event["source_type"] == source_type
                    and custom_field.items() <= event["custom_fields"].items()
                ):
                    filtered_job_events.append(event)

            filtered_job_events.sort(key=lambda x: x["timestamp"], reverse=True)
            filtered_events[job_id] = filtered_job_events[:count]

        return filtered_events

    @routes.get("/events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_event(self, req) -> aiohttp.web.Response:
        job_id = req.query.get("job_id")
        if job_id is None:
            all_events = {
                job_id: list(job_events.values())
                for job_id, job_events in DataSource.events.items()
            }
            all_events = MOCK_EVENTS
            logger.info(f"all_events {type(all_events)}: {all_events}")

            return dashboard_optional_utils.rest_response(
                success=True, message="All events fetched.", events=all_events
            )
        # logger.info(f"DataSource.events {type(DataSource.events)}: {DataSource.events}")
        # logger.info(
        #     f"DataSource.events.items() {type(DataSource.events.items())}: {DataSource.events.items()}"
        # )
        # job_events = DataSource.events.get(job_id, {})
        # job_events = MOCK_EVENTS
        # logger.info(f"job_events {type(job_events)}: {job_events}")

        return dashboard_optional_utils.rest_response(
            success=True,
            message="Job events fetched.",
            job_id=job_id,
            events=list(MOCK_EVENTS.values()),
        )

    async def run(self, server):
        event_pb2_grpc.add_ReportEventServiceServicer_to_server(self, server)
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: self._update_events(parse_event_strings(data)),
            self.monitor_thread_pool_executor,
        )

    @staticmethod
    def is_minimal_module():
        return False
