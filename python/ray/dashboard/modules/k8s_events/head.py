import logging
import time
from collections import deque
from typing import Dict, List

import aiohttp.web
from google.protobuf.json_format import MessageToDict, ParseDict

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.modules.k8s_events import k8s_events_pb2
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable

logger = logging.getLogger(__name__)
routes = SubprocessRouteTable

class K8sEventsHead(SubprocessModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # Circular buffer to store the last 1000 events
        self._events = deque(maxlen=1000)

    @routes.post("/api/v0/k8s_events")
    @dashboard_optional_utils.aiohttp_cache
    async def post_event(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        try:
            data = await req.json()
        except Exception as e:
            return dashboard_optional_utils.rest_response(
                status_code=400,
                message=f"Failed to parse JSON: {e}",
            )

        try:
            # Validate and parse into Protobuf
            event = k8s_events_pb2.PlatformEvent()
            ParseDict(data, event)
            
            # Add server-side timestamp if missing (optional, but good practice)
            if not event.timestamp_ms:
                event.timestamp_ms = int(time.time() * 1000)

            # Store event
            # Convert back to dict for storage/serving to avoid pickling issues if any,
            # but keeping proto object is also fine. Storing dict is safer for serialization.
            self._events.append(MessageToDict(event, preserving_proto_field_name=True))
            
            logger.info(f"Received K8s/Platform event: {event.source} - {event.message}")

            return dashboard_optional_utils.rest_response(
                status_code=200,
                message="Event received",
            )
        except Exception as e:
            logger.error(f"Error processing K8s event: {e}")
            return dashboard_optional_utils.rest_response(
                status_code=500,
                message=f"Internal error: {e}",
            )

    @routes.get("/api/v0/k8s_events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_events(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        # Optional: Support filtering by source or node_id queries?
        # For PoC, just return all.
        
        limit = int(req.query.get("limit", 100))
        # Get last N events
        events_list = list(self._events)[-limit:]
        
        return dashboard_optional_utils.rest_response(
            status_code=200,
            message="Retrieved K8s events",
            events=events_list,
        )

    async def run(self):
        await super().run()

    @staticmethod
    def is_minimal_module():
        return False
