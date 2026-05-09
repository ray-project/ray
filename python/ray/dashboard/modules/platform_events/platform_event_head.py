import asyncio
import collections
import logging
import os

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated.events_base_event_pb2 import RayEvent

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable
# Max number of events to cache in memory.
MAX_EVENTS_TO_CACHE = 1000


class PlatformEventsHead(dashboard_utils.DashboardHeadModule):
    """
    Dashboard Head Module for managing and serving infrastructure platform events.

    This module acts as a generic, platform-agnostic REST controller and cache
    for events coming from the underlying hosting environment (e.g., Kubernetes).
    It delegates platform-specific event watching to dynamic providers and exposes
    a single unified API endpoint (/api/v0/platform_events) to query the events.
    """

    @classmethod
    def is_enabled(cls) -> bool:
        is_enabled = os.environ.get(
            "RAY_DASHBOARD_INGEST_PLATFORM_EVENTS", "False"
        ).lower() in ("true", "1")
        if not is_enabled:
            logger.info(
                "Skipping PlatformEventsHead because RAY_DASHBOARD_INGEST_PLATFORM_EVENTS is not enabled."
            )
        return is_enabled

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._events = collections.OrderedDict()
        self._provider = None

    async def run(self):
        await super().run()

        # Detect infrastructure platform.
        if "KUBERNETES_SERVICE_HOST" in os.environ:
            try:
                from ray.dashboard.modules.platform_events.providers.k8s_provider import (
                    KubernetesEventProvider,
                )

                # Create a thread-safe callback wrapper to delegate event processing back to the main loop
                loop = asyncio.get_running_loop()

                def thread_safe_callback(ray_event: RayEvent):
                    loop.call_soon_threadsafe(self._process_event_callback, ray_event)

                self._provider = KubernetesEventProvider(thread_safe_callback)
                logger.info("Initialized Kubernetes platform event provider.")
                await self._provider.run()
            except Exception as e:
                logger.exception(
                    f"Error running Kubernetes platform event provider: {e}"
                )
        else:
            logger.info(
                "No supported platform environment detected (e.g. KUBERNETES_SERVICE_HOST not found). "
                "Platform events will be disabled."
            )

    def _process_event_callback(self, ray_event: RayEvent):
        """Callback running in the main asyncio loop to cache events."""
        uid = ray_event.event_id.decode()

        if uid in self._events:
            # Update existing event fields
            existing_event = self._events[uid]
            existing_event.timestamp.CopyFrom(ray_event.timestamp)
            existing_event.message = ray_event.message
            existing_event.platform_event.message = ray_event.platform_event.message

            # Update count if present
            count_val = ray_event.platform_event.custom_fields.get("count")
            if count_val:
                existing_event.platform_event.custom_fields["count"] = count_val
            else:
                existing_event.platform_event.custom_fields.pop("count", None)

            # Move to end to maintain most-recent order
            self._events.move_to_end(uid)
            return

        # Add new event and enforce max size
        if len(self._events) >= MAX_EVENTS_TO_CACHE:
            self._events.popitem(last=False)

        self._events[uid] = ray_event

    @routes.get("/api/v0/platform_events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_platform_events(self, req):
        """Return recently observed platform events as a JSON array."""
        # Sort by timestamp ascending (oldest first) to fix out-of-order
        # issues caused by potential reconnects/re-deliveries.
        sorted_events = sorted(
            self._events.values(),
            key=lambda e: e.timestamp.seconds + e.timestamp.nanos / 1e9,
        )
        payload = [
            dashboard_utils.message_to_dict(
                e, always_print_fields_with_no_presence=True
            )
            for e in sorted_events
        ]
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Retrieved platform events",
            events=payload,
        )

    @staticmethod
    def is_minimal_module():
        return False

    async def cleanup(self):
        if self._provider:
            await self._provider.cleanup()
        logger.info("Platform events watcher stopped.")
