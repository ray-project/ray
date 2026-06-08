import asyncio
import collections
import json
import logging
import os

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.dashboard.consts import (
    DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
    GCS_RPC_TIMEOUT_SECONDS,
)

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
        self._loop = None

    async def run(self):
        await super().run()
        self._loop = asyncio.get_running_loop()

        if "PLATFORM_EVENT" in ray_constants.RAY_ENABLE_PYTHON_RAY_EVENT_TYPES:
            try:
                from ray._raylet import EventRecorder

                head_node_id_hex = await dashboard_utils.get_head_node_id(
                    self.gcs_client, timeout=GCS_RPC_TIMEOUT_SECONDS
                )

                if head_node_id_hex:
                    key = f"{DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{head_node_id_hex}"
                    value = await self.gcs_client.async_internal_kv_get(
                        key.encode(),
                        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
                        timeout=GCS_RPC_TIMEOUT_SECONDS,
                    )
                    if value:
                        ip, _, grpc_port = json.loads(value.decode())
                        EventRecorder.initialize(
                            aggregator_port=int(grpc_port),
                            node_ip=ip,
                            node_id_hex=head_node_id_hex,
                            max_buffer_size=10000,
                            metric_source="platform_events",
                        )
                        logger.info("Initialized EventRecorder for PlatformEventsHead.")
            except Exception as e:
                logger.exception(
                    f"Failed to initialize EventRecorder in PlatformEventsHead: {e}"
                )

        # Detect infrastructure platform.
        if "KUBERNETES_SERVICE_HOST" in os.environ:
            try:
                from ray.dashboard.modules.platform_events.providers.k8s_provider import (
                    KubernetesEventProvider,
                )

                self._provider = KubernetesEventProvider(self._process_event_callback)
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
        """Thread-safe entry point that dispatches event caching to the main asyncio loop."""
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._process_event_internal, ray_event)
        else:
            # Shutdown path
            self._process_event_internal(ray_event)

    def _process_event_internal(self, ray_event: RayEvent):
        """Internal callback running in the main asyncio loop to cache events."""
        uid = ray_event.event_id.decode()

        if uid in self._events:
            # Refresh with the latest delivery
            self._events[uid] = ray_event
            self._events.move_to_end(uid)
        else:
            # Add new event and enforce max size
            if len(self._events) >= MAX_EVENTS_TO_CACHE:
                self._events.popitem(last=False)
            self._events[uid] = ray_event

        if "PLATFORM_EVENT" in ray_constants.RAY_ENABLE_PYTHON_RAY_EVENT_TYPES:
            try:
                from ray._common.observability.platform_events import (
                    PlatformEventBuilder,
                )
                from ray._raylet import EventRecorder

                builder = PlatformEventBuilder(
                    event_uid=uid,
                    platform=ray_event.platform_event.source.platform,
                    object_kind=ray_event.platform_event.object_kind,
                    object_name=ray_event.platform_event.object_name,
                    reason=ray_event.platform_event.reason,
                    message=ray_event.message,
                    severity=ray_event.severity,
                    component=ray_event.platform_event.source.component,
                    source_metadata=dict(ray_event.platform_event.source.metadata),
                    custom_fields=dict(ray_event.platform_event.custom_fields),
                )
                cython_event = builder.build(
                    event_id=uid.encode(),
                    timestamp_ns=(
                        ray_event.timestamp.seconds * 1_000_000_000
                        + ray_event.timestamp.nanos
                    ),
                )
                EventRecorder.emit(cython_event)
            except Exception as e:
                logger.warning(f"Failed to emit platform event via EventRecorder: {e}")

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
        if "PLATFORM_EVENT" in ray_constants.RAY_ENABLE_PYTHON_RAY_EVENT_TYPES:
            try:
                from ray._raylet import EventRecorder

                EventRecorder.shutdown()
            except Exception as e:
                logger.warning(f"Failed to shutdown EventRecorder: {e}")
        logger.info("Platform events watcher stopped.")
