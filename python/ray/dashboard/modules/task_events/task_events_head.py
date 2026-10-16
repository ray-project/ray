import logging

import aiohttp.web

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import (
    events_event_aggregator_service_pb2,
    gcs_service_pb2,
)
from ray.dashboard.modules.task_events import task_event_query
from ray.dashboard.modules.task_events.ray_event_converter import convert_to_task_events
from ray.dashboard.modules.task_events.task_event_manager import TaskEventManager
from ray.dashboard.modules.task_events.task_event_storage import TaskEventStorage
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes

logger = logging.getLogger(__name__)


class TaskEventsHead(SubprocessModule):
    """Dashboard-head sink for task events.

    Defines the HTTP API external clients interact with. Events
    are held in memory in a ``TaskEventStorage``; the background upkeep of that store
    (reconciling it against GCS worker-death and job-finished signals, plus periodic GC) is
    delegated to a ``TaskEventManager``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._store = TaskEventStorage()
        self._manager = None

    @classmethod
    def is_enabled(cls) -> bool:
        """Only load while the "task events out of GCS" migration is enabled; otherwise
        the module (and its GCS pubsub subscriptions) shouldn't run at all."""
        return ray._config.enable_task_events_to_dashboard_head()

    @property
    def num_task_events_stored(self) -> int:
        """Number of task attempts currently held in the store (for tests)."""
        return self._store.num_task_events_stored

    def _deserialize_request(
        self, body: bytes
    ) -> events_event_aggregator_service_pb2.AddEventsRequest:
        """Deserialize the binary-proto POST body into an ``AddEventsRequest``."""
        return events_event_aggregator_service_pb2.AddEventsRequest.FromString(body)

    @routes.post("/api/task_events")
    async def add_task_events(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        body = await request.read()
        try:
            add_events_request = self._deserialize_request(body)
        except Exception as e:
            logger.warning(f"Failed to deserialize task events request: {e}")
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.BAD_REQUEST,
                message=f"Failed to deserialize task events request: {e}",
            )

        task_events, dropped_task_attempts = convert_to_task_events(add_events_request)
        self._store.record_data_loss_from_worker(dropped_task_attempts)
        for task_event in task_events:
            self._store.add_or_replace_task_event(task_event)
        logger.debug(
            "Received %d task events (%d attempts stored)",
            len(task_events),
            self._store.num_task_events_stored,
        )
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="",
        )

    @routes.post("/api/task_events/query")
    async def get_task_events(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """Answer a serialized ``GetTaskEventsRequest`` with a serialized reply.

        Internal read endpoint for the State API (``StateHead``); the request/reply protos
        match GCS's ``GetTaskEvents`` so the caller is transport-only.
        """
        body = await request.read()
        try:
            query_request = gcs_service_pb2.GetTaskEventsRequest.FromString(body)
            reply = task_event_query.get_task_events(self._store, query_request)
        except Exception as e:
            logger.warning(f"Failed to query task events: {e}")
            return aiohttp.web.Response(status=400, text=str(e))
        return aiohttp.web.Response(
            body=reply.SerializeToString(),
            content_type="application/octet-stream",
        )

    async def run(self):
        await super().run()
        self._manager = TaskEventManager(
            self._store, self.gcs_address, self.aiogrpc_gcs_channel
        )
        self._manager.start()
