import collections
import logging

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import events_event_aggregator_service_pb2
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes

logger = logging.getLogger(__name__)


class TaskEventsHead(SubprocessModule):
    """Dashboard-head endpoint that receives task events from per-node aggregators.

    The per-node aggregator agent POSTs an ``AddEventsRequest`` payload (the same
    proto the aggregator sends to GCS) to this module, which holds the received
    ``RayEvent``s in memory.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO(Task 3): replace this unbounded in-memory buffer with real storage +
        # GC/eviction. It exists only so the ingestion endpoint has somewhere to put
        # events for now; nothing reads it yet (State-API serving is a later task).
        self._events = collections.deque()

    @property
    def num_events_received(self) -> int:
        """Number of task events currently held in the in-memory buffer (for tests)."""
        return len(self._events)

    def _deserialize_request(
        self, body: bytes
    ) -> events_event_aggregator_service_pb2.AddEventsRequest:
        """Deserialize the POST body into an ``AddEventsRequest``.

        Kept as a single seam because the on-the-wire encoding between the aggregator
        and this endpoint is not finalized (binary proto vs JSON). Swap this method to
        change the wire format without touching the handler.
        """
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
                status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                message=f"Failed to deserialize task events request: {e}",
            )

        events_data = add_events_request.events_data
        self._events.extend(events_data.events)
        logger.debug(
            "Received %d task events (%d total buffered)",
            len(events_data.events),
            len(self._events),
        )
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="",
        )

    async def run(self):
        await super().run()
