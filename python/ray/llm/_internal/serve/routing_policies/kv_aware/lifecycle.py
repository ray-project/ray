from enum import Enum
from typing import TYPE_CHECKING, Optional

import ray
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    SELECTION_MODEL_NAME,
    SELECTION_ROUTING_GROUP,
)

if TYPE_CHECKING:
    from dynamo.llm import ActiveSequenceEventPublisher

logger = get_logger(__name__)


class LifecycleEvent(str, Enum):
    PREFILL_COMPLETE = "prefill_complete"
    FINISHED = "finished"


class LifecyclePublisher:
    """Publishes lifecycle events directly to every router's selection service.

    Dynamo owns the wire format and nonblocking ZMQ publisher. Delivery is best
    effort; missed completion events are covered by reservation expiry.
    """

    def __init__(self):
        self._publisher: Optional["ActiveSequenceEventPublisher"] = None

    def start(self, worker_id: int, dp_rank: int, block_size: int) -> str:
        """Bind a publisher and return the endpoint to advertise."""
        from dynamo.llm import ActiveSequenceEventPublisher

        self._publisher = ActiveSequenceEventPublisher(
            model_name=SELECTION_MODEL_NAME,
            routing_group=SELECTION_ROUTING_GROUP,
            worker_id=worker_id,
            dp_rank=dp_rank,
            block_size=block_size,
        )
        port = self._publisher.endpoint.rsplit(":", 1)[1]
        endpoint = f"tcp://{ray.util.get_node_ip_address()}:{port}"
        logger.info("Publishing KV lifecycle events on %s.", endpoint)
        return endpoint

    def publish(self, request_id: str, event: LifecycleEvent) -> None:
        """Publish ``event`` for ``request_id`` without blocking."""
        if self._publisher is None:
            return
        try:
            if event is LifecycleEvent.PREFILL_COMPLETE:
                self._publisher.mark_prefill_completed(request_id)
            else:
                self._publisher.free(request_id)
        except Exception as e:
            logger.warning("Dropping KV lifecycle event for %s: %s", request_id, e)

    def close(self) -> None:
        if self._publisher is not None:
            self._publisher.close()
            self._publisher = None
