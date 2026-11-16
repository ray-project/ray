from enum import Enum
from typing import Optional

import msgpack
import zmq

import ray
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_LIFECYCLE_ZMQ_SEND_QUEUE_LIMIT,
    SELECTION_MODEL_NAME,
    SELECTION_ROUTING_GROUP,
)

logger = get_logger(__name__)

# Must match the selection service's REPLICA_TOPIC
# (lib/router/kv-router/src/services/common/replica_sync.rs).
_TOPIC = b"dynamo.slot-tracker.v1"


class LifecycleEvent(str, Enum):
    """The selection service's ActiveSequenceEventData variants
    (lib/router/slot-tracker/src/wire.rs)."""

    PREFILL_COMPLETE = "MarkPrefillCompleted"
    FINISHED = "Free"


class LifecyclePublisher:
    """Publishes each request's prefill-complete and finished events on a ZMQ
    PUB socket that every LLMRouter's selection service subscribes to.

    Every router counts the load of every request, including requests other
    routers routed, so every router needs these events to update that load.
    Events come straight from this LLMServer, not through the router that
    routed the request, so the other routers still get them if it fails.

    Best effort: a router misses events sent before it connects, or once
    KV_LIFECYCLE_ZMQ_SEND_QUEUE_LIMIT events are queued for it. Its selection
    service drops the load of those requests after
    DYN_ROUTER_ACTIVE_REQUEST_EXPIRY_SECS (300 s by default).
    """

    def __init__(self):
        self._socket: Optional[zmq.Socket] = None
        self._event: dict = {}

    def start(self, worker_id: int, dp_rank: int, block_size: int) -> str:
        """Bind to a port the OS picks and return the endpoint to advertise."""
        socket = zmq.Context.instance().socket(zmq.PUB)
        # Drop unsent events on close, so shutdown never waits on a gone router.
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDHWM, KV_LIFECYCLE_ZMQ_SEND_QUEUE_LIMIT)
        try:
            port = socket.bind_to_random_port("tcp://*")
        except zmq.ZMQError as e:
            socket.close()
            raise RuntimeError(
                f"Failed to bind the ZMQ socket for KV lifecycle events: {e}"
            ) from e
        self._socket = socket
        # The selection service's ScopedReplicaEvent
        # (lib/router/scheduler/src/replication.rs).
        self._event = {
            "model_name": SELECTION_MODEL_NAME,
            "routing_group": SELECTION_ROUTING_GROUP,
            # Must equal the routers' block size, or the selection service drops
            # the event.
            "block_size": block_size,
            "event": {
                "worker": {"worker_id": worker_id, "dp_rank": dp_rank},
                # router_id names the sender: routers use their own random id,
                # an LLMServer uses its worker id. With the worker id, a router
                # that gets the event before it learns of the request (from the
                # router that routed it) keeps the event for 30 s and applies it
                # once it learns of the request.
                "router_id": worker_id,
                # Prefill and free events find the booking by request id, and
                # the booking already holds the LoRA name.
                "lora_name": None,
            },
        }
        endpoint = f"tcp://{ray.util.get_node_ip_address()}:{port}"
        logger.info("Publishing KV lifecycle events on %s.", endpoint)
        return endpoint

    def publish(self, request_id: str, event: LifecycleEvent) -> None:
        """Publish ``event`` for ``request_id`` without blocking."""
        if self._socket is None:
            return
        message = {
            **self._event,
            "event": {
                **self._event["event"],
                "request_id": request_id,
                "data": event.value,
            },
        }
        try:
            self._socket.send_multipart(
                [_TOPIC, msgpack.packb(message)], flags=zmq.NOBLOCK
            )
        except zmq.ZMQError as e:
            logger.warning("Dropping KV lifecycle event for %s: %s", request_id, e)

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None
