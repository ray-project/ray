"""Request lifecycle events from an LLMServer to the LLMRouter that selected
the request (its owner).

- The selecting LLMRouter names its lifecycle endpoint in a request header. The
  LLMServer's serving wrapper binds it as the request's owner
  (``set_request_lifecycle_owner``).
- ``LifecycleEventForwarder`` pushes each event from the LLMServer to the owning
  LLMRouter over ZMQ.
- ``LifecycleReceiver`` is the owning LLMRouter's ZMQ PULL endpoint.
- ``LifecycleCodec`` is the wire format both ends share.

Free of engine imports, so any engine integration can use it.
"""

import asyncio
import contextlib
import contextvars
from enum import Enum
from typing import Awaitable, Callable, List, Optional

import msgpack
import zmq
import zmq.asyncio as zmq_asyncio

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT,
)
from ray.llm._internal.serve.routing_policies.kv_aware.network_utils import (
    ZmqPushSender,
)
from ray.util import metrics

logger = get_logger(__name__)


class PushOutcome(str, Enum):
    """``outcome`` tag of the lifecycle events counter."""

    PUSHED = "pushed"
    DROPPED = "dropped"


_lifecycle_events_counter = metrics.Counter(
    "serve_llm_kv_lifecycle_events",
    description="KV lifecycle events engines pushed to the selecting router.",
    tag_keys=("outcome",),
)

# Lifecycle endpoint of the router that selected the current request.
_request_lifecycle_owner: contextvars.ContextVar[
    Optional[str]
] = contextvars.ContextVar("kv_request_lifecycle_owner", default=None)


def set_request_lifecycle_owner(owner: Optional[str]) -> None:
    """Bind the request's owner. Not reset: streaming runs ``generate()`` later."""
    _request_lifecycle_owner.set(owner)


def get_request_lifecycle_owner() -> Optional[str]:
    """Lifecycle endpoint of the router that selected the current request."""
    return _request_lifecycle_owner.get()


class LifecycleCodec:
    """Wire format of lifecycle events: one msgpack frame holding a list of
    ``[hook, args]`` pairs."""

    @staticmethod
    def encode(events: List[tuple]) -> bytes:
        return msgpack.packb([[hook, list(args)] for hook, args in events])

    @staticmethod
    def decode(payload: bytes) -> List[tuple]:
        """Decode one message, raising ValueError if it is malformed."""
        try:
            return [(hook, tuple(args)) for hook, args in msgpack.unpackb(payload)]
        except Exception as e:
            raise ValueError(f"undecodable payload: {e}") from e


class LifecycleEventForwarder(ZmqPushSender):
    """Pushes lifecycle events to the selecting router, one PUSH socket per
    owner, so a request's events arrive in order. Best effort: an event whose
    push fails is dropped."""

    _payload = "KV lifecycle events"
    # Queue while connecting so a router's first events aren't dropped.
    _immediate = False

    def report(self, owner: str, method_name: str, *args) -> None:
        payload = LifecycleCodec.encode([(method_name, args)])
        pushed = self._push(owner, [payload])
        outcome = PushOutcome.PUSHED if pushed else PushOutcome.DROPPED
        _lifecycle_events_counter.inc(tags={"outcome": outcome.value})


class LifecycleReceiver:
    """ZMQ PULL endpoint for lifecycle events of requests this router selected.

    Invariants:
    - Binds a port the OS picks, so routers on one node never collide.
    - Applies one message at a time, in arrival order. A request's events come
      from one LLMServer's PUSH socket, so they apply in the order sent.
    - At most once: a lost message is never redelivered. Dynamo expires a
      booking whose free never arrives.
    - A malformed message or a failing handler is logged and skipped. Only
      ``close`` stops the loop.
    - Idempotent: a repeated event, or one for an unknown request, changes
      nothing. The handler must guarantee this.
    """

    def __init__(
        self,
        *,
        handler: Callable[[List[tuple]], Awaitable[None]],
        receive_queue_limit: int = KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT,
    ):
        self._handler = handler
        self._receive_queue_limit = receive_queue_limit
        self._socket = None
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> int:
        """Bind to a free port, start receiving, and return the port."""
        context = zmq_asyncio.Context.instance()
        socket = context.socket(zmq.PULL)
        # Close immediately instead of waiting on pending messages.
        socket.setsockopt(zmq.LINGER, 0)
        # Cap unread messages. Past it, LLMServers' pushes back up and drop.
        socket.setsockopt(zmq.RCVHWM, self._receive_queue_limit)
        try:
            port = socket.bind_to_random_port("tcp://*")
        except zmq.ZMQError:
            socket.close(linger=0)
            raise
        self._socket = socket
        self._task = asyncio.create_task(self._run())
        logger.info("KV lifecycle events listening on port %d.", port)
        return port

    async def _run(self) -> None:
        socket = self._socket
        while True:
            try:
                payload = await socket.recv()
            except asyncio.CancelledError:
                raise
            except (zmq.ContextTerminated, zmq.ZMQError) as e:
                if socket.closed or getattr(e, "errno", None) in (
                    zmq.ETERM,
                    zmq.ENOTSOCK,
                ):
                    break
                logger.exception("Failed to receive KV lifecycle events.")
                await asyncio.sleep(1)
                continue
            try:
                events = LifecycleCodec.decode(payload)
            except ValueError as e:
                logger.warning("Dropping malformed KV lifecycle message: %s", e)
                continue
            try:
                await self._handler(events)
            except Exception:
                logger.exception("Failed to apply KV lifecycle events.")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._socket is not None:
            self._socket.close(linger=0)
            self._socket = None
