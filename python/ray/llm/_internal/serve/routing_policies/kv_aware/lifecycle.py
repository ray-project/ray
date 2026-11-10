"""LifecycleEventForwarder: Push events to the selecting router over ZMQ.
LifecycleReceiver: Receive and apply events on the selecting router.
LifecycleCodec: Encode and decode the shared msgpack wire format.
RequestLifecycleOwnerMiddleware: Bind the router's endpoint from the request
    header throughout the handler and response stream.
bind_request_lifecycle_owner: Bind the router's endpoint for a request that
    arrives without HTTP, e.g. a P/D prefill call.
"""

import asyncio
import contextlib
import contextvars
from enum import Enum
from typing import Awaitable, Callable, Iterator, List, Optional

import msgpack
import zmq
import zmq.asyncio as zmq_asyncio
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_LIFECYCLE_OWNER_HEADER,
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


def get_request_lifecycle_owner() -> Optional[str]:
    """Lifecycle endpoint of the router that selected the current request."""
    return _request_lifecycle_owner.get()


@contextlib.contextmanager
def bind_request_lifecycle_owner(owner: Optional[str]) -> Iterator[None]:
    """Report the lifecycle events of requests run in this block to ``owner``.

    Bind before the request starts: tasks it spawns inherit the owner.
    """
    token = _request_lifecycle_owner.set(owner)
    try:
        yield
    finally:
        _request_lifecycle_owner.reset(token)


class RequestLifecycleOwnerMiddleware:
    """Bind the owner before vLLM spawns its handler task, through streaming."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # vLLM prepares the response in a child task but consumes the stream
        # outside it. Bind here so both inherit the request's owner.
        owner = Headers(scope=scope).get(KV_LIFECYCLE_OWNER_HEADER)
        with bind_request_lifecycle_owner(owner):
            await self.app(scope, receive, send)


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
