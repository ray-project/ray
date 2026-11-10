"""ZMQ primitives for the KV router's calls between replicas.

The KV router talks to replicas over plain ZMQ:

- One-way push: ``ZmqPushSender`` keeps one PUSH socket per endpoint and never
  waits for delivery. Its receiver binds a PULL socket. At most once.
- Request/reply: ``ZmqRpcClient`` keeps one DEALER socket per server and
  multiplexes concurrent calls on it. ``ZmqRpcServer`` binds a ROUTER socket
  and runs each call as a task.
"""

import asyncio
import contextlib
import itertools
from collections import OrderedDict
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple, TypeVar

import zmq
import zmq.asyncio as zmq_asyncio

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_ZMQ_MAX_SOCKETS,
    KV_TOKEN_ZMQ_SEND_QUEUE_LIMIT,
)

logger = get_logger(__name__)


# zmq.Context is generic in its socket type and zmq.asyncio.Context subclasses
# Context[zmq.asyncio.Socket], so the returned socket matches the context passed.
_SocketT = TypeVar("_SocketT", bound=zmq.Socket)


def close_socket(socket: zmq.Socket) -> None:
    socket.close(linger=0)


def new_socket(context: zmq.Context[_SocketT], socket_type: int) -> _SocketT:
    socket = context.socket(socket_type)
    # LINGER=0: drop queued payloads on close. ZMQ defaults to -1, which
    # blocks teardown until every queued message is delivered.
    socket.setsockopt(zmq.LINGER, 0)
    return socket


class ZmqPushSender:
    """Best-effort one-way ZMQ sender with one PUSH socket per endpoint."""

    # Log wording: the payload, and the caller's fallback when a push fails.
    _payload = "payloads"
    _fallback = "dropping them"
    _immediate = True

    def __init__(
        self,
        *,
        send_queue_limit: int = KV_TOKEN_ZMQ_SEND_QUEUE_LIMIT,
        max_sockets: int = KV_TOKEN_ZMQ_MAX_SOCKETS,
    ):
        self._send_queue_limit = send_queue_limit
        # Bound sockets/queues retained across replica churn; evict the least
        # recently used endpoint when this limit is exceeded.
        self._max_sockets = max_sockets
        self._context = None
        self._sockets: OrderedDict[str, zmq.Socket] = OrderedDict()

    def _push(self, endpoint: str, frames: List[bytes]) -> bool:
        """Enqueue a multipart message without waiting for delivery.

        Returns False when the ZMQ pipe is unavailable or backed up.
        """
        socket = self._get_socket(endpoint)
        if socket is None:
            return False
        try:
            socket.send_multipart(
                frames,
                flags=zmq.DONTWAIT,
                copy=False,
            )
        except zmq.Again:
            logger.debug(
                "ZMQ pipe for %s to %s is not connected or its send queue is "
                "full; %s.",
                self._payload,
                endpoint,
                self._fallback,
            )
            return False
        except zmq.ZMQError as e:
            # A genuine socket failure (terminated context, unusable peer)
            # rather than backpressure. Drop the socket so the next push redials
            # instead of reusing a broken one.
            self._discard_socket(endpoint)
            logger.warning(
                "Failed to send %s over ZMQ to %s; %s: %s",
                self._payload,
                endpoint,
                self._fallback,
                e,
            )
            return False
        return True

    def close(self) -> None:
        for socket in self._sockets.values():
            close_socket(socket)
        self._sockets.clear()
        if self._context is not None:
            self._context.term()
            self._context = None

    def __del__(self) -> None:
        self.close()

    def _get_socket(self, endpoint: str) -> Optional[zmq.Socket]:
        """Return a connected PUSH socket for ``endpoint``, or None if it is unusable."""
        socket = self._sockets.get(endpoint)
        if socket is not None:
            self._sockets.move_to_end(endpoint)
            return socket

        if self._context is None:
            # The default shared context caps at 1023 sockets.
            self._context = zmq.Context()
            self._context.set(zmq.MAX_SOCKETS, self._max_sockets + 1)

        try:
            socket = new_socket(self._context, zmq.PUSH)
        except zmq.ZMQError as e:
            logger.warning(
                "Failed to create the ZMQ socket for %s to %s; %s: %s",
                self._payload,
                endpoint,
                self._fallback,
                e,
            )
            return None
        # SNDHWM is the send high-water mark: how many payloads queue per replica
        # before sends fail. Must be set before connect().
        socket.setsockopt(zmq.SNDHWM, self._send_queue_limit)
        # Fail the send when the replica is not connected, instead of queueing
        # a payload it may never receive.
        socket.setsockopt(zmq.IMMEDIATE, int(self._immediate))
        try:
            socket.connect(endpoint)
        except zmq.ZMQError as e:
            # e.g. a malformed endpoint advertised in a replica's routing stats.
            # This socket never made it into self._sockets, so release it now
            # instead of leaving it to GC.
            close_socket(socket)
            logger.warning(
                "Failed to connect the ZMQ socket for %s to %s; %s: %s",
                self._payload,
                endpoint,
                self._fallback,
                e,
            )
            return None

        self._sockets[endpoint] = socket
        while len(self._sockets) > self._max_sockets:
            _, old = self._sockets.popitem(last=False)
            close_socket(old)
        return socket

    def _discard_socket(self, endpoint: str) -> None:
        socket = self._sockets.pop(endpoint, None)
        if socket is not None:
            close_socket(socket)


# Request/reply wire format. Client -> server: [call_id, kind, payload], where
# kind is _CALL or _CANCEL. Server -> client: [call_id, status, payload], where
# status is _OK (payload is the result) or _ERROR (payload is a UTF-8 message).
_CALL, _CANCEL = b"call", b"cancel"
_OK, _ERROR = b"ok", b"error"
# The client declares a server dead when its connection is silent this long, so
# a call to a vanished node fails instead of waiting out its timeout.
_RPC_HEARTBEAT_INTERVAL_MS = 5_000
_RPC_HEARTBEAT_TIMEOUT_MS = 30_000


class ZmqRpcError(RuntimeError):
    """The server's handler raised; the message carries its error."""


class _RpcChannel:
    """One DEALER connection to a ``ZmqRpcServer`` and its calls in flight.

    The channel closes itself when the connection drops, failing its calls
    with ``ConnectionError``: their replies can no longer arrive.
    """

    def __init__(self, context: zmq_asyncio.Context, endpoint: str):
        self.endpoint = endpoint
        self._pending: Dict[bytes, asyncio.Future] = {}
        socket = new_socket(context, zmq.DEALER)
        socket.setsockopt(zmq.HEARTBEAT_IVL, _RPC_HEARTBEAT_INTERVAL_MS)
        socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, _RPC_HEARTBEAT_TIMEOUT_MS)
        try:
            self._monitor = socket.get_monitor_socket(zmq.EVENT_DISCONNECTED)
            socket.connect(endpoint)
        except zmq.ZMQError:
            close_socket(socket)
            raise
        self._socket = socket
        self._task = asyncio.create_task(self._run())

    @property
    def closed(self) -> bool:
        return self._task.done()

    @property
    def idle(self) -> bool:
        return not self._pending

    async def call(self, call_id: bytes, payload: bytes) -> bytes:
        future = asyncio.get_running_loop().create_future()
        self._pending[call_id] = future
        try:
            try:
                # Queues while the connection is still being established.
                await self._socket.send_multipart([call_id, _CALL, payload], copy=False)
            except zmq.ZMQError as e:
                # E.g. the connection dropped and closed the socket mid-send.
                raise ConnectionError(f"Failed to call {self.endpoint}: {e}") from e
            return await future
        except BaseException:
            if self._pending.pop(call_id, None) is not None:
                # Cancelled or timed out: stop the work nobody awaits anymore.
                self._send_cancel(call_id)
            raise

    def _send_cancel(self, call_id: bytes) -> None:
        """Best effort, without waiting: the server may already be gone."""
        if self.closed:
            return
        with contextlib.suppress(zmq.ZMQError):
            sent = self._socket.send_multipart(
                [call_id, _CANCEL, b""], flags=zmq.NOBLOCK
            )
            sent.add_done_callback(lambda f: f.cancelled() or f.exception())

    async def _run(self) -> None:
        poller = zmq_asyncio.Poller()
        poller.register(self._socket, zmq.POLLIN)
        poller.register(self._monitor, zmq.POLLIN)
        error = ConnectionError(f"Lost the connection to {self.endpoint}.")
        try:
            while True:
                ready = dict(await poller.poll())
                if self._socket in ready:
                    self._resolve(await self._socket.recv_multipart())
                if self._monitor in ready:
                    # Only EVENT_DISCONNECTED is monitored. Replies that
                    # arrived before the drop still count.
                    await self._monitor.recv_multipart()
                    await self._drain_replies()
                    return
        except asyncio.CancelledError:
            error = ConnectionError(f"Closed the connection to {self.endpoint}.")
            raise
        except zmq.ZMQError as e:
            error = ConnectionError(f"Connection to {self.endpoint} failed: {e}")
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()
            with contextlib.suppress(zmq.ZMQError):
                self._socket.disable_monitor()
            close_socket(self._monitor)
            close_socket(self._socket)

    async def _drain_replies(self) -> None:
        while True:
            try:
                frames = await self._socket.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.Again:
                return
            self._resolve(frames)

    def _resolve(self, frames: List[bytes]) -> None:
        if len(frames) != 3:
            logger.warning("Dropping a malformed ZMQ RPC reply from %s.", self.endpoint)
            return
        call_id, status, payload = frames
        future = self._pending.pop(call_id, None)
        if future is None or future.done():
            return  # The caller already gave up.
        if status == _OK:
            future.set_result(payload)
        else:
            future.set_exception(ZmqRpcError(payload.decode(errors="replace")))

    def abort(self) -> None:
        """Close the connection, failing calls in flight."""
        self._task.cancel()

    async def close(self) -> None:
        self.abort()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


class ZmqRpcClient:
    """Calls ``ZmqRpcServer`` endpoints over one DEALER socket per server.

    Invariants:
    - A call returns the handler's reply, or raises ``ZmqRpcError`` (the handler
      raised), ``ConnectionError`` (the connection dropped, e.g. the server
      died), or ``asyncio.TimeoutError``.
    - A call that times out or is cancelled cancels its handler on the server.
    - Holds at most ``max_channels`` connections, closing idle ones, least
      recently used first, past that. A dropped connection is reopened by the
      next call.
    - Must be used from one event loop.
    """

    def __init__(self, *, max_channels: int = KV_TOKEN_ZMQ_MAX_SOCKETS):
        self._max_channels = max_channels
        self._context: Optional[zmq_asyncio.Context] = None
        self._channels: "OrderedDict[str, _RpcChannel]" = OrderedDict()
        self._call_ids = itertools.count()

    async def call(self, endpoint: str, payload: bytes, *, timeout_s: float) -> bytes:
        channel = self._get_channel(endpoint)
        call_id = next(self._call_ids).to_bytes(8, "big")
        return await asyncio.wait_for(channel.call(call_id, payload), timeout_s)

    def _get_channel(self, endpoint: str) -> _RpcChannel:
        channel = self._channels.get(endpoint)
        if channel is not None and not channel.closed:
            self._channels.move_to_end(endpoint)
            return channel
        if self._context is None:
            self._context = zmq_asyncio.Context()
            # A channel holds a DEALER, its monitor's PAIR, and libzmq's own
            # end of that PAIR. Evicted channels close asynchronously.
            self._context.set(zmq.MAX_SOCKETS, max(1023, 4 * self._max_channels))
        for stale in [c for c in self._channels.values() if c.closed]:
            del self._channels[stale.endpoint]
        idle = [c for c in self._channels.values() if c.idle]
        while len(self._channels) >= self._max_channels and idle:
            evicted = idle.pop(0)
            del self._channels[evicted.endpoint]
            evicted.abort()
        channel = self._channels[endpoint] = _RpcChannel(self._context, endpoint)
        return channel

    async def close(self) -> None:
        channels, self._channels = list(self._channels.values()), OrderedDict()
        await asyncio.gather(*(channel.close() for channel in channels))
        if self._context is not None:
            self._context.term()
            self._context = None


class ZmqRpcServer:
    """Serves ``ZmqRpcClient`` calls on a ROUTER socket bound to a free port.

    Invariants:
    - Runs each call as its own task on this event loop, so a slow call never
      delays the others.
    - A cancel frame from the caller cancels the call's task, which sends no
      reply.
    - Replies with the handler's error when it raises.
    - A reply to a caller that has disconnected is dropped.
    """

    def __init__(self, handler: Callable[[bytes], Awaitable[bytes]]):
        self._handler = handler
        self._socket: Optional[zmq_asyncio.Socket] = None
        self._task: Optional[asyncio.Task] = None
        self._calls: Dict[Tuple[bytes, bytes], asyncio.Task] = {}

    async def start(self) -> int:
        """Bind to a free port, start serving calls, and return the port."""
        socket = new_socket(zmq_asyncio.Context.instance(), zmq.ROUTER)
        try:
            port = socket.bind_to_random_port("tcp://*")
        except zmq.ZMQError:
            close_socket(socket)
            raise
        self._socket = socket
        self._task = asyncio.create_task(self._run())
        return port

    async def _run(self) -> None:
        socket = self._socket
        while True:
            try:
                frames = await socket.recv_multipart()
            except asyncio.CancelledError:
                raise
            except zmq.ZMQError as e:
                if socket.closed or e.errno in (zmq.ETERM, zmq.ENOTSOCK):
                    break
                logger.exception("Failed to receive a ZMQ RPC call.")
                await asyncio.sleep(1)
                continue
            if len(frames) != 4:
                logger.warning("Dropping a malformed ZMQ RPC call.")
                continue
            identity, call_id, kind, payload = frames
            key = (identity, call_id)
            if kind == _CANCEL:
                task = self._calls.pop(key, None)
                if task is not None:
                    task.cancel()
            elif kind == _CALL:
                task = asyncio.create_task(self._serve(identity, call_id, payload))
                self._calls[key] = task
                task.add_done_callback(lambda _, key=key: self._calls.pop(key, None))

    async def _serve(self, identity: bytes, call_id: bytes, payload: bytes) -> None:
        try:
            reply = [_OK, await self._handler(payload)]
        except asyncio.CancelledError:
            return  # The caller gave up, or the server is closing.
        except Exception as e:
            logger.warning("ZMQ RPC handler failed: %r", e)
            reply = [_ERROR, f"{type(e).__name__}: {e}".encode()]
        with contextlib.suppress(zmq.ZMQError):
            await self._socket.send_multipart([identity, call_id, *reply], copy=False)

    async def close(self) -> None:
        tasks: Set[asyncio.Task] = set(self._calls.values())
        if self._task is not None:
            tasks.add(self._task)
            self._task = None
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._calls.clear()
        if self._socket is not None:
            close_socket(self._socket)
            self._socket = None
