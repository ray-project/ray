"""ZMQ socket helpers shared by the KV router's one-way channels."""

from collections import OrderedDict
from typing import List, Optional, TypeVar

import zmq

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
