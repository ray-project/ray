import asyncio
import contextlib
import dataclasses
import time
from collections import OrderedDict
from typing import List, Optional, TypeVar

import numpy as np
import zmq
import zmq.asyncio as zmq_asyncio

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_STAGING_MAX_BYTES,
    KV_TOKEN_STAGING_MAX_ENTRIES,
    KV_TOKEN_STAGING_TTL_S,
    KV_TOKEN_ZMQ_MAX_SOCKETS,
    KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT,
    KV_TOKEN_ZMQ_SEND_QUEUE_LIMIT,
)

logger = get_logger(__name__)


def encode_prompt_token_ids(token_ids: List[int]) -> bytes:
    """Encode token IDs as compact raw little-endian uint32 bytes."""
    try:
        arr = np.asarray(token_ids, dtype="<u4")
    except OverflowError as e:
        raise ValueError("prompt token ids must fit in uint32") from e
    if arr.ndim != 1:
        raise ValueError("prompt token ids must be a one-dimensional sequence")
    return arr.tobytes()


def decode_prompt_token_ids(payload: bytes) -> List[int]:
    """Decode a payload produced by :func:`encode_prompt_token_ids`."""
    return np.frombuffer(payload, dtype="<u4").tolist()


# zmq.Context is generic in its socket type and zmq.asyncio.Context subclasses
# Context[zmq.asyncio.Socket], so the returned socket matches the context passed.
_SocketT = TypeVar("_SocketT", bound=zmq.Socket)


def _close_socket(socket: zmq.Socket) -> None:
    socket.close(linger=0)


def _new_socket(context: zmq.Context[_SocketT], socket_type: int) -> _SocketT:
    socket = context.socket(socket_type)
    # LINGER=0: drop queued payloads on close. ZMQ defaults to -1, which
    # blocks teardown until every queued message is delivered.
    socket.setsockopt(zmq.LINGER, 0)
    return socket


class TokenSender:
    """Best-effort one-way prompt-token sender for selected LLMServer replicas."""

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

    def push(self, endpoint: str, key: str, payload: bytes) -> bool:
        """Enqueue a token payload without waiting for delivery.

        Returns False when the ZMQ pipe is unavailable or backed up; callers
        should omit the token-key header so the engine tokenizes normally.
        """
        socket = self._get_socket(endpoint)
        if socket is None:
            return False
        try:
            socket.send_multipart(
                [key.encode("ascii"), payload],
                flags=zmq.DONTWAIT,
                copy=False,
            )
        except zmq.Again:
            logger.debug(
                "Prompt-token ZMQ pipe to %s is not connected or its send queue "
                "is full; falling back to engine tokenization.",
                endpoint,
            )
            return False
        except zmq.ZMQError as e:
            # A genuine socket failure (terminated context, unusable peer)
            # rather than backpressure. Drop the socket so the next push redials
            # instead of reusing a broken one.
            self._discard_socket(endpoint)
            logger.warning(
                "Failed to send prompt tokens over ZMQ to %s; falling back to "
                "engine tokenization: %s",
                endpoint,
                e,
            )
            return False
        return True

    def close(self) -> None:
        for socket in self._sockets.values():
            _close_socket(socket)
        self._sockets.clear()

    def _get_socket(self, endpoint: str) -> Optional[zmq.Socket]:
        """Return a connected PUSH socket for ``endpoint``, or None if it is unusable."""
        socket = self._sockets.get(endpoint)
        if socket is not None:
            self._sockets.move_to_end(endpoint)
            return socket

        if self._context is None:
            self._context = zmq.Context.instance()

        socket = _new_socket(self._context, zmq.PUSH)
        # SNDHWM is the send high-water mark: how many payloads queue per replica
        # before sends fail. Must be set before connect().
        socket.setsockopt(zmq.SNDHWM, self._send_queue_limit)
        # Fail the send when the replica is not connected, instead of queueing
        # a payload it may never receive.
        socket.setsockopt(zmq.IMMEDIATE, 1)
        try:
            socket.connect(endpoint)
        except zmq.ZMQError as e:
            # e.g. a malformed endpoint advertised in a replica's routing stats.
            # This socket never made it into self._sockets, so release it now
            # instead of leaving it to GC.
            _close_socket(socket)
            logger.warning(
                "Failed to connect prompt-token ZMQ socket to %s; falling back "
                "to engine tokenization: %s",
                endpoint,
                e,
            )
            return None

        self._sockets[endpoint] = socket
        while len(self._sockets) > self._max_sockets:
            _, old = self._sockets.popitem(last=False)
            _close_socket(old)
        return socket

    def _discard_socket(self, endpoint: str) -> None:
        socket = self._sockets.pop(endpoint, None)
        if socket is not None:
            _close_socket(socket)


@dataclasses.dataclass
class _StagedTokens:
    payload: bytes
    created_at_s: float


class TokenStore:
    """LLMServer replica-local staging area for prompt tokens sent from LLMRouter.

    Keys are single-use: the engine ``pop``s each payload once, and anything not
    claimed is dropped by one of the bounds below.

    Invariants:
        Bounded: after every ``put``, at most ``max_entries`` entries and
            ``max_bytes`` total payload bytes. A payload over ``max_bytes`` is
            rejected rather than evicting everything to fit it.
        Eviction: oldest write first, for both TTL expiry and bound overflow.
            ``put`` re-dates an existing key, so entries stay ordered by write
            time and expiry only has to scan the front.
        Expiry: an entry older than ``ttl_s`` is never returned.
        Concurrency: an ``asyncio.Lock`` serializes mutations, so concurrent
            coroutines on one event loop are safe. Not thread-safe.
        Complexity: ``put`` and ``pop`` are amortized O(1) -- an entry is
            swept or evicted at most once. Space is bounded by ``max_bytes``.
    """

    def __init__(
        self,
        *,
        ttl_s: float = KV_TOKEN_STAGING_TTL_S,
        max_entries: int = KV_TOKEN_STAGING_MAX_ENTRIES,
        max_bytes: int = KV_TOKEN_STAGING_MAX_BYTES,
    ):
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._entries: OrderedDict[str, _StagedTokens] = OrderedDict()
        self._total_bytes = 0
        self._lock = asyncio.Lock()

    async def put(self, key: str, *, payload: bytes) -> None:
        if len(payload) > self._max_bytes:
            raise ValueError(
                "prompt token payload exceeds staging byte cap "
                f"({len(payload)} > {self._max_bytes})"
            )

        now = time.monotonic()
        async with self._lock:
            self._sweep(now)
            # Keys are per-request uuid4s, so a duplicate is collision-only. Be defensive:
            # _total_bytes is a running counter that is never recomputed, so a missed
            # subtraction here would permanently shrink the effective cap and start evicting
            # entries that in-flight requests still need.
            old = self._entries.pop(key, None)
            if old is not None:
                self._total_bytes -= len(old.payload)

            entry = _StagedTokens(payload=payload, created_at_s=now)
            self._entries[key] = entry
            self._total_bytes += len(payload)
            self._evict_to_limits()

    async def pop(self, key: str) -> Optional[_StagedTokens]:
        now = time.monotonic()
        async with self._lock:
            self._sweep(now)
            entry = self._entries.pop(key, None)
            if entry is not None:
                self._total_bytes -= len(entry.payload)
            return entry

    def _sweep(self, now: float) -> None:
        while self._entries:
            _, entry = next(iter(self._entries.items()))
            if now - entry.created_at_s <= self._ttl_s:
                return
            _, expired = self._entries.popitem(last=False)
            self._total_bytes -= len(expired.payload)

    def _evict_to_limits(self) -> None:
        while len(self._entries) > self._max_entries:
            _, evicted = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted.payload)
        while self._entries and self._total_bytes > self._max_bytes:
            _, evicted = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted.payload)


class TokenReceiver:
    """Best-effort ZMQ receiver for prompt-token payloads.

    Delivery is opportunistic: if a request starts before its keyed payload is
    staged, the OpenAI serving wrapper falls back to normal tokenization.
    """

    def __init__(
        self,
        *,
        bind_endpoint: str,
        store: TokenStore,
        receive_queue_limit: int = KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT,
    ):
        self._bind_endpoint = bind_endpoint
        self._store = store
        self._receive_queue_limit = receive_queue_limit
        self._socket = None
        self._task: Optional[asyncio.Task] = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> bool:
        if self.is_running:
            return True

        context = zmq_asyncio.Context.instance()
        socket = _new_socket(context, zmq.PULL)
        # RCVHWM is the receive high-water mark: how many payloads queue here
        # before ZMQ makes senders fail instead.
        socket.setsockopt(zmq.RCVHWM, self._receive_queue_limit)
        try:
            socket.bind(self._bind_endpoint)
        except Exception:
            _close_socket(socket)
            logger.exception(
                "Failed to bind token channel ZMQ endpoint %s; "
                "token channel is disabled.",
                self._bind_endpoint,
            )
            return False
        self._socket = socket
        self._task = asyncio.create_task(self._run())
        logger.info(
            "Prompt-token ZMQ token channel listening on %s.", self._bind_endpoint
        )
        return True

    async def _run(self) -> None:
        assert self._socket is not None
        while True:
            try:
                parts = await self._socket.recv_multipart(copy=True)
                await self._handle_message(parts)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to stage prompt-token ZMQ payload.")

    async def _handle_message(self, parts: List[bytes]) -> None:
        if len(parts) != 2:
            logger.warning(
                "Dropping prompt-token ZMQ message with %d frame(s); expected 2.",
                len(parts),
            )
            return
        key_frame, payload = parts
        await self._store.put(key_frame.decode("ascii"), payload=payload)

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._socket is not None:
            _close_socket(self._socket)
            self._socket = None
