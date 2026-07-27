import asyncio
import contextlib
import dataclasses
import functools
import time
from collections import OrderedDict
from typing import Any, List, Optional

import numpy as np
import zmq
import zmq.asyncio as zmq_asyncio
from starlette.requests import Request

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
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


def _close_socket(socket) -> None:
    socket.close(linger=0)


def _new_socket(context, socket_type):
    socket = context.socket(socket_type)
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
        self._sockets: OrderedDict[str, Any] = OrderedDict()

    def push(self, endpoint: str, key: str, payload: bytes) -> bool:
        """Enqueue a token payload without waiting for delivery.

        Returns False when the ZMQ pipe is unavailable or backed up; callers
        should omit the token-key header so the engine tokenizes normally.
        """
        socket = self._get_socket(endpoint)
        try:
            socket.send_multipart(
                [key.encode("ascii"), payload],
                flags=zmq.DONTWAIT,
                copy=False,
            )
        except zmq.Again:
            logger.debug(
                "Prompt-token ZMQ pipe to %s is not ready or is at HWM; "
                "falling back to engine tokenization.",
                endpoint,
            )
            return False
        return True

    def close(self) -> None:
        for socket in self._sockets.values():
            _close_socket(socket)
        self._sockets.clear()

    def _get_socket(self, endpoint: str):
        socket = self._sockets.get(endpoint)
        if socket is not None:
            self._sockets.move_to_end(endpoint)
            return socket

        if self._context is None:
            self._context = zmq.Context.instance()

        socket = _new_socket(self._context, zmq.PUSH)
        socket.setsockopt(zmq.SNDTIMEO, 0)
        socket.setsockopt(zmq.SNDHWM, self._send_queue_limit)
        socket.setsockopt(zmq.IMMEDIATE, 1)
        socket.connect(endpoint)

        self._sockets[endpoint] = socket
        while len(self._sockets) > self._max_sockets:
            _, old = self._sockets.popitem(last=False)
            _close_socket(old)
        return socket


@dataclasses.dataclass
class _StagedTokens:
    payload: bytes
    created_at_s: float


class TokenStore:
    """Replica-local staging area for binary prompt token vectors."""

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
        if self._max_bytes > 0 and len(payload) > self._max_bytes:
            raise ValueError(
                "prompt token payload exceeds staging byte cap "
                f"({len(payload)} > {self._max_bytes})"
            )

        now = time.monotonic()
        async with self._lock:
            self._sweep(now)
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
        if self._ttl_s <= 0:
            return
        while self._entries:
            _, entry = next(iter(self._entries.items()))
            if now - entry.created_at_s <= self._ttl_s:
                return
            _, expired = self._entries.popitem(last=False)
            self._total_bytes -= len(expired.payload)

    def _evict_to_limits(self) -> None:
        while self._max_entries > 0 and len(self._entries) > self._max_entries:
            _, evicted = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted.payload)
        while self._max_bytes > 0 and self._total_bytes > self._max_bytes:
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


def _token_ids_from_entry(entry: _StagedTokens) -> List[int]:
    return np.frombuffer(entry.payload, dtype="<u4").tolist()


async def inject_prompt_token_ids(
    request: Any,
    raw_request: Optional[Request],
    store: TokenStore,
) -> None:
    if raw_request is None:
        return
    token_key = raw_request.headers.get(KV_TOKEN_KEY_HEADER)
    if not token_key:
        return

    entry = await store.pop(token_key)
    if entry is None:
        return

    kv_transfer_params = getattr(request, "kv_transfer_params", None)
    if not isinstance(kv_transfer_params, dict):
        kv_transfer_params = {}
        request.kv_transfer_params = kv_transfer_params
    kv_transfer_params["prompt_token_ids"] = _token_ids_from_entry(entry)


def _install_prompt_token_forwarding(
    serving: Any,
    method_name: str,
    store: TokenStore,
) -> None:
    if serving is None:
        return
    orig = getattr(serving, method_name, None)
    if orig is None or getattr(orig, "_kv_token_channel_wrapped", False):
        return

    @functools.wraps(orig)
    async def wrapped(request: Any, raw_request: Optional[Request] = None, *args, **kw):
        effective_raw_request = kw.get("raw_request", raw_request)
        await inject_prompt_token_ids(request, effective_raw_request, store)
        if "raw_request" in kw and raw_request is None:
            return await orig(request, *args, **kw)
        return await orig(request, raw_request, *args, **kw)

    setattr(wrapped, "_kv_token_channel_wrapped", True)
    setattr(serving, method_name, wrapped)


def install_prompt_token_forwarding(
    state: Any,
    store: TokenStore,
) -> None:
    for attr_name, method_name in (
        ("openai_serving_chat", "create_chat_completion"),
        ("openai_serving_completion", "create_completion"),
    ):
        _install_prompt_token_forwarding(
            getattr(state, attr_name, None),
            method_name,
            store,
        )
