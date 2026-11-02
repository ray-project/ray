import asyncio
import contextlib
import dataclasses
import time
from collections import OrderedDict
from typing import List, Optional

import numpy as np
import zmq
import zmq.asyncio as zmq_asyncio

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_STAGING_MAX_BYTES,
    KV_TOKEN_STAGING_MAX_ENTRIES,
    KV_TOKEN_STAGING_TTL_S,
    KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT,
)
from ray.llm._internal.serve.routing_policies.kv_aware.network_utils import (
    ZmqPushSender,
    close_socket,
    new_socket,
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


class TokenSender(ZmqPushSender):
    """Best-effort one-way prompt-token sender for selected LLMServer replicas.

    A failed push means the caller omits the token-key header, so the engine
    tokenizes normally.
    """

    _payload = "prompt tokens"
    _fallback = "falling back to engine tokenization"

    def push(self, endpoint: str, key: str, payload: bytes) -> bool:
        return self._push(endpoint, [key.encode("ascii"), payload])


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
        Concurrency: the token receiver and engine HTTP handlers are coroutines on
            the same serve replica event loop. ``put`` and ``pop`` therefore run
            atomically because they contain no suspension points. Not thread-safe.
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

    def put(self, key: str, *, payload: bytes) -> None:
        if len(payload) > self._max_bytes:
            raise ValueError(
                "prompt token payload exceeds staging byte cap "
                f"({len(payload)} > {self._max_bytes})"
            )

        now = time.monotonic()
        self._sweep(now)
        # Keys are per-request uuid4s, so a duplicate is collision-only. Be defensive:
        # _total_bytes is a running counter that is never recomputed, so a missed
        # subtraction here would permanently shrink the effective cap and start evicting
        # entries that in-flight requests still need.
        #
        # Popping and re-adding the entry is necessary to honor the invariant that the
        # oldest entry is at the front of _entries, so TTL expiry and eviction can scan
        # from the front.
        old = self._entries.pop(key, None)
        if old is not None:
            self._total_bytes -= len(old.payload)

        entry = _StagedTokens(payload=payload, created_at_s=now)
        self._entries[key] = entry
        self._total_bytes += len(payload)
        self._evict_to_limits()

    def pop(self, key: str) -> Optional[_StagedTokens]:
        now = time.monotonic()
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
        socket = new_socket(context, zmq.PULL)
        # RCVHWM is the receive high-water mark: how many payloads queue here
        # before ZMQ makes senders fail instead.
        socket.setsockopt(zmq.RCVHWM, self._receive_queue_limit)
        try:
            socket.bind(self._bind_endpoint)
        except Exception:
            close_socket(socket)
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
        socket = self._socket
        assert socket is not None
        while True:
            try:
                parts = await socket.recv_multipart(copy=True)
            except asyncio.CancelledError:
                raise
            except zmq.ContextTerminated:
                logger.info("ZMQ context terminated; stopping prompt-token receiver.")
                break
            except zmq.ZMQError as e:
                if socket.closed or e.errno in (zmq.ETERM, zmq.ENOTSOCK):
                    logger.info("ZMQ socket closed; stopping prompt-token receiver.")
                    break
                logger.exception("Failed to receive prompt-token ZMQ payload.")
                # Prevent a persistent receive failure from spinning the loop
                # and consuming CPU while flooding logs.
                await asyncio.sleep(1)
                continue
            except Exception:
                logger.exception("Failed to receive prompt-token ZMQ payload.")
                # Unexpected persistent failures need the same retry backoff.
                await asyncio.sleep(1)
                continue

            try:
                self._handle_message(parts)
            except Exception:
                logger.exception("Failed to stage prompt-token ZMQ payload.")

    def _handle_message(self, parts: List[bytes]) -> None:
        if len(parts) != 2:
            logger.warning(
                "Dropping prompt-token ZMQ message with %d frame(s); expected 2.",
                len(parts),
            )
            return
        key_frame, payload = parts
        self._store.put(key_frame.decode("ascii"), payload=payload)

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._socket is not None:
            close_socket(self._socket)
            self._socket = None
