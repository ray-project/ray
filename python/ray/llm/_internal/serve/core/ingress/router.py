import asyncio
import json
import os
import socket
from types import SimpleNamespace
from typing import TYPE_CHECKING, Awaitable, List, Optional, Set, Tuple
import uuid
from urllib.parse import quote
import zlib

import aiohttp
from fastapi import FastAPI, HTTPException, Request
import numpy as np

from ray import serve
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_PROMPT_TOKEN_CRC32_FIELD,
    KV_PROMPT_TOKEN_CRC32_HEADER,
    KV_PROMPT_TOKEN_KEY_FIELD,
    KV_PROMPT_TOKEN_LEN_FIELD,
    KV_PROMPT_TOKEN_LEN_HEADER,
    KV_PROMPT_TOKEN_SIDE_CHANNEL_PATH,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

# Type-only import as LLMConfig transitively pulls in vLLM. This file should
# remain engine-agnostic.
if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

logger = get_logger(__name__)

_BODY_TRUNCATED_HEADER = "x-body-truncated"
_TOKEN_PUSH_TIMEOUT_S = float(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_PUSH_TIMEOUT_S", "30")
)
_TOKEN_PUSH_CONCURRENCY = int(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_PUSH_CONCURRENCY", "512")
)
_TOKEN_PUSH_PER_HOST = int(
    os.environ.get("RAY_SERVE_KV_PROMPT_TOKEN_PUSH_PER_HOST", "128")
)

# A request body routes on one of these fields. Body-aware routers read it off
# the namespace; a body without any of them degrades to load-balancing. Extend
# as routers learn to route additional request types.
_ROUTING_KEY_FIELDS = ("messages", "prompt")

router_app = FastAPI()


def _parse_routing_payload(body: bytes) -> Optional[SimpleNamespace]:
    """Wrap a request body as a namespace a body-aware router routes on.

    Routers read a routing field (``messages`` or ``prompt``) off the first
    positional routing arg, the parsed request the normal ingress forwards.
    Direct streaming has only the raw body, so this wraps the parsed body in a
    namespace exposing every field by attribute, which a router reads the same
    way regardless of request type. Returns ``None`` for an empty, non-object,
    unparseable, or keyless body, so the caller falls back to load-balancing.
    """
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    if not any(data.get(field) for field in _ROUTING_KEY_FIELDS):
        return None
    return SimpleNamespace(**data)


def _host_for_url(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _encode_prompt_token_ids(token_ids: List[int]) -> bytes:
    """Encode token IDs as compact raw little-endian uint32 bytes."""
    arr = np.asarray(token_ids, dtype=np.int64)
    if arr.ndim != 1:
        raise ValueError("prompt token ids must be a one-dimensional sequence")
    uint32_max = np.iinfo(np.uint32).max
    if np.any((arr < 0) | (arr > uint32_max)):
        raise ValueError("prompt token ids must fit in uint32")
    return arr.astype("<u4", copy=False).tobytes()


@serve.ingress(router_app)
class LLMRouter:
    """Ingress request router for direct streaming.

    When direct streaming is enabled, HAProxy calls /internal/route on this
    deployment to get a data plane replica, then forwards traffic directly
    to the matching LLMServer replica's backend HTTP port.

    Replica selection is delegated to the underlying deployment's configured
    request router, and this class translates the resulting pick into a backend
    HTTP endpoint.

    /internal/route HTTP contract
    -----------------------------
    Request:
        POST /internal/route
        Content-Type: application/json
        Body: the target ChatCompletions or Completions request payload.
            Wrapped in a namespace by ``_parse_routing_payload`` and passed to
            ``choose_replica`` positionally, exposing the request fields the way
            the parsed request does. Body-aware policies then score replicas the
            same way on both paths.

    Truncated bodies:
        HAProxy may forward only a prefix of the body for routing and sets the
        ``x-body-truncated`` header. A truncated prefix is usually not valid
        JSON, so no routing key is derived and the request falls back to the
        default load-balanced pick.

    Session affinity:
        If the client request carried the session-id header configured by
        ``RAY_SERVE_SESSION_ID_HEADER_KEY`` (default ``x-session-id``),
        HAProxy's Lua action forwards it to ``/internal/route`` on the same
        name. This handler reads it and applies
        ``handle.options(session_id=...)`` before calling
        ``choose_replica`` so session-aware policies (e.g.
        ``ConsistentHashRouter``) pin all turns of a session to one replica.

    Responses:
        200 ``{"host": str, "port": int, "replica_id": str}``: pick
            succeeded.
        4xx/5xx FastAPI ``{"detail": str}``: informational only; HAProxy
            treats any non-200 as a routing failure. When using KV aware routing,
            a pre-routing ``/tokenize`` rejection is surfaced here.

    Health:
        ``GET /health`` is exposed as a human-operator convenience.
        Serve uses ``check_health()`` for replica readiness, not HTTP.
    """

    # Warn once per replica when no routing key is derived. Class-level default
    # keeps the guard safe before __init__ runs.
    _warned_no_routing_key: bool = False

    async def __init__(
        self,
        server: DeploymentHandle,
        llm_config: Optional["LLMConfig"] = None,
    ):
        self._handle: DeploymentHandle = server
        self._tokenizer = None
        self._token_push_session: Optional[aiohttp.ClientSession] = None
        self._pending_token_push_tasks: Set[asyncio.Task] = set()
        self._token_push_semaphore = asyncio.Semaphore(_TOKEN_PUSH_CONCURRENCY)
        # Holds the KVTokenTracker (KV-aware deployments only) so the
        # engine-facing on_lifecycle_events method can book load into it.
        self._kv_token_tracker = None
        # A non-None llm_config signals pre-routing tokenization, which the
        # builder binds only for a KV-aware request router.
        if llm_config is not None:
            # Build the tracker before _handle._init() below, which initializes
            # the KVAwareRouter that looks it up. server.deployment_id is the
            # tracked LLMServer deployment.
            from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (  # noqa: E501
                build_kv_token_tracker,
            )

            self._kv_token_tracker = build_kv_token_tracker(
                llm_config, server.deployment_id
            )
            # Lazy import: this module pulls in vLLM's renderer;
            # keep it off the non-KV ingress import path.
            from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
                Tokenizer,
            )

            self._tokenizer = await asyncio.to_thread(Tokenizer, llm_config)
        self._handle._init()

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        routing_payload = _parse_routing_payload(body)
        if routing_payload is None and not self._warned_no_routing_key:
            self._warned_no_routing_key = True
            logger.warning(
                "Could not derive a routing key from the request body. "
                "body_truncated=%s. Falling back to load-balanced replica "
                "selection. A configured body-aware router such as "
                "PrefixCacheAffinityRouter cannot take effect for these "
                "requests. For truncated bodies, raise HAProxy's routing body "
                "limit.",
                body_truncated,
            )
        # Tokenize only a parseable, routable body; a truncated or unparseable
        # body has no routing payload, so fall back to token-less routing.
        request_token_ids = None
        if self._tokenizer is not None and routing_payload is not None:
            from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
                TokenizeError,
            )

            try:
                request_token_ids = await self._tokenizer.tokenize(
                    vars(routing_payload)
                )
            except TokenizeError as e:
                raise HTTPException(status_code=e.status_code, detail=e.message)
        # HAProxy forwards the configured session header on the same name,
        # but use the same case-insensitive, separator-tolerant matcher as
        # proxy.py / ingress.py so a `-`/`_` rewrite anywhere in the path
        # doesn't silently drop session affinity.
        session_id = next(
            (v for k, v in request.headers.items() if _matches_session_id_header(k)),
            None,
        )
        handle = (
            self._handle.options(session_id=session_id) if session_id else self._handle
        )
        try:
            host, port, replica_id = await self._pick_replica(
                handle=handle,
                routing_payload=routing_payload,
                request_token_ids=request_token_ids,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))

        response = {"host": host, "port": port, "replica_id": replica_id}
        if request_token_ids:
            try:
                metadata, push = self._build_prompt_token_push(
                    host=host,
                    port=port,
                    replica_id=replica_id,
                    request_token_ids=request_token_ids,
                )
                response.update(metadata)
                self._schedule_token_push(push, replica_id, len(request_token_ids))
            except Exception as e:
                logger.exception(
                    "Failed to prepare %s prompt token IDs for selected replica %s",
                    len(request_token_ids),
                    replica_id,
                )
                raise HTTPException(
                    status_code=503,
                    detail=f"failed to prepare prompt token ids: {e}",
                ) from e
        return response

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    async def on_lifecycle_events(self, batch):
        """Engine-facing intake for request lifecycle events.

        Engine replicas broadcast each batch to every LLMRouter replica to
        book request load; this applies it to the KVTokenTracker on this
        ingress replica's event loop.
        """
        return await self._kv_token_tracker.on_lifecycle_events(batch)

    def _get_token_push_session(self) -> aiohttp.ClientSession:
        if self._token_push_session is None or self._token_push_session.closed:
            timeout = aiohttp.ClientTimeout(total=_TOKEN_PUSH_TIMEOUT_S)
            connector = aiohttp.TCPConnector(
                limit=_TOKEN_PUSH_CONCURRENCY,
                limit_per_host=_TOKEN_PUSH_PER_HOST,
                force_close=False,
                ttl_dns_cache=300,
                family=socket.AF_INET,
                happy_eyeballs_delay=None,
            )
            self._token_push_session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
        return self._token_push_session

    def _schedule_token_push(
        self, awaitable, replica_id: str, token_count: int
    ) -> None:
        task = asyncio.create_task(awaitable)
        self._pending_token_push_tasks.add(task)

        def _done(done: asyncio.Task) -> None:
            self._pending_token_push_tasks.discard(done)
            try:
                done.result()
            except Exception:
                logger.exception(
                    "Failed to stage %s prompt token IDs on selected replica %s",
                    token_count,
                    replica_id,
                )

        task.add_done_callback(_done)

    def _build_prompt_token_push(
        self,
        *,
        host: str,
        port: int,
        replica_id: str,
        request_token_ids: List[int],
    ) -> Tuple[dict, Awaitable[None]]:
        payload = _encode_prompt_token_ids(request_token_ids)
        token_count = len(request_token_ids)
        crc32 = f"{zlib.crc32(payload) & 0xFFFFFFFF:08x}"
        key = uuid.uuid4().hex
        url = (
            f"http://{_host_for_url(host)}:{port}"
            f"{KV_PROMPT_TOKEN_SIDE_CHANNEL_PATH}/{quote(key, safe='')}"
        )
        headers = {
            "content-type": "application/octet-stream",
            KV_PROMPT_TOKEN_LEN_HEADER: str(token_count),
            KV_PROMPT_TOKEN_CRC32_HEADER: crc32,
        }

        metadata = {
            KV_PROMPT_TOKEN_KEY_FIELD: key,
            KV_PROMPT_TOKEN_LEN_FIELD: str(token_count),
            KV_PROMPT_TOKEN_CRC32_FIELD: crc32,
        }
        return metadata, self._push_prompt_token_payload(
            url=url,
            headers=headers,
            payload=payload,
            replica_id=replica_id,
        )

    async def _push_prompt_token_payload(
        self,
        *,
        url: str,
        headers: dict,
        payload: bytes,
        replica_id: str,
    ) -> None:
        session = self._get_token_push_session()
        async with self._token_push_semaphore:
            async with session.put(url, data=payload, headers=headers) as resp:
                if resp.status // 100 != 2:
                    detail = await resp.text()
                    raise RuntimeError(
                        "selected replica rejected prompt token side-channel push "
                        f"for {replica_id}: status={resp.status}, detail={detail[:200]!r}"
                    )

    async def _pick_replica(
        self,
        handle: DeploymentHandle,
        routing_payload: Optional[SimpleNamespace] = None,
        request_token_ids: Optional[List[int]] = None,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``handle`` is the LLMServer deployment handle, optionally configured
        with ``.options(session_id=...)`` by the caller so session-aware
        routers see the session id on ``RequestMetadata``.

        ``routing_payload``, when present, is passed to ``choose_replica``
        positionally. It lands in ``pending_request.args`` where the normal
        ingress puts the parsed request, so a body-aware policy scores replicas
        as on the normal path. When ``None``, nothing is forwarded. The router
        sees empty ``args`` and falls back to its default load-balanced pick.

        ``request_token_ids``, when present, is forwarded as a keyword arg so a
        KV-aware request router can score replicas on prompt-prefix overlap.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot``
        RPC and the rejection-retry loop: the real request goes out via
        HAProxy, so Serve's capacity semaphore isn't load-bearing here, and
        the extra RPC + retry introduced burstiness compared to the prior
        local round-robin implementation.
        """
        route_args = (routing_payload,) if routing_payload is not None else ()
        choose_replica_kwargs = {"_reserve": False}
        if request_token_ids is not None:
            choose_replica_kwargs[REQUEST_TOKEN_IDS_KWARG] = request_token_ids
        async with handle.choose_replica(
            *route_args, **choose_replica_kwargs
        ) as selection:
            replica = selection._replica
            endpoint = replica.backend_http_endpoint
            if endpoint is None:
                raise RuntimeError(
                    f"replica {selection.replica_id} has no backend HTTP endpoint"
                )
            host, port = endpoint
            return host, port, replica.replica_id.to_full_id_str()
