import json
from types import SimpleNamespace
from typing import List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.serve.core.ingress.tokenizer import (
    REQUEST_TOKEN_IDS_KWARG,
    TokenizeError,
    Tokenizer,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

_BODY_TRUNCATED_HEADER = "x-body-truncated"

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
        self, server: DeploymentHandle, pre_routing_tokenization: bool = False
    ):
        self._handle: DeploymentHandle = server
        self._handle._init()
        # Pre-routing tokenization is only useful to a KV-aware request router,
        # which scores replicas based on the prompt token IDs.
        self._tokenizer = Tokenizer(self._handle) if pre_routing_tokenization else None

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
        return {"host": host, "port": port, "replica_id": replica_id}

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

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
        actor RPC and the rejection-retry loop: the real request goes out via
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
