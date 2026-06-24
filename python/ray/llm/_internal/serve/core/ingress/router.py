import json
from typing import Any, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

_BODY_TRUNCATED_HEADER = "x-body-truncated"

router_app = FastAPI()


class _RoutingPayload:
    """Routing key for the direct-streaming path.

    Body-aware routers (e.g. ``PrefixCacheAffinityRouter``) read ``.messages``
    / ``.prompt`` off the first positional routing arg, which on the normal
    OpenAI ingress is the parsed ``ChatCompletionRequest`` /
    ``CompletionRequest``. Direct streaming forwards only the raw body, so this
    wraps the leniently parsed body in the same attribute shape and no router
    special-casing is needed. Only the field present in the body is set, so
    ``hasattr`` tells a chat request from a completion request.
    """

    __slots__ = ("messages", "prompt")

    def __init__(self, *, messages: Any = None, prompt: Any = None):
        if messages is not None:
            self.messages = messages
        if prompt is not None:
            self.prompt = prompt


def _parse_routing_payload(body: bytes) -> Optional[_RoutingPayload]:
    """Leniently derive a routing key from a (possibly truncated) request body.

    A cheap ``json.loads`` into the ``messages`` / ``prompt`` routing key, not
    a full request parse. Direct streaming skips ingress-side Pydantic
    validation for throughput and HAProxy may forward only a body prefix.
    Returns ``None`` when no usable key can be derived (empty, non-object,
    unparseable, or no non-empty ``messages`` / ``prompt``) so the caller
    degrades to the default load-balanced pick rather than failing.
    """
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    messages = data.get("messages")
    prompt = data.get("prompt")
    # Empty messages/prompt carry no routing signal and would normalize to an
    # empty string downstream, so treat them as no key.
    if not messages and not prompt:
        return None
    return _RoutingPayload(messages=messages, prompt=prompt)


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
        Body: the target ChatCompletions / Completions request payload.
            Leniently parsed into a routing key (an object exposing
            ``messages`` / ``prompt``, see ``_RoutingPayload``) and passed to
            ``choose_replica`` positionally, the same shape the normal OpenAI
            ingress forwards, so body-aware policies score replicas identically
            on both paths.

    Truncated bodies:
        HAProxy may forward only a prefix of the request body for routing,
        flagged by the ``x-body-truncated`` header. A truncated prefix is
        usually invalid JSON, so no routing key is derived and the request
        falls back to the default load-balanced pick rather than failing.

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
            treats any non-200 as a routing failure.

    Health:
        ``GET /health`` is exposed as a human-operator convenience.
        Serve uses ``check_health()`` for replica readiness, not HTTP.
    """

    # Rate-limit the "no routing key" notice to once per replica so a
    # body-aware router silently degrading to load-balancing is surfaced
    # without flooding the log on every request. Class-level default so the
    # guard is safe even before __init__ runs.
    _warned_no_routing_key: bool = False

    async def __init__(self, server: DeploymentHandle):
        self._handle: DeploymentHandle = server
        self._handle._init()

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        routing_payload = _parse_routing_payload(body)
        if routing_payload is None and not self._warned_no_routing_key:
            self._warned_no_routing_key = True
            logger.warning(
                "Could not derive a routing key (messages/prompt) from the "
                "request body (body_truncated=%s). Falling back to "
                "load-balanced replica selection. A configured body-aware "
                "router (e.g. PrefixCacheAffinityRouter) cannot take effect "
                "for these requests. For truncated bodies, raise HAProxy's "
                "routing body limit.",
                body_truncated,
            )
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
            )
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))
        return {"host": host, "port": port, "replica_id": replica_id}

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    async def _pick_replica(
        self,
        handle: DeploymentHandle,
        routing_payload: Optional[_RoutingPayload] = None,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``handle`` is the LLMServer deployment handle, optionally configured
        with ``.options(session_id=...)`` so session-aware routers see the
        session id on ``RequestMetadata``.

        ``routing_payload``, when present, is passed to ``choose_replica``
        positionally so it lands in ``pending_request.args`` where the normal
        ingress puts the parsed request, letting a body-aware policy score
        replicas with no router-side special-casing. When ``None``, nothing is
        forwarded and the router sees empty ``args``, degrading to its default
        load-balanced pick rather than failing.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot`` RPC
        and the rejection-retry loop. The real request goes out via HAProxy, so
        Serve's capacity semaphore is not load-bearing here, and the extra RPC
        plus retry added burstiness versus the prior local round-robin.
        """
        route_args = (routing_payload,) if routing_payload is not None else ()
        async with handle.choose_replica(
            *route_args,
            _reserve=False,
        ) as selection:
            replica = selection._replica
            endpoint = replica.backend_http_endpoint
            if endpoint is None:
                raise RuntimeError(
                    f"replica {selection.replica_id} has no backend HTTP endpoint"
                )
            host, port = endpoint
            return host, port, replica.replica_id.to_full_id_str()
