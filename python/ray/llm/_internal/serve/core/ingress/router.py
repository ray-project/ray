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
    """A minimal routing key for the direct-streaming path.

    Body-aware request routers (e.g. ``PrefixCacheAffinityRouter``) score
    replicas by reading ``.messages`` / ``.prompt`` off the first positional
    routing argument -- the parsed ``ChatCompletionRequest`` /
    ``CompletionRequest`` that the normal OpenAI ingress forwards. On the
    direct-streaming path the native engine ASGI app is the ingress, so the
    Serve router only ever sees the raw HTTP body. This wraps the leniently
    parsed body in an object exposing the *same* attributes, so a single
    router contract holds on both paths and body-aware routers need no
    special-casing.

    Only the attribute actually present in the body is set, so ``hasattr``
    distinguishes a chat request from a completion request the same way a
    parsed request object would (a ``prompt``-only body must not expose a
    ``messages`` attribute, and vice versa).
    """

    __slots__ = ("messages", "prompt")

    def __init__(self, *, messages: Optional[Any] = None, prompt: Optional[Any] = None):
        if messages is not None:
            self.messages = messages
        if prompt is not None:
            self.prompt = prompt


def _parse_routing_payload(body: bytes) -> Optional[_RoutingPayload]:
    """Leniently derive a routing key from a (possibly truncated) request body.

    Direct streaming exists to skip ingress-side Pydantic validation for
    throughput, and HAProxy may forward only a prefix of the body. So this is
    deliberately a cheap ``json.loads`` into a lightweight routing key, not a
    full request parse: routing only needs ``messages`` / ``prompt``.

    Returns ``None`` when no routing key can be derived -- an unparseable body
    (e.g. a HAProxy-truncated prefix that isn't valid JSON), a non-object body,
    or one carrying neither ``messages`` nor ``prompt``. Callers fall back to
    the default load-balanced pick in that case rather than failing the
    request.
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
    if messages is None and prompt is None:
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
            The body is leniently parsed into a routing key (an object
            exposing ``messages`` / ``prompt``, see ``_RoutingPayload``) and
            passed to ``choose_replica`` as the first positional argument --
            the same shape the normal OpenAI ingress forwards -- so body-aware
            policies (e.g. prefix-cache-aware) score replicas identically on
            both paths.

    Truncated bodies:
        HAProxy may forward only a prefix of the request body for routing.
        When it does, it sets the ``x-body-truncated`` header. A truncated
        prefix usually isn't valid JSON, so no routing key can be derived; the
        request then falls back to the default load-balanced pick rather than
        failing.

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

    async def __init__(self, server: DeploymentHandle):
        self._handle: DeploymentHandle = server
        self._handle._init()
        # Rate-limit the "no routing key" notice to once per replica so a
        # body-aware router silently degrading to load-balancing is surfaced
        # without flooding the log on every request.
        self._warned_no_routing_key: bool = False

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        routing_payload = _parse_routing_payload(body)
        if routing_payload is None and not self._warned_no_routing_key:
            self._warned_no_routing_key = True
            logger.warning(
                "Could not derive a routing key from the request body "
                "(body_truncated=%s); falling back to load-balanced replica "
                "selection. If a body-aware request router (e.g. "
                "PrefixCacheAffinityRouter) is configured, it cannot take "
                "effect for these requests. This is expected for bodies "
                "truncated below the `messages`/`prompt` field; raise "
                "HAProxy's routing body limit if content-based routing is "
                "required.",
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
        with ``.options(session_id=...)`` by the caller so session-aware
        routers see the session id on ``RequestMetadata``.

        ``routing_payload``, when present, is passed to ``choose_replica`` as
        the first positional argument so it lands in ``pending_request.args``
        exactly where the normal ingress puts the parsed request -- letting a
        body-aware policy score replicas against ``messages`` / ``prompt``
        with no router-side special-casing. When ``None`` (truncated or
        unparseable body), nothing is forwarded and the configured router
        sees an empty ``args``, degrading to its default load-balanced pick
        rather than failing the request.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot``
        actor RPC and the rejection-retry loop: the real request goes out via
        HAProxy, so Serve's capacity semaphore isn't load-bearing here, and
        the extra RPC + retry introduced burstiness compared to the prior
        local round-robin implementation.
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
