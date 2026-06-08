from typing import List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.serve.core.ingress.tokenizer import (
    REQUEST_TOKEN_IDS_KWARG,
    TokenizeError,
    Tokenizer,
)
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

_BODY_TRUNCATED_HEADER = "x-body-truncated"

router_app = FastAPI()


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
            The body is plumbed through to ``choose_replica`` so future
            body-aware policies (e.g. prefix-cache-aware) can score replicas
            against ``messages`` / ``prompt``.

    Truncated bodies:
        HAProxy may forward only a prefix of the request body for routing.
        When it does, it must set the ``x-body-truncated`` header; both the
        body bytes and this signal are forwarded to ``choose_replica`` for
        future body-aware policies.

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
        request_token_ids = None
        if self._tokenizer is not None:
            try:
                request_token_ids = await self._tokenizer.tokenize(body, body_truncated)
            except TokenizeError as e:
                raise HTTPException(status_code=e.status_code, detail=e.message)
        try:
            host, port, replica_id = await self._pick_replica(
                handle=handle,
                request_body=body,
                body_truncated=body_truncated,
                request_token_ids=request_token_ids,
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
        request_body: Optional[bytes] = None,
        body_truncated: bool = False,
        request_token_ids: Optional[List[int]] = None,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``handle`` is the LLMServer deployment handle, optionally configured
        with ``.options(session_id=...)`` by the caller so session-aware
        routers see the session id on ``RequestMetadata``.

        ``request_body`` (possibly a HAProxy-truncated prefix, indicated by
        ``body_truncated``) and ``request_token_ids`` (the prompt token IDs) are
        forwarded to ``choose_replica`` so a KV-aware request router can score
        replicas without changing the /internal/route contract.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot``
        actor RPC and the rejection-retry loop: the real request goes out via
        HAProxy, so Serve's capacity semaphore isn't load-bearing here, and
        the extra RPC + retry introduced burstiness compared to the prior
        local round-robin implementation.
        """
        choose_replica_kwargs = {
            "request_body": request_body,
            "body_truncated": body_truncated,
            "_reserve": False,
        }
        if request_token_ids is not None:
            choose_replica_kwargs[REQUEST_TOKEN_IDS_KWARG] = request_token_ids
        async with handle.choose_replica(**choose_replica_kwargs) as selection:
            replica = selection._replica
            endpoint = replica.backend_http_endpoint
            if endpoint is None:
                raise RuntimeError(
                    f"replica {selection.replica_id} has no backend HTTP endpoint"
                )
            host, port = endpoint
            return host, port, replica.replica_id.to_full_id_str()
