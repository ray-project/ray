from typing import Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.common.utils.lora_utils import get_base_model_id
from ray.llm._internal.serve.utils.server_utils import extract_model_id_from_body
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

    Multiplex (LoRA) affinity:
        When the deployment has a LoRA config (``multiplex_enabled``), this
        handler reads the requested ``model`` from the forwarded body and, if
        it names a LoRA adapter (``base:adapter``), applies
        ``handle.options(multiplexed_model_id=...)`` before ``choose_replica``
        so multiplex-aware policies (e.g. ``PowerOfTwoChoicesRouter``) steer to
        a replica that already has the adapter loaded. Requires the body to be
        forwarded (``RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1``); without
        it the model id is unavailable and routing falls back to load balancing.

    Responses:
        200 ``{"host": str, "port": int, "replica_id": str}``: pick
            succeeded.
        4xx/5xx FastAPI ``{"detail": str}``: informational only; HAProxy
            treats any non-200 as a routing failure.

    Health:
        ``GET /health`` is exposed as a human-operator convenience.
        Serve uses ``check_health()`` for replica readiness, not HTTP.
    """

    async def __init__(self, server: DeploymentHandle, multiplex_enabled: bool = False):
        self._handle: DeploymentHandle = server
        self._handle._init()
        # Only parse the body for a multiplexed model id when the deployment
        # actually has a LoRA config; otherwise the requested model is always
        # the base model and there is nothing to pin on.
        self._multiplex_enabled = multiplex_enabled

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
        options = {}
        if session_id:
            options["session_id"] = session_id
        multiplexed_model_id = self._multiplexed_model_id(body)
        if multiplexed_model_id:
            options["multiplexed_model_id"] = multiplexed_model_id
        handle = self._handle.options(**options) if options else self._handle
        try:
            host, port, replica_id = await self._pick_replica(
                handle=handle,
                request_body=body,
                body_truncated=body_truncated,
            )
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))
        return {"host": host, "port": port, "replica_id": replica_id}

    def _multiplexed_model_id(self, body: bytes) -> Optional[str]:
        """Return the requested LoRA adapter id to pin on, or None.

        None means "no multiplex hint" (base model, multiplex disabled, or body
        unavailable), in which case routing falls back to load balancing. A
        base-model request must not set ``multiplexed_model_id``: doing so would
        pin every base request to one replica instead of balancing across all
        of them (the same reasoning as ``OpenAiIngress`` in ingress.py).
        """
        if not self._multiplex_enabled:
            return None
        model_id = extract_model_id_from_body(body)
        if model_id and get_base_model_id(model_id) != model_id:
            return model_id
        return None

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    async def _pick_replica(
        self,
        handle: DeploymentHandle,
        request_body: Optional[bytes] = None,
        body_truncated: bool = False,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``handle`` is the LLMServer deployment handle, optionally configured by
        the caller with ``.options(session_id=...)`` and/or
        ``.options(multiplexed_model_id=...)`` so session-aware and
        multiplex-aware routers see those fields on ``RequestMetadata``.

        ``request_body`` (possibly a HAProxy-truncated prefix, indicated by
        ``body_truncated``) is forwarded to ``choose_replica`` so a future
        body-aware policy can score replicas against the request's prompt /
        messages without changing the /internal/route contract or the call
        site.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot``
        actor RPC and the rejection-retry loop: the real request goes out via
        HAProxy, so Serve's capacity semaphore isn't load-bearing here, and
        the extra RPC + retry introduced burstiness compared to the prior
        local round-robin implementation.
        """
        async with handle.choose_replica(
            request_body=request_body,
            body_truncated=body_truncated,
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
