from typing import Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
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

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        try:
            host, port, replica_id = await self._pick_replica(
                request_body=body, body_truncated=body_truncated
            )
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))
        return {"host": host, "port": port, "replica_id": replica_id}

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    async def _pick_replica(
        self,
        request_body: Optional[bytes] = None,
        body_truncated: bool = False,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``request_body`` (possibly a HAProxy-truncated prefix, indicated by
        ``body_truncated``) is forwarded to ``choose_replica`` so a future
        body-aware policy can score replicas against the request's prompt /
        messages without changing the /internal/route contract or the call
        site.
        """
        async with self._handle.choose_replica(
            request_body=request_body, body_truncated=body_truncated
        ) as selection:
            replica = selection._replica
            endpoint = replica.backend_http_endpoint
            if endpoint is None:
                raise RuntimeError(
                    f"replica {selection.replica_id} has no backend HTTP endpoint"
                )
            host, port = endpoint
            return host, port, replica.replica_id.to_full_id_str()
