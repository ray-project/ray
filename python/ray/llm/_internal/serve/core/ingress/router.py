import asyncio
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.serve._private.common import ReplicaID
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve.handle import DeploymentHandle

_BODY_TRUNCATED_HEADER = "x-body-truncated"

# Long enough to cover LLMServer cold start (model load + engine startup);
# short enough that a wedged replica fails the constructor and Serve retries.
_HANDLE_PRIMING_TIMEOUT_S = 60.0

# Cache key for `_ready_replicas`. ``id`` discriminates dict reassignment
# (Serve's ``update_replicas`` rebinds), ``len`` discriminates in-place pop.
_ReplicaCacheSignature = Tuple[int, int]

router_app = FastAPI()


@serve.ingress(router_app)
class LLMRouter:
    """Ingress request router for direct streaming.

    When direct streaming is enabled, HAProxy calls /internal/route on this
    deployment to get a data plane replica, then forwards traffic directly
    to the matching LLMServer replica's backend HTTP port.

    /internal/route HTTP contract
    -----------------------------
    Request:
        POST /internal/route
        Content-Type: application/json
        Body: the target ChatCompletions / Completions request payload.
            Today the router uses round-robin and ignores the body, but it
            is plumbed through so future routing policies (e.g. prefix
            cache aware) can score replicas against ``messages`` /
            ``prompt``. HAProxy should continue forwarding the payload
            (subject to truncation below).

    Truncated bodies:
        HAProxy may forward only a prefix of the request body for routing.
        When it does, it must set the ``x-body-truncated`` header. The
        router forwards both the body bytes and this signal to
        ``_pick_replica`` for future body-aware policies.

    Responses:
        200 ``{"host": str, "port": int, "replica_id": str}`` — pick
            succeeded.
        4xx/5xx FastAPI ``{"detail": str}`` — informational only; HAProxy
            treats any non-200 as a routing failure.

    Health:
        GET / and GET /health return ``{"status": "ok"}``. Serve does not
        accept HTTP traffic on this replica until ``__init__`` finishes,
        so reachability of these paths implies the router is fully
        initialized.
    """

    async def __init__(self, llm_deployment_name: str):
        self._round_robin_counter = 0
        self._cached_replica_signature: Optional[_ReplicaCacheSignature] = None
        self._cached_sorted_replicas: List[RunningReplica] = []

        # The /internal/route hot path reads the handle's local request
        # router directly, so we issue one llm_config call to force handle
        # initialization before serving traffic. Failure fails the replica
        # and Serve retries the constructor.
        self._handle: DeploymentHandle = serve.get_deployment_handle(
            llm_deployment_name
        )
        await asyncio.wait_for(
            self._handle.llm_config.remote(), timeout=_HANDLE_PRIMING_TIMEOUT_S
        )

    async def check_health(self):
        """Serve-native health hook. The replica is healthy if it's
        accepting calls; nothing further to verify here."""

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        try:
            host, port, replica_id = await self._pick_replica(
                request_body=body, body_truncated=body_truncated
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        return {"host": host, "port": port, "replica_id": replica_id}

    @router_app.get("/")
    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    @staticmethod
    def _replica_id_sort_key(replica: RunningReplica) -> str:
        return replica.replica_id.unique_id

    def _ready_replicas(self, request_router) -> List[RunningReplica]:
        """Return backend-HTTP-ready replicas, sorted stably by unique id.

        Caches the sorted list and invalidates only when the replica id set
        changes. Hot-path call (per /internal/route request).
        """
        curr_replicas: Dict[ReplicaID, RunningReplica] = request_router.curr_replicas
        signature = (id(curr_replicas), len(curr_replicas))
        if signature != self._cached_replica_signature:
            self._cached_replica_signature = signature
            self._cached_sorted_replicas = sorted(
                (
                    r
                    for r in curr_replicas.values()
                    if r.backend_http_endpoint is not None
                ),
                key=self._replica_id_sort_key,
            )
        return self._cached_sorted_replicas

    async def _pick_replica(
        self,
        request_body: Optional[bytes] = None,
        body_truncated: bool = False,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica.

        Today this is plain round-robin and ignores the payload. The
        ``request_body`` (possibly a HAProxy-truncated prefix, indicated by
        ``body_truncated``) is plumbed through so a future prefix cache aware
        policy can score replicas against the request's prompt / messages
        without changing the /internal/route contract or the call site.
        """
        del request_body, body_truncated
        request_router = self._handle._get_request_router()
        if request_router is None:
            raise RuntimeError("request router not initialized")

        candidates = self._ready_replicas(request_router)
        if not candidates:
            raise RuntimeError("no backend-http replicas")

        index = self._round_robin_counter % len(candidates)
        self._round_robin_counter += 1
        replica = candidates[index]
        host, port = replica.backend_http_endpoint
        return host, port, replica.replica_id.to_full_id_str()
