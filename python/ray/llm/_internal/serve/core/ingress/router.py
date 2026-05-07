import random
from typing import FrozenSet, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.serve._private.common import ReplicaID
from ray.serve.handle import DeploymentHandle

_BODY_TRUNCATED_HEADER = "x-body-truncated"

_ReplicaCacheSignature = FrozenSet[ReplicaID]

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
        200 ``{"host": str, "port": int, "replica_id": str}``: pick
            succeeded.
        4xx/5xx FastAPI ``{"detail": str}``: informational only; HAProxy
            treats any non-200 as a routing failure.

    Health:
        ``GET /health`` is exposed as a human-operator convenience.
        Serve uses ``check_health()`` for replica readiness, not HTTP.
    """

    async def __init__(self, server: DeploymentHandle):
        # Randomized so multiple LLMRouter replicas don't lockstep on the
        # same replica sequence.
        self._round_robin_counter = random.randrange(2**31)
        self._cached_dict_id: Optional[int] = None
        self._cached_replica_signature: Optional[_ReplicaCacheSignature] = None
        self._cached_endpoints: List[Tuple[str, int, str]] = []
        self._handle: DeploymentHandle = server

        # Force the handle's local router and request router to construct
        # synchronously so /internal/route can read them in the hot path.
        # `curr_replicas` is populated separately by controller broadcast;
        # /internal/route returns 503 (HAProxy retries) until then, which
        # decouples router liveness from LLMServer cold start.
        self._handle._init()
        self._request_router = self._handle._get_request_router()
        if self._request_router is None:
            raise RuntimeError(
                "DeploymentHandle._get_request_router() returned None after "
                "_init(); Serve internals may have changed."
            )

    async def check_health(self):
        if self._handle._get_request_router() is None:
            raise RuntimeError("request router not initialized")

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        try:
            host, port, replica_id = self._pick_replica(
                request_body=body, body_truncated=body_truncated
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        return {"host": host, "port": port, "replica_id": replica_id}

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    def _ready_endpoints(self) -> List[Tuple[str, int, str]]:
        """Backend (host, port, full_id) tuples, cached on replica-set change."""
        curr_replicas = self._request_router.curr_replicas
        # RequestRouter swaps the dict wholesale on every controller broadcast,
        # so dict identity is a cheap "did anything change" check; the keyset
        # check then filters out broadcasts that didn't actually change the
        # replica set.
        if id(curr_replicas) == self._cached_dict_id:
            return self._cached_endpoints
        signature = frozenset(curr_replicas.keys())
        if signature != self._cached_replica_signature:
            self._cached_replica_signature = signature
            ready = sorted(
                (r for r in curr_replicas.values() if r.backend_http_endpoint),
                key=lambda r: r.replica_id.unique_id,
            )
            self._cached_endpoints = [
                (*r.backend_http_endpoint, r.replica_id.to_full_id_str()) for r in ready
            ]
        self._cached_dict_id = id(curr_replicas)
        return self._cached_endpoints

    def _pick_replica(
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
        candidates = self._ready_endpoints()
        if not candidates:
            raise RuntimeError("no backend-http replicas")

        index = self._round_robin_counter % len(candidates)
        self._round_robin_counter += 1
        return candidates[index]
