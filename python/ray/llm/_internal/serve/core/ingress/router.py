"""LLMRouter: the dedicated ingress request router deployment.

When ingress bypass is enabled, HAProxy calls /internal/route on this
deployment to get a backend HTTP host/port, then forwards traffic directly
to the matching LLMServer replica's backend HTTP port. This deployment is
distinct from Serve's per-deployment request router.
"""

import asyncio
import random
from typing import Dict, List, Tuple

import orjson
from fastapi import FastAPI

from ray import serve
from ray._common.utils import get_or_create_event_loop
from ray.llm._internal.common.utils.lora_utils import get_base_model_id
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

DIRECT_ROUTER_OPTIMISTIC_LOAD_ENV = "RAY_SERVE_LLM_DIRECT_ROUTER_OPTIMISTIC_LOAD"
DIRECT_ROUTER_POLL_INTERVAL_ENV = "RAY_SERVE_LLM_DIRECT_ROUTER_POLL_INTERVAL_S"
DIRECT_ROUTER_POLICY_ENV = "RAY_SERVE_LLM_DIRECT_ROUTER_POLICY"
DIRECT_ROUTER_POLICY_POW2 = "pow2"
DIRECT_ROUTER_POLICY_ROUND_ROBIN = "round_robin"
DEFAULT_DIRECT_ROUTER_POLICY = DIRECT_ROUTER_POLICY_POW2
DEFAULT_DIRECT_ROUTER_POLL_INTERVAL_S = 0.05
_ROUND_ROBIN_COUNTER_MAX_START = 2**31

# Placeholder app used only to make Serve wrap this class as ASGI. The actual
# hot-path ASGI app is late-bound per replica by __serve_build_asgi_app__.
ingress_request_router_app = FastAPI()


@serve.ingress(ingress_request_router_app)
class LLMRouter:
    """Lightweight ingress request router deployment for ingress bypass."""

    def __init__(
        self,
        llm_deployments=None,
        llm_deployment_names=None,
        llm_configs_pre=None,
        optimistic_load: bool = False,
        poll_interval_s: float = DEFAULT_DIRECT_ROUTER_POLL_INTERVAL_S,
        routing_policy: str = DEFAULT_DIRECT_ROUTER_POLICY,
    ):
        self._default_serve_handles: Dict[str, DeploymentHandle] = {}
        self._llm_configs: Dict[str, LLMConfig] = {}
        self._di_load_cache = {}
        self._di_poller_task = None
        self._di_optimistic_load = optimistic_load
        self._di_poll_interval_s = poll_interval_s
        self._di_routing_policy = self._normalize_routing_policy(routing_policy)
        self._di_round_robin_counter = random.randint(0, _ROUND_ROBIN_COUNTER_MAX_START)

        self._init_completed = asyncio.Event()
        if llm_deployment_names is not None:
            # Late-bind path: resolve handles by name when the deployment is ready.
            get_or_create_event_loop().create_task(
                self._setup_by_names(llm_deployment_names, llm_configs_pre or [])
            )
        else:
            get_or_create_event_loop().create_task(self._setup(llm_deployments or []))

    async def _setup_by_names(self, names, configs_pre):
        from ray import serve as _serve

        # Pre-fill configs from build-time data so we can answer routing decisions
        # before the LLMServer replica is actually up.
        for cfg in configs_pre:
            self._llm_configs[cfg.model_id] = cfg
        # Wait until each named deployment is registered, then grab its handle.
        for name, cfg in zip(names, configs_pre):
            for _ in range(600):  # up to ~60s
                try:
                    handle = _serve.get_deployment_handle(name)
                    # Initialize the handle's local request router. The
                    # routing hot path reads it directly instead of issuing a
                    # Serve request.
                    await handle.llm_config.remote()
                    break
                except Exception:
                    await asyncio.sleep(0.1)
            else:
                raise RuntimeError(
                    f"LLMServer deployment {name} did not register in time"
                )
            self._default_serve_handles[cfg.model_id] = handle
        self._init_completed.set()

    async def __serve_build_asgi_app__(self):
        async def app(scope, receive, send):
            await self._handle_asgi_request(scope, receive, send)

        return app

    async def _handle_asgi_request(self, scope, receive, send):
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
            return

        if scope["type"] != "http":
            await self._send_json(send, {"error": "not found"}, status_code=404)
            return

        path = scope.get("path", "")
        if path == "/internal/route":
            if scope.get("method") != "POST":
                await self._send_json(
                    send, {"error": "method not allowed"}, status_code=405
                )
                return

            await self._handle_route_endpoint(receive, send)
            return

        if path in {"/", "/health"}:
            if not self._init_completed.is_set():
                await self._send_json(send, {"status": "initializing"}, status_code=503)
                return

            await self._send_json(send, {"status": "ok"})
            return

        await self._send_json(send, {"error": "not found"}, status_code=404)

    async def _handle_lifespan(self, receive, send):
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _read_body(self, receive) -> bytes:
        body = b""
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] != "http.request":
                continue
            body += message.get("body", b"")
            more_body = message.get("more_body", False)
        return body

    async def _send_json(self, send, payload, *, status_code: int = 200):
        body = orjson.dumps(payload)
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode("ascii")],
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def _handle_route_endpoint(self, receive, send):
        try:
            parsed = orjson.loads(await self._read_body(receive))
        except Exception:
            await self._send_json(send, {"error": "invalid json"}, status_code=400)
            return

        if not isinstance(parsed, dict):
            await self._send_json(send, {"error": "invalid json"}, status_code=400)
            return

        default_model_id = (
            next(iter(self._llm_configs.keys())) if self._llm_configs else None
        )
        model = parsed.get("model", default_model_id)
        if model and model not in self._llm_configs:
            base = get_base_model_id(model)
            if base not in self._llm_configs:
                model = None
        model_id = model or default_model_id

        if model_id is None:
            await self._send_json(send, {"error": "no model"}, status_code=404)
            return

        try:
            host, port, replica_id = await self._pick_replica(model_id)
        except Exception as e:
            await self._send_json(send, {"error": str(e)}, status_code=503)
            return

        response = {"host": host, "port": port, "replica_id": replica_id}
        await self._send_json(send, response)

    async def _setup(self, llm_deployments: List[DeploymentHandle]):
        for handle in llm_deployments:
            llm_config = await handle.llm_config.remote()
            self._default_serve_handles[llm_config.model_id] = handle
            self._llm_configs[llm_config.model_id] = llm_config
        self._init_completed.set()

    async def check_health(self):
        await self._init_completed.wait()

    async def _start_load_poller(self, replicas):
        """Background task: poll all replica queue lengths periodically."""
        while True:
            for replica in replicas:
                try:
                    load = await replica.get_queue_len(deadline_s=0.5)
                    self._set_replica_load(replica, load)
                except Exception:
                    pass
            await asyncio.sleep(self._di_poll_interval_s)

    def _get_replica_load(self, replica) -> int:
        return self._di_load_cache.get(replica.replica_id, 0)

    def _set_replica_load(self, replica, load: int):
        self._di_load_cache[replica.replica_id] = load

    def _record_replica_pick(self, replica):
        if self._di_optimistic_load:
            self._set_replica_load(replica, self._get_replica_load(replica) + 1)

    def _choose_best_loaded_replica(self, candidates):
        best = min(candidates, key=self._get_replica_load)
        self._record_replica_pick(best)
        return best

    def _replica_unique_id(self, replica) -> str:
        replica_id = replica.replica_id
        return getattr(replica_id, "unique_id", None) or str(replica_id)

    def _replica_full_id(self, replica) -> str:
        replica_id = replica.replica_id
        to_full_id_str = getattr(replica_id, "to_full_id_str", None)
        if to_full_id_str is not None:
            return to_full_id_str()
        return str(replica_id)

    def _replica_route_result(self, replica) -> Tuple[str, int, str]:
        host, port = replica.backend_http_endpoint
        return host, port, self._replica_full_id(replica)

    @staticmethod
    def _normalize_routing_policy(routing_policy: str) -> str:
        normalized = (routing_policy or DEFAULT_DIRECT_ROUTER_POLICY).lower()
        normalized = normalized.replace("-", "_")
        if normalized in {"pow2", "power_of_two", "power_of_two_choices"}:
            return DIRECT_ROUTER_POLICY_POW2
        if normalized in {"round_robin", "rr"}:
            return DIRECT_ROUTER_POLICY_ROUND_ROBIN

        logger.warning(
            "Unknown direct router policy %r; falling back to %s.",
            routing_policy,
            DEFAULT_DIRECT_ROUTER_POLICY,
        )
        return DEFAULT_DIRECT_ROUTER_POLICY

    def _choose_round_robin_candidates(self, replicas):
        candidates = sorted(replicas, key=self._replica_unique_id)
        if not candidates:
            return []

        index = self._di_round_robin_counter % len(candidates)
        self._di_round_robin_counter += 1
        if len(candidates) == 1:
            return [candidates[index]]

        return [candidates[index], candidates[(index + 1) % len(candidates)]]

    async def _choose_replica_candidates(
        self, request_router, backend_http_replicas, model_id: str
    ):
        if self._di_routing_policy == DIRECT_ROUTER_POLICY_ROUND_ROBIN:
            return self._choose_round_robin_candidates(backend_http_replicas)

        replica_tiers = await request_router.choose_replicas(
            candidate_replicas=backend_http_replicas,
            pending_request=None,
        )
        if not replica_tiers or not replica_tiers[0]:
            raise RuntimeError(f"P2C returned no candidates for {model_id}")

        candidates = [
            replica
            for replica in replica_tiers[0]
            if replica.backend_http_endpoint is not None
        ]
        if not candidates:
            raise RuntimeError("P2C candidates have no backend HTTP endpoint")
        return candidates

    async def _pick_replica(self, model_id: str) -> Tuple[str, int, str]:
        """Pick a replica using the configured direct routing policy."""
        base_model_id = get_base_model_id(model_id)
        handle = self._default_serve_handles.get(base_model_id)
        if handle is None:
            raise RuntimeError(f"No handle for model {model_id}")

        request_router = handle._get_request_router()
        if request_router is None:
            raise RuntimeError(f"Request router not initialized for {model_id}")

        backend_http_replicas = [
            replica
            for replica in request_router.curr_replicas.values()
            if replica.backend_http_endpoint is not None
        ]
        if not backend_http_replicas:
            raise RuntimeError(f"No backend-http-enabled replicas for {model_id}")

        # Start the background poller on first routing decision. The fast path
        # below reads from the cache only, avoiding per-request queue RPCs.
        if self._di_poller_task is None or self._di_poller_task.done():
            self._di_poller_task = asyncio.get_running_loop().create_task(
                self._start_load_poller(backend_http_replicas)
            )

        candidates = await self._choose_replica_candidates(
            request_router, backend_http_replicas, model_id
        )
        best = self._choose_best_loaded_replica(candidates)
        return self._replica_route_result(best)
