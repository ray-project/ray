"""LLMRouter: the dedicated ingress request router deployment.

When ingress bypass is enabled, HAProxy calls /internal/route on this
deployment to get a replica ID, then forwards traffic directly to the
matching LLMServer replica's backend HTTP port. This deployment is
distinct from Serve's per-deployment request router.
"""

import asyncio
from typing import Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ray import serve
from ray._common.utils import get_or_create_event_loop
from ray.llm._internal.common.utils.lora_utils import get_base_model_id
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

ingress_request_router_app = FastAPI()


@serve.ingress(ingress_request_router_app)
class LLMRouter:
    """Lightweight ingress request router deployment for ingress bypass."""

    def __init__(
        self,
        llm_deployments=None,
        llm_deployment_names=None,
        llm_configs_pre=None,
    ):
        self._default_serve_handles: Dict[str, DeploymentHandle] = {}
        self._llm_configs: Dict[str, LLMConfig] = {}
        self._rr_counter = 0

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
                    break
                except Exception:
                    await asyncio.sleep(0.1)
            else:
                raise RuntimeError(
                    f"LLMServer deployment {name} did not register in time"
                )
            self._default_serve_handles[cfg.model_id] = handle
        self._init_completed.set()

    @ingress_request_router_app.post("/internal/route")
    async def route(self, request: Request):
        try:
            parsed = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid json"}, status_code=400)

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
            return JSONResponse({"error": "no model"}, status_code=404)

        try:
            replica_id = await self._pick_replica(model_id)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=503)

        return JSONResponse({"replica_id": replica_id})

    async def _setup(self, llm_deployments: List[DeploymentHandle]):
        for handle in llm_deployments:
            llm_config = await handle.llm_config.remote()
            self._default_serve_handles[llm_config.model_id] = handle
            self._llm_configs[llm_config.model_id] = llm_config
        self._init_completed.set()

    async def check_health(self):
        await self._init_completed.wait()

    @ingress_request_router_app.get("/")
    @ingress_request_router_app.get("/health")
    async def health(self):
        if not self._init_completed.is_set():
            return JSONResponse({"status": "initializing"}, status_code=503)
        return JSONResponse({"status": "ok"})

    async def _pick_replica(self, model_id: str) -> str:
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

        replica_tiers = await request_router.choose_replicas(
            candidate_replicas=backend_http_replicas,
            pending_request=None,
        )
        for tier in replica_tiers:
            for replica in tier:
                if replica.backend_http_endpoint is not None:
                    return replica.replica_id.to_full_id_str()

        idx = self._rr_counter % len(backend_http_replicas)
        self._rr_counter += 1
        best = backend_http_replicas[idx]
        return best.replica_id.to_full_id_str()
