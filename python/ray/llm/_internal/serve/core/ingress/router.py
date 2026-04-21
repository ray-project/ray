"""LLMRouter: the dedicated ingress request router deployment.

When ingress bypass is enabled, HAProxy calls /internal/route on this
deployment to get a replica ID, then forwards traffic directly to the
matching LLMServer replica's backend HTTP port. This deployment is
distinct from Serve's per-deployment request router.
"""

import asyncio
from typing import Dict, List

from fastapi import FastAPI
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

    def __init__(self, llm_deployments: List[DeploymentHandle]):
        self._default_serve_handles: Dict[str, DeploymentHandle] = {}
        self._llm_configs: Dict[str, LLMConfig] = {}
        self._rr_counter = 0

        self._init_completed = asyncio.Event()
        get_or_create_event_loop().create_task(self._setup(llm_deployments))
        get_or_create_event_loop().create_task(self._install_route_middleware())

    async def _install_route_middleware(self):
        while not hasattr(self, "_asgi_app"):
            await asyncio.sleep(0.01)

        original_app = self._asgi_app
        router_self = self

        async def route_middleware(scope, receive, send):
            if (
                scope["type"] == "http"
                and scope.get("method") == "POST"
                and scope.get("path") == "/internal/route"
            ):
                await router_self._handle_route_raw(scope, receive, send)
                return
            await original_app(scope, receive, send)

        self._asgi_app = route_middleware
        logger.info("LLMRouter route middleware installed")

    async def _handle_route_raw(self, scope, receive, send):
        import orjson

        body_parts = []
        while True:
            message = await receive()
            body_parts.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        body_bytes = b"".join(body_parts)

        try:
            parsed = orjson.loads(body_bytes)
        except Exception:
            await self._send_json(send, {"error": "invalid json"}, 400)
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
            await self._send_json(send, {"error": "no model"}, 404)
            return

        try:
            replica_id = await self._pick_replica(model_id)
        except Exception as e:
            await self._send_json(send, {"error": str(e)}, 503)
            return

        await self._send_json(send, {"replica_id": replica_id}, 200)

    async def _send_json(self, send, data, status):
        import orjson

        body = orjson.dumps(data)
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode()],
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

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
