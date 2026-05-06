"""LLMRouter: the dedicated ingress request router deployment.

When ingress bypass is enabled, HAProxy calls /internal/route on this
deployment to get a backend HTTP host/port, then forwards traffic directly
to the matching LLMServer replica's backend HTTP port. This deployment is
distinct from Serve's per-deployment request router.
"""

import asyncio
import re
from typing import Dict, List, Optional, Tuple

import orjson
from fastapi import FastAPI

from ray import serve
from ray._common.utils import get_or_create_event_loop
from ray.llm._internal.common.utils.lora_utils import get_base_model_id
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.serve.handle import DeploymentHandle

# Placeholder app used only to make Serve wrap this class as ASGI. The actual
# hot-path ASGI app is late-bound per replica by __serve_build_asgi_app__.
ingress_request_router_app = FastAPI()

_BODY_TRUNCATED_HEADER = b"x-body-truncated"


@serve.ingress(ingress_request_router_app)
class LLMRouter:
    """Lightweight ingress request router deployment for ingress bypass."""

    def __init__(
        self,
        llm_deployment_names: List[str],
        llm_configs_pre: List[LLMConfig],
    ):
        self._default_serve_handles: Dict[str, DeploymentHandle] = {}
        self._llm_configs: Dict[str, LLMConfig] = {}
        self._round_robin_counter = 0

        self._init_completed = asyncio.Event()
        get_or_create_event_loop().create_task(
            self._setup(llm_deployment_names, llm_configs_pre)
        )

    async def _setup(self, llm_deployment_names, llm_configs):
        from ray import serve as _serve

        # Pre-fill configs from build-time data so we can answer routing decisions
        # before the LLMServer replica is actually up.
        for cfg in llm_configs:
            self._llm_configs[cfg.model_id] = cfg

        # Wait until each named deployment is registered, then grab its handle.
        for name, cfg in zip(llm_deployment_names, llm_configs):
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

            await self._handle_route_endpoint(scope, receive, send)
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

    async def _read_body(self, receive) -> Optional[bytes]:
        body_chunks = []
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] == "http.disconnect":
                return None
            if message["type"] != "http.request":
                continue
            body_chunks.append(message.get("body", b""))
            more_body = message.get("more_body", False)
        return b"".join(body_chunks)

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

    def _has_header(self, scope, header_name: bytes) -> bool:
        for name, _ in scope.get("headers", []):
            if name.lower() == header_name:
                return True
        return False

    def _parse_route_body(self, body: bytes, body_truncated: bool) -> Optional[dict]:
        try:
            parsed = orjson.loads(body)
        except Exception:
            return {} if body_truncated else None

        if isinstance(parsed, dict):
            return parsed
        return {} if body_truncated else None

    @staticmethod
    def _extract_json_string_from_prefix(body: bytes, field: str) -> Optional[str]:
        pattern = (
            rb'"' + re.escape(field.encode("utf-8")) + rb'"\s*:\s*"((?:\\.|[^"\\])*)"'
        )
        match = re.search(pattern, body)
        if match is None:
            return None

        encoded = b'"' + match.group(1) + b'"'
        try:
            decoded = orjson.loads(encoded)
        except Exception:
            return match.group(1).decode("utf-8", errors="ignore")

        return decoded if isinstance(decoded, str) else None

    def _default_model_id(self) -> Optional[str]:
        return next(iter(self._llm_configs.keys())) if self._llm_configs else None

    def _resolve_model_id(self, parsed: dict, body: bytes) -> Optional[str]:
        default_model_id = self._default_model_id()
        model = parsed.get("model") or self._extract_json_string_from_prefix(
            body, "model"
        )
        model = model or default_model_id
        if model and model not in self._llm_configs:
            base = get_base_model_id(model)
            if base not in self._llm_configs:
                model = None
        return model or default_model_id

    async def _handle_route_endpoint(self, scope, receive, send):
        body = await self._read_body(receive)
        if body is None:
            return

        body_truncated = self._has_header(scope, _BODY_TRUNCATED_HEADER)
        parsed = self._parse_route_body(body, body_truncated)
        if parsed is None:
            await self._send_json(send, {"error": "invalid json"}, status_code=400)
            return

        model_id = self._resolve_model_id(parsed, body)
        if model_id is None:
            await self._send_json(send, {"error": "no model"}, status_code=404)
            return

        try:
            host, port, replica_id = await self._pick_replica(
                model_id,
                request_body=body,
                body_truncated=body_truncated,
            )
        except Exception as e:
            await self._send_json(send, {"error": str(e)}, status_code=503)
            return

        await self._send_json(
            send, {"host": host, "port": port, "replica_id": replica_id}
        )

    async def check_health(self):
        await self._init_completed.wait()

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

    def _choose_round_robin_replica(self, replicas):
        candidates = sorted(replicas, key=self._replica_unique_id)
        if not candidates:
            return None

        index = self._round_robin_counter % len(candidates)
        self._round_robin_counter += 1
        return candidates[index]

    async def _pick_replica(
        self,
        model_id: str,
        request_body: Optional[bytes] = None,
        body_truncated: bool = False,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica using simple round-robin routing.

        The request body prefix is accepted here so future prefix-cache-aware
        policies can route on the same HAProxy path without changing the
        /internal/route contract again. Round-robin ignores it.
        """
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

        replica = self._choose_round_robin_replica(backend_http_replicas)
        return self._replica_route_result(replica)
