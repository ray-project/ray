import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import ray
from ray import serve
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.pow_2_router import (
    PowerOfTwoChoicesRequestRouter,
)
from ray.serve._private.request_router.replica_wrapper import RunningReplica

logger = logging.getLogger(__name__)


_DYNAMO_ROUTE_TOKEN_HEADER = "x-ray-serve-dynamo-route-token"
DYNAMO_KV_ROUTER_DEPLOYMENT_ACTOR_NAME = "dynamo_kv_router"


def _maybe_remote_call(actor: Any, method_name: str, *args, **kwargs):
    method = getattr(actor, method_name)
    remote = getattr(method, "remote", None)
    if remote is not None:
        return remote(*args, **kwargs)
    return method(*args, **kwargs)


async def _resolve_maybe_ref(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _get_replica_worker_id(replica: RunningReplica) -> Optional[int]:
    worker_id = getattr(replica, "dynamo_worker_id", None)
    if worker_id is not None:
        return int(worker_id)

    routing_stats = getattr(replica, "routing_stats", {}) or {}
    worker_id = routing_stats.get("dynamo_worker_id")
    if worker_id is not None:
        return int(worker_id)

    rank = getattr(replica, "rank", None)
    if rank is not None:
        return int(rank)
    return None


class DynamoKVRouterActorImpl:
    """Deployment-scoped Dynamo KV scorer and lifecycle owner."""

    def __init__(self, config: Dict[str, Any]):
        self._config = dict(config)
        self._prepared_route_ttl_s = float(
            self._config.get("prepared_route_ttl_s", 30.0)
        )
        self._prepared: Dict[str, Dict[str, Any]] = {}
        self._active: Dict[str, Dict[str, Any]] = {}
        self._prefill_marked = set()
        self._kv_router = self._build_kv_router()
        self._tokenizer = None

    def _build_kv_router(self):
        endpoint_path = self._config.get("endpoint")
        if not endpoint_path:
            logger.warning("Dynamo KV router endpoint is not configured.")
            return None

        try:
            from dynamo._core import DistributedRuntime, KvRouter, KvRouterConfig
        except ImportError:
            logger.warning("Dynamo Python bindings are not importable.")
            return None

        try:
            runtime = DistributedRuntime.detached()
        except Exception:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            runtime = DistributedRuntime(
                loop,
                self._config.get("discovery_backend", "mem"),
                self._config.get("request_plane", "tcp"),
            )

        router_config = KvRouterConfig(**self._config.get("kv_router_config", {}))
        return KvRouter(
            runtime.endpoint(endpoint_path),
            int(self._config.get("block_size", 16)),
            router_config,
        )

    def _get_tokenizer(self):
        if self._tokenizer is not None:
            return self._tokenizer

        from transformers import AutoTokenizer

        tokenizer_source = (
            self._config.get("tokenizer_source")
            or self._config.get("model_source")
            or self._config.get("model_id")
        )
        if not tokenizer_source:
            raise ValueError("No tokenizer source configured for Dynamo KV routing.")

        self._tokenizer = AutoTokenizer.from_pretrained(
            tokenizer_source,
            trust_remote_code=bool(self._config.get("trust_remote_code", False)),
        )
        return self._tokenizer

    def _tokenize_request(self, request_body: bytes) -> tuple[List[int], Optional[int]]:
        body = json.loads(request_body.decode("utf-8") if request_body else "{}")
        if "prompt_token_ids" in body and isinstance(body["prompt_token_ids"], list):
            return [int(token) for token in body["prompt_token_ids"]], body.get(
                "max_tokens"
            )

        tokenizer = self._get_tokenizer()
        if isinstance(body.get("prompt"), str):
            return tokenizer.encode(
                body["prompt"],
                add_special_tokens=bool(body.get("add_special_tokens", False)),
            ), body.get("max_tokens")

        if isinstance(body.get("messages"), list):
            token_ids = tokenizer.apply_chat_template(
                body["messages"],
                tokenize=True,
                add_generation_prompt=bool(body.get("add_generation_prompt", True)),
            )
            return [int(token) for token in token_ids], body.get("max_tokens")

        raise ValueError("Unsupported Dynamo KV routing request shape.")

    def _prune_prepared_routes(self) -> None:
        now = time.monotonic()
        expired = [
            token
            for token, route in self._prepared.items()
            if now - route["created_at"] > self._prepared_route_ttl_s
        ]
        for token in expired:
            self._prepared.pop(token, None)

    async def rank_and_prepare_route(
        self,
        request_body: bytes,
        body_truncated: bool,
        candidate_worker_ids: List[int],
        internal_request_id: str,
    ) -> List[Dict[str, Any]]:
        if body_truncated or self._kv_router is None or not candidate_worker_ids:
            return []

        self._prune_prepared_routes()
        token_ids, expected_output_tokens = self._tokenize_request(request_body)
        rankings = await self._kv_router.rank_workers(
            token_ids,
            allowed_worker_ids=candidate_worker_ids,
        )

        prepared = []
        for rank in rankings:
            token = f"{internal_request_id}:{rank['worker_id']}:{uuid.uuid4().hex}"
            route = {
                "created_at": time.monotonic(),
                "token_ids": token_ids,
                "expected_output_tokens": expected_output_tokens,
                "worker_id": int(rank["worker_id"]),
                "dp_rank": int(rank.get("dp_rank", 0)),
                "overlap_blocks": int(rank.get("overlap_blocks", 0)),
            }
            self._prepared[token] = route
            prepared.append({**rank, "dynamo_route_token": token})
        return prepared

    async def start_direct_request(self, route_token: str) -> None:
        if self._kv_router is None:
            return
        route = self._prepared.pop(route_token, None)
        if route is None:
            return
        await self._kv_router.add_request(
            request_id=route_token,
            token_ids=route["token_ids"],
            worker_id=route["worker_id"],
            dp_rank=route["dp_rank"],
            overlap_blocks=route["overlap_blocks"],
            expected_output_tokens=route["expected_output_tokens"],
        )
        self._active[route_token] = route

    async def mark_prefill_complete(self, route_token: str) -> None:
        if self._kv_router is None or route_token in self._prefill_marked:
            return
        if route_token not in self._active:
            return
        self._prefill_marked.add(route_token)
        await self._kv_router.mark_prefill_complete(route_token)

    async def finish_direct_request(self, route_token: str) -> None:
        self._prepared.pop(route_token, None)
        self._prefill_marked.discard(route_token)
        if self._active.pop(route_token, None) is None or self._kv_router is None:
            return
        await self._kv_router.free(route_token)


DynamoKVRouterActor = ray.remote(num_cpus=0)(DynamoKVRouterActorImpl)


class DynamoKVRequestRouter(PowerOfTwoChoicesRequestRouter):
    """Request router that asks Dynamo to rank Ray-feasible direct-stream replicas."""

    def initialize_state(self, **kwargs):
        self._selection_metadata: Dict[str, Dict[ReplicaID, Dict[str, Any]]] = {}
        self._actor = kwargs.get("actor")
        self._deployment_actor_name = kwargs.get(
            "deployment_actor_name", DYNAMO_KV_ROUTER_DEPLOYMENT_ACTOR_NAME
        )
        self._deployment_actor_full_name: Optional[str] = None

    def _try_discover_deployment_actor(self) -> bool:
        if self._actor is not None:
            return True

        if self._deployment_actor_full_name is not None:
            try:
                self._actor = ray.get_actor(
                    self._deployment_actor_full_name, namespace=SERVE_NAMESPACE
                )
                return True
            except Exception:
                self._deployment_actor_full_name = None

        prefix = (
            f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}"
            f"{self._deployment_id.app_name}::"
            f"{self._deployment_id.name}::"
        )
        suffix = f"::{self._deployment_actor_name}"

        try:
            actors = ray.util.list_named_actors(all_namespaces=True)
            for actor_info in actors:
                if actor_info["namespace"] != SERVE_NAMESPACE:
                    continue
                name = actor_info["name"]
                if name.startswith(prefix) and name.endswith(suffix):
                    self._actor = ray.get_actor(name, namespace=SERVE_NAMESPACE)
                    self._deployment_actor_full_name = name
                    return True
        except Exception:
            logger.exception(
                "Failed to discover Dynamo KV deployment actor "
                f"{self._deployment_actor_name!r}."
            )
        return False

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        if pending_request is None:
            return await super().choose_replicas(candidate_replicas, pending_request)

        request_body = pending_request.kwargs.get("request_body")
        body_truncated = bool(pending_request.kwargs.get("body_truncated", False))
        if not isinstance(request_body, (bytes, bytearray)) or body_truncated:
            return await super().choose_replicas(candidate_replicas, pending_request)

        worker_to_replica: Dict[int, RunningReplica] = {}
        for replica in candidate_replicas:
            worker_id = _get_replica_worker_id(replica)
            if worker_id is not None:
                worker_to_replica[worker_id] = replica

        if not worker_to_replica:
            return await super().choose_replicas(candidate_replicas, pending_request)

        try:
            if not self._try_discover_deployment_actor():
                return await super().choose_replicas(candidate_replicas, pending_request)
            rankings = await _resolve_maybe_ref(
                _maybe_remote_call(
                    self._actor,
                    "rank_and_prepare_route",
                    bytes(request_body),
                    body_truncated,
                    list(worker_to_replica),
                    pending_request.metadata.internal_request_id,
                )
            )
        except Exception:
            logger.exception("Dynamo KV ranking failed; falling back to default routing.")
            return await super().choose_replicas(candidate_replicas, pending_request)

        ranked_replicas = []
        per_replica_metadata = {}
        for rank in rankings:
            replica = worker_to_replica.get(int(rank["worker_id"]))
            if replica is None:
                continue
            ranked_replicas.append([replica])
            token = rank.get("dynamo_route_token")
            if token:
                per_replica_metadata[replica.replica_id] = {
                    "dynamo_route_token": token
                }

        if not ranked_replicas:
            return await super().choose_replicas(candidate_replicas, pending_request)

        self._selection_metadata[pending_request.metadata.internal_request_id] = (
            per_replica_metadata
        )
        return ranked_replicas

    def get_selection_metadata(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
    ) -> Dict[str, Any]:
        request_metadata = self._selection_metadata.get(
            pending_request.metadata.internal_request_id, {}
        )
        metadata = dict(request_metadata.pop(replica_id, {}))
        if not request_metadata:
            self._selection_metadata.pop(
                pending_request.metadata.internal_request_id, None
            )
        return metadata


class DynamoDirectStreamingLifecycleMiddleware:
    def __init__(self, app, actor_name: str, actor: Any = None):
        self._app = app
        self._actor_name = actor_name
        self._actor = actor

    def _get_actor(self):
        if self._actor is not None:
            return self._actor
        return serve.get_deployment_actor(self._actor_name)

    @staticmethod
    def _route_token_from_scope(scope: Dict[str, Any]) -> Optional[str]:
        for key, value in scope.get("headers", []):
            if key.decode("latin1").lower() == _DYNAMO_ROUTE_TOKEN_HEADER:
                return value.decode("latin1")
        return None

    async def _call_actor(self, method_name: str, route_token: str) -> None:
        await _resolve_maybe_ref(
            _maybe_remote_call(self._get_actor(), method_name, route_token)
        )

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        route_token = self._route_token_from_scope(scope)
        if not route_token:
            await self._app(scope, receive, send)
            return

        started = False
        finished = False
        prefill_marked = False

        async def finish_once():
            nonlocal finished
            if started and not finished:
                finished = True
                await self._call_actor("finish_direct_request", route_token)

        async def send_wrapper(message):
            nonlocal prefill_marked
            if message.get("type") == "http.response.body":
                body = message.get("body", b"")
                if body and not prefill_marked:
                    prefill_marked = True
                    await self._call_actor("mark_prefill_complete", route_token)
                if not message.get("more_body", False):
                    await finish_once()
            await send(message)

        try:
            await self._call_actor("start_direct_request", route_token)
            started = True
            await self._app(scope, receive, send_wrapper)
        except BaseException:
            await finish_once()
            raise
        finally:
            await finish_once()


def wrap_with_dynamo_direct_streaming_lifecycle(app, actor_name: Optional[str]):
    if not actor_name:
        return app
    return DynamoDirectStreamingLifecycleMiddleware(app, actor_name)
