import logging
import random
from typing import List, Optional

import ray
from ray.actor import ActorHandle
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.tokenizer import REQUEST_TOKEN_IDS_KWARG
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    get_worker_id,
)
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import RequestRouter
from ray.serve.config import RequestRouterConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


class KVAwareRouter(RequestRouter):
    """Routes each request to the candidate that best balances expected KV-cache
    overlap against the worker's current prefill/decode load.

    Scoring is delegated to the deployment-scoped ``KVRouterActor`` (which owns the
    Dynamo selection service and the global KV index); this per-handle router stays
    thin and simply maps candidate replicas to/from Dynamo worker ids.
    """

    def initialize_state(self):
        """Resolve the deployment's ``KVRouterActor``.

        The actor is attached to this deployment via ``DeploymentActorConfig``
        whenever the request router is a ``KVAwareRouter``, so it exists by the time
        requests route. We resolve its Serve-generated name and block on a cheap
        call to confirm it finished initializing, so the first routed request finds
        a ready scorer.
        """
        self._kv_router_actor = self._discover_kv_router_actor()
        # Synchronization barrier: Ray defers actor methods until __init__ completes,
        # so awaiting any method blocks until KVRouterActor is constructed.
        ray.get(self._kv_router_actor.ready.remote())

    def _discover_kv_router_actor(self) -> ActorHandle:
        """Handle to this deployment's ``KVRouterActor`` by its Serve-scoped name."""
        prefix = (
            f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}"
            f"{self._deployment_id.app_name}::{self._deployment_id.name}::"
        )
        suffix = f"::{KV_ROUTER_ACTOR_NAME}"
        for entry in ray.util.list_named_actors(all_namespaces=True):
            name = entry.get("name") or ""
            if (
                entry.get("namespace") == SERVE_NAMESPACE
                and name.startswith(prefix)
                and name.endswith(suffix)
            ):
                return ray.get_actor(name, namespace=SERVE_NAMESPACE)
        raise RuntimeError(
            f"KVRouterActor for deployment {self._deployment_id} not found; it must "
            "be attached via DeploymentActorConfig when using KVAwareRouter."
        )

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Choose the candidate replica(s) to route ``pending_request`` to.

        Maps the candidate replicas to their Dynamo worker ids, asks the
        ``KVRouterActor`` to rank them via ``select_worker``, and routes to
        the chosen worker's replica. With direct streaming enabled, HAProxy
        then forwards the original request to that replica.

        Requests with no prompt token ids have nothing to score on, so they route
        to a random candidate. This covers the pre-routing ``/tokenize`` RPC (routed
        before token ids exist) and token-less fallbacks (batch prompts,
        truncated/unparseable bodies).
        TODO (jeffreywang): Move pre-routing tokenization to KVRouterActor while
        ensuring tokenization correctness.

        Args:
            candidate_replicas: The replicas eligible to serve the request.
            pending_request: The request being routed.

        Returns:
            Ranked groups of replicas.
        """
        token_ids = (
            pending_request.kwargs.get(REQUEST_TOKEN_IDS_KWARG)
            if pending_request is not None
            else None
        )
        if not token_ids:
            return [[random.choice(candidate_replicas)]] if candidate_replicas else []

        worker_id_to_replica = {
            get_worker_id(replica.replica_id.unique_id): replica
            for replica in candidate_replicas
        }
        selection = await self._kv_router_actor.select_worker.remote(
            pending_request.metadata.request_id,
            token_ids,
            list(worker_id_to_replica),
        )
        return [[worker_id_to_replica[selection["worker_id"]]]]


def is_kv_aware(llm_config: LLMConfig) -> bool:
    """Whether ``llm_config`` selects a ``KVAwareRouter`` for replica selection."""
    request_router_config = llm_config.deployment_config.get("request_router_config")
    if isinstance(request_router_config, dict):
        request_router_config = RequestRouterConfig(**request_router_config)
    return isinstance(request_router_config, RequestRouterConfig) and issubclass(
        request_router_config.get_request_router_class(), KVAwareRouter
    )
