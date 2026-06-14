import logging
from typing import List, Optional

import ray
from ray.actor import ActorHandle
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

logger = logging.getLogger(SERVE_LOGGER_NAME)


class KVAwareRouter(RequestRouter):
    """Routes each request to the candidate that best balances expected KV-cache
    overlap against the worker's current prefill/decode load.

    Scoring is delegated to the deployment-scoped ``KVRouterActor`` (which owns the
    Dynamo ``KvRouter`` and the global KV indexer); this per-handle router stays
    thin and simply maps candidate replicas to/from Dynamo worker ids.
    """

    def initialize_state(self, kv_router_actor: Optional[ActorHandle] = None):
        """Resolve the deployment's ``KVRouterActor``.

        The actor is attached to this deployment via ``DeploymentActorConfig``
        whenever the request router is a ``KVAwareRouter``, so it exists by the time
        requests route. We resolve its Serve-generated name and block on a cheap
        call to confirm it finished initializing (its ``__init__`` builds the Dynamo
        ``KvRouter`` and event broker), so the first routed request finds a ready
        scorer.

        Args:
            kv_router_actor: Test-injection seam; when provided, used directly
                instead of resolving the deployment actor by name.
        """
        if kv_router_actor is not None:
            self._kv_router_actor = kv_router_actor
        else:
            self._kv_router_actor = self._discover_kv_router_actor()
            ray.get(self._kv_router_actor.get_candidate_worker_ids.remote())

    def _discover_kv_router_actor(self) -> ActorHandle:
        """Handle to this deployment's ``KVRouterActor`` by its Serve-scoped name.

        Serve names deployment actors
        ``SERVE_DEPLOYMENT_ACTOR::<app>::<deployment>::<code_version>::<logical>``;
        we match on the app/deployment prefix and the logical-name suffix (the
        ``code_version`` segment is not known from the router).
        """
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
        """Route ``pending_request`` to the replica with the best KV score.

        Maps the candidate replicas to their Dynamo worker ids, asks the
        ``KVRouterActor`` to rank them (query-only ``select_worker``), and routes to
        the chosen worker's replica. HAProxy then forwards the original request to
        that replica (direct streaming).

        Args:
            candidate_replicas: The replicas eligible to serve the request.
            pending_request: The request being routed.

        Returns:
            A single ranked group containing the chosen replica.
        """
        # TODO: fall back to default routing when there are no token ids to score
        # on (``pending_request`` is None, or a body the tokenizer skipped). This
        # branch implements the KV-scoring happy path only.
        token_ids = pending_request.kwargs[REQUEST_TOKEN_IDS_KWARG]

        worker_id_to_replica = {
            get_worker_id(replica.replica_id.unique_id): replica
            for replica in candidate_replicas
        }
        selection = await self._kv_router_actor.select_worker.remote(
            pending_request.metadata.request_id,
            token_ids,
            list(worker_id_to_replica),
        )
        chosen = worker_id_to_replica[selection["worker_id"]]
        return [[chosen]]
