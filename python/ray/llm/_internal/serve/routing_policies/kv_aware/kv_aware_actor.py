import asyncio
import hashlib
import logging
from typing import Any, Dict, List, Optional

import ray
from ray import serve
from ray.serve._private.common import DeploymentTargetInfo, ReplicaID
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"


def get_worker_id(replica_unique_id: str) -> int:
    """
    Deterministically derive a Dynamo worker id from a replica's unique id.
    """
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


@ray.remote
class KVRouterActor:
    """Deployment-scoped Ray actor hosting the KV-aware router.

    KVRouterActor, independent of any replica's lifetime, is attached to the LLMServer
    deployment via Serve's DeploymentActorConfig. It exposes the KV-aware routing interfaces:
    - Replica membership tracking
    - KV-aware scoring

    TODO (jeffreywang): The radix tree that backs them lands in a later PR.
    """

    def __init__(self):
        self._replica_id_by_worker: Dict[int, str] = {}
        self._long_poll_client: Optional[LongPollClient] = None
        self._start_replica_tracking()

    def _start_replica_tracking(self) -> None:
        """Subscribe to this deployment's running replicas via LongPollClient."""
        deployment_id = serve.get_deployment_actor_context().deployment_id
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        self._long_poll_client = LongPollClient(
            controller,
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    deployment_id,
                ): self._on_deployment_targets,
            },
            call_in_event_loop=asyncio.get_event_loop(),
            client_id=f"{type(self).__name__}:{deployment_id}",
        )

    def _on_deployment_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: sync the worker mapping to the running replicas.

        Diffs the snapshot against the current mapping and updates the reverse
        worker->replica map.
        """
        new = {
            get_worker_id(r.replica_id.unique_id): r.replica_id.to_full_id_str()
            for r in target_info.running_replicas
        }
        current = set(self._replica_id_by_worker)
        added = new.keys() - current
        removed = current - new.keys()

        for worker_id in removed:
            self.remove_worker(worker_id)
            self._replica_id_by_worker.pop(worker_id, None)
        for worker_id in added:
            self._replica_id_by_worker[worker_id] = new[worker_id]
            self.add_worker(worker_id)

        if added or removed:
            logger.info(
                "KV router replica membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    def get_tracked_worker_id(self, replica_id_full_str: str) -> Optional[int]:
        """Return the worker id for a currently-tracked replica, or ``None``."""
        unique_id = ReplicaID.from_full_id_str(replica_id_full_str).unique_id
        worker_id = get_worker_id(unique_id)
        return worker_id if worker_id in self._replica_id_by_worker else None

    def get_replica_id(self, worker_id: int) -> Optional[str]:
        """Return the replica's full id string for ``worker_id``, or ``None``."""
        return self._replica_id_by_worker.get(worker_id)

    def get_candidate_worker_ids(self) -> List[int]:
        """Return the currently tracked worker ids, sorted ascending."""
        return sorted(self._replica_id_by_worker)

    def add_worker(self, worker_id: int) -> None:
        """Register a worker with the KV router when a replica is added."""
        pass

    def remove_worker(self, worker_id: int) -> None:
        """Deregister a worker from the KV router when a replica is removed."""
        pass

    async def select_worker(
        self,
        request_id: str,
        token_ids: List[int],
        allowed_worker_ids: List[int],
    ) -> Dict[str, Any]:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

        Returns:
            A dict describing the selected worker:
            ``worker_id`` (int): the chosen worker.
            ``dp_rank`` (int): data-parallel rank within the worker.
            ``overlap_blocks`` (int): KV blocks already cached on that worker.
            ``score`` (float): the worker's routing score (higher is better).
        """
        raise NotImplementedError("KVRouterActor.select_worker is not implemented")

    async def add_request(
        self,
        request_id: str,
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Commit a routed request to the worker chosen by ``select_worker``.

        Args:
            request_id: Unique identifier for the request.
            expected_output_tokens: Predicted number of output tokens, used to
                estimate the request's decode-phase KV footprint.
        """
        raise NotImplementedError("KVRouterActor.add_request is not implemented")

    async def mark_prefill_complete(self, request_id: str) -> None:
        """Signals the transition from prefill to decode.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.mark_prefill_complete is not implemented"
        )

    async def free(self, request_id: str) -> None:
        """Release the KV-cache accounting for a finished request.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError("KVRouterActor.free is not implemented")
