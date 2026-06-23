import asyncio
import hashlib
import logging
from typing import Dict, List, Optional, TypedDict

import ray
from ray import serve
from ray.serve._private.common import DeploymentTargetInfo
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"


def get_worker_id(replica_unique_id: str) -> int:
    """Deterministically derive a Dynamo worker id from a replica's unique id."""
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


class WorkerSelection(TypedDict):
    """The worker chosen by ``KVRouterActor.select_worker`` for a request."""

    # The chosen worker.
    worker_id: int
    # Data-parallel rank within the worker.
    dp_rank: int
    # KV blocks already cached on that worker.
    overlap_blocks: int
    # The worker's routing score.
    score: float


class KVRouterActor:
    """Deployment-scoped Ray actor backing KV-aware routing.

    Attached to the LLMServer deployment via Serve's ``DeploymentActorConfig``,
    independent of any replica's lifetime. So far it tracks live replica
    membership and exposes the KV-aware routing interfaces (``select_worker``
    and the request-lifecycle hooks); KV indexing and scoring are still to come.

    1. Created once per deployment, attached to the LLMServer deployment via
       Serve's ``DeploymentActorConfig`` (independent of any replica's lifetime).
    2. TODO (jeffreywang): Own an in-process Dynamo ``SelectionService``.
    3. Tracks live replicas via a ``LongPollClient`` on ``DEPLOYMENT_TARGETS``,
       mapping each running replica to a Dynamo worker id.
    4. TODO (jeffreywang): The ``SelectionService`` maintains a global KV index
       radix tree, fed by every replica's KV events; each node records which
       workers hold that KV block.
    5. TODO (jeffreywang): Scoring ranks candidate workers by KV-cache overlap
       (queried from the KV index) plus prefill/decode load to pick the best
       worker.
    """

    def __init__(self):
        # _replica_id_by_worker Maps a Dynamo worker id to the running replica's full
        # id string, kept in sync with the deployment's live replicas over LongPoll.
        # NOTE (jeffreywang): _replica_id_by_worker is later used by select_worker
        # to get candidate workers to route among.
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
            # Relies on KVRouterActor being an async actor (it defines async
            # methods), so Ray runs __init__ inside the actor's event loop.
            call_in_event_loop=asyncio.get_running_loop(),
            client_id=f"{type(self).__name__}:{deployment_id}",
        )

    def _on_deployment_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: reconcile tracked workers against the running-replica
        snapshot.

        Each running replica is mapped to a Dynamo worker id (``get_worker_id``);
        newly running replicas are added and departed ones dropped.
        """
        members: Dict[int, str] = {}
        for replica in target_info.running_replicas:
            worker_id = get_worker_id(replica.replica_id.unique_id)
            members[worker_id] = replica.replica_id.to_full_id_str()

        registered = set(self._replica_id_by_worker)
        added = members.keys() - registered
        removed = registered - members.keys()

        for worker_id in removed:
            self._replica_id_by_worker.pop(worker_id, None)
        for worker_id in added:
            self._replica_id_by_worker[worker_id] = members[worker_id]

        if added or removed:
            logger.info(
                "KV router replica membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    async def select_worker(
        self,
        request_id: str,
        token_ids: List[int],
        allowed_worker_ids: List[int],
    ) -> WorkerSelection:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

        Returns:
            The selected worker (see ``WorkerSelection``).
        """
        raise NotImplementedError("KVRouterActor.select_worker is not implemented")

    async def on_request_added(
        self,
        request_id: str,
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Commit a routed request to the worker chosen by ``select_worker``.

        Args:
            request_id: Unique identifier for the request.
            expected_output_tokens: Predicted number of output tokens.
        """
        raise NotImplementedError("KVRouterActor.on_request_added is not implemented")

    async def on_prefill_complete(self, request_id: str) -> None:
        """Record a request's transition from prefill to decode.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.on_prefill_complete is not implemented"
        )

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Adds any newly crossed decode blocks to the request's accounted load.

        Args:
            request_id: Unique identifier for the request.
            cumulative_output_tokens: Total output tokens generated so far.
        """
        raise NotImplementedError("KVRouterActor.on_decode_progress is not implemented")

    async def on_request_completed(self, request_id: str) -> None:
        """Release the KV-cache accounting for a finished request.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.on_request_completed is not implemented"
        )
