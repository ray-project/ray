# flake8: noqa

# __begin_define_capacity_queue_router__
import asyncio
from typing import Dict, List, Optional, Tuple

import ray
from ray.exceptions import RayActorError
from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve.request_router import (
    PendingRequest,
    ReplicaResult,
    RequestRouter,
    RunningReplica,
)


class CapacityQueueRouter(RequestRouter):
    """Custom request router that uses a CapacityQueue deployment actor.

    Acquires capacity tokens from a centralized CapacityQueue before routing
    requests. Each token guarantees the target replica has capacity.
    """

    def __init__(self, deployment_id, *args, **kwargs):
        super().__init__(deployment_id, *args, **kwargs)
        self._capacity_queue = None
        self._acquired_tokens: Dict[str, str] = {}
        self._capacity_queue_actor_name: str = "capacity_queue"

    def initialize_state(self, **kwargs):
        if "capacity_queue_actor_name" in kwargs:
            self._capacity_queue_actor_name = kwargs["capacity_queue_actor_name"]

    @property
    def supports_rejection_protocol(self) -> bool:
        return False

    def _discover_capacity_queue(self):
        """Discover the CapacityQueue deployment actor by listing named actors."""
        prefix = (
            f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}"
            f"{self._deployment_id.app_name}::"
            f"{self._deployment_id.name}::"
        )
        suffix = f"::{self._capacity_queue_actor_name}"

        actors = ray.util.list_named_actors(all_namespaces=True)
        for actor_info in actors:
            if actor_info["namespace"] == SERVE_NAMESPACE:
                name = actor_info["name"]
                if name.startswith(prefix) and name.endswith(suffix):
                    self._capacity_queue = ray.get_actor(
                        name, namespace=SERVE_NAMESPACE
                    )
                    return
        raise ValueError(
            f"Could not find CapacityQueue deployment actor for "
            f"{self._deployment_id}. Make sure the deployment is configured "
            f"with a CapacityQueue deployment actor named "
            f"'{self._capacity_queue_actor_name}'."
        )

    async def _acquire_token_for_replica(self) -> Tuple[str, RunningReplica]:
        """Acquire a capacity token and return the matching replica."""
        attempt = 0
        acquired_replica_id = None
        try:
            while True:
                if self._capacity_queue is None:
                    self._discover_capacity_queue()

                # Wait for replicas to be available
                while len(self._replicas) == 0:
                    self._replicas_updated_event.clear()
                    await self._replicas_updated_event.wait()

                # Acquire a token
                acquire_ref = None
                try:
                    acquire_ref = self._capacity_queue.acquire.remote()
                    acquired_replica_id = await acquire_ref
                except asyncio.CancelledError:
                    if acquire_ref is not None:
                        ray.cancel(acquire_ref)
                    raise
                except RayActorError:
                    # Queue actor died or is unavailable. The router enters
                    # a backoff-retry loop until the controller recreates
                    # the deployment actor and the router rediscovers it.
                    # No exception is raised to the caller.
                    self._capacity_queue = None
                    await self._backoff(attempt)
                    attempt += 1
                    continue
                except Exception:
                    await self._backoff(attempt)
                    attempt += 1
                    continue

                if acquired_replica_id is None:
                    await self._backoff(attempt)
                    attempt += 1
                    continue

                # Find matching replica in local state.
                # The capacity queue uses replica_id strings (ReplicaID.unique_id),
                # so we need to match against local replicas.
                for rid, replica in self._replicas.items():
                    if rid.unique_id == acquired_replica_id:
                        result_id = acquired_replica_id
                        acquired_replica_id = None
                        return result_id, replica

                # Replica not found locally - release and wait for update
                self._safe_release(acquired_replica_id)
                acquired_replica_id = None
                self._replicas_updated_event.clear()
                await self._replicas_updated_event.wait()
        except asyncio.CancelledError:
            if acquired_replica_id is not None:
                self._safe_release(acquired_replica_id)
            raise

    def _safe_release(self, replica_id: str) -> None:
        """Release a token, handling queue actor death."""
        if self._capacity_queue is None:
            return
        try:
            self._capacity_queue.release.remote(replica_id)
        except RayActorError:
            self._capacity_queue = None

    def _release_token(self, internal_request_id: str) -> None:
        """Release a capacity token back to the queue."""
        replica_id = self._acquired_tokens.pop(internal_request_id, None)
        if replica_id is not None:
            self._safe_release(replica_id)

    async def _choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> RunningReplica:
        """Choose a replica by acquiring a token from the capacity queue."""
        internal_request_id = pending_request.metadata.internal_request_id
        replica_id_str, replica = await self._acquire_token_for_replica()
        self._acquired_tokens[internal_request_id] = replica_id_str
        return replica

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        # Not used because _choose_replica_for_request is overridden
        return [candidate_replicas]

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        pass

    def on_request_completed(
        self,
        replica_id: ReplicaID,
        internal_request_id: str,
    ):
        """Release the capacity token when a request completes."""
        self._release_token(internal_request_id)

    def on_request_cancelled(
        self,
        internal_request_id: str,
    ):
        """Release the capacity token when a request is cancelled."""
        self._release_token(internal_request_id)


# __end_define_capacity_queue_router__
