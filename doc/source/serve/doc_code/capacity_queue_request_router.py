# flake8: noqa

# __begin_define_capacity_queue__
import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Set, Tuple

import ray
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import DeploymentID, DeploymentTargetInfo
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger("ray.serve")


@dataclass
class CapacityQueueStats:
    """Statistics for a CapacityQueue."""

    queue_size: int
    num_waiters: int
    num_replicas: int
    total_capacity: int
    total_in_flight: int
    total_acquires: int
    total_releases: int
    total_timeouts: int
    max_waiters_seen: int


@dataclass
class ReplicaCapacityInfo:
    """Tracks capacity information for a single replica."""

    replica_id: str
    max_capacity: int
    in_flight: int = 0


@ray.remote(num_cpus=0)
class CapacityQueue:
    """Global capacity manager using least-loaded replica selection.

    This actor tracks capacity per replica and routes requests to the replica
    with the fewest in-flight requests. Routers acquire tokens to route requests,
    and release them when requests complete.

    Key properties:
    - Zero rejections: A token guarantees the replica has capacity
    - Least-loaded distribution: Requests go to the least loaded replica
    - Backpressure: If no capacity is available, routers wait
    """

    def __init__(
        self,
        acquire_timeout_s: float = 30.0,
        deployment_id_name: str = "",
        deployment_id_app: str = "",
        _enable_long_poll: bool = True,
    ):
        self._acquire_timeout_s = acquire_timeout_s

        # Waiters: (asyncio.Future, timestamp) for requests waiting for capacity
        self._waiters: Deque[Tuple[asyncio.Future, float]] = deque()

        # Registered replicas and their capacity info
        self._replicas: Dict[str, ReplicaCapacityInfo] = {}

        # Statistics
        self._total_acquires: int = 0
        self._total_releases: int = 0
        self._total_timeouts: int = 0
        self._max_waiters_seen: int = 0

        # Subscribe to replica updates from the Serve controller so the queue
        # automatically registers new replicas and unregisters dead ones.
        self._long_poll_client: Optional[LongPollClient] = None
        if _enable_long_poll and deployment_id_name:
            deployment_id = DeploymentID(
                name=deployment_id_name, app_name=deployment_id_app
            )
            controller_handle = ray.get_actor(
                SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
            )
            self._long_poll_client = LongPollClient(
                controller_handle,
                {
                    (
                        LongPollNamespace.DEPLOYMENT_TARGETS,
                        deployment_id,
                    ): self._update_deployment_targets,
                },
                call_in_event_loop=get_or_create_event_loop(),
            )

    def register_replica(self, replica_id: str, capacity: int) -> None:
        """Register a replica with its capacity.

        If the replica is already registered, this is a no-op.
        """
        if replica_id in self._replicas:
            return
        self._replicas[replica_id] = ReplicaCapacityInfo(
            replica_id=replica_id,
            max_capacity=capacity,
            in_flight=0,
        )
        self._fulfill_waiters()

    def unregister_replica(self, replica_id: str) -> None:
        """Unregister a replica and remove its capacity."""
        if replica_id not in self._replicas:
            return
        self._replicas.pop(replica_id)

    def _update_deployment_targets(
        self, deployment_target_info: DeploymentTargetInfo
    ) -> None:
        """Handle deployment target updates from the controller (via long poll).

        Automatically registers new replicas and unregisters removed ones so the
        queue always reflects the set of live replicas.
        """
        running_replicas = deployment_target_info.running_replicas
        current_ids: Set[str] = {r.replica_id.unique_id for r in running_replicas}
        registered_ids: Set[str] = set(self._replicas.keys())

        for rid in registered_ids - current_ids:
            self.unregister_replica(rid)

        replica_by_uid = {r.replica_id.unique_id: r for r in running_replicas}
        for uid in current_ids - registered_ids:
            self.register_replica(uid, replica_by_uid[uid].max_ongoing_requests)

    def _get_least_loaded_replica(self) -> Optional[str]:
        """Find the replica with fewest in-flight requests that has capacity.

        Ties are broken randomly.
        """
        best_replicas: List[str] = []
        best_in_flight: float = float("inf")

        for replica_id, info in self._replicas.items():
            available = info.max_capacity - info.in_flight
            if available > 0:
                if info.in_flight < best_in_flight:
                    best_replicas = [replica_id]
                    best_in_flight = info.in_flight
                elif info.in_flight == best_in_flight:
                    best_replicas.append(replica_id)

        if not best_replicas:
            return None
        return random.choice(best_replicas)

    def _get_total_available_capacity(self) -> int:
        """Get total available capacity across all replicas."""
        return sum(
            max(0, info.max_capacity - info.in_flight)
            for info in self._replicas.values()
        )

    def _has_available_capacity(self) -> bool:
        """Check if any replica has available capacity."""
        return any(
            info.max_capacity > info.in_flight for info in self._replicas.values()
        )

    async def acquire(self, timeout_s: Optional[float] = None) -> Optional[str]:
        """Acquire a capacity token from the least loaded replica.

        Returns the replica_id to route to, or None on timeout.
        Caller MUST call release() when the request completes.
        """
        self._total_acquires += 1
        timeout = timeout_s if timeout_s is not None else self._acquire_timeout_s

        # Fast path: find least loaded replica
        replica_id = self._get_least_loaded_replica()
        if replica_id is not None:
            self._replicas[replica_id].in_flight += 1
            return replica_id

        # Slow path: wait for capacity
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        waiter_entry = (future, time.time())
        self._waiters.append(waiter_entry)
        self._max_waiters_seen = max(self._max_waiters_seen, len(self._waiters))

        try:
            replica_id = await asyncio.wait_for(future, timeout=timeout)
            return replica_id
        except asyncio.TimeoutError:
            self._total_timeouts += 1
            self._waiters = deque(
                (f, t) for f, t in self._waiters if f is not future
            )
            return None
        except asyncio.CancelledError:
            self._waiters = deque(
                (f, t) for f, t in self._waiters if f is not future
            )
            raise

    def release(self, replica_id: str) -> None:
        """Release a capacity token when a request completes.

        MUST be called after acquire() when the request finishes.
        """
        self._total_releases += 1

        if replica_id not in self._replicas:
            return

        self._replicas[replica_id].in_flight = max(
            0, self._replicas[replica_id].in_flight - 1
        )
        self._fulfill_waiters()

    def _fulfill_waiters(self) -> None:
        """Try to fulfill waiting requests with available capacity."""
        while self._waiters and self._has_available_capacity():
            future, _ = self._waiters.popleft()
            if future.done():
                continue

            replica_id = self._get_least_loaded_replica()
            if replica_id is None:
                self._waiters.appendleft((future, time.time()))
                break

            self._replicas[replica_id].in_flight += 1
            future.set_result(replica_id)

    def get_stats(self) -> CapacityQueueStats:
        """Get queue statistics."""
        total_capacity = sum(r.max_capacity for r in self._replicas.values())
        total_in_flight = sum(r.in_flight for r in self._replicas.values())

        return CapacityQueueStats(
            queue_size=self._get_total_available_capacity(),
            num_waiters=len(self._waiters),
            num_replicas=len(self._replicas),
            total_capacity=total_capacity,
            total_in_flight=total_in_flight,
            total_acquires=self._total_acquires,
            total_releases=self._total_releases,
            total_timeouts=self._total_timeouts,
            max_waiters_seen=self._max_waiters_seen,
        )

    def get_queue_length(self) -> int:
        """Get total available capacity."""
        return self._get_total_available_capacity()

    def get_num_waiters(self) -> int:
        """Get number of requests waiting for capacity."""
        return len(self._waiters)

    def get_registered_replicas(self) -> List[str]:
        """Get list of registered replica IDs."""
        return list(self._replicas.keys())


# __end_define_capacity_queue__


# __begin_define_capacity_queue_router__
from ray.exceptions import RayActorError
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve._private.common import ReplicaID
from ray.serve.request_router import (
    PendingRequest,
    RequestRouter,
    ReplicaResult,
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
                    # Queue actor died or is unavailable — reconnect
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
