# flake8: noqa

import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Set, Tuple

import ray
from ray import serve
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
    """Centralized capacity manager that routes requests to the least-loaded replica.

    A Ray actor that tracks per-replica in-flight counts and hands out capacity
    tokens. Routers call ``acquire()`` to get a replica_id, route to it, then
    call ``release()`` when done. If no capacity is available, acquire blocks
    until ``acquire_timeout_s`` expires (returns None).

    Replica membership is kept in sync via long-poll subscription to the Serve
    controller. After a crash/restart, CapacityQueue may temporarily over-provision;
    this self-heals because replicas enforce their own limits and reject excess.
    """

    def __init__(
        self,
        acquire_timeout_s: float,
        token_ttl_s: Optional[float],
        _enable_long_poll: bool = True,
    ):
        self._acquire_timeout_s = acquire_timeout_s
        self._token_ttl_s = token_ttl_s

        # Waiters: (asyncio.Future, timestamp) for requests waiting for capacity
        self._waiters: Deque[Tuple[asyncio.Future, float]] = deque()

        # Registered replicas and their capacity info
        self._replicas: Dict[str, ReplicaCapacityInfo] = {}

        # Track when each in-flight token was acquired: {replica_id -> [timestamp, ...]}
        self._in_flight_timestamps: Dict[str, Deque[float]] = {}

        # Statistics
        self._total_acquires: int = 0
        self._total_releases: int = 0
        self._total_timeouts: int = 0
        self._total_ttl_reclaims: int = 0
        self._max_waiters_seen: int = 0

        # Subscribe to replica updates from the Serve controller so the queue
        # automatically registers new replicas and unregisters dead ones.
        self._long_poll_client: Optional[LongPollClient] = None
        if _enable_long_poll:
            deployment_context = serve.get_deployment_actor_context()
            deployment_id = deployment_context.deployment_id
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

        # Start background TTL reaper if token_ttl_s is set.
        if self._token_ttl_s is not None and self._token_ttl_s > 0:
            self._ttl_task = get_or_create_event_loop().create_task(
                self._reap_expired_tokens()
            )

    async def _reap_expired_tokens(self) -> None:
        """Periodically reclaim tokens that have exceeded their TTL."""
        interval = self._token_ttl_s / 2
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            reclaimed = 0
            for replica_id, timestamps in list(self._in_flight_timestamps.items()):
                while timestamps and (now - timestamps[0]) > self._token_ttl_s:
                    timestamps.popleft()
                    if replica_id in self._replicas:
                        self._decrement_in_flight(replica_id)
                        reclaimed += 1
            if reclaimed > 0:
                self._total_ttl_reclaims += reclaimed
                logger.info(f"TTL reaper reclaimed {reclaimed} expired token(s).")
                self._fulfill_waiters()

    def _decrement_in_flight(self, replica_id: str) -> None:
        """Decrement in-flight count for a replica, clamping at zero."""
        info = self._replicas[replica_id]
        info.in_flight = max(0, info.in_flight - 1)

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
        logger.debug(
            f"Registered replica {replica_id} with capacity {capacity}. "
            f"Total replicas: {len(self._replicas)}."
        )
        self._fulfill_waiters()

    def unregister_replica(self, replica_id: str) -> None:
        """Unregister a replica and remove its capacity."""
        if replica_id not in self._replicas:
            return
        info = self._replicas.pop(replica_id)
        self._in_flight_timestamps.pop(replica_id, None)
        logger.debug(
            f"Unregistered replica {replica_id} "
            f"(had {info.in_flight}/{info.max_capacity} in-flight). "
            f"Total replicas: {len(self._replicas)}."
        )

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
            self._record_acquire_timestamp(replica_id)
            return replica_id

        # Slow path: wait for capacity
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        waiter_entry = (future, time.time())
        self._waiters.append(waiter_entry)
        self._max_waiters_seen = max(self._max_waiters_seen, len(self._waiters))

        try:
            replica_id = await asyncio.wait_for(future, timeout=timeout)
            self._record_acquire_timestamp(replica_id)
            return replica_id
        except asyncio.TimeoutError:
            self._total_timeouts += 1
            self._waiters = deque((f, t) for f, t in self._waiters if f is not future)
            return None
        except asyncio.CancelledError:
            self._waiters = deque((f, t) for f, t in self._waiters if f is not future)
            raise

    def release(self, replica_id: str) -> None:
        """Release a capacity token when a request completes.

        MUST be called after acquire() when the request finishes.
        """
        self._total_releases += 1

        if replica_id not in self._replicas:
            logger.warning(f"Release called for unknown replica {replica_id}.")
            return

        self._decrement_in_flight(replica_id)
        # Remove the oldest timestamp for this replica.
        if replica_id in self._in_flight_timestamps:
            ts = self._in_flight_timestamps[replica_id]
            if ts:
                ts.popleft()
        self._fulfill_waiters()

    def _record_acquire_timestamp(self, replica_id: str) -> None:
        """Record the timestamp of an acquired token for TTL tracking."""
        if self._token_ttl_s is not None:
            if replica_id not in self._in_flight_timestamps:
                self._in_flight_timestamps[replica_id] = deque()
            self._in_flight_timestamps[replica_id].append(time.time())

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
            self._record_acquire_timestamp(replica_id)
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

    def get_replica_in_flight(self) -> Dict[str, Tuple[int, int]]:
        """Get per-replica (in_flight, max_capacity) for convergence checks."""
        return {
            rid: (info.in_flight, info.max_capacity)
            for rid, info in self._replicas.items()
        }
