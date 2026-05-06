"""Consistent-hash request router to honor session stickiness.

ConsistentHashRouter implements a consistent-hash ring with virtual nodes
to map session IDs to replicas. When the assigned replica rejects the request
due to backpressure, the router falls back to the next replica in the ring,
with at most DEFAULT_NUM_FALLBACK_REPLICAS replicas before backing off the request.
"""

import asyncio
import bisect
import logging
import time
from typing import FrozenSet, List, Optional

import mmh3

from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import RequestRouter

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Fixed seed for consistent hashing. A seed change invalidates every running
# affinity map in the fleet and would silently remap every session the instant
# a new router picks up the controller broadcast.
CONSISTENT_HASH_SEED = 0xF9B4CA77

DEFAULT_NUM_VIRTUAL_NODES = 100
DEFAULT_NUM_FALLBACK_REPLICAS = 2


def _hash_bytes(key: bytes) -> int:
    """Hash ``key`` with MurmurHash3 and return the low 64 bits."""
    return mmh3.hash64(key, seed=CONSISTENT_HASH_SEED, signed=False)[0]


class ConsistentHashRouter(RequestRouter):
    """Routes each request to a replica chosen by consistent hashing over
    ``session_id`` (falling back to ``internal_request_id``).

    Every ``ConsistentHashRouter`` instance owns a private ring built from
    the controller's long-poll replica broadcast. Routers on different
    processes converge because they consume the same broadcast and use the
    same hash + vnode count; router restarts are free because a fresh
    router rebuilds the identical ring.

    Consistent hashing is a deterministic, affinity-first strategy: the chosen
    replica is a pure function of session ID and ring state. Mixing in queue-depth,
    locality, or multiplexed model signals breaks determinism and therefore breaks
    affinity.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Ring state
        self._ring_hashes: List[int] = []
        self._ring_replicas: List[ReplicaID] = []

        # Change-detection cache: Compared against the incoming set on
        # each update_replicas tick.
        self._replica_set_snapshot: FrozenSet[ReplicaID] = frozenset()

        # Tunables populated by ``initialize_state``.
        self._num_virtual_nodes: int = DEFAULT_NUM_VIRTUAL_NODES
        self._num_fallback_replicas: int = DEFAULT_NUM_FALLBACK_REPLICAS

    def initialize_state(self, **kwargs) -> None:
        num_virtual_nodes = kwargs.get("num_virtual_nodes", DEFAULT_NUM_VIRTUAL_NODES)
        num_fallback_replicas = kwargs.get(
            "num_fallback_replicas", DEFAULT_NUM_FALLBACK_REPLICAS
        )

        if not isinstance(num_virtual_nodes, int) or num_virtual_nodes <= 0:
            raise ValueError(
                f"num_virtual_nodes must be a positive int, got {num_virtual_nodes!r}."
            )
        if not isinstance(num_fallback_replicas, int) or num_fallback_replicas < 0:
            raise ValueError(
                "num_fallback_replicas must be a non-negative int, got "
                f"{num_fallback_replicas!r}."
            )

        self._num_virtual_nodes = num_virtual_nodes
        self._num_fallback_replicas = num_fallback_replicas

    def update_replicas(self, replicas: List[RunningReplica]) -> None:
        """
        Update available replicas and rebuild the ring when the set changes.
        """
        super().update_replicas(replicas)

        new_snapshot = frozenset(self._replica_id_set)
        if new_snapshot == self._replica_set_snapshot:
            return

        self._rebuild_ring(new_snapshot)

    def _rebuild_ring(self, new_snapshot: FrozenSet[ReplicaID]) -> None:
        """
        Rebuild ring state from the current replica set.

        Every vnode is hashed from f"{unique_id}:{vnode_index}". Using
        the replica's unique_id rather than the full ReplicaID keeps the
        key stable across controller restarts.
        """
        new_hashes: List[int] = []
        new_replicas: List[ReplicaID] = []

        for replica_id in new_snapshot:
            for i in range(self._num_virtual_nodes):
                key = f"{replica_id.unique_id}:{i}".encode()
                new_hashes.append(_hash_bytes(key))
                new_replicas.append(replica_id)

        if new_hashes:
            paired = sorted(zip(new_hashes, new_replicas), key=lambda p: p[0])
            new_hashes = [p[0] for p in paired]
            new_replicas = [p[1] for p in paired]

        self._ring_hashes = new_hashes
        self._ring_replicas = new_replicas
        self._replica_set_snapshot = new_snapshot

        logger.info(
            f"Rebuilt consistent-hash ring for {self._deployment_id}: "
            f"{len(new_snapshot)} replicas, "
            f"{len(new_hashes)} vnodes."
        )

    def _routing_key(self, pending_request: Optional[PendingRequest]) -> Optional[str]:
        """Prefer session_id; fall back to internal_request_id.

        Hashing on internal_request_id (a fresh UUID per request)
        spreads session-less traffic uniformly through the same code
        path, avoiding mixing with another routing strategy (e.g. pow-2).
        """
        if pending_request is None:
            return None
        metadata = pending_request.metadata
        if metadata.session_id:
            return metadata.session_id
        # internal_request_id is always populated by the proxy or by
        # get_request_metadata's fallback to generate_request_id().
        return metadata.internal_request_id

    def _lookup_ranked_replicas(self, routing_key: str) -> List[ReplicaID]:
        """
        Walk the ring and return the primary plus up to K distinct
        clockwise successor replicas.
        """
        if not self._ring_hashes:
            return []

        key_hash = _hash_bytes(routing_key.encode())
        start_idx = bisect.bisect_right(self._ring_hashes, key_hash)
        if start_idx == len(self._ring_hashes):
            # Wrapped past the last vnode
            start_idx = 0

        # Walk clockwise from the owner, collecting the primary and up to K
        # fallback replicas, ensuring that each replica only appears once
        # even if it owns multiple virtual nodes.
        ranked: List[ReplicaID] = []
        seen: set = set()
        max_ranked = 1 + self._num_fallback_replicas
        n = len(self._ring_hashes)

        num_replicas = len(self._replica_set_snapshot)
        for offset in range(n):
            replica_id = self._ring_replicas[(start_idx + offset) % n]
            if replica_id in seen:
                continue
            seen.add(replica_id)
            ranked.append(replica_id)
            if len(ranked) == max_ranked or len(seen) == num_replicas:
                break

        return ranked

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Return ranked candidates for the outer retry loop.

        Each rank is a single-element list containing one replica, so the
        retry loop walks primary -> fallback_1 -> fallback_2 in strict
        hash order with backoff.
        """
        if not candidate_replicas:
            return []

        if pending_request is not None:
            # Enable exponential-backoff sleep between outer retry iterations.
            pending_request.routing_context.should_backoff = True

        routing_key = self._routing_key(pending_request)
        if routing_key is None:
            # Returning [] here would livelock the framework's retry loop.
            # Yield candidates so the task can loop back for real requests.
            return [candidate_replicas]

        ranked_ids = self._lookup_ranked_replicas(routing_key)

        ranks: List[List[RunningReplica]] = []
        for replica_id in ranked_ids:
            replica = self._replicas.get(replica_id)
            if replica is not None:
                ranks.append([replica])

        return ranks

    async def _fulfill_pending_requests(self):
        """Overrides the base loop with two consistent-hash-specific
        invariants:

        1. Exit immediately when there is nothing to route (rather than
           routing for a None pending request, which the base class does
           assuming a FIFO fallback that ConsistentHashRouter does not
           provide).
        2. The routing task that owns a popped pending request must keep
           retrying the inner loop until the request is fulfilled, even if
           there are now more routing tasks than the target. Without this,
           concurrent same-session bursts would orphan their pending
           requests under load.
        """
        try:
            while len(self._routing_tasks) <= self.target_num_routing_tasks:
                start_time = time.time()
                backoff_index = 0
                pending_request = self._get_next_pending_request_to_route()
                if pending_request is None:
                    # No work for this task; let it exit. Future calls to
                    # _maybe_start_routing_tasks (on new requests or replica
                    # updates) will spawn fresh tasks. Drain done entries
                    # from the front of the fulfill deque first so that
                    # ``num_pending_requests`` accurately reflects active
                    # work for the next round of task accounting.
                    while (
                        len(self._pending_requests_to_fulfill) > 0
                        and self._pending_requests_to_fulfill[0].future.done()
                    ):
                        self._pending_requests_to_fulfill.popleft()
                    return
                request_metadata = pending_request.metadata
                gen_choose_replicas_with_backoff = self._choose_replicas_with_backoff(
                    pending_request
                )
                try:
                    async for candidates in gen_choose_replicas_with_backoff:
                        while (
                            len(self._pending_requests_to_fulfill) > 0
                            and self._pending_requests_to_fulfill[0].future.done()
                        ):
                            self._pending_requests_to_fulfill.popleft()

                        # Note: unlike the base class, we deliberately do NOT
                        # break on `len(routing_tasks) > target` here when our
                        # popped pending_request hasn't been fulfilled yet, so
                        # that the popping task always finishes its work.
                        if (
                            pending_request.future.done()
                            and len(self._routing_tasks) > self.target_num_routing_tasks
                        ):
                            break

                        replica = await self._select_from_candidate_replicas(
                            candidates, backoff_index
                        )
                        if replica is not None:
                            self._fulfill_next_pending_request(
                                replica, request_metadata
                            )
                            break

                        backoff_index += 1
                        if backoff_index >= 50 and backoff_index % 50 == 0:
                            routing_time_elapsed = time.time() - start_time
                            warning_log = (
                                "Failed to route request after "
                                f"{backoff_index} attempts over "
                                f"{routing_time_elapsed:.2f}s. Retrying. "
                                f"Request ID: {request_metadata.request_id}."
                            )
                            logger.warning(warning_log)
                finally:
                    await gen_choose_replicas_with_backoff.aclose()

        except Exception:
            logger.exception("Unexpected error in _fulfill_pending_requests.")
        finally:
            self._routing_tasks.remove(asyncio.current_task(loop=self._event_loop))
            self.num_routing_tasks_gauge.set(self.curr_num_routing_tasks)
