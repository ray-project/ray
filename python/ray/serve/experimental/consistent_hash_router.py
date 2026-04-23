"""Consistent-hash request router to honor session stickiness.

ConsistentHashRouter implements a consistent-hash ring with virtual nodes
to map session IDs to replicas. When the assigned replica rejects the request
due to backpressure, the router falls back to the next replica in the ring,
with at most DEFAULT_FALLBACK_REPLICAS replicas before backing off the request.
"""

import bisect
import logging
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

DEFAULT_VIRTUAL_NODES = 100
DEFAULT_FALLBACK_REPLICAS = 2


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
        self._virtual_nodes: int = DEFAULT_VIRTUAL_NODES
        self._fallback_replicas: int = DEFAULT_FALLBACK_REPLICAS

    def initialize_state(self, **kwargs) -> None:
        virtual_nodes = kwargs.get("virtual_nodes", DEFAULT_VIRTUAL_NODES)
        fallback_replicas = kwargs.get("fallback_replicas", DEFAULT_FALLBACK_REPLICAS)

        if not isinstance(virtual_nodes, int) or virtual_nodes <= 0:
            raise ValueError(
                f"virtual_nodes must be a positive int, got {virtual_nodes!r}."
            )
        if not isinstance(fallback_replicas, int) or fallback_replicas < 0:
            raise ValueError(
                "fallback_replicas must be a non-negative int, got "
                f"{fallback_replicas!r}."
            )

        self._virtual_nodes = virtual_nodes
        self._fallback_replicas = fallback_replicas

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
            for i in range(self._virtual_nodes):
                key = f"{replica_id.unique_id}:{i}".encode()
                new_hashes.append(_hash_bytes(key))
                new_replicas.append(replica_id)

        if new_hashes:
            paired = sorted(zip(new_hashes, new_replicas), key=lambda p: p[0])
            new_hashes, new_replicas = map(list, zip(*paired))

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
        max_ranked = 1 + self._fallback_replicas
        n = len(self._ring_hashes)
        for offset in range(n):
            replica_id = self._ring_replicas[(start_idx + offset) % n]
            if replica_id in seen:
                continue
            seen.add(replica_id)
            ranked.append(replica_id)
            if len(ranked) == max_ranked:
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
