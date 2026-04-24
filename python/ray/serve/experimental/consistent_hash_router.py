"""Consistent-hash request router to honor session stickiness.

ConsistentHashRouter implements a consistent-hash ring with virtual nodes
to map session IDs to replicas. When the assigned replica rejects the request
due to backpressure, the router falls back to the next replica in the ring,
with at most DEFAULT_FALLBACK_REPLICAS replicas before backing off the request.
"""

import asyncio
import bisect
import logging
import time
from collections import OrderedDict
from typing import FrozenSet, List, Optional

import mmh3

from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import DEFAULT_LATENCY_BUCKET_MS, SERVE_LOGGER_NAME
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import RequestRouter
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Fixed seed for consistent hashing. A seed change invalidates every running
# affinity map in the fleet and would silently remap every session the instant
# a new router picks up the controller broadcast.
CONSISTENT_HASH_SEED = 0xF9B4CA77

DEFAULT_VIRTUAL_NODES = 100
DEFAULT_FALLBACK_REPLICAS = 2

# Bounded LRU size for the top-sessions gauge
TOP_SESSIONS_MAX = 20


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

        # Metrics
        default_tags = {
            "application": self._deployment_id.app_name,
            "deployment": self._deployment_id.name,
            "actor_id": self._self_actor_id,
            "handle_source": self._handle_source.value,
        }
        common_tag_keys = ("application", "deployment", "actor_id", "handle_source")

        # Primary (rank=0) vs. fallback (rank>=1) usage; the affinity
        # health signal. rank=-1 means the accepting replica wasn't in
        # the current ranked list (staleness window).
        self.fallback_counter = metrics.Counter(
            "serve_consistent_hash_fallback",
            description=("Routing decisions per rank."),
            tag_keys=common_tag_keys + ("rank",),
        ).set_default_tags(default_tags)

        # Hottest session_ids over a rolling window.
        self.top_sessions_gauge = metrics.Gauge(
            "serve_consistent_hash_top_sessions",
            description=(
                f"Per-session routing count for the top {TOP_SESSIONS_MAX} "
                "recently-active session_ids."
            ),
            tag_keys=common_tag_keys + ("session_id",),
        ).set_default_tags(default_tags)

        # In-memory LRU backing the gauge above.
        self._top_sessions_lru: "OrderedDict[str, int]" = OrderedDict()

        # Ring rebuild count and latency; catches scale-event thrash.
        self.ring_rebuilds_counter = metrics.Counter(
            "serve_consistent_hash_ring_rebuilds",
            description=("Number of full consistent-hash ring rebuilds."),
            tag_keys=common_tag_keys,
        ).set_default_tags(default_tags)

        self.ring_rebuild_duration_ms_histogram = metrics.Histogram(
            "serve_consistent_hash_ring_rebuild_duration_ms",
            description="Latency of a full consistent-hash ring rebuild, in ms.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=common_tag_keys,
        ).set_default_tags(default_tags)

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
        start = time.perf_counter()

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

        duration_ms = (time.perf_counter() - start) * 1000.0
        self.ring_rebuilds_counter.inc()
        self.ring_rebuild_duration_ms_histogram.observe(duration_ms)

        logger.info(
            f"Rebuilt consistent-hash ring for {self._deployment_id}: "
            f"{len(new_snapshot)} replicas, "
            f"{len(new_hashes)} vnodes, "
            f"{duration_ms:.3f}ms."
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

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ) -> None:
        """Emit per-rank and top-sessions metrics after a replica accepts
        the request.
        """
        rank = self._compute_rank(self._routing_key(pending_request), replica_id)
        self.fallback_counter.inc(tags={"rank": str(rank)})

        session_id = pending_request.metadata.session_id if pending_request else ""
        if session_id:
            self._touch_top_session(session_id)

    def _compute_rank(self, routing_key: str, replica_id: ReplicaID) -> int:
        """Position of ``replica_id`` in the current ranked walk
        (0 = primary); -1 if the ring drifted between routing and
        this call and the replica is no longer a ranked candidate."""
        try:
            return self._lookup_ranked_replicas(routing_key).index(replica_id)
        except ValueError:
            return -1

    def _touch_top_session(self, session_id: str) -> None:
        """Record one more routing event for ``session_id`` in the LRU
        and update its gauge value. On eviction, zero the evicted series
        so Prometheus doesn't show it frozen at its final count."""
        if session_id in self._top_sessions_lru:
            self._top_sessions_lru[session_id] += 1
            self._top_sessions_lru.move_to_end(session_id)
        else:
            if len(self._top_sessions_lru) >= TOP_SESSIONS_MAX:
                evicted_id, _ = self._top_sessions_lru.popitem(last=False)
                self.top_sessions_gauge.set(0, tags={"session_id": evicted_id})
            self._top_sessions_lru[session_id] = 1

        self.top_sessions_gauge.set(
            self._top_sessions_lru[session_id],
            tags={"session_id": session_id},
        )
