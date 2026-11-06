"""Locality-first actor placement."""

from __future__ import annotations

import heapq
import logging
import math
from collections import Counter
from typing import AbstractSet, Dict, List, Tuple

from typing_extensions import override

from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.execution.interfaces.common import LogicalActorId
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    _COLLOCATE_BYTES_PER_ACTOR,
    ActorPlacementStrategy,
    OpPlacementView,
    max_placeable_actors,
)

logger = logging.getLogger(__name__)


class LocalityBasedActorPlacement(ActorPlacementStrategy):
    """Upscales collocate with the op's producers/consumers and queued input
    first, then spread the remainder across nodes in proportion to free
    capacity. Downscales take the cheapest drains (pendings, then idle actors)
    on the nodes capacity-short upscalers want."""

    def __init__(self):
        # Last upscale's phase breakdown per op_id: (collocate_count,
        # spread_count). Read by the sizer's telemetry to log WHY each
        # placement landed.
        self.last_upscale_breakdown: Dict[str, Tuple[int, int]] = {}

    @override
    def upscale_breakdown(self) -> Dict[str, Tuple[int, int]]:
        return self.last_upscale_breakdown

    @staticmethod
    def _fits(
        view: OpPlacementView,
        free: Dict[NodeIdStr, ExecutionResources],
        node_id: NodeIdStr,
    ) -> bool:
        return max_placeable_actors(free[node_id], view.per_actor_usage) >= 1

    @staticmethod
    def _weighted_neighbor_count(view: OpPlacementView, node_id: NodeIdStr) -> float:
        """max(upstream, downstream) actors on the node, each side scaled by its
        edge weight: one actor here bridges a producer AND a consumer. The x2
        makes the neutral 0.5/0.5 weights reproduce the unweighted count."""
        return max(
            2
            * view.upstream_edge_weight
            * view.upstream_actors_by_node.get(node_id, 0),
            2
            * view.downstream_edge_weight
            * view.downstream_actors_by_node.get(node_id, 0),
        )

    @classmethod
    def _locality_rank(
        cls,
        view: OpPlacementView,
        node_id: NodeIdStr,
        *,
        extra_actors: int = 0,
        include_queue: bool = True,
    ) -> Tuple:
        """Rank nodes by locality (ascending = better):

        1. structural imbalance: max(upstream, downstream) - here
        2. queue pressure per actor: decays as actors land on the node
        3. upstream actors (more upstream = better)
        4. node_id (deterministic tie-break)

        ``include_queue=False`` is for nodes whose capacity frees only after
        busy actors drain. By then the current queue will have been consumed
        or moved, so ranking on it chases a stale signal; the neighbor-count
        term still holds, so it ranks on that alone.
        """
        here = view.actors_by_node.get(node_id, 0) + extra_actors
        queue = view.input_queue_bytes_by_node.get(node_id, 0) if include_queue else 0
        return (
            -(cls._weighted_neighbor_count(view, node_id) - here),
            # Queue pressure per actor: decays as actors land on the node.
            -queue / (here + 1),
            -view.upstream_actors_by_node.get(node_id, 0),
            node_id,
        )

    @override
    def rank_node(self, view: OpPlacementView, node_id: NodeIdStr) -> Tuple:
        return self._locality_rank(view, node_id)

    def _locality_demand(self, view: OpPlacementView, node_id: NodeIdStr) -> int:
        """Additional actors the node's locality signal warrants, ignoring
        capacity (placement caps it at what fits):

            max(upstream, downstream) - here + actors_to_clear_queue

        where actors_to_clear_queue = ceil(queued_input_bytes / bytes_per_actor),
        or 0 if the node already has idle or pending actors.
        """
        here = view.actors_by_node.get(node_id, 0)
        actors_to_clear_queue = 0
        # Idle or pending actors here will absorb the queue, so it adds no
        # extra demand.
        if view.idle_actors_by_node.get(
            node_id, 0
        ) == 0 and not view.pending_ids_by_node.get(node_id):
            bytes_per_actor = view.input_bytes_per_actor or _COLLOCATE_BYTES_PER_ACTOR
            queue_bytes = view.input_queue_bytes_by_node.get(node_id, 0)
            actors_to_clear_queue = -(-queue_bytes // bytes_per_actor)  # int ceil
        return max(
            0,
            math.ceil(self._weighted_neighbor_count(view, node_id))
            - here
            + actors_to_clear_queue,
        )

    @override
    def place_upscale(
        self,
        view: OpPlacementView,
        *,
        free: Dict[NodeIdStr, ExecutionResources],
        count: int,
    ) -> List[NodeIdStr]:
        placed: List[NodeIdStr] = []
        # Actors placed on each node during this call (Counter: missing nodes
        # read as 0). Feeds the locality rank, which is recomputed every time
        # an actor is placed.
        placed_per_node: Counter = Counter()

        def locality_rank(node_id: NodeIdStr) -> Tuple:
            return self._locality_rank(
                view, node_id, extra_actors=placed_per_node[node_id]
            )

        # Phase 1 -- COLLOCATE: nodes with unmet locality demand, best rank
        # first, re-ranked after every placement. Demand is fixed for the
        # call (placements are tracked separately), so compute it once.
        demand = {
            node_id: self._locality_demand(view, node_id)
            for node_id in view.filter_schedulable_nodes(free)
        }
        heap = [
            (locality_rank(node_id), node_id)
            for node_id, node_demand in demand.items()
            if node_demand > 0 and self._fits(view, free, node_id)
        ]
        heapq.heapify(heap)
        while len(placed) < count and heap:
            _, node_id = heapq.heappop(heap)
            placed.append(node_id)
            placed_per_node[node_id] += 1
            free[node_id] = free[node_id].subtract(view.per_actor_usage)
            if placed_per_node[node_id] < demand[node_id] and self._fits(
                view, free, node_id
            ):
                heapq.heappush(heap, (locality_rank(node_id), node_id))
        collocate_count = len(placed)

        # Phase 2 -- SPREAD: whatever locality did not place, in proportion to
        # each node's free actor slots.
        remainder = count - len(placed)
        if remainder > 0:
            self._proportional_spread(
                view=view,
                free=free,
                num_actors_to_place=remainder,
                placed=placed,
                placed_per_node=placed_per_node,
            )

        self.last_upscale_breakdown[view.op_id] = (
            collocate_count,
            len(placed) - collocate_count,
        )
        logger.debug(
            "place_upscale[%s]: source=%s requested=%d placed=%d "
            "(collocate=%d spread=%d) nodes=%s",
            view.op_id,
            view.is_source,
            count,
            len(placed),
            collocate_count,
            len(placed) - collocate_count,
            dict(Counter(placed)),
        )
        return placed

    def _proportional_spread(
        self,
        *,
        view: OpPlacementView,
        free: Dict[NodeIdStr, ExecutionResources],
        num_actors_to_place: int,
        placed: List[NodeIdStr],
        placed_per_node: Counter,
    ) -> None:
        """Spread actors across nodes in proportion to their free actor slots:
        each node gets ``num_actors_to_place * (node_slots / total_slots)``,
        and the rounding leftovers go to the largest fractional remainders, so
        a node with twice the room takes about twice the actors."""

        def actors_per_node(node_id: NodeIdStr) -> int:
            cap = max_placeable_actors(free[node_id], view.per_actor_usage)
            # An actor with no resource request fits anywhere: every node can
            # take the whole remainder.
            return num_actors_to_place if math.isinf(cap) else int(cap)

        max_actors_per_node = {
            node_id: actors_per_node(node_id)
            for node_id in view.filter_schedulable_nodes(free)
            if self._fits(view, free, node_id)
        }
        total_actor_slots = sum(max_actors_per_node.values())
        if total_actor_slots == 0:
            return
        num_actors_to_place = min(num_actors_to_place, total_actor_slots)
        per_node_actor_share: Dict[NodeIdStr, int] = {}
        frac: Dict[NodeIdStr, int] = {}
        for node_id, w in max_actors_per_node.items():
            per_node_actor_share[node_id], frac[node_id] = divmod(
                num_actors_to_place * w, total_actor_slots
            )
        leftover = num_actors_to_place - sum(per_node_actor_share.values())
        # Leftovers go to the largest fractional remainders; ties broken toward
        # the least-contended node (fewer constrained ops can use it), then
        # toward nodes with fewest of our actors.
        for node_id in sorted(
            max_actors_per_node,
            key=lambda node_id: (
                -frac[node_id],
                view.constrained_op_count_by_node.get(node_id, 0),
                view.actors_by_node.get(node_id, 0) + placed_per_node[node_id],
                node_id,
            ),
        )[:leftover]:
            per_node_actor_share[node_id] += 1
        for node_id, k in per_node_actor_share.items():
            if k > 0:
                placed.extend([node_id] * k)
                free[node_id] = free[node_id].subtract(view.per_actor_usage.scale(k))
                placed_per_node[node_id] += k

    @override
    def pick_downscale_victims(
        self,
        view: OpPlacementView,
        *,
        count: int,
        favored_nodes: AbstractSet[NodeIdStr],
    ) -> List[LogicalActorId]:
        all_pending = [aid for ids in view.pending_ids_by_node.values() for aid in ids]
        all_running = [aid for ids in view.victim_ids_by_node.values() for aid in ids]
        # Full drain: everyone goes; pendings first (cancelled without a drain).
        if count >= len(all_pending) + len(all_running):
            return all_pending + all_running

        victims: List[LogicalActorId] = []

        def take(actor_ids: List[LogicalActorId]) -> None:
            for actor_id in actor_ids:
                if len(victims) >= count:
                    return
                victims.append(actor_id)

        def cheap(node_id: NodeIdStr) -> List[LogicalActorId]:
            """Pendings + idle actors: evicting frees capacity ~immediately."""
            pendings = view.pending_ids_by_node.get(node_id, [])
            num_idle = view.idle_actors_by_node.get(node_id, 0)
            return [*pendings, *view.victim_ids_by_node.get(node_id, [])[:num_idle]]

        def busy(node_id: NodeIdStr) -> List[LogicalActorId]:
            """Everyone else, least-busy first: frees capacity after a drain."""
            num_idle = view.idle_actors_by_node.get(node_id, 0)
            return view.victim_ids_by_node.get(node_id, [])[num_idle:]

        nodes = sorted(
            set(view.victim_ids_by_node) | set(view.pending_ids_by_node),
            key=lambda node_id: (
                0 if node_id in favored_nodes else 1,
                view.input_queue_bytes_by_node.get(node_id, 0),
                node_id,
            ),
        )
        # Cost-class-major: every cheap victim across all nodes before any
        # busy actor. Killing a busy actor wastes its in-flight work AND
        # delivers the capacity late. Within a class, favored nodes go first.
        for node_id in nodes:
            take(cheap(node_id))
        for node_id in nodes:
            take(busy(node_id))

        logger.debug(
            "pick_downscale_victims[%s]: wanted=%d picked=%d favored=%s victims=%s",
            view.op_id,
            count,
            len(victims),
            sorted(favored_nodes),
            victims,
        )
        return victims

    @override
    def compute_favored_nodes(
        self,
        *,
        capacity_short_views: List[Tuple[OpPlacementView, int]],
        all_nodes: AbstractSet[NodeIdStr],
        fast_free_nodes: AbstractSet[NodeIdStr],
    ) -> AbstractSet[NodeIdStr]:
        """Each capacity-short upscaler nominates its best nodes on both reclaim
        timescales, ranked by the common locality rank.

        Fast-free nodes (a downscaler has pending/idle victims there) free
        capacity ~immediately, so the queue snapshot is still valid and counts.
        Slow-free nodes only free after a busy-actor drain, by which time the
        queue snapshot is stale -- rank those on the structural signal alone.
        """
        favored: set = set()
        for view, unmet in capacity_short_views:
            if unmet <= 0:
                continue
            # Only nodes this op is allowed on: freeing a node it can't use is
            # wasted eviction.
            eligible = view.filter_schedulable_nodes(all_nodes)
            fast = sorted(
                (n for n in eligible if n in fast_free_nodes),
                key=lambda n: self._locality_rank(view, n),
            )
            slow = sorted(
                (n for n in eligible if n not in fast_free_nodes),
                key=lambda n: self._locality_rank(view, n, include_queue=False),
            )
            favored.update(fast[:unmet])
            favored.update(slow[:unmet])
        return favored
