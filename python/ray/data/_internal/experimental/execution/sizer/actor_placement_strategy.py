"""The placement strategy interface: where new actors go, which actors leave."""

from __future__ import annotations

import abc
import math
from dataclasses import dataclass, field
from typing import AbstractSet, Dict, Hashable, Iterable, List, Optional, Tuple, TypeVar

from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.execution.interfaces.common import LogicalActorId

# Fallback bytes-per-actor used to convert a node's queued input into a
# collocation fill when a view carries no per-actor input-bytes figure
# (hand-built views; the operator always derives one). Only affects how
# aggressively a queue-heavy node is collocated before the remainder spreads.
_COLLOCATE_BYTES_PER_ACTOR = 384 * 1024 * 1024  # 384 MiB

# Resource dimensions the sizer models capacity in.
PLACEMENT_RESOURCE_DIMS = ("cpu", "gpu", "memory")

# Slack for float division when counting fits: ``0.3 // 0.1 == 2.0``.
# ExecutionResources rounds cpu/gpu to 5 digits, so this never admits a real
# overfit.
FIT_EPS = 1e-6


def max_placeable_actors(
    free: ExecutionResources, per_actor: ExecutionResources
) -> float:
    """How many actors of shape ``per_actor`` fit into ``free``.

    Dimensions the actor doesn't request don't constrain it; an actor with no
    resource requests at all fits anywhere (``inf``).
    """
    count = float("inf")
    for dim in PLACEMENT_RESOURCE_DIMS:
        per_actor_dim = getattr(per_actor, dim)
        if per_actor_dim > 0:
            count = min(count, math.floor(getattr(free, dim) / per_actor_dim + FIT_EPS))
    return count


# A capacity slot: a node, or a placement-group bundle.
SlotT = TypeVar("SlotT", bound=Hashable)


def pack_actors(
    free: Dict[SlotT, ExecutionResources],
    slots: Iterable[SlotT],
    per_actor: ExecutionResources,
    count: int,
) -> List[SlotT]:
    """Pack up to ``count`` actors into ``slots``(nodes/bundles) in order, deducting their
    resources from ``free`` (mutated). Returns one slot per packed actor."""
    placed: List[SlotT] = []
    for slot in slots:
        wanted = count - len(placed)
        if wanted <= 0:
            break
        fit = max_placeable_actors(free[slot], per_actor)
        n = wanted if math.isinf(fit) else min(int(fit), wanted)
        if n > 0:
            free[slot] = free[slot].subtract(per_actor.scale(n))
            placed.extend([slot] * n)
    return placed


@dataclass(frozen=True)
class OpPlacementView:
    """One operator's placement snapshot for a tick: where its actors, its
    queued input and its neighbors' actors are, per node. The strategy decides
    from this view alone."""

    op_id: str
    # True for source operators (no upstream actor-pool operator).
    is_source: bool
    per_actor_usage: ExecutionResources
    # Input bytes one actor is expected to hold in flight: its tasks-in-flight
    # cap times the target block size (None only for hand-built views, which
    # then use ``_COLLOCATE_BYTES_PER_ACTOR``). Used to convert a node's queued
    # input into a reasonable collocation fill.
    input_bytes_per_actor: Optional[int]
    # Alive non-draining actors + targeted pendings, per node.
    actors_by_node: Dict[NodeIdStr, int]
    # Bytes of this op's queued input per node.
    input_queue_bytes_by_node: Dict[NodeIdStr, int]
    # Alive actors of upstream producer / downstream consumer actor-pool
    # operators, per node.
    upstream_actors_by_node: Dict[NodeIdStr, int]
    downstream_actors_by_node: Dict[NodeIdStr, int]
    # Downscale victims per node: running actors not already draining,
    # cheapest to drain first (see ``victim_drain_cost_by_node``). Busy actors
    # ARE victims -- they just take longer to drain. Empty unless the view was
    # built with ``rank_victims``.
    victim_ids_by_node: Dict[NodeIdStr, List[LogicalActorId]]
    # Non-draining actors with zero tasks in flight, per node. Evicting
    # these (like cancelling pendings) frees capacity ~immediately.
    idle_actors_by_node: Dict[NodeIdStr, int]
    # Pending (not yet ready) actors per target node -- the cheapest victims:
    # cancelling one frees its claimed capacity immediately.
    pending_ids_by_node: Dict[NodeIdStr, List[LogicalActorId]]
    # The op's schedulable nodes (label_selector / node pin), before the
    # has-capacity filter. None = unconstrained. Each strategy's fit check
    # enforces capacity, so ``filter_schedulable_nodes(free)`` = match ∩
    # free-keys and the fit check narrows that to nodes with room.
    match_nodes: Optional[AbstractSet[NodeIdStr]] = None
    # How many constrained operators can use each node (lower = less
    # contended); breaks ties when several ops want the same node.
    constrained_op_count_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # Per-victim drain cost aligned with ``victim_ids_by_node``: (tasks in
    # flight, unconsumed output blocks). Both must reach zero before a
    # draining victim exits, so lower = frees capacity sooner.
    victim_drain_cost_by_node: Dict[NodeIdStr, List[Tuple[int, int]]] = field(
        default_factory=dict
    )
    # Pendings with no target node (placement-group or initial-size actors):
    # cancelled by id, since no node-shaped downscale can reach them.
    untargeted_pending_ids: List[LogicalActorId] = field(default_factory=list)
    # This op's input/output share of its total observed byte flow (each in
    # [0, 1], summing to 1). 0.5/0.5 = no data yet; the rank multiplies each
    # side's actor counts by 2 x weight so the neutral value preserves the
    # unweighted rank exactly.
    upstream_edge_weight: float = 0.5
    downstream_edge_weight: float = 0.5

    def filter_schedulable_nodes(
        self, node_ids: Iterable[NodeIdStr]
    ) -> List[NodeIdStr]:
        """The subset of ``node_ids`` this op is allowed on. Capacity is not
        checked here; callers still gate on fit."""
        if self.match_nodes is None:
            return list(node_ids)
        return [node_id for node_id in node_ids if node_id in self.match_nodes]


class ActorPlacementStrategy(abc.ABC):
    """Decides node assignment / victim selection for one operator's delta."""

    @abc.abstractmethod
    def place_upscale(
        self,
        view: OpPlacementView,
        *,
        free: Dict[NodeIdStr, ExecutionResources],
        count: int,
    ) -> List[NodeIdStr]:
        """Assign ``count`` new actors to nodes.

        Args:
            view: The operator's placement snapshot.
            free: Free capacity per allocated node. MUTATED in place: chosen
                capacity is deducted, so successive calls for different
                operators in the same tick never double-book.
            count: Number of actors to place.

        Returns:
            One node id per placed actor (a node repeats when several actors
            land on it). May be shorter than ``count`` on a capacity
            shortfall; the next sizing tick reconciles.
        """
        ...

    @abc.abstractmethod
    def pick_downscale_nodes(
        self,
        view: OpPlacementView,
        *,
        count: int,
        favored_nodes: AbstractSet[NodeIdStr],
    ) -> List[NodeIdStr]:
        """Pick where a downscale sheds ``count`` actors.

        Args:
            view: The operator's placement snapshot.
            count: Number of actors to shed.
            favored_nodes: Nodes that capacity-short upscaling operators
                want freed -- shedding there is preferred so the freed
                capacity benefits the upscaler.

        Returns:
            One node id per actor to shed (a node repeats when several leave
            it), cheapest-to-free first. May be shorter than ``count``.
        """
        ...

    @abc.abstractmethod
    def compute_favored_nodes(
        self,
        *,
        capacity_short_views: List[Tuple[OpPlacementView, int]],
        all_nodes: AbstractSet[NodeIdStr],
        fast_free_nodes: AbstractSet[NodeIdStr],
    ) -> AbstractSet[NodeIdStr]:
        """Nodes that capacity-short upscaling operators (those whose delta
        does not fit in free capacity) want freed.

        Fed into ``pick_downscale_nodes`` as ``favored_nodes`` so downscales
        shed where the freed capacity benefits the upscalers.

        Args:
            capacity_short_views: (view, unmet delta) per capacity-short
                upscaling op, scarcest first.
            all_nodes: Every node in the allocation.
            fast_free_nodes: Nodes where a downscaling op has pending/idle
                victims -- eviction there is ~instant, so "capacity free now"
                ranking applies; elsewhere the capacity arrives only after a
                slow drain of busy actors.

        Returns:
            The nodes to favor when picking downscale victims.
        """
        ...

    def upscale_breakdown(self) -> Dict[str, Tuple[int, int]]:
        """Per op id, how the last upscale split into (collocated, spread)
        actors, for telemetry. Strategies that don't track this return {}."""
        return {}

    @abc.abstractmethod
    def rank_node(self, view: OpPlacementView, node_id: NodeIdStr) -> Tuple:
        """Sort key for landing one more of this op's actors on ``node_id``
        (lower is better). Lets decisions that plan across operators (the
        pipeline optimizer) order candidate nodes the way ``place_upscale``
        would."""
        ...
