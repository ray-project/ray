"""Phase 3: resolve each delta to target nodes (up) or exact victims (down).

Nothing here keeps state between ticks: placement is re-derived from current
state every time, so a failed placement or a slow drain simply retries when
scale_how_many re-emits its delta.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Dict, List, Sequence, Set, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import LogicalActorId, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ExperimentalActorPoolScalingRequest,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    PLACEMENT_RESOURCE_DIMS,
    ActorPlacementStrategy,
    OpPlacementView,
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.models import TickCapacitySnapshot
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles
from ray.data._internal.experimental.execution.sizer.telemetry import SizerTelemetry

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest

# Sorts eviction candidates cheapest first: (cost class, tasks in flight,
# unconsumed outputs, op index, rank in the op's victim list, actor id).
# Class 0 = pending (cancel, no drain), 1 = running.
_EvictionCandidate = Tuple[int, int, int, int, int, LogicalActorId]


def resolve_evictions(
    *,
    nodes_to_reclaim: Dict[NodeIdStr, ExecutionResources],
    profiles: OpProfiles,
) -> Dict[ExperimentalActorPoolMapOperator, List[LogicalActorId]]:
    """Pick the victims that erase each node's overage, and no more.

    Per node, candidates from every op are taken cheapest first (pending, then
    fewest tasks in flight / unconsumed outputs). A candidate is taken only if
    it frees a dimension that is still over budget, checked against what is
    left after the victims already taken -- so a {1 CPU, 1 GPU} overage costs
    one CPU actor and one GPU actor, not every CPU actor on the node.
    PG-managed ops are skipped (not in our allocation).
    """
    if all(over.is_zero() for over in nodes_to_reclaim.values()):
        return {}
    zero = ExecutionResources.zero()
    ops = profiles.non_pg_ops
    views = {
        op: placement_view.build_op_placement_view(op, rank_victims=True) for op in ops
    }
    victims: Dict[ExperimentalActorPoolMapOperator, List[LogicalActorId]] = {}
    for node_id, over_budget in nodes_to_reclaim.items():
        remaining = over_budget
        for _, _, _, op_index, _, actor_id in sorted(
            _eviction_candidates(ops, views, node_id)
        ):
            if remaining.is_zero():
                break
            per_actor_usage = views[ops[op_index]].per_actor_usage
            if not _frees_over_budget_dim(per_actor_usage, remaining):
                continue
            victims.setdefault(ops[op_index], []).append(actor_id)
            remaining = remaining.subtract(per_actor_usage).max(zero)
    return victims


def _eviction_candidates(
    ops: Sequence[ExperimentalActorPoolMapOperator],
    views: Dict[ExperimentalActorPoolMapOperator, OpPlacementView],
    node_id: NodeIdStr,
) -> List[_EvictionCandidate]:
    candidates: List[_EvictionCandidate] = []
    for op_index, op in enumerate(ops):
        view = views[op]
        for rank, actor_id in enumerate(view.pending_ids_by_node.get(node_id, [])):
            candidates.append((0, 0, 0, op_index, rank, actor_id))
        costs = view.victim_drain_cost_by_node.get(node_id, [])
        for rank, actor_id in enumerate(view.victim_ids_by_node.get(node_id, [])):
            in_flight, unconsumed = costs[rank] if rank < len(costs) else (0, 0)
            candidates.append((1, in_flight, unconsumed, op_index, rank, actor_id))
    return candidates


def _frees_over_budget_dim(
    per_actor_usage: ExecutionResources, over_budget: ExecutionResources
) -> bool:
    """True if evicting one actor of this shape lowers a dimension still over
    budget, so we never evict a CPU-only actor to cover a GPU overage."""
    return any(
        getattr(per_actor_usage, dim) > 0 and getattr(over_budget, dim) > 0
        for dim in PLACEMENT_RESOURCE_DIMS
    )


def evict_from_reclaimed_nodes(
    *,
    placed: Dict[ExperimentalActorPoolMapOperator, ExperimentalActorPoolScalingRequest],
    nodes_to_reclaim: Dict[NodeIdStr, ExecutionResources],
    profiles: OpProfiles,
) -> Set[ExperimentalActorPoolMapOperator]:
    """Shed actors off nodes we're losing (reservation shrink or node
    draining). These drain gracefully and are marked non-reclaimable so a
    later upscale can't revive them. Eviction takes precedence over this
    tick's normal sizing for the same op (which re-requests next tick).

    ``nodes_to_reclaim`` includes DRAINING (e.g. preempted) nodes, which the
    pool's own preemption hook also drains; whichever runs first stamps the
    drain-reason metric label (alloc_shrink here vs preemption there)."""
    evicted_ops: Set[ExperimentalActorPoolMapOperator] = set()
    for op, victims in resolve_evictions(
        nodes_to_reclaim=nodes_to_reclaim, profiles=profiles
    ).items():
        if not victims:
            continue
        placed[op] = ExperimentalActorPoolScalingRequest(
            delta=-len(victims),
            force=True,
            reason="allocation shrank",
            actor_ids_to_downscale=tuple(victims),
            reclaimable=False,
        )
        evicted_ops.add(op)
    return evicted_ops


def build_placement_views(
    *,
    request: Dict[ExperimentalActorPoolMapOperator, "ActorPoolScalingRequest"],
    capacity: TickCapacitySnapshot,
) -> Dict[ExperimentalActorPoolMapOperator, OpPlacementView]:
    """A placement view per requested op, carrying its schedulable nodes and
    the per-node contention the strategy sorts on. Only downscales pay for sorted
    victim lists; upscales just need counts."""
    views = {}
    for op, req in request.items():
        view = placement_view.build_op_placement_view(op, rank_victims=req.delta < 0)
        views[op] = replace(
            view,
            match_nodes=capacity.schedulable_nodes_by_op.get(op),
            constrained_op_count_by_node=capacity.constrained_op_count_by_node,
        )
    return views


def compute_favored_nodes(
    *,
    request: Dict[ExperimentalActorPoolMapOperator, "ActorPoolScalingRequest"],
    views: Dict[ExperimentalActorPoolMapOperator, OpPlacementView],
    reserved: Dict[NodeIdStr, ExecutionResources],
    free: Dict[NodeIdStr, ExecutionResources],
    placement: ActorPlacementStrategy,
    profiles: OpProfiles,
    ordered_ops: Sequence[ExperimentalActorPoolMapOperator],
) -> Set[NodeIdStr]:
    """Nodes that capacity-short upscalers (delta does not fit in free
    capacity) want freed.

    Feeds victim selection, so a downscale sheds where the freed capacity
    actually helps someone rather than wherever is convenient.
    """
    # Nodes where a downscaling op has pending/idle victims -- eviction there
    # is ~instant, so the "capacity free now" ranking applies; nodes that can
    # only free capacity via a slow drain of busy actors are ranked by durable
    # source/sink locality instead.
    fast_free_nodes: Set[NodeIdStr] = set()
    for op, req in request.items():
        if req.delta < 0:
            view = views[op]
            fast_free_nodes.update(view.pending_ids_by_node)
            fast_free_nodes.update(view.idle_actors_by_node)

    # (view, unmet delta) per capacity-short upscaling op, scarcest first. A
    # pin is a normal size-1 schedulable set, so pinned ops DO participate --
    # and every op's fit test / favored nomination is restricted to its
    # schedulable subset of free, else an op with plenty of free capacity it
    # cannot use never looks short.
    capacity_short: List[Tuple[OpPlacementView, int]] = []
    for op in ordered_ops:
        req = request.get(op)
        if req is None or req.delta <= 0:
            continue
        # PG-managed ops consume their PG reservation, not the free map, so
        # evicting a non-PG actor never helps them.
        if profiles.is_pg_managed(op):
            continue
        view = views[op]
        fits = sum(
            max_placeable_actors(free[node_id], view.per_actor_usage)
            for node_id in view.filter_schedulable_nodes(free)
        )
        if fits < req.delta:
            capacity_short.append((view, req.delta - int(fits)))
    if not capacity_short:
        return set()

    return set(
        placement.compute_favored_nodes(
            capacity_short_views=capacity_short,
            all_nodes=set(reserved),
            fast_free_nodes=fast_free_nodes,
        )
    )


def place_downscales(
    *,
    placed: Dict[ExperimentalActorPoolMapOperator, ExperimentalActorPoolScalingRequest],
    request: Dict[ExperimentalActorPoolMapOperator, "ActorPoolScalingRequest"],
    views: Dict[ExperimentalActorPoolMapOperator, OpPlacementView],
    favored: Set[NodeIdStr],
    free: Dict[NodeIdStr, ExecutionResources],
    placement: ActorPlacementStrategy,
    evicted_ops: Set[ExperimentalActorPoolMapOperator],
) -> None:
    """Resolve each downscale delta to exact victim actors."""
    for op, req in request.items():
        if req.delta >= 0 or op in evicted_ops:
            continue
        # The delta is the net change to the LIVE (non-draining) pool;
        # draining actors are already excluded from the view.
        view = views[op]
        if op.has_completed():
            # Completed op (execution finished, outputs taken): the whole pool
            # is torn down, so there is no placement trade-off to make --
            # target every actor (running + pending).
            victims = [
                aid for ids in view.victim_ids_by_node.values() for aid in ids
            ] + [aid for ids in view.pending_ids_by_node.values() for aid in ids]
        else:
            victims = placement.pick_downscale_victims(
                view, count=-req.delta, favored_nodes=favored
            )
        if not victims:
            continue
        placed[op] = ExperimentalActorPoolScalingRequest.from_base(
            req,
            delta=-len(victims),
            force=True,
            actor_ids_to_downscale=tuple(victims),
        )
        # Refund capacity for PENDING victims: they never landed, so
        # cancelling them frees their claimed capacity for this tick's
        # upscales. Running victims hold their capacity until dead.
        victim_set = set(victims)
        for node_id, pending_ids in view.pending_ids_by_node.items():
            if node_id not in free:
                continue
            for actor_id in pending_ids:
                if actor_id in victim_set:
                    free[node_id] = free[node_id].add(view.per_actor_usage)


def place_upscales(
    *,
    placed: Dict[ExperimentalActorPoolMapOperator, ExperimentalActorPoolScalingRequest],
    request: Dict[ExperimentalActorPoolMapOperator, "ActorPoolScalingRequest"],
    views: Dict[ExperimentalActorPoolMapOperator, OpPlacementView],
    free: Dict[NodeIdStr, ExecutionResources],
    placement: ActorPlacementStrategy,
    profiles: OpProfiles,
    ordered_ops: Sequence[ExperimentalActorPoolMapOperator],
    evicted_ops: Set[ExperimentalActorPoolMapOperator],
    telemetry: SizerTelemetry,
) -> None:
    """Resolve each upscale delta to target nodes, in the shared processing
    order (scarcest first, so the throughput-gating op gets first pick of
    remaining capacity). A pin is just a size-1 constraint-match set, so
    pinned ops go through the normal targeted placement."""
    upscale_ops = [
        op
        for op in ordered_ops
        if (req := request.get(op)) is not None
        and req.delta > 0
        and op not in evicted_ops
    ]

    for op in upscale_ops:
        req = request[op]
        # PG-managed op: capacity was already capped in scale_how_many (PG
        # bundle count or user concurrency), and the op's placement group
        # picks the node. Emit untargeted -- no free-map math, no strategy --
        # reclaiming draining actors first as usual.
        if profiles.is_pg_managed(op):
            reclaim_n = min(req.delta, op.actor_pool.num_reclaimable_draining_actors())
            new_n = req.delta - reclaim_n
            if reclaim_n == 0 and new_n == 0:
                continue
            placed[op] = ExperimentalActorPoolScalingRequest.from_base(
                req,
                delta=reclaim_n + new_n,
                num_to_reclaim=reclaim_n,
                target_nodes_to_scale_actors_on=(),
            )
            continue
        # Reclaim still-draining actors from a prior (unfinished) downscale
        # first: they already hold their node's resources and need no
        # startup, so reviving them is free and avoids the kill+recreate
        # churn of an oscillating op. Only the remainder needs new actors
        # (which consume free capacity via place_upscale).
        reclaim_n = min(req.delta, op.actor_pool.num_reclaimable_draining_actors())
        new_n = req.delta - reclaim_n
        view = views[op]
        if reclaim_n > 0:
            # Reclaimed (revived) draining actors reappear on their nodes
            # alongside the new actors, but the view excludes draining
            # actors: count them back in so placement doesn't double-serve
            # the locality demand they already satisfy.
            actors_by_node = dict(view.actors_by_node)
            for node_id in op.actor_pool.reclaimable_node_ids(limit=reclaim_n):
                actors_by_node[node_id] = actors_by_node.get(node_id, 0) + 1
            view = replace(view, actors_by_node=actors_by_node)
        nodes = placement.place_upscale(view, free=free, count=new_n)
        if len(nodes) < new_n:
            # The free map has no room for the remainder: drop it and let
            # scale_how_many re-request it next tick. Every placement stays
            # under the locality strategy -- no untargeted fallback.
            telemetry.log_placement_shortfall(
                op=op,
                num_placed=len(nodes),
                num_new=new_n,
                num_reclaimed=reclaim_n,
                free=free,
                per_actor=view.per_actor_usage,
            )
        if reclaim_n == 0 and not nodes:
            continue
        placed[op] = ExperimentalActorPoolScalingRequest.from_base(
            req,
            delta=reclaim_n + len(nodes),
            num_to_reclaim=reclaim_n,
            target_nodes_to_scale_actors_on=tuple(nodes),
        )
