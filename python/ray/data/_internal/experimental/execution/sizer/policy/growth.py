"""Growing an op into free capacity: how far it may grow, how many actors
fit, and the requests that place them."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Set

from ray.data._internal.execution.interfaces.common import ActorId, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    max_placeable_actors,
    pack_actors,
)
from ray.data._internal.experimental.execution.sizer.models import PlacedSizingRequest
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool import (
        _ExperimentalActorPool,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
    )

logger = logging.getLogger(__name__)

# Balanced growth spends at most this fraction of the free capacity per tick,
# split equally among the unfinished ops.
BALANCED_FREE_FRACTION = 0.5


def free_schedulable_nodes(
    ctx: DecisionContext, op: "ExperimentalActorPoolMapOperator"
) -> List[NodeIdStr]:
    """Nodes in the free map that ``op`` may run on."""
    schedulable = ctx.schedulable_nodes.get(op, frozenset())
    return [node for node in ctx.free if node in schedulable]


def fill_free_nodes(
    ctx: DecisionContext,
    op: "ExperimentalActorPoolMapOperator",
    want: int,
    nodes: Optional[List[NodeIdStr]] = None,
) -> List[NodeIdStr]:
    """Up to ``want`` target nodes for new ``op`` actors, one entry per actor,
    taken from free capacity and debited off ``ctx.free``. ``nodes`` sets the
    order to fill in (default: every free node ``op`` may run on)."""
    return pack_actors(
        ctx.free,
        free_schedulable_nodes(ctx, op) if nodes is None else nodes,
        op.actor_pool.per_actor_resource_usage(),
        want,
    )


def fill_pg_bundles(
    ctx: DecisionContext, op: "ExperimentalActorPoolMapOperator", want: int
) -> int:
    """How many of ``want`` new actors of a PG-managed op fit, debited off its
    bundles (static PG) or its headroom (PG capacity unknown). Ray core picks
    the bundle, so only the count matters."""
    bundles = ctx.schedulable_bundles.get(op)
    if bundles is None:
        fit = max(0, min(want, ctx.unknown_pg_headroom.get(op, 0)))
        if fit:
            ctx.unknown_pg_headroom[op] -= fit
        return fit
    return len(
        pack_actors(
            ctx.pg_free, bundles, op.actor_pool.per_actor_resource_usage(), want
        )
    )


def grow_pg_op(
    op: "ExperimentalActorPoolMapOperator",
    delta: int,
    ctx: DecisionContext,
    reason: str,
) -> Optional[PlacedSizingRequest]:
    """Up to ``delta`` more actors for a PG-managed op, or None if its
    placement group has no room. The PG places them, so the request is
    untargeted. Draining actors are reclaimed first: they still hold their
    bundles, so only new ones need room in the PG."""
    num_to_reclaim = min(delta, op.actor_pool.num_reclaimable_draining_actors())
    num_new = fill_pg_bundles(ctx, op, delta - num_to_reclaim)
    if num_to_reclaim + num_new <= 0:
        return None
    return upscale_request(
        op,
        ScaleUpRequest.untargeted(
            num_to_reclaim + num_new, num_to_reclaim=num_to_reclaim, reason=reason
        ),
    )


def upscale_request(
    op: "ExperimentalActorPoolMapOperator", request: ScaleUpRequest
) -> PlacedSizingRequest:
    """``request`` as a one-op placed request."""
    return PlacedSizingRequest(request={op: request})


def pending_actor_ids(pool: "_ExperimentalActorPool") -> Set[ActorId]:
    """Every actor the pool is currently waiting on, across target nodes."""
    return {
        actor_id
        for ids in pool.pending_ids_by_target_node().values()
        for actor_id in ids
    }


# --- how far an op may grow ---------------------------------------------------


def growth_ceiling(op: "ExperimentalActorPoolMapOperator", ctx: DecisionContext) -> int:
    """Most actors ``op`` may grow to (see ``DecisionContext.growth_ceiling``)."""
    return ctx.growth_ceiling.get(op, op.actor_pool.bounded_max_size())


def placeable_slots(
    op: "ExperimentalActorPoolMapOperator", ctx: DecisionContext
) -> int:
    """New ``op`` actors that fit in free capacity on its nodes, or in its
    placement group (bundles, or headroom when its PG capacity is unknown).
    Counted per node/bundle, so fragmented CPU/GPU/memory is not mistaken
    for placeable actors."""
    per_actor = op.actor_pool.per_actor_resource_usage()
    if ctx.is_pg_managed(op):
        if op not in ctx.schedulable_bundles:
            return max(0, ctx.unknown_pg_headroom.get(op, 0))
        return int(
            sum(
                max_placeable_actors(ctx.pg_free[bundle], per_actor)
                for bundle in ctx.schedulable_bundles[op]
                if bundle in ctx.pg_free
            )
        )
    return int(
        sum(
            max_placeable_actors(ctx.free[node], per_actor)
            for node in ctx.schedulable_nodes.get(op, ())
            if node in ctx.free
        )
    )


def free_growth_share(
    op: "ExperimentalActorPoolMapOperator", ctx: DecisionContext
) -> int:
    """Cap new actors at floor(free actor slots / competing operators).

    Peers must be unfinished, below their pool cap, use the same primary
    resource (CPU or GPU), and share at least one eligible node/bundle. Keep
    pending or temporarily held peers in the denominator so their startup
    does not erase their share. Unknown PG capacity is already per-op
    headroom. A nonzero capacity always permits one actor to avoid stranding
    the last few slots.
    """
    pg = ctx.is_pg_managed(op)
    slots = placeable_slots(op, ctx)
    if pg and op not in ctx.schedulable_bundles:
        return slots
    domains = ctx.schedulable_bundles if pg else ctx.schedulable_nodes
    eligible = set(domains.get(op, ()))
    per_actor = op.actor_pool.per_actor_resource_usage()
    competitors = sum(
        1
        for peer in ctx.actor_pool_ops
        if ctx.is_pg_managed(peer) == pg
        and not peer.has_completed()
        and peer.actor_pool.serving_size() < peer.actor_pool.bounded_max_size()
        and (peer.actor_pool.per_actor_resource_usage().gpu > 0) == (per_actor.gpu > 0)
        and eligible.intersection(domains.get(peer, ()))
    )
    share = max(1, math.floor(slots / max(1, competitors))) if slots else 0
    logger.debug(
        "SizingPolicy: %s free-capacity growth cap=%d "
        "(placeable=%d, competing_ops=%d)",
        op.name,
        share,
        slots,
        competitors,
    )
    return share


def balanced_step(op: "ExperimentalActorPoolMapOperator", ctx: DecisionContext) -> int:
    """How many actors a balanced stage grows by this tick:
    floor(BALANCED_FREE_FRACTION x free actor slots / unfinished ops), at
    least one. Repeated every tick while the stages stay balanced, so the
    pipeline converges on the free capacity instead of crawling one actor
    per tick."""
    slots = placeable_slots(op, ctx)
    unfinished = sum(1 for peer in ctx.actor_pool_ops if not peer.has_completed())
    return max(1, math.floor(BALANCED_FREE_FRACTION * slots / max(1, unfinished)))


# --- growing into free capacity -----------------------------------------------


@dataclass(frozen=True)
class FreeGrowth:
    """How one op grows into free capacity this tick: draining actors it
    takes back, and new actors to place."""

    num_to_reclaim: int
    num_new: int


def plan_free_growth(
    op: "ExperimentalActorPoolMapOperator", target: int, ctx: DecisionContext
) -> Optional[FreeGrowth]:
    """How ``op`` would grow toward ``target`` (within its growth ceiling)
    as far as free capacity allows, or None if it cannot grow at all. Reads
    ``ctx`` only; ``apply_free_growth`` claims the capacity."""
    pool = op.actor_pool
    size = pool.serving_size()
    ceiling = growth_ceiling(op, ctx)
    if size >= ceiling:
        logger.debug(
            "SizingPolicy: %s not grown: at its growth ceiling (%d)",
            op.name,
            ceiling,
        )
        return None
    step = min(max(1, target - size), ceiling - size)
    if not ctx.is_pg_managed(op):
        per_actor = pool.per_actor_resource_usage()
        if not any(
            max_placeable_actors(ctx.free[n], per_actor) >= 1
            for n in free_schedulable_nodes(ctx, op)
        ):
            logger.debug(
                "SizingPolicy: %s not grown into free capacity: no allowed "
                "node has room after capacity held for transfers",
                op.name,
            )
            return None
    num_to_reclaim = min(step, pool.num_reclaimable_draining_actors())
    num_new = min(step - num_to_reclaim, free_growth_share(op, ctx))
    if num_to_reclaim + num_new <= 0:
        logger.debug("SizingPolicy: %s not grown: placement group full", op.name)
        return None
    return FreeGrowth(num_to_reclaim=num_to_reclaim, num_new=num_new)


def apply_free_growth(
    op: "ExperimentalActorPoolMapOperator",
    growth: FreeGrowth,
    ctx: DecisionContext,
    placement: "ActorPlacementStrategy",
    reason: str,
) -> Optional[PlacedSizingRequest]:
    """Claim ``growth``'s capacity and build its request: new actors on the
    nodes the locality strategy ranks best, or into the op's placement
    group. None if nothing could be placed after all."""
    if ctx.is_pg_managed(op):
        grown = grow_pg_op(
            op,
            growth.num_to_reclaim + growth.num_new,
            ctx,
            reason=f"optimizer: {reason} grows into placement group",
        )
        if grown is not None:
            logger.debug(
                "SizingPolicy: %s %s grows +%d into its placement group",
                reason,
                op.name,
                grown.request[op].delta,
            )
        return grown
    view = placement_view.build_op_placement_view(op, rank_victims=False)
    ranked = sorted(
        free_schedulable_nodes(ctx, op),
        key=lambda node: placement.rank_node(view, node),
    )
    targets = fill_free_nodes(ctx, op, growth.num_new, ranked)
    if not targets and not growth.num_to_reclaim:
        return None
    logger.debug(
        "SizingPolicy: %s %s grows +%d into free capacity",
        reason,
        op.name,
        len(targets) + growth.num_to_reclaim,
    )
    return upscale_request(
        op,
        ScaleUpRequest.on_nodes(
            targets,
            num_to_reclaim=growth.num_to_reclaim,
            reason=f"optimizer: {reason} grows into free capacity",
        ),
    )
