"""Growing an op into free capacity: how far it may grow, how many actors
fit, and the requests that place them."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Set

from ray.data._internal.execution.execution_flags import SIZER_UTIL_THRESHOLD
from ray.data._internal.execution.interfaces.common import ActorId
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.models import PlacedSizingRequest
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import OpState
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
    num_new = ctx.capacity.claim_in_pg(op, delta - num_to_reclaim)
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


def growth_ceiling(
    op: "ExperimentalActorPoolMapOperator", op_state: Optional["OpState"]
) -> int:
    """Most actors ``op`` may grow to: its pool's bounded max size, or fewer
    once every upstream op has completed and its remaining work needs fewer.
    Every upscale, bottleneck or per-op, stays within it. Without an
    ``op_state`` only the pool bounds it."""
    ceiling = op.actor_pool.bounded_max_size()
    remaining = None if op_state is None else remaining_work_cap(op, op_state)
    return ceiling if remaining is None else min(ceiling, remaining)


def remaining_work_cap(
    op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
) -> Optional[int]:
    """Most actors ``op`` can still use, or None while more input can come.

    Once every upstream op has completed, the op's backlog (queued plus
    in-flight inputs) is all the work it has left.
    """
    upstream = op.input_dependencies
    if not upstream or not all(dep.has_completed() for dep in upstream):
        return None
    return max(op.actor_pool.min_size(), backlog_actor_target(op, op_state))


def backlog_actor_target(
    op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
) -> int:
    """Actors needed to work through what this op currently has.

    Counts in-flight tasks as well as the queue: a well-sized steady-state
    pool has a shallow queue but deep in-flight work, and demand that
    collapsed the moment the queue drained would sawtooth. Divided by what
    one actor can run concurrently at the target utilization.
    """
    pool = op.actor_pool
    avg = op.metrics.average_num_inputs_per_task or 1
    enqueued = op_state.total_enqueued_input_blocks()
    expected = math.ceil(enqueued / avg)
    total_tasks = pool.num_tasks_in_flight() + expected
    return math.ceil(
        total_tasks / (pool.max_actor_concurrency() * SIZER_UTIL_THRESHOLD)
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
    slots = ctx.capacity.placeable_new(op)
    if pg and op not in ctx.schedulable_bundles:
        return slots
    domains = ctx.schedulable_bundles if pg else ctx.schedulable_nodes
    eligible = set(domains.get(op, ()))
    per_actor = op.actor_pool.per_actor_resource_usage()
    competitors = sum(
        1
        for peer in ctx.apmo_profiles.actor_pool_ops
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
    slots = ctx.capacity.placeable_new(op)
    unfinished = sum(
        1 for peer in ctx.apmo_profiles.actor_pool_ops if not peer.has_completed()
    )
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
    ceiling = growth_ceiling(op, ctx.topology.get(op))
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
        # This checks: Does any node fit this actor?
        if not any(
            max_placeable_actors(ctx.capacity.free[n], per_actor) >= 1
            for n in ctx.capacity.free_nodes(op)
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
        ctx.capacity.free_nodes(op),
        key=lambda node: placement.rank_node(view, node),
    )
    targets = ctx.capacity.claim_on_nodes(op, growth.num_new, ranked)
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
