"""Helpers the optimizer rules share: finding free nodes for an op, building
upscale requests, and reading a pool's pending actors."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Set

from ray.data._internal.execution.interfaces.common import LogicalActorId, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    pack_actors,
)
from ray.data._internal.experimental.execution.sizer.models import PlacedSizingRequest
from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool import (
        _ExperimentalActorPool,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )


def free_schedulable_nodes(
    ctx: OptimizationContext, op: "ExperimentalActorPoolMapOperator"
) -> List[NodeIdStr]:
    """Nodes in the free map that ``op`` may run on."""
    schedulable = ctx.schedulable_nodes.get(op, frozenset())
    return [node for node in ctx.free if node in schedulable]


def fill_free_nodes(
    ctx: OptimizationContext,
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
    ctx: OptimizationContext, op: "ExperimentalActorPoolMapOperator", want: int
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


def upscale_request(
    op: "ExperimentalActorPoolMapOperator", request: ScaleUpRequest
) -> PlacedSizingRequest:
    """``request`` as a one-op placed request."""
    return PlacedSizingRequest(request={op: request})


def pending_actor_ids(pool: "_ExperimentalActorPool") -> Set[LogicalActorId]:
    """Every actor the pool is currently waiting on, across target nodes."""
    return {
        actor_id
        for ids in pool.pending_ids_by_target_node().values()
        for actor_id in ids
    }
