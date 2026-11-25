"""Helpers for computing this dataset's per-node and per-bundle usage,
capacity and free room."""

from __future__ import annotations

from typing import TYPE_CHECKING, AbstractSet, Dict

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    BundleKey,
    PlacementGroupSlots,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (  # noqa: E501
    is_constrained,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    pack_actors,
)
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )


def committed_by_node(profiles: OpProfiles) -> Dict[NodeIdStr, ExecutionResources]:
    """Resources the non-PG pools hold per node: running actors plus pendings
    at their target node.

    PG-managed ops are excluded: their actors live inside a placement group,
    whose bundles ``ClusterStateSnapshot.build`` takes out of the slice
    instead.
    """
    committed: Dict[NodeIdStr, ExecutionResources] = {}
    for op in profiles.non_pg_ops:
        for node_id, usage in op.actor_pool.committed_usage_by_node.items():
            committed[node_id] = committed.get(node_id, ExecutionResources.zero()).add(
                usage
            )
    return committed


def draining_by_node(profiles: OpProfiles) -> Dict[NodeIdStr, ExecutionResources]:
    """The part of ``committed_by_node`` held by draining actors: already on
    its way out, and held only until each actor is fully drained."""
    draining: Dict[NodeIdStr, ExecutionResources] = {}
    for op in profiles.non_pg_ops:
        for node_id, usage in op.actor_pool.draining_usage_by_node().items():
            draining[node_id] = draining.get(node_id, ExecutionResources.zero()).add(
                usage
            )
    return draining


def nodes_to_reclaim(
    reserved: Dict[NodeIdStr, ExecutionResources], profiles: OpProfiles
) -> Dict[NodeIdStr, ExecutionResources]:
    """Per-node excess of committed usage over the coordinator's slice, net
    of draining actors.

    Draining actors already leave without further help, and they are never
    eviction victims, so counting them would remove replacements for them
    on every tick until they finish draining.
    """
    zero = ExecutionResources.zero()
    draining = draining_by_node(profiles)
    reclaim: Dict[NodeIdStr, ExecutionResources] = {}
    for node_id, used in committed_by_node(profiles).items():
        leaving = draining.get(node_id, zero)
        over = used.subtract(leaving).subtract(reserved.get(node_id, zero)).max(zero)
        if not over.is_zero():
            reclaim[node_id] = over
    return reclaim


def pg_free_by_bundle(
    pg_slots: PlacementGroupSlots,
) -> Dict[BundleKey, ExecutionResources]:
    """Each bundle's shape minus what the PG ops already hold in it: the PG
    counterpart of ``reserved - committed_by_node``.

    Ray core picks the bundle and never reports it back, so held actors
    (``total_size``: a draining actor keeps its bundle until dead) are
    assumed packed first-fit, most constrained op first. If core packed
    differently the result is optimistic; the excess pendings wait and are
    released by the stuck-pending expiry.
    """
    bundles_by_op = pg_slots.static_bundles_by_op
    free = {key: shape.copy() for key, shape in pg_slots.shapes.items()}
    for op in sorted(bundles_by_op, key=lambda o: len(bundles_by_op[o])):
        pool = op.actor_pool
        pack_actors(
            free,
            bundles_by_op[op],
            pool.per_actor_resource_usage(),
            pool.total_size(),
        )
    return free


def unknown_pg_headroom(
    pg_slots: PlacementGroupSlots,
) -> Dict["ExperimentalActorPoolMapOperator", int]:
    """How many more actors each op whose PG capacity is unknown may add
    under its cap.

    Counts ``total_size``, not ``serving_size``: a draining actor keeps its
    PG until it is actually dead.
    """
    return {
        op: max(0, cap - op.actor_pool.total_size())
        for op, cap in pg_slots.unknown_cap_by_op.items()
    }


def constrained_op_count_by_node(
    profiles: OpProfiles,
    schedulable_nodes_by_op: Dict[
        "ExperimentalActorPoolMapOperator", AbstractSet[NodeIdStr]
    ],
) -> Dict[NodeIdStr, int]:
    """How many constrained non-PG ops can use each node.

    An unconstrained op can use every node, so counting it would add the
    same +1 everywhere and never change an ordering; only constrained ops
    tell nodes apart. The ledger fills the least contended nodes first,
    and placement breaks ties the same way.
    """
    count: Dict[NodeIdStr, int] = {}
    for op, nodes in schedulable_nodes_by_op.items():
        if profiles.is_pg_managed(op) or not is_constrained(op):
            continue
        for node_id in nodes:
            count[node_id] = count.get(node_id, 0) + 1
    return count
