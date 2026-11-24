"""What the sizer's phases hand each other.

The placed requests one tick hands the pools (``PlacedRequests``), a
batch of placed requests the policy builds (``PlacedSizingRequest``), and the
capacity one tick decides against (``ClusterStateSnapshot``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, Dict, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    BundleKey,
    DatasetReservationView,
    PlacementGroupSlots,
)
from ray.data._internal.experimental.execution.sizer.capacity import (
    committed_by_node,
    constrained_op_count_by_node,
    nodes_to_reclaim,
    pg_free_by_bundle,
    unknown_pg_headroom,
)
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    ordered_actor_pool_ops,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_models import (
        PlacedScalingRequest,
    )


@dataclass(frozen=True)
class PlacedRequests:
    """What WHERE hands APPLY: every request of the tick, each naming its
    nodes.

    ``by_op``: per op, its requests in the order to apply them. Usually one;
    a donor that sheds on several nodes has one per node.
    """

    by_op: Dict["ExperimentalActorPoolMapOperator", Tuple["PlacedScalingRequest", ...]]


@dataclass(frozen=True)
class PlacedSizingRequest:
    """A batch of placed requests the policy builds, one per op."""

    request: Dict["ExperimentalActorPoolMapOperator", "PlacedScalingRequest"]


@dataclass
class ClusterStateSnapshot:
    """This dataset's capacity for one tick: what it may use, what is free,
    and where each op can run.

    Terms (per node unless noted), each defined once; the fields below are
    built from them:

    - "slice": what the coordinator reserved for this dataset on the node,
      PG part included.
    - "pg_reserved": the part of the slice this dataset's placement groups
      hold Only PG actors may use it.
    - "draining node": a node Ray core is draining(scheduled for termination)
    - "committed": what the non-PG pools hold on the node(running + pending + draining)
    - "draining actors": actors that cannot accept more tasks, because they
      are scheduled for downscale.
    - "bundle": one bundle of a placement group, a slot a PG actor must fit
      inside (`BundleKey`). Its shape is fixed at PG creation.
    - "pg_held": what the PG ops' actors hold inside a bundle (their
      `total_size`, assumed packed first-fit: Ray Core doesn't say which bundle).
    - "pg_cap": the most actors a PG op may hold when its bundles are unknown.
    """

    # ops with placement constrains (like GPU or PG) can only land
    # on a subset of nodes
    schedulable_nodes_by_op: Dict[
        "ExperimentalActorPoolMapOperator", AbstractSet[NodeIdStr]
    ]

    # = slice - pg_reserved - draining nodes.
    reserved_by_node: Dict[NodeIdStr, ExecutionResources]
    # = `reserved_by_node` - committed, floored at 0, minus the nodes in
    # `nodes_to_reclaim` (dropped whole: an actor placed there would be evicted).
    free_by_node: Dict[NodeIdStr, ExecutionResources]
    # = committed - draining actors - reserved, where above 0.
    # How much more we currently commit on a node than our slice allows.
    # Draining actors are not counted: they are already leaving. Draining
    # nodes are not in reserved, so everything we hold on one counts as over.
    nodes_to_reclaim: Dict[NodeIdStr, ExecutionResources] = field(default_factory=dict)

    # = shape - pg_held, per bundle. The PG counterpart of free_by_node.
    pg_free_by_bundle: Dict[BundleKey, ExecutionResources] = field(default_factory=dict)
    # Bundle shapes, which bundles each static-PG op may use, and each pg_cap.
    pg_slots: PlacementGroupSlots = field(default_factory=PlacementGroupSlots)
    # = pg_cap - the op's total_size, floored at 0, per PG op with unknown
    # bundles. The count counterpart of pg_free_by_bundle.
    unknown_pg_headroom_by_op: Dict["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )

    # How many constrained non-PG ops can use each node (lower = less
    # contended). Used in granting order and placement tie-break.
    constrained_op_count_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # The bundles each static-PG op may use.
    schedulable_bundles_by_op: Dict[
        "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
    ] = field(default_factory=dict)

    # The order every step walks operators in (scarcest first), so grants in
    # the how-many pass line up with placements in scale_where.
    ordered_ops: Tuple["ExperimentalActorPoolMapOperator", ...] = ()
    # How long the coordinator's per-node slice has been unchanged.
    seconds_since_allocation_change: float = float("inf")

    @classmethod
    def build(
        cls,
        reservation: "DatasetReservationView",
        profiles: OpProfiles,
        *,
        seconds_since_allocation_change: float,
        trust_empty_reservation: bool = True,
    ) -> "ClusterStateSnapshot":
        """Capacity and schedulability as of now; see the class docstring
        for how each field is derived.

        So sometimes the autoscaling coordinator can give empty reservations.
        (Either on cold start cache, or possibly reserveation shrink). This flag
        `trust_empty_reservation` tells us if the empty reservation is safe to trust.
        """
        zero = ExecutionResources.zero()

        # 1. First take out pg and draining nodes from reservation.
        pg_reserved = reservation.pg_reserved_by_node
        reserved = {
            node_id: res.subtract(pg_reserved.get(node_id, zero)).max(zero)
            for node_id, res in reservation.reserved_by_node.items()
            if node_id not in reservation.draining_nodes
        }

        # 2. Any nodes that are not part of our reservation anymore, but we still have
        # actors scheduled on them, must be reclaimed
        reclaim = (
            nodes_to_reclaim(reserved, profiles)
            if reserved or trust_empty_reservation
            else {}
        )

        # 3. Calculate current usage (running + pending) of the cluster.
        committed = committed_by_node(profiles)

        # 4. Calculate the amount of free resources available in this snapshot.
        free = {
            node_id: res.subtract(committed.get(node_id, zero)).max(zero)
            for node_id, res in reserved.items()
            if node_id not in reclaim
        }

        # 5. Build a reservation view, which allows us to determine
        # which nodes an operator can schedule on.
        view_for_match = DatasetReservationView(
            reserved_by_node=reserved,
            labels_by_node=reservation.labels_by_node,
        )
        schedulable_nodes_by_op = {}
        for op in profiles.actor_pool_ops:
            per_actor = op.actor_pool.per_actor_resource_usage()
            schedulable_nodes_by_op[op] = op.placement_constraint().matching_nodes(
                view_for_match, per_actor
            )

        pg_slots = reservation.pg_slots
        return cls(
            free_by_node=free,
            reserved_by_node=reserved,
            nodes_to_reclaim=reclaim,
            schedulable_nodes_by_op=schedulable_nodes_by_op,
            constrained_op_count_by_node=constrained_op_count_by_node(
                profiles, schedulable_nodes_by_op
            ),
            pg_slots=pg_slots,
            pg_free_by_bundle=pg_free_by_bundle(pg_slots),
            schedulable_bundles_by_op=dict(pg_slots.static_bundles_by_op),
            unknown_pg_headroom_by_op=unknown_pg_headroom(pg_slots),
            ordered_ops=tuple(
                ordered_actor_pool_ops(
                    profiles.topology,
                    profiles.actor_pool_ops,
                    schedulable_nodes_by_op,
                )
            ),
            seconds_since_allocation_change=seconds_since_allocation_change,
        )

    def get_global_limits(self) -> ExecutionResources:
        """This dataset's whole slice, summed."""
        total = ExecutionResources.zero()
        for resources in self.reserved_by_node.values():
            total = total.add(resources)
        return total
