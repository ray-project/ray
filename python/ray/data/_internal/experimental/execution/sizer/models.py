"""What the sizer's phases hand each other.

Per-operator deltas before placement (``PerOpSizingRequest``) and after
(``PlacedSizingRequest``), and the capacity one tick decides against
(``TickCapacitySnapshot`` inside ``TickContext``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, Dict, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.sizer.pg_bundles import BundleKey

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_models import (
        ExperimentalActorPoolScalingRequest,
    )


@dataclass(frozen=True)
class PerOpSizingRequest:
    """What scale_how_many decided: per operator, how many actors to add or
    remove. Not yet tied to any node."""

    request: Dict["ExperimentalActorPoolMapOperator", "ActorPoolScalingRequest"]


@dataclass(frozen=True)
class PlacedSizingRequest:
    """What scale_where resolved: the same deltas, now naming the target nodes
    to grow on and the exact actors to shed."""

    request: Dict[
        "ExperimentalActorPoolMapOperator", "ExperimentalActorPoolScalingRequest"
    ]


@dataclass
class TickCapacitySnapshot:
    """This dataset's capacity for one tick: what it may use, what is free,
    and where each op can run.

    Derived once at tick start from the ``DatasetAllocationView``, so every
    phase sizes and places against the same numbers. Draining nodes are not
    in it (see ``nodes_to_reclaim``).
    """

    # reserved - committed (non-PG pools), floored at zero. Excludes nodes to
    # reclaim: an actor placed there would be evicted next tick.
    free_by_node: Dict[NodeIdStr, ExecutionResources]
    # This dataset's coordinator slice, before subtracting what we hold.
    reserved_by_node: Dict[NodeIdStr, ExecutionResources]
    # Nodes each op's constraint allows (every allocated node when
    # unconstrained). Static for the tick: it says where an op may run, never
    # how much is free, so a node freed mid-tick becomes usable without
    # touching this.
    schedulable_nodes_by_op: Dict[
        "ExperimentalActorPoolMapOperator", AbstractSet[NodeIdStr]
    ]
    per_actor_resources_by_op: Dict[
        "ExperimentalActorPoolMapOperator", ExecutionResources
    ]
    # Per-node amount by which our committed usage exceeds the allocation --
    # the coordinator shrank our slice, or a node is draining. scale_where
    # sheds exactly enough actors to erase it.
    nodes_to_reclaim: Dict[NodeIdStr, ExecutionResources] = field(default_factory=dict)
    # How many constrained ops can use each node (lower = less contended).
    # Drives grant order and the placement tie-break.
    constrained_op_count_by_node: Dict[NodeIdStr, int] = field(default_factory=dict)
    # Static-PG ops size against their PG's bundles, not the free map: each
    # bundle is a slot an actor must fit inside, like a node. Free = bundle
    # shape minus the PG ops' held actors (see
    # ``PlacementGroupBundles.free_by_bundle``).
    pg_free_by_bundle: Dict[BundleKey, ExecutionResources] = field(default_factory=dict)
    schedulable_bundles_by_op: Dict[
        "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
    ] = field(default_factory=dict)
    # Ops whose PG capacity is unknown (a PG minted per actor, or a PG with no
    # bundles in the view yet): no bundles to inspect, so just how many more
    # actors their pool's max size allows.
    unknown_pg_headroom_by_op: Dict["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )

    def get_global_limits(self) -> ExecutionResources:
        """This dataset's whole slice, summed."""
        total = ExecutionResources.zero()
        for resources in self.reserved_by_node.values():
            total = total.add(resources)
        return total


@dataclass
class TickContext:
    """The state one sizing tick shares across its phases.

    Built on the sizing cadence and threaded through optimize -> how_many ->
    where. It dies with the tick, so nothing here needs consume-and-clear
    bookkeeping; anything that must outlive a tick (signal windows, streaks,
    idle counters) belongs to the component that owns it.
    """

    capacity: TickCapacitySnapshot
    # True during warmup: until every critical op has produced output there is
    # no backpressure to react to, so each op is sized to an equal share of
    # the allocation. Turns off for good once the pipeline flows end to end.
    warmup: bool = True
    # The order every phase walks operators in (scarcest first), so grants in
    # scale_how_many line up with placements in scale_where.
    ordered_ops: Tuple["ExperimentalActorPoolMapOperator", ...] = ()


@dataclass
class OpShortfall:
    """How many actors one op asked for and was not granted."""

    num_actors: int
    # True when a rule (silent bottleneck) inferred this want rather than the
    # op asking for it and being clamped. Transfers serve real asks first when
    # donors are scarce.
    silent: bool = False
