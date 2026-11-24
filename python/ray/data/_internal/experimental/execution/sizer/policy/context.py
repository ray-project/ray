"""The per-tick facts the bottleneck policy decides on."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, Dict, Optional, Sequence, Tuple

from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    BundleKey,
)
from ray.data._internal.experimental.execution.sizer.capacity_ledger import (
    ConstraintAwareCapacityLedger,
)
from ray.data._internal.experimental.execution.sizer.policy.states import SizerPhase

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
        BottleneckTick,
    )
    from ray.data._internal.experimental.execution.sizer.models import (
        ClusterStateSnapshot,
    )


@dataclass
class DecisionContext:
    """One tick's inputs for the sizing policy.

    Every decision checks and spends free capacity through ``capacity``, the
    tick's one ledger. Holding an operator goes through ``OpStatusMachine``,
    not this context.
    """

    topology: "Topology"
    phase: SizerPhase
    capacity: ConstraintAwareCapacityLedger
    # Every actor-pool op, and the subset the sizer places itself (not
    # PG-managed). Moving actors between ops only works for the latter.
    actor_pool_ops: Sequence["ExperimentalActorPoolMapOperator"]
    non_pg_ops: Sequence["ExperimentalActorPoolMapOperator"]
    seconds_since_allocation_change: float
    now: float
    # This tick's bottleneck model, or None before the observer has one.
    bottleneck: Optional["BottleneckTick"] = None
    # Nodes each op may run on this tick; intersect with ``free``.
    schedulable_nodes: Dict[
        "ExperimentalActorPoolMapOperator", AbstractSet[NodeIdStr]
    ] = field(default_factory=dict)
    # The bundles each static-PG op may use (see ``ClusterStateSnapshot``).
    schedulable_bundles: Dict[
        "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
    ] = field(default_factory=dict)
    # Nearest downstream actor-pool ops of each op.
    next_ops: Dict[
        "ExperimentalActorPoolMapOperator",
        Tuple["ExperimentalActorPoolMapOperator", ...],
    ] = field(default_factory=dict)
    # The snapshot this context reads, for the per-op rules (warmup shares,
    # the op order). None in tests that only exercise the bottleneck model.
    snapshot: Optional["ClusterStateSnapshot"] = None
    # Ops the bottleneck model has seen a full window of.
    modeled_ops: AbstractSet["ExperimentalActorPoolMapOperator"] = frozenset()

    @classmethod
    def from_snapshot(
        cls,
        snapshot: "ClusterStateSnapshot",
        *,
        topology: "Topology",
        phase: SizerPhase,
        actor_pool_ops: Sequence["ExperimentalActorPoolMapOperator"],
        non_pg_ops: Sequence["ExperimentalActorPoolMapOperator"],
        now: float,
        bottleneck: Optional["BottleneckTick"],
        next_ops: Dict[
            "ExperimentalActorPoolMapOperator",
            Tuple["ExperimentalActorPoolMapOperator", ...],
        ],
        modeled_ops: AbstractSet["ExperimentalActorPoolMapOperator"] = frozenset(),
    ) -> "DecisionContext":
        """This tick's context over ``snapshot`` plus the sizer's own state,
        with a fresh ledger over the snapshot's free capacity."""
        return cls(
            topology=topology,
            phase=phase,
            capacity=ConstraintAwareCapacityLedger(snapshot),
            actor_pool_ops=actor_pool_ops,
            non_pg_ops=non_pg_ops,
            seconds_since_allocation_change=snapshot.seconds_since_allocation_change,
            now=now,
            bottleneck=bottleneck,
            schedulable_nodes=snapshot.schedulable_nodes_by_op,
            schedulable_bundles=snapshot.schedulable_bundles_by_op,
            next_ops=next_ops,
            snapshot=snapshot,
            modeled_ops=modeled_ops,
        )

    @property
    def warmup(self) -> bool:
        return self.phase is SizerPhase.WARMUP

    def is_pg_managed(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        return op not in self.non_pg_ops
