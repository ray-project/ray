"""The per-tick facts the bottleneck policy decides on."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, Dict, Optional, Tuple

from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    BundleKey,
)
from ray.data._internal.experimental.execution.sizer.capacity_ledger import (
    ConstraintAwareCapacityLedger,
)
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles
from ray.data._internal.experimental.execution.sizer.policy.states import SizerPhase

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator as EAPMO,
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
    apmo_profiles: OpProfiles
    seconds_since_allocation_change: float
    now: float
    bottleneck: "BottleneckTick" | None = None
    # Nodes each op may run on this tick; intersect with ``free``.
    schedulable_nodes: Dict["EAPMO", AbstractSet[NodeIdStr]] = field(
        default_factory=dict
    )
    # The bundles each static-PG op may use (see ``ClusterStateSnapshot``).
    schedulable_bundles: Dict["EAPMO", Tuple[BundleKey, ...]] = field(
        default_factory=dict
    )
    # The snapshot this context reads, for the per-op rules (warmup shares,
    # the op order). None in tests that only exercise the bottleneck model.
    snapshot: Optional["ClusterStateSnapshot"] = None
    # Ops the bottleneck model has seen a full window of.
    modeled_ops: AbstractSet["EAPMO"] = frozenset()

    @classmethod
    def from_snapshot(
        cls,
        snapshot: "ClusterStateSnapshot",
        *,
        topology: "Topology",
        phase: SizerPhase,
        apmo_profiles: OpProfiles,
        now: float,
        bottleneck: Optional["BottleneckTick"],
        modeled_ops: AbstractSet["EAPMO"] = frozenset(),
    ) -> "DecisionContext":
        """This tick's context over ``snapshot`` plus the sizer's own state,
        with a fresh ledger over the snapshot's free capacity."""
        return cls(
            topology=topology,
            phase=phase,
            capacity=ConstraintAwareCapacityLedger(snapshot),
            apmo_profiles=apmo_profiles,
            seconds_since_allocation_change=snapshot.seconds_since_allocation_change,
            now=now,
            bottleneck=bottleneck,
            schedulable_nodes=snapshot.schedulable_nodes_by_op,
            schedulable_bundles=snapshot.schedulable_bundles_by_op,
            snapshot=snapshot,
            modeled_ops=modeled_ops,
        )

    @property
    def warmup(self) -> bool:
        return self.phase is SizerPhase.WARMUP

    def is_pg_managed(self, op: "EAPMO") -> bool:
        return op not in self.apmo_profiles.non_pg_ops
