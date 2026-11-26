"""The per-tick facts the sizing policy decides on."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

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
        Snapshot,
    )
    from ray.data._internal.experimental.execution.sizer.models import (
        ClusterStateSnapshot,
    )


@dataclass
class DecisionContext:
    """One tick's inputs for the sizing policy.

    Every upscale checks and spends free capacity through ``capacity``, the
    tick's one ledger.
    """

    topology: "Topology"
    phase: SizerPhase
    capacity: ConstraintAwareCapacityLedger
    apmo_profiles: OpProfiles
    # The cluster this tick: warmup limits, the op order, allocation changes.
    snapshot: "ClusterStateSnapshot"
    # The sizer's clock this tick; a downscale's cooldown starts from it.
    now: float
    # This tick's bottleneck snapshot, or None before it has a full window.
    bottleneck_snapshot: Optional["Snapshot[EAPMO]"] = None

    @classmethod
    def from_snapshot(
        cls,
        *,
        snapshot: "ClusterStateSnapshot",
        topology: "Topology",
        phase: SizerPhase,
        apmo_profiles: OpProfiles,
        now: float,
        bottleneck_snapshot: Optional["Snapshot[EAPMO]"],
    ) -> "DecisionContext":
        """This tick's context over ``snapshot`` plus the sizer's own state,
        with a fresh ledger over the snapshot's free capacity."""
        return cls(
            topology=topology,
            phase=phase,
            capacity=ConstraintAwareCapacityLedger(snapshot),
            apmo_profiles=apmo_profiles,
            snapshot=snapshot,
            now=now,
            bottleneck_snapshot=bottleneck_snapshot,
        )
