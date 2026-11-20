"""The per-tick facts the bottleneck policy decides on."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, Dict, Optional, Sequence, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.sizer.pg_bundles import BundleKey
from ray.data._internal.experimental.execution.sizer.policy.states import SizerPhase

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
        BottleneckTick,
    )


@dataclass
class DecisionContext:
    """One tick's inputs for the bottleneck policy: data only, no callbacks.

    The policy writes back debits to ``free`` / ``pg_free`` /
    ``unknown_pg_headroom`` for the capacity it claims. Those maps are the
    tick snapshot's own, so the how-many pass later in the tick sees the
    debits. Holding an operator goes through ``OpLifecycles``, not this
    context.
    """

    topology: "Topology"
    phase: SizerPhase
    free: Dict[NodeIdStr, ExecutionResources]
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
    # Static-PG ops spend their PG's bundles (each like a node); ops whose PG
    # capacity is unknown, a plain count (see ``TickCapacitySnapshot``).
    pg_free: Dict[BundleKey, ExecutionResources] = field(default_factory=dict)
    schedulable_bundles: Dict[
        "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
    ] = field(default_factory=dict)
    unknown_pg_headroom: Dict["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )
    # Most actors each op may grow to: its pool's bounded max size, or, once
    # every upstream op has completed, the actors its remaining work can use.
    # An op missing here is bounded by its pool alone.
    growth_ceiling: Dict["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )
    # Nearest downstream actor-pool ops of each op.
    next_ops: Dict[
        "ExperimentalActorPoolMapOperator",
        Tuple["ExperimentalActorPoolMapOperator", ...],
    ] = field(default_factory=dict)

    @property
    def warmup(self) -> bool:
        return self.phase is SizerPhase.WARMUP

    def is_pg_managed(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        return op not in self.non_pg_ops

    def claim_free(
        self, node: NodeIdStr, per_actor: ExecutionResources, count: int
    ) -> None:
        """Debit ``count`` actors of ``per_actor`` off ``node``, so later
        decisions and the how-many pass this tick don't hand the same capacity
        out."""
        self.free[node] = (
            self.free[node]
            .subtract(per_actor.scale(count))
            .max(ExecutionResources.zero())
        )
