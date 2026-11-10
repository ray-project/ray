"""The per-tick facts every optimizer rule reads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, Dict, Sequence, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.sizer.models import OpShortfall
from ray.data._internal.experimental.execution.sizer.pg_bundles import BundleKey
from ray.data._internal.experimental.execution.sizer.signals import (
    BoolStreak,
    WindowedSignal,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )


@dataclass
class OptimizationContext:
    """One tick's inputs for the optimizer rules: data only, no callbacks.

    Rules write back two things: ``silent_shortfalls``, and debits to
    ``free`` / ``pg_free`` / ``unknown_pg_headroom`` for the capacity they
    claim. Those maps are the tick snapshot's own, so the per-op passes later
    in the tick see the debits. Blocking an operator goes through the shared
    OpScalingGuard, not this context.
    """

    # On the context because rules inspect their operators' consumers (queue
    # depth, pool state) to judge whether growing is worthwhile.
    topology: "Topology"
    warmup: bool
    free: Dict[NodeIdStr, ExecutionResources]
    # Every actor-pool op, and the subset the sizer places itself (not
    # PG-managed). Moving actors between ops only works for the latter.
    actor_pool_ops: Sequence["ExperimentalActorPoolMapOperator"]
    non_pg_ops: Sequence["ExperimentalActorPoolMapOperator"]
    # True once an op has asked to grow and been granted zero for a full
    # SIZER_SHORTFALL_TICKS run of sizing ticks.
    shortfall_streak: Dict["ExperimentalActorPoolMapOperator", BoolStreak]
    # Last sizing pass's ungranted ask per op.
    shortfalls: Dict["ExperimentalActorPoolMapOperator", OpShortfall]
    # Tasks in flight per op, one sample per scheduling-loop step.
    busy_tasks_signal: Dict["ExperimentalActorPoolMapOperator", WindowedSignal]
    # Tasks in flight minus output-blocked tasks per op.
    productive_tasks_signal: Dict["ExperimentalActorPoolMapOperator", WindowedSignal]
    # Whether the op's output read backpressured at every sample of the window.
    backpressured_streak: Dict["ExperimentalActorPoolMapOperator", BoolStreak]
    # Whether the op's output read not backpressured at every sample.
    not_backpressured_streak: Dict["ExperimentalActorPoolMapOperator", BoolStreak]
    seconds_since_allocation_change: float
    now: float
    # Nodes each op may run on this tick; rules intersect with ``free``.
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
    # How many actors each op should add in one tick (see
    # ``OpSizingPolicy.growth_delta``).
    growth_delta: Dict["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )
    # Nearest downstream actor-pool ops of each op.
    next_ops: Dict[
        "ExperimentalActorPoolMapOperator",
        Tuple["ExperimentalActorPoolMapOperator", ...],
    ] = field(default_factory=dict)
    # Wants a rule could not fill from free capacity. The optimizer merges
    # these into the next sizing pass so they build a shortfall streak and can
    # qualify the op for a capacity transfer.
    silent_shortfalls: Dict["ExperimentalActorPoolMapOperator", OpShortfall] = field(
        default_factory=dict
    )

    def is_pg_managed(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        return op not in self.non_pg_ops

    def claim_free(
        self, node: NodeIdStr, per_actor: ExecutionResources, count: int
    ) -> None:
        """Debit ``count`` actors of ``per_actor`` off ``node``, so later rules
        and the per-op passes this tick don't hand the same capacity out."""
        self.free[node] = (
            self.free[node]
            .subtract(per_actor.scale(count))
            .max(ExecutionResources.zero())
        )
