"""Cross-operator sizing corrections, applied as registered rules."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, AbstractSet, Any, Callable, Dict, List, Sequence

from ray.data._internal.experimental.execution.sizer.metrics import (
    OptimizerMetrics,
)
from ray.data._internal.experimental.execution.sizer.models import (
    OpShortfall,
    PlacedSizingRequest,
    TickCapacitySnapshot,
)
from ray.data._internal.experimental.execution.sizer.optimizer.capacity_transfer import (  # noqa: E501
    CapacityTransfer,
)
from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)
from ray.data._internal.experimental.execution.sizer.optimizer.rule import (
    OptimizationRule,
)
from ray.data._internal.experimental.execution.sizer.optimizer.signal_observer import (
    BackpressureProbe,
    SignalObserver,
)
from ray.data._internal.experimental.execution.sizer.optimizer.silent_bottleneck import (  # noqa: E501
    SilentBottleneckGrant,
)
from ray.data._internal.experimental.execution.sizer.scaling_guard import OpScalingGuard

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
    )
    from ray.data._internal.experimental.execution.sizer.op_profile import (
        OpProfiles,
    )

logger = logging.getLogger(__name__)


class PipelineOptimizer:
    """The sizer's own passes judge one operator at a time. Anything that
    needs the whole topology -- moving capacity between operators, growing an
    operator that cannot ask for itself -- lives here. The optimizer holds
    what persists across ticks (observed signals, per-rule state) so the rules
    themselves stay stateless."""

    def __init__(
        self,
        dataset_id: str,
        placement: "ActorPlacementStrategy",
        clock: Callable[[], float],
        is_output_backpressured: BackpressureProbe,
        guard: OpScalingGuard,
    ):
        self._clock = clock
        self._metrics = OptimizerMetrics(dataset_id)
        self._signals = SignalObserver(is_output_backpressured)
        self._rules: List[OptimizationRule] = []
        # Each rule's own state type; the optimizer only carries it.
        self._rule_state: Dict[str, Any] = {}
        self.register(
            CapacityTransfer(
                self._metrics, placement, guard, reset_signals=self._signals.reset
            )
        )
        self.register(SilentBottleneckGrant(self._metrics, guard, placement))

    def register(self, rule: OptimizationRule) -> None:
        self._rules.append(rule)
        self._rule_state[rule.name] = rule.new_state()

    # --- observation ------------------------------------------------------

    def observe(self, ops: Sequence["ExperimentalActorPoolMapOperator"]) -> None:
        """Sample per-op signals; cheap to call every scheduling-loop step."""
        self._signals.observe(ops)

    def record_sizing_results(
        self,
        ordered_ops: Sequence["ExperimentalActorPoolMapOperator"],
        ungranted_ops: AbstractSet["ExperimentalActorPoolMapOperator"],
        shortfalls: Dict["ExperimentalActorPoolMapOperator", OpShortfall],
        backpressured_with_demand: AbstractSet[
            "ExperimentalActorPoolMapOperator"
        ] = frozenset(),
    ) -> None:
        """Report what one sizing pass granted, for the shortfall streaks."""
        self._signals.record_sizing_results(
            ordered_ops, ungranted_ops, shortfalls, backpressured_with_demand
        )

    # --- optimization -----------------------------------------------------

    def optimize_pipeline(
        self,
        topology: "Topology",
        *,
        warmup: bool,
        capacity: TickCapacitySnapshot,
        profiles: "OpProfiles",
        growth_delta: Dict["ExperimentalActorPoolMapOperator", int],
        seconds_since_allocation_change: float,
    ) -> List[PlacedSizingRequest]:
        """Advance in-flight work, then collect this tick's proposals.

        Rules run one at a time and share the OpScalingGuard, so an operator a
        rule claims is off-limits to the rules after it -- and to the sizer's
        own warm loop later in the same tick.
        """
        ctx = OptimizationContext(
            topology=topology,
            warmup=warmup,
            # Shared, not copied (here and the two PG maps below): rules debit
            # what they claim, so the per-op passes later this tick (which copy
            # from the same snapshot) don't hand it out again.
            free=capacity.free_by_node,
            pg_free=capacity.pg_free_by_bundle,
            schedulable_bundles=capacity.schedulable_bundles_by_op,
            unknown_pg_headroom=capacity.unknown_pg_headroom_by_op,
            actor_pool_ops=profiles.actor_pool_ops,
            non_pg_ops=profiles.non_pg_ops,
            schedulable_nodes=capacity.schedulable_nodes_by_op,
            growth_delta=growth_delta,
            next_ops=profiles.next_ops,
            seconds_since_allocation_change=seconds_since_allocation_change,
            now=self._clock(),
            **self._signals.signal_dict(),  # type: ignore[arg-type]
        )
        for rule in self._rules:
            rule.advance(ctx, self._rule_state[rule.name])
        requests: List[PlacedSizingRequest] = []
        for rule in self._rules:
            requests.extend(rule.apply(ctx, self._rule_state[rule.name]))
        self._signals.merge_silent_shortfalls(ctx.silent_shortfalls)
        return requests
