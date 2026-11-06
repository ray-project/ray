"""Grow a bottleneck that cannot ask for itself."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from typing_extensions import override

from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.metrics import (
    OptimizerMetrics,
)
from ray.data._internal.experimental.execution.sizer.models import (
    OpShortfall,
    PlacedSizingRequest,
)
from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)
from ray.data._internal.experimental.execution.sizer.optimizer.rule import (
    OptimizationRule,
)
from ray.data._internal.experimental.execution.sizer.optimizer.utils import (
    fill_free_nodes,
    fill_pg_bundles,
    free_schedulable_nodes,
    upscale_request,
)
from ray.data._internal.experimental.execution.sizer.scaling_guard import OpScalingGuard

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
    )

logger = logging.getLogger(__name__)

_REASON = "optimizer: silent bottleneck grant"


class SilentBottleneckGrant(OptimizationRule[None]):
    """Grow a silent bottleneck: an op that limits the pipeline but never asks
    to grow.

    scale_how_many never grows a backpressured op, since more actors would
    only pile up output. But backpressure can come from the op's own per-actor
    output limits while its consumers are idle and waiting. Then the op is the
    bottleneck and nothing asks for it to grow.

    The op grows by one growth step (as in scale_how_many): from free
    capacity, or from its PG headroom if it is PG-managed. For a non-PG op,
    what free capacity can't cover is recorded as a shortfall, so
    CapacityTransfer can take it from donors on a later tick.
    """

    name = "silent_bottleneck"

    def __init__(
        self,
        metrics: OptimizerMetrics,
        guard: OpScalingGuard,
        placement: "ActorPlacementStrategy",
    ):
        self._metrics = metrics
        self._guard = guard
        self._placement = placement

    @override
    def apply(
        self, ctx: OptimizationContext, state: None = None
    ) -> List[PlacedSizingRequest]:
        if ctx.warmup:
            self._metrics.record_refusal(self.name, "warmup")
            return []
        requests: List[PlacedSizingRequest] = []
        for op in ctx.actor_pool_ops:
            if not self._guard.can_scale(op) or op.actor_pool.num_pending_actors() > 0:
                continue
            if not self._is_silent_bottleneck(op, ctx):
                continue
            delta = ctx.growth_delta[op]
            if ctx.is_pg_managed(op):
                request = self._grow_pg_op(op, delta, ctx)
            else:
                request = self._grow_on_free_nodes(op, delta, ctx)
            if request is None:
                self._metrics.record_refusal(self.name, "no_free_capacity")
                continue
            requests.append(request)
            self._metrics.record_outcome(self.name, op.name, "granted")
            logger.debug(
                "PipelineOptimizer: silent bottleneck grant +%d %s",
                request.request[op].delta,
                op.name,
            )
        return requests

    def _is_silent_bottleneck(
        self, op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> bool:
        # A silent bottleneck, held for the whole signal window: every actor
        # busy, output backpressured, and every next actor-pool op keeping up.
        # Plus room to grow (growth_delta > 0).
        consumers = ctx.next_ops.get(op, ())
        return (
            ctx.growth_delta.get(op, 0) > 0
            and bool(consumers)
            and self._every_actor_busy(op, ctx)
            and self._is_backpressured(op, ctx)
            and all(self._keeps_up(consumer, ctx) for consumer in consumers)
        )

    @staticmethod
    def _every_actor_busy(
        op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> bool:
        """At every sample, tasks in flight >= actors, i.e. about one task per
        actor.

        We don't require every concurrency slot to be full. A backpressured
        actor gets no new tasks until its outputs are consumed, so its slots
        keep emptying and refilling, even when it is the bottleneck.
        """
        busy = ctx.busy_tasks_signal.get(op)
        fewest_tasks = busy.minimum() if busy is not None else None
        return fewest_tasks is not None and fewest_tasks >= op.actor_pool.serving_size()

    @staticmethod
    def _is_backpressured(
        op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> bool:
        streak = ctx.backpressured_streak.get(op)
        return streak is not None and streak.is_streak()

    @staticmethod
    def _keeps_up(
        consumer: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> bool:
        """A consumer keeps up when it takes the producer's output as fast as
        it arrives, so it isn't what limits the pipeline. All must hold:

        - its input queue is at most one block per actor;
        - it has no shortfall and no pending actors (not short, not resizing);
        - its own output is not backpressured for the whole window. Sinks skip
          this: they have no downstream to be backpressured by.
        """
        state = ctx.topology.get(consumer)
        if state is None:
            return False
        pool = consumer.actor_pool
        if pool.num_pending_actors() > 0 or consumer in ctx.shortfalls:
            return False
        if consumer.output_dependencies:
            clear = ctx.not_backpressured_streak.get(consumer)
            if clear is None or not clear.is_streak():
                return False
        return state.total_enqueued_input_blocks() <= max(1, pool.serving_size())

    def _grow_on_free_nodes(
        self,
        op: "ExperimentalActorPoolMapOperator",
        delta: int,
        ctx: OptimizationContext,
    ) -> Optional[PlacedSizingRequest]:
        # Draining actors still hold their node's resources, so reviving them
        # is free; only the remainder needs room on free nodes.
        num_to_reclaim = min(delta, op.actor_pool.num_reclaimable_draining_actors())
        # Fill nodes in the order the locality strategy ranks them, as
        # place_upscale and CapacityTransfer do, so the new actors land near
        # the producers and consumers this op exchanges data with.
        view = placement_view.build_op_placement_view(op, rank_victims=False)
        nodes = sorted(
            free_schedulable_nodes(ctx, op),
            key=lambda node: self._placement.rank_node(view, node),
        )
        targets = fill_free_nodes(ctx, op, delta - num_to_reclaim, nodes)
        num_granted = num_to_reclaim + len(targets)
        if num_granted < delta:
            # scale_how_many holds a backpressured op, so it never records its
            # own shortfall. Record what free capacity couldn't cover, so
            # CapacityTransfer can pick this op as a recipient.
            ctx.silent_shortfalls[op] = OpShortfall(
                num_actors=delta - num_granted, silent=True
            )
        if num_granted == 0:
            return None
        return upscale_request(
            op,
            delta=num_granted,
            reason=_REASON,
            target_nodes=tuple(targets),
            num_to_reclaim=num_to_reclaim,
        )

    @staticmethod
    def _grow_pg_op(
        op: "ExperimentalActorPoolMapOperator", delta: int, ctx: OptimizationContext
    ) -> Optional[PlacedSizingRequest]:
        # The PG places the actors. Draining actors revive into bundles they
        # still hold; only new ones need room in the PG.
        num_to_reclaim = min(delta, op.actor_pool.num_reclaimable_draining_actors())
        num_new = fill_pg_bundles(ctx, op, delta - num_to_reclaim)
        if num_to_reclaim + num_new <= 0:
            return None
        return upscale_request(
            op,
            delta=num_to_reclaim + num_new,
            reason=_REASON,
            num_to_reclaim=num_to_reclaim,
        )
