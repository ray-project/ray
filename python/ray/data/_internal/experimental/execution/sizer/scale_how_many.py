"""Phase 2 policy: how many actors one operator wants, from its own signals."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import (
    SIZER_DOWNSCALE_FACTOR,
    SIZER_DOWNSCALE_HYSTERESIS,
    SIZER_OUTQUEUE_FULL_FRACTION,
    SIZER_RESERVATION_RATIO,
    SIZER_UPSCALE_FACTOR,
    SIZER_UTIL_THRESHOLD,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    FIT_EPS,
)
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles
from ray.data._internal.experimental.execution.sizer.pg_bundles import (
    PlacementGroupBundles,
)
from ray.data._internal.experimental.execution.sizer.signals import BoolStreak

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import AutoscalingActorPool
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.streaming_executor_state import OpState
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


class OpSizingPolicy:
    """The per-operator greedy scaling decision.

    Two regimes. Before any output exists there is no feedback to act on, so
    warmup hands every operator an equal share of the allocation. Once the
    pipeline flows, sizing follows backpressure: grow what has work and is not
    backpressured, drain what has gone idle, and leave everything else alone.

    These deltas are what the operator *wants*; the sizer's grant pass clamps
    them to what the cluster can actually place.
    """

    def __init__(
        self,
        profiles: OpProfiles,
        *,
        pg_bundles: Optional[PlacementGroupBundles] = None,
        unknown_pg_cap_by_op: Optional[
            Dict["ExperimentalActorPoolMapOperator", int]
        ] = None,
    ):
        self._profiles = profiles
        # PG-managed ops warm up against their placement group, not the
        # allocation: static-PG ops against their bundles, ops whose PG
        # capacity is unknown against their pool's max size.
        self._pg_bundles = pg_bundles or PlacementGroupBundles()
        self._unknown_pg_cap_by_op = dict(unknown_pg_cap_by_op or {})
        self.reservation_ratio = SIZER_RESERVATION_RATIO
        self.upscale_factor = SIZER_UPSCALE_FACTOR
        self.downscale_factor = SIZER_DOWNSCALE_FACTOR
        self.downscale_hysteresis = SIZER_DOWNSCALE_HYSTERESIS
        self.outqueue_full_fraction = SIZER_OUTQUEUE_FULL_FRACTION
        self.util_upscaling_threshold = SIZER_UTIL_THRESHOLD
        # Divisor in the upscale target; mirrors the actor autoscaler's guard.
        if self.util_upscaling_threshold <= 0:
            raise ValueError(
                "RAY_DATA_SIZER_UTIL_THRESHOLD must be positive, got "
                f"{self.util_upscaling_threshold}"
            )
        # Consecutive idle ticks per op, for the hysteretic drain arm.
        self._idle_streaks: Dict["PhysicalOperator", BoolStreak] = {}

    def is_output_backpressured(
        self, op: "ExperimentalActorPoolMapOperator"
    ) -> Tuple[bool, int, int]:
        """Is this op's output backpressured? -> (backpressured, blocked, running).

        Denominated in RUNNING actors, so idle actors count as unblocked
        capacity instead of vanishing from the denominator -- a
        task-denominated fraction reads 1.0 whenever every busy actor is
        blocked, however idle the pool is.

        Both the warm loop and the optimizer's signal observer read this, so
        they judge backpressure by the same number.
        """
        blocked, running = op.output_backpressured_actors()
        if running == 0:
            return False, blocked, running
        return (blocked / running) >= self.outqueue_full_fraction, blocked, running

    def max_growth_step(self, op: "ExperimentalActorPoolMapOperator") -> int:
        """The most this op may grow in one tick."""
        pool = op.actor_pool
        return max(1, math.ceil(self.upscale_factor * pool.serving_size()))

    def growth_delta(
        self, op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
    ) -> int:
        """How many actors ``op`` should add this tick: one growth step, capped
        at its demand and its max size. Shared with the optimizer's silent
        bottleneck rule, so both grow an op by the same amount."""
        return self._step_toward(op, demand=self.backlog_actor_target(op, op_state))

    def _step_toward(
        self, op: "ExperimentalActorPoolMapOperator", *, demand: int
    ) -> int:
        current = op.actor_pool.serving_size()
        return max(
            0,
            min(
                self.max_growth_step(op),
                demand - current,
                self._profiles.pool_cap(op) - current,
            ),
        )

    # --- warmup: feedforward equal share --------------------------------------

    def warmup_delta(
        self,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
    ) -> Optional[ActorPoolScalingRequest]:
        """Grow toward this op's warmup share, clamped to [min_size, max_size].

        share = ceiling / per_actor / concurrency, where the ceiling is this
        op's equal share of what it sizes against: the allocation for non-PG
        ops, its PG's bundles for static-PG ops (``_pg_warmup_ceiling``). An
        op whose PG capacity is unknown has nothing to inspect and takes
        ratio x its pool's max size. Either way the ledger then clamps the
        grant to what fits per node / bundle.

        Upscale only: a shrinking share just stops further growth, and the
        delta self-zeroes because serving_size() counts pendings.
        """
        pool = op.actor_pool
        if op in self._unknown_pg_cap_by_op:
            share = self._unknown_pg_cap_by_op[op] * self.reservation_ratio
        else:
            resource_type = self._profiles.resource_type(op)
            per_actor = getattr(pool.per_actor_resource_usage(), resource_type)
            if per_actor <= 0:
                # The sizer rejects this shape when built; defensive only.
                return None
            if op in self._pg_bundles.bundles_by_op:
                ceiling = self._pg_warmup_ceiling(op, resource_type)
            else:
                ceiling = self._warmup_ceiling(resource_type, cluster_capacity)
            # An actor with max_actor_concurrency=k serves ~k tasks at once, so
            # we need k times fewer actors for the same share.
            concurrency = max(1, pool.max_actor_concurrency())
            share = ceiling / per_actor / concurrency
        # FIT_EPS: float division can land just under a whole actor
        # (0.3 / 0.1 == 2.999...), which int() would truncate away.
        target = int(min(max(share + FIT_EPS, pool.min_size()), pool.max_size()))
        delta = target - pool.serving_size()
        logger.debug(
            "OperatorSizer: warmup %s share=%.1f target=%d current=%d delta=%d",
            op.name,
            share,
            target,
            pool.serving_size(),
            max(delta, 0),
        )
        if delta > 0:
            return ActorPoolScalingRequest.upscale(
                delta=delta, reason="warmup equal-share"
            )
        return None

    def _pg_warmup_ceiling(
        self, op: "ExperimentalActorPoolMapOperator", resource_type: str
    ) -> float:
        """``_warmup_ceiling`` over the op's PG bundles instead of the
        allocation, shared with the ops that can land in the same bundles."""
        bundles_by_op = self._pg_bundles.bundles_by_op
        op_bundles = set(bundles_by_op[op])
        limit = sum(
            getattr(self._pg_bundles.shapes[key], resource_type) for key in op_bundles
        )
        sharers = [
            other
            for other, keys in bundles_by_op.items()
            if op_bundles.intersection(keys)
            and not other.has_execution_finished()
            and self._profiles.resource_type(other) == resource_type
        ]
        return self._equal_share(limit, sharers, resource_type)

    def _warmup_ceiling(
        self, resource_type: str, cluster_capacity: ExecutionResources
    ) -> float:
        """One non-PG op's share: (capacity - floors) * ratio / autoscaled ops.

        Recomputed every call so the share follows a cluster that is still
        ramping. Every eligible op's floor comes off the top, fixed pools
        included, but only autoscaled ops divide the remainder -- a fixed pool
        never consumes a share. PG ops are left out: they size against their
        placement group, not this allocation.
        """
        return self._equal_share(
            getattr(cluster_capacity, resource_type),
            self._eligible_ops_by_resource_type(resource_type),
            resource_type,
        )

    def _equal_share(
        self,
        limit: float,
        ops: List["ExperimentalActorPoolMapOperator"],
        resource_type: str,
    ) -> float:
        """(limit - every op's floor) * ratio / autoscaled ops."""
        min_reserved = 0.0
        autoscaled_count = 0
        for op in ops:
            min_resources, _ = op.min_max_resource_requirements()
            min_reserved += getattr(min_resources, resource_type)
            pool = op.actor_pool
            if pool.min_size() != pool.max_size():
                autoscaled_count += 1
        if autoscaled_count == 0:
            return 0.0
        return (
            max(limit - min_reserved, 0.0) * self.reservation_ratio / autoscaled_count
        )

    def _eligible_ops_by_resource_type(
        self, resource_type: str
    ) -> List["ExperimentalActorPoolMapOperator"]:
        return [
            op
            for op in self._profiles.non_pg_ops
            if not op.has_execution_finished()
            and self._profiles.resource_type(op) == resource_type
        ]

    # --- warm: backpressure-driven --------------------------------------------

    def warm_delta(
        self,
        op: "ExperimentalActorPoolMapOperator",
        op_state: "OpState",
        backpressured_with_demand: Set["ExperimentalActorPoolMapOperator"],
    ) -> Optional[ActorPoolScalingRequest]:
        """Size one operator from its queues. States are checked in this order,
        first match wins:

        | pendings | has work | output backpressured | and              | action    |
        |----------|----------|----------------------|------------------|-----------|
        | yes      |    -     |          -           |                  | hold      |
        | no       |   no     |          -           | idle for N ticks | downscale |
        | no       |   no     |          -           | idle < N ticks   | hold      |
        | no       |   yes    |         yes          |                  | hold      |
        | no       |   yes    |         no           | below demand     | upscale   |
        | no       |   yes    |         no           | at demand or max | hold      |

        "Has work" means a non-empty input queue OR tasks in flight: a steady
        pool can have an empty queue and still be busy, and draining it
        mid-flight would be wrong. Upscales are bounded by demand, which is
        load-bearing -- at cold start the output queue is empty, so unbounded
        growth would fill the cluster before any backpressure appeared.
        Downscales are hysteretic, never breach min_size, and take only
        pending/idle actors; busy ones leave by attrition.

        An op backpressured while below its demand is reported through
        ``backpressured_with_demand``: neither satisfied nor ungranted, which
        the optimizer treats as no evidence either way.
        """
        pool = op.actor_pool
        cur = pool.serving_size()
        pending = pool.num_pending_actors()
        enqueued = op_state.total_enqueued_input_blocks()
        has_work = enqueued > 0 or pool.num_tasks_in_flight() > 0
        backpressured, blocked, running = self.is_output_backpressured(op)
        demand = self.backlog_actor_target(op, op_state)
        streak = self._idle_streak(op)

        if pending > 0:
            # Mid-resize: don't undo an upscale on the tick its actors land.
            request, decision = None, f"hold (sizing in flight: {pending} pending)"
        elif not has_work:
            streak.record(True)
            request, decision = self._drain_if_idle(pool, streak)
        elif backpressured:
            # Growing an op its consumers cannot drain just buffers memory.
            # A busy op whose consumers ARE keeping up is the optimizer's
            # silent-bottleneck case, not ours.
            streak.reset()
            request, decision = None, "hold (output backpressured)"
            if demand > cur:
                backpressured_with_demand.add(op)
        else:
            streak.reset()
            request, decision = self._grow_toward_demand(op, demand=demand)

        logger.debug(
            "OperatorSizer: warm %s in_q=%d out_blocked=%d/%d demand=%d "
            "current=%d -> %s",
            op.name,
            enqueued,
            blocked,
            running,
            demand,
            cur,
            decision,
        )
        return request

    def _drain_if_idle(
        self, pool: "AutoscalingActorPool", streak: BoolStreak
    ) -> Tuple[Optional[ActorPoolScalingRequest], str]:
        """Shed idle actors once the pool has been idle for the full streak.

        Over-provisioned pools that are still working are the optimizer's
        problem (it can move that capacity to an op that needs it); here we
        only reclaim actors with nothing to do.
        """
        if streak.is_streak() and pool.serving_size() > pool.min_size():
            streak.reset()
            return (
                ActorPoolScalingRequest.downscale(
                    delta=-self._down_delta(pool), reason="drain (idle)"
                ),
                "downscale (drain: idle)",
            )
        return None, (
            f"hold (idle, streak {streak.run_length}/{self.downscale_hysteresis})"
        )

    def _grow_toward_demand(
        self, op: "ExperimentalActorPoolMapOperator", *, demand: int
    ) -> Tuple[Optional[ActorPoolScalingRequest], str]:
        """Has work and is not backpressured: grow toward demand."""
        step = self._step_toward(op, demand=demand)
        if step > 0:
            return (
                ActorPoolScalingRequest.upscale(
                    delta=step, reason="has work, output not backpressured"
                ),
                f"want +{step} (demand={demand})",
            )
        if demand <= op.actor_pool.serving_size():
            return None, f"hold (at demand={demand})"
        return None, "hold (at max size)"

    def backlog_actor_target(
        self, op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
    ) -> int:
        """Actors needed to work through what this op currently has.

        Counts in-flight tasks as well as the queue: a well-sized steady-state
        pool has a shallow queue but deep in-flight work, and demand that
        collapsed the moment the queue drained would sawtooth. Divided by what
        one actor can run concurrently at the target utilization.
        """
        pool = op.actor_pool
        avg = op.metrics.average_num_inputs_per_task or 1
        enqueued = op_state.total_enqueued_input_blocks()
        expected = math.ceil(enqueued / avg)
        total_tasks = pool.num_tasks_in_flight() + expected
        return math.ceil(
            total_tasks / (pool.max_actor_concurrency() * self.util_upscaling_threshold)
        )

    def _idle_streak(self, op: "ExperimentalActorPoolMapOperator") -> BoolStreak:
        if op not in self._idle_streaks:
            self._idle_streaks[op] = BoolStreak(self.downscale_hysteresis)
        return self._idle_streaks[op]

    def _down_delta(self, pool: "AutoscalingActorPool") -> int:
        """How many actors one downscale may take, never below min_size."""
        return min(
            max(1, int(self.downscale_factor * pool.serving_size())),
            pool.serving_size() - pool.min_size(),
        )
