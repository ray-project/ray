"""Per-op how_many: what one operator wants from its own state alone.

Growth from feedback is the bottleneck policy's (``policy``). What is left
per op: the equal share an op grows toward before the bottleneck model has
data for it (warmup, and ops the model has not seen yet), shedding actors
once an op has gone idle, and the actors its remaining work can use once
every upstream op has completed."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import (
    SIZER_DOWNSCALE_FACTOR,
    SIZER_DOWNSCALE_HYSTERESIS,
    SIZER_RESERVATION_RATIO,
    SIZER_UTIL_THRESHOLD,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    PlacementGroupSlots,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    FIT_EPS,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    has_produced_output,
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
    """The per-operator wants: equal-share growth before the model has data,
    and idle shedding.

    These deltas are what the operator *wants*; the sizer's grant pass clamps
    them to what the cluster can actually place.
    """

    def __init__(
        self,
        profiles: OpProfiles,
    ):
        self._profiles = profiles
        self.reservation_ratio = SIZER_RESERVATION_RATIO
        self.downscale_factor = SIZER_DOWNSCALE_FACTOR
        self.downscale_hysteresis = SIZER_DOWNSCALE_HYSTERESIS
        self.util_upscaling_threshold = SIZER_UTIL_THRESHOLD
        # Divisor in the backlog target; mirrors the actor autoscaler's guard.
        if self.util_upscaling_threshold <= 0:
            raise ValueError(
                "RAY_DATA_SIZER_UTIL_THRESHOLD must be positive, got "
                f"{self.util_upscaling_threshold}"
            )
        # Consecutive idle ticks per op, for the hysteretic drain arm.
        self._idle_streaks: Dict["PhysicalOperator", BoolStreak] = {}

    # --- warmup: feedforward equal share --------------------------------------

    def warmup_delta(
        self,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
        pg_slots: PlacementGroupSlots,
    ) -> Optional[ActorPoolScalingRequest]:
        """Grow toward this op's warmup share, clamped to [min_size, max_size].

        share = ceiling / per_actor / concurrency, where the ceiling is this
        op's equal share of what it sizes against: the allocation for non-PG
        ops, its PG's bundles for static-PG ops (``_pg_warmup_ceiling``). An
        op whose PG capacity is unknown has nothing to inspect and takes
        ratio x its pool's max size. Either way the ledger then clamps the
        grant to what fits per node / bundle.

        Upscale only: a shrinking share just stops further growth, and the
        delta self-zeroes because serving_size() counts pendings. An op holds
        at min until every upstream actor-pool op has produced output, so a
        stage fed by a tiny, finite upstream never takes a full share.
        """
        if not all(has_produced_output(prev) for prev in self._profiles.prev_ops[op]):
            return None
        pool = op.actor_pool
        target = self.warmup_target(op, cluster_capacity, pg_slots)
        if target is None:
            return None
        delta = target - pool.serving_size()
        if delta > 0:
            return ActorPoolScalingRequest.upscale(
                delta=delta, reason="warmup equal-share"
            )
        return None

    def warmup_target(
        self,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
        pg_slots: PlacementGroupSlots,
    ) -> Optional[int]:
        """This op's warmup share in actors, clamped to [min_size, max_size]
        (see ``warmup_delta``). Also what an op held at zero or min reserves
        (``SizingTicker``).

        A CPU op's share is also capped by memory: GPU ops take their GPU share (and its memory) first, and the
        CPU ops whose actors request memory split what is left equally. A
        CPU-only share alone can pack memory-heavy CPU stages so tightly that
        the GPU stage cannot place its actors.
        """
        pool = op.actor_pool
        share = self._share_by_resource_type(op, cluster_capacity, pg_slots)
        if share is None:
            return None
        memory_share = self._memory_share(op, cluster_capacity, pg_slots)
        if memory_share is not None:
            share = min(share, memory_share)
        # FIT_EPS: float division can land just under a whole actor
        # (0.3 / 0.1 == 2.999...), which int() would truncate away.
        target = int(min(max(share + FIT_EPS, pool.min_size()), pool.max_size()))
        logger.debug(
            "OperatorSizer: warmup %s share=%.1f target=%d current=%d",
            op.name,
            share,
            target,
            pool.serving_size(),
        )
        return target

    def _share_by_resource_type(
        self,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
        pg_slots: PlacementGroupSlots,
    ) -> Optional[float]:
        """The op's equal share of its resource type (CPU or GPU), in actors."""
        pool = op.actor_pool
        if op in pg_slots.unknown_cap_by_op:
            return pg_slots.unknown_cap_by_op[op] * self.reservation_ratio
        resource_type = self._profiles.resource_type(op)
        per_actor = getattr(pool.per_actor_resource_usage(), resource_type)
        if per_actor <= 0:
            # The sizer rejects this shape when built; defensive only.
            return None
        if op in pg_slots.static_bundles_by_op:
            ceiling = self._pg_warmup_ceiling(op, resource_type, pg_slots)
        else:
            ceiling = self._warmup_ceiling(resource_type, cluster_capacity)
        # An actor with max_actor_concurrency=k serves ~k tasks at once, so we
        # need k times fewer actors for the same share.
        concurrency = max(1, pool.max_actor_concurrency())
        return ceiling / per_actor / concurrency

    def _memory_share(
        self,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
        pg_slots: PlacementGroupSlots,
    ) -> Optional[float]:
        """A non-PG CPU op's share of memory, in actors, or None when memory
        does not bound it (a GPU or PG op, or actors that request none).

        usable = memory x ratio - GPU ops' warmup shares' memory - fixed CPU
        pools' memory; split equally among the autoscaled CPU ops whose actors
        request memory.
        """
        if (
            op in pg_slots.unknown_cap_by_op
            or op in pg_slots.static_bundles_by_op
            or self._profiles.resource_type(op) != "cpu"
        ):
            return None
        per_actor_memory = op.actor_pool.per_actor_resource_usage().memory
        if per_actor_memory <= 0:
            return None
        usable = cluster_capacity.memory * self.reservation_ratio
        for gpu_op in self._eligible_ops_by_resource_type("gpu"):
            gpu_share = self._share_by_resource_type(gpu_op, cluster_capacity, pg_slots)
            if gpu_share is None:
                continue
            gpu_pool = gpu_op.actor_pool
            actors = min(
                max(gpu_share + FIT_EPS, gpu_pool.min_size()), gpu_pool.max_size()
            )
            usable -= int(actors) * gpu_pool.per_actor_resource_usage().memory
        sharers = 0
        for cpu_op in self._eligible_ops_by_resource_type("cpu"):
            cpu_pool = cpu_op.actor_pool
            cpu_memory = cpu_pool.per_actor_resource_usage().memory
            if cpu_memory <= 0:
                continue
            if cpu_pool.min_size() == cpu_pool.max_size():
                usable -= cpu_pool.min_size() * cpu_memory
            else:
                sharers += 1
        if sharers == 0:
            return None
        return max(usable, 0.0) / sharers / per_actor_memory

    def _pg_warmup_ceiling(
        self,
        op: "ExperimentalActorPoolMapOperator",
        resource_type: str,
        pg_slots: PlacementGroupSlots,
    ) -> float:
        """``_warmup_ceiling`` over the op's PG bundles instead of the
        allocation, shared with the ops that can land in the same bundles."""
        bundles_by_op = pg_slots.static_bundles_by_op
        op_bundles = set(bundles_by_op[op])
        limit = sum(getattr(pg_slots.shapes[key], resource_type) for key in op_bundles)
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

    # --- after warmup: idle shedding ------------------------------------------

    def shrink_idle_delta(
        self, op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
    ) -> Optional[ActorPoolScalingRequest]:
        """Once the model decides an op's growth, the only per-op decision:
        shed idle actors once the op has had no work (empty input queue,
        nothing in flight) for the full hysteresis, which is starved for the
        whole window in the model's terms. Holds while actors pend. Growth
        comes only from the bottleneck policy."""
        pool = op.actor_pool
        pending = pool.num_pending_actors()
        has_work = (
            op_state.total_enqueued_input_blocks() > 0 or pool.num_tasks_in_flight() > 0
        )
        streak = self._idle_streak(op)
        if pending > 0:
            request, decision = None, f"hold (sizing in flight: {pending} pending)"
        elif has_work:
            streak.reset()
            request, decision = (
                None,
                "hold (has work; growth is the bottleneck policy's)",
            )
        else:
            streak.record(True)
            request, decision = self._drain_if_idle(pool, streak)
        logger.debug(
            "OperatorSizer: how_many %s current=%d -> %s",
            op.name,
            pool.serving_size(),
            decision,
        )
        return request

    def _drain_if_idle(
        self, pool: "AutoscalingActorPool", streak: BoolStreak
    ) -> Tuple[Optional[ActorPoolScalingRequest], str]:
        """Shed idle actors once the pool has been idle for the full streak.

        Over-provisioned pools that are still working are the bottleneck
        policy's problem (it can move that capacity to an op that needs it);
        here we only reclaim actors with nothing to do.
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

    def remaining_work_cap(
        self, op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
    ) -> Optional[int]:
        """Most actors ``op`` can still use, or None while more input can come.

        Once every upstream op has completed, the op's backlog (queued plus
        in-flight inputs) is all the work it has left.
        """
        upstream = op.input_dependencies
        if not upstream or not all(dep.has_completed() for dep in upstream):
            return None
        return max(op.actor_pool.min_size(), self.backlog_actor_target(op, op_state))

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
