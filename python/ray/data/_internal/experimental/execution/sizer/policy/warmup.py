"""Warmup: each op's equal share of the allocation, in actors.

Until the bottleneck observer has measured an op, there is no feedback to step it
by, so it upscales toward an equal share of what it sizes against: the
dataset's allocation for most ops, its placement group's bundles for a
static-PG op. A held op reserves the same share (``warmup_target``).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import SIZER_RESERVATION_RATIO
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

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


class WarmupShares:
    """Each op's warmup share (``warmup_target``) and the upscale toward it
    (``upscale``)."""

    def __init__(self, *, profiles: OpProfiles):
        self._profiles = profiles
        self.reservation_ratio = SIZER_RESERVATION_RATIO

    def upscale(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
        pg_slots: PlacementGroupSlots,
    ) -> Optional[ActorPoolScalingRequest]:
        """Upscale toward this op's warmup share, clamped to [min_size,
        max_size].

        share = ceiling / per_actor / concurrency, where the ceiling is this
        op's equal share of what it sizes against: the allocation for non-PG
        ops, its PG's bundles for static-PG ops (``_pg_warmup_ceiling``). An
        op whose PG capacity is unknown has nothing to inspect and takes
        ratio x its pool's max size. Either way the policy then cuts it to
        what the ledger has free per node / bundle.

        Upscale only: a smaller share just stops further upscales, and the
        delta self-zeroes because serving_size() counts pendings. An op holds
        at min until every upstream actor-pool op has produced output, so a
        stage fed by a tiny, finite upstream never takes a full share.
        """
        if not all(has_produced_output(prev) for prev in self._profiles.prev_ops[op]):
            return None
        pool = op.actor_pool
        target = self.warmup_target(
            op=op, cluster_capacity=cluster_capacity, pg_slots=pg_slots
        )
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
        *,
        op: "ExperimentalActorPoolMapOperator",
        cluster_capacity: ExecutionResources,
        pg_slots: PlacementGroupSlots,
    ) -> Optional[int]:
        """This op's warmup share in actors, clamped to [min_size, max_size]
        (see ``upscale``). Also what an op held at zero or min reserves
        (``OperatorSizer._reserve_for_held_ops``).

        A CPU op's share is also capped by memory: GPU ops take their GPU
        share (and its memory) first, and the CPU ops whose actors request
        memory split what is left equally. A
        CPU-only share alone can pack memory-heavy CPU stages so tightly that
        the GPU stage cannot place its actors.
        """
        pool = op.actor_pool
        share = self._share_by_resource_type(
            op=op, cluster_capacity=cluster_capacity, pg_slots=pg_slots
        )
        if share is None:
            return None
        memory_share = self._memory_share(
            op=op, cluster_capacity=cluster_capacity, pg_slots=pg_slots
        )
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
        *,
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
            ceiling = self._pg_warmup_ceiling(
                op=op, resource_type=resource_type, pg_slots=pg_slots
            )
        else:
            ceiling = self._warmup_ceiling(
                resource_type=resource_type, cluster_capacity=cluster_capacity
            )
        # An actor with max_actor_concurrency=k serves ~k tasks at once, so we
        # need k times fewer actors for the same share.
        concurrency = max(1, pool.max_actor_concurrency())
        return ceiling / per_actor / concurrency

    def _memory_share(
        self,
        *,
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
        for gpu_op in self._eligible_ops_by_resource_type(resource_type="gpu"):
            gpu_share = self._share_by_resource_type(
                op=gpu_op, cluster_capacity=cluster_capacity, pg_slots=pg_slots
            )
            if gpu_share is None:
                continue
            gpu_pool = gpu_op.actor_pool
            actors = min(
                max(gpu_share + FIT_EPS, gpu_pool.min_size()), gpu_pool.max_size()
            )
            usable -= int(actors) * gpu_pool.per_actor_resource_usage().memory
        sharers = 0
        for cpu_op in self._eligible_ops_by_resource_type(resource_type="cpu"):
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
        *,
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
        return self._equal_share(limit=limit, ops=sharers, resource_type=resource_type)

    def _warmup_ceiling(
        self, *, resource_type: str, cluster_capacity: ExecutionResources
    ) -> float:
        """One non-PG op's share: (capacity - floors) * ratio / autoscaled ops.

        Recomputed every call so the share follows a cluster that is still
        ramping. Every eligible op's floor comes off the top, fixed pools
        included, but only autoscaled ops divide the remainder -- a fixed pool
        never consumes a share. PG ops are left out: they size against their
        placement group, not this allocation.
        """
        return self._equal_share(
            limit=getattr(cluster_capacity, resource_type),
            ops=self._eligible_ops_by_resource_type(resource_type=resource_type),
            resource_type=resource_type,
        )

    def _equal_share(
        self,
        *,
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
        self, *, resource_type: str
    ) -> List["ExperimentalActorPoolMapOperator"]:
        return [
            op
            for op in self._profiles.non_pg_ops
            if not op.has_execution_finished()
            and self._profiles.resource_type(op) == resource_type
        ]
