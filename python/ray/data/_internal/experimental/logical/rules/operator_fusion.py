"""Actor-only-backend overrides for operator fusion.

Active only when ``actor_only_backend_enabled()`` is true. Swapped into the physical
ruleset in place of the default :class:`FuseOperators` by
``ray.data._internal.logical.optimizers`` (see ``_maybe_install_actor_only_rules``).

Relative to the default fusion rule, this rule (all gating lives in :meth:`_can_fuse`, so it
applies uniformly to the Map->Map, Map->AllToAll, and
MapBatches->StreamingRepartition loops) additionally allows Actor->Actor
fusion, restricted by :meth:`_fuse_compute_strategy` to operators with
identical compute strategies.
"""
import inspect
import logging
from typing import Optional

from typing_extensions import override

from ray.data._internal.compute import (
    ActorPoolStrategy,
    ComputeStrategy,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.logical.rules.operator_fusion import (
    FuseOperators as _OSSFuseOperators,
)

logger = logging.getLogger(__name__)

__all__ = ["FuseOperators"]


def _udf_is_async(fn) -> bool:
    """Return True if ``fn`` is an async UDF.

    Handles both plain-function UDFs (``fn`` itself is a coroutine / async
    generator function) and callable-class UDFs passed either as the class
    itself (e.g. ``map_batches(MyClass, ...)``) or as an instance.

    When ``fn`` is a class, async-ness lives on ``fn.__call__`` (the instance
    method).  Using ``type(fn).__call__`` would resolve to ``type.__call__``
    (the metaclass), which is always sync.
    """
    if inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn):
        return True
    if not callable(fn):
        return False
    # Class passed directly (the common map_batches(MyClass, ...) form).
    if isinstance(fn, type):
        call = fn.__call__
    else:
        call = type(fn).__call__
    return inspect.iscoroutinefunction(call) or inspect.isasyncgenfunction(call)


class FuseOperators(_OSSFuseOperators):
    """Actor-only-backend fusion rule. See module docstring."""

    @override
    def _can_fuse(self, down_op: PhysicalOperator, up_op: PhysicalOperator) -> bool:
        # Do not fuse two actor ops when either UDF is async.
        #   * Mixed (sync + async): an async UDF runs its coroutines on a
        #     dedicated event-loop thread with its own ``max_concurrency``,
        #     while a sync UDF runs inline in the actor task; collapsing the
        #     two into a single actor pool conflates their concurrency /
        #     backpressure models, so the async stage no longer behaves per
        #     its own ``max_concurrency``.
        #   * Async + async: fusing deadlocks the task before its first UDF
        #     call. The downstream transform's producer coroutine pulls the
        #     upstream transform's sync bridge generator on the actor's shared
        #     asyncio loop thread, which then blocks in ``output_queue.get()``
        #     before the upstream producer coroutine (queued behind it on the
        #     same loop) can run. See
        #     ``plan_udf_map_op.py::_generate_transform_fn_for_async_map``,
        #     whose sync<->async queue bridge assumes its consumer is the task
        #     main thread, never the loop thread.
        if isinstance(up_op, ActorPoolMapOperator) and isinstance(
            down_op, ActorPoolMapOperator
        ):
            up_logical_op = self._op_map[up_op]
            down_logical_op = self._op_map[down_op]
            if (
                isinstance(up_logical_op, AbstractUDFMap)
                and isinstance(down_logical_op, AbstractUDFMap)
                and (
                    _udf_is_async(up_logical_op.fn) or _udf_is_async(down_logical_op.fn)
                )
            ):
                return False
        return super()._can_fuse(down_op, up_op)

    @override
    def _can_fuse_op_types(
        self, up_op: PhysicalOperator, down_op: PhysicalOperator
    ) -> bool:
        if super()._can_fuse_op_types(up_op, down_op):
            return True
        # Additionally allow Actor->Actor fusion on top of the default
        # Task-based fusion rules. Also, disable Read Operators.
        # TODO: In the future, we want to control the amount of threads per actor.
        return (
            isinstance(up_op, ActorPoolMapOperator)
            and isinstance(down_op, ActorPoolMapOperator)
            and not up_op.is_read_op
            and not down_op.is_read_op
        )

    @classmethod
    @override
    def _fuse_compute_strategy(
        cls, up_compute: ComputeStrategy, down_compute: ComputeStrategy
    ) -> Optional[ComputeStrategy]:
        # Actor->Actor: fusable only when both operators use the identical
        # compute strategy (so the fused op has an unambiguous actor pool
        # configuration). Everything else defers to the default rules (Task->Task,
        # Task->Actor allowed; Actor->Task disallowed).
        if (
            isinstance(up_compute, ActorPoolStrategy)
            and isinstance(down_compute, ActorPoolStrategy)
            and up_compute == down_compute
        ):
            return down_compute
        return super()._fuse_compute_strategy(up_compute, down_compute)
