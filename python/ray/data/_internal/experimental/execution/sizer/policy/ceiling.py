"""How far an op may upscale: its pool's max size, or the actors its
remaining work can use once every upstream op has completed."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.execution_flags import SIZER_UTIL_THRESHOLD

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import OpState
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )


def upscale_ceiling(
    *, op: "ExperimentalActorPoolMapOperator", op_state: Optional["OpState"]
) -> int:
    """Most actors ``op`` may upscale to: its pool's bounded max size, or
    fewer once every upstream op has completed and its remaining work needs
    fewer. Every upscale stays within it. Without an ``op_state`` only the
    pool bounds it."""
    ceiling = op.actor_pool.bounded_max_size()
    remaining = (
        None if op_state is None else remaining_work_cap(op=op, op_state=op_state)
    )
    return ceiling if remaining is None else min(ceiling, remaining)


def remaining_work_cap(
    *, op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
) -> Optional[int]:
    """Most actors ``op`` can still use, or None while more input can come.

    Once every upstream op has completed, the op's backlog (queued plus
    in-flight inputs) is all the work it has left.
    """
    upstream = op.input_dependencies
    if not upstream or not all(dep.has_completed() for dep in upstream):
        return None
    return max(op.actor_pool.min_size(), backlog_actor_target(op=op, op_state=op_state))


def backlog_actor_target(
    *, op: "ExperimentalActorPoolMapOperator", op_state: "OpState"
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
        total_tasks / (pool.max_actor_concurrency() * SIZER_UTIL_THRESHOLD)
    )
