import logging
from typing import TYPE_CHECKING

from .backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )

logger = logging.getLogger(__name__)


class UserSpecifiedTaskLimitBackpressurePolicy(BackpressurePolicy):
    """Backpressures task submission based on the user-specified ``concurrency``.

    When you pass a regular function to ``map_batches`` and specify ``concurrency``,
    Ray Data guarantees that the system launches at most ``concurrency`` tasks.

    .. testcode::

        import ray

        ds = (
            ray.data.range(100)
            # Ray Data launches at most 2 tasks.
            .map_batches(lambda batch: batch, concurrency=2)
            .materialize()
        )

    This policy only applies to ``TaskPoolMapOperator``. For ``ActorPoolMapOperator``,
    ``concurrency`` represents the exact number of actors to launch rather than the
    maximum number of tasks to launch.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._task_limits: dict["PhysicalOperator", int] = {}

        for op in self._topology:
            if (
                isinstance(op, TaskPoolMapOperator)
                and op.get_max_concurrency_limit() is not None
            ):
                self._task_limits[op] = op.get_max_concurrency_limit()

        logger.debug(
            f"UserSpecifiedTaskLimitBackpressurePolicy initialized with: {self._task_limits}"
        )

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        return op.num_active_tasks() < self._task_limits.get(op, float("inf"))
