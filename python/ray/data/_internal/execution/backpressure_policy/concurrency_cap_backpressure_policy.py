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


class ConcurrencyCapBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy that caps the concurrency of each operator.

    The policy will limit the number of concurrently running tasks based on its
    concurrency cap parameter.

    NOTE: Only support setting concurrency cap for `TaskPoolMapOperator` for now.
    TODO(chengsu): Consolidate with actor scaling logic of `ActorPoolMapOperator`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._concurrency_caps: dict["PhysicalOperator", float] = {}

        # When preserve_order is enabled, reduce concurrency to prevent
        # out-of-order task completion that leads to ordered queue buildup
        self._preserve_order = self._data_context.execution_options.preserve_order
        concurrency_multiplier = (
            self._data_context.concurrency_cap_on_preserve_order_multiplier
            if self._preserve_order
            else 1.0
        )

        for op, _ in self._topology.items():
            if isinstance(op, TaskPoolMapOperator) and op.get_concurrency() is not None:
                # Apply concurrency reduction when preserve_order is enabled
                # Ensure minimum concurrency cap of 1 to prevent blocking all tasks
                calculated_cap = op.get_concurrency() * concurrency_multiplier
                self._concurrency_caps[op] = max(1.0, calculated_cap)
            else:
                self._concurrency_caps[op] = float("inf")

        logger.debug(
            "ConcurrencyCapBackpressurePolicy initialized with: "
            f"{self._concurrency_caps} (preserve_order={self._preserve_order})"
        )

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        return op.metrics.num_tasks_running < self._concurrency_caps[op]
