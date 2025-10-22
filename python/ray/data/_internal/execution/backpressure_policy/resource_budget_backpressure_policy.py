import logging
from typing import TYPE_CHECKING, Optional

from .backpressure_policy import BackpressurePolicy

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )

logger = logging.getLogger(__name__)


class ResourceBudgetBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy based on resource budgets in ResourceManager."""

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        if self._resource_manager._op_resource_allocator is not None:
            return self._resource_manager._op_resource_allocator.can_submit_new_task(op)

        return True

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """Determine maximum bytes to read based on the resource budgets.

        Args:
            op: The operator to get the limit for.

        Returns:
            The maximum bytes that can be read, or None if no limit.
        """
        return self._resource_manager.max_task_output_bytes_to_read(op)
