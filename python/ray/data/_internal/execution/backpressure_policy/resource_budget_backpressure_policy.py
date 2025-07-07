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
        budget = self._resource_manager.get_budget(op)
        if budget is None:
            return False
        return op.incremental_resource_usage().satisfies_limit(budget)

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """Return the maximum bytes of pending task outputs can be read for
        the given operator. None means no limit.

        This delegates to the resource manager's max_task_output_bytes_to_read method.

        Args:
            op: The operator to get the limit for.

        Returns:
            The maximum bytes that can be read, or None if no limit.
        """
        return self._resource_manager.max_task_output_bytes_to_read(op)
