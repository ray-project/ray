import logging
from typing import TYPE_CHECKING

from .backpressure_policy import BackpressurePolicy

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )

logger = logging.getLogger(__name__)


class ResourceBudgetBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy based on resource budgets in ResourceManager."""

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        budget = self._resource_manager.get_budget_for_scheduling(op)
        if budget is None:
            return True
        return op.incremental_resource_usage().satisfies_limit(budget)
