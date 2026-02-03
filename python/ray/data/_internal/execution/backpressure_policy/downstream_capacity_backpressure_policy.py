import logging
from typing import TYPE_CHECKING, Optional

from .backpressure_policy import BackpressurePolicy
from ray._private.ray_constants import env_float
from ray.data._internal.execution.resource_manager import (
    ResourceManager,
)
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)


def get_available_object_store_budget_fraction(
    resource_manager: "ResourceManager",
    op: "PhysicalOperator",
    consider_downstream_ineligible_ops: bool,
) -> Optional[float]:
    """Get available object store memory budget fraction for the operator.

    Args:
        resource_manager: The resource manager to use.
        op: The operator to get the budget fraction for.
        consider_downstream_ineligible_ops: If True, include downstream ineligible
            ops in the calculation. If False, only consider this op's usage/budget.

    Returns:
        The available budget fraction, or None if not available.
    """
    op_usage = resource_manager.get_op_usage(
        op, include_ineligible_downstream=consider_downstream_ineligible_ops
    )
    op_budget = resource_manager.get_budget(op)
    if op_usage is None or op_budget is None:
        return None

    total_usage = op_usage.object_store_memory

    total_budget = op_budget.object_store_memory
    total_mem = total_usage + total_budget
    if total_mem == 0:
        return None

    return total_budget / total_mem


def get_utilized_object_store_budget_fraction(
    resource_manager: "ResourceManager",
    op: "PhysicalOperator",
    consider_downstream_ineligible_ops: bool,
) -> Optional[float]:
    """Get utilized object store memory budget fraction for the operator.

    Args:
        resource_manager: The resource manager to use.
        op: The operator to get the utilized fraction for.
        consider_downstream_ineligible_ops: If True, include downstream ineligible
            ops in the calculation. If False, only consider this op's usage/budget.

    Returns:
        The utilized budget fraction, or None if not available.
    """
    available_fraction = get_available_object_store_budget_fraction(
        resource_manager,
        op,
        consider_downstream_ineligible_ops=consider_downstream_ineligible_ops,
    )
    if available_fraction is None:
        return None
    return 1 - available_fraction


class DownstreamCapacityBackpressurePolicy(BackpressurePolicy):
    """Backpressure policy based on downstream processing capacity.

    To backpressure a given operator, use queue size build up / downstream capacity ratio.
    This ratio represents the upper limit of buffering in object store between pipeline stages
    to optimize for throughput.
    """

    # Threshold for per-Op object store budget utilization vs total
    # (utilization / total) ratio to enable downstream capacity backpressure.
    OBJECT_STORE_BUDGET_UTIL_THRESHOLD = env_float(
        "RAY_DATA_DOWNSTREAM_CAPACITY_OBJECT_STORE_BUDGET_UTIL_THRESHOLD", 0.9
    )

    @property
    def name(self) -> str:
        return "DownstreamCapacity"

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        super().__init__(data_context, topology, resource_manager)
        self._backpressure_capacity_ratio = (
            self._data_context.downstream_capacity_backpressure_ratio
        )
        if self._backpressure_capacity_ratio is not None:
            logger.debug(
                f"DownstreamCapacityBackpressurePolicy enabled with backpressure capacity ratio: {self._backpressure_capacity_ratio}"
            )

    def _get_queue_size_bytes(self, op: "PhysicalOperator") -> int:
        """Get the output current queue size
        (this operator + ineligible downstream operators) in bytes for the given operator.
        """
        op_outputs_usage = self._topology[op].output_queue_bytes()
        # Also account the downstream ineligible operators' memory usage.
        op_outputs_usage += sum(
            self._resource_manager.get_op_usage(next_op).object_store_memory
            for next_op in self._resource_manager._get_downstream_ineligible_ops(op)
        )
        return op_outputs_usage

    def _get_downstream_capacity_size_bytes(self, op: "PhysicalOperator") -> int:
        """Get the downstream capacity size for the given operator.

        Downstream capacity size is the sum of the pending task inputs of the
        downstream eligible operators.

        If an output dependency is ineligible, skip it and recurse down to find
        eligible output dependencies. If there are no output dependencies,
        return external consumer bytes.
        """
        if not op.output_dependencies:
            # No output dependencies, return external consumer bytes.
            return self._resource_manager.get_external_consumer_bytes()

        total_capacity_size_bytes = 0
        for output_dependency in op.output_dependencies:
            if self._resource_manager.is_op_eligible(output_dependency):
                # Output dependency is eligible, add its pending task inputs.
                total_capacity_size_bytes += (
                    output_dependency.metrics.obj_store_mem_pending_task_inputs or 0
                )
            else:
                # Output dependency is ineligible, recurse down to find eligible ops.
                total_capacity_size_bytes += self._get_downstream_capacity_size_bytes(
                    output_dependency
                )
        return total_capacity_size_bytes

    def _should_skip_backpressure(self, op: "PhysicalOperator") -> bool:
        """Check if backpressure should be skipped for the operator.
        TODO(srinathk10): Extract this to common logic to skip invoking BackpressurePolicy.
        """
        if self._backpressure_capacity_ratio is None:
            # Downstream capacity backpressure is disabled.
            return True

        if not self._resource_manager.is_op_eligible(op):
            # Operator is not eligible for backpressure.
            return True

        if self._resource_manager._is_blocking_materializing_op(op):
            # Operator is materializing, so no need to perform backpressure.
            return True

        return False

    def _get_queue_ratio(self, op: "PhysicalOperator") -> float:
        """Get queue/capacity ratio for the operator."""
        queue_size_bytes = self._get_queue_size_bytes(op)
        downstream_capacity_size_bytes = self._get_downstream_capacity_size_bytes(op)
        if downstream_capacity_size_bytes == 0:
            # No downstream capacity to backpressure against, so no backpressure.
            return 0
        return queue_size_bytes / downstream_capacity_size_bytes

    def _should_apply_backpressure(self, op: "PhysicalOperator") -> bool:
        """Check if backpressure should be applied for the operator.

        Returns True if backpressure should be applied, False otherwise.
        """
        if self._should_skip_backpressure(op):
            return False

        utilized_budget_fraction = get_utilized_object_store_budget_fraction(
            self._resource_manager, op, consider_downstream_ineligible_ops=True
        )
        if (
            utilized_budget_fraction is not None
            and utilized_budget_fraction <= self.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        ):
            # Utilized budget fraction is below threshold, so should skip backpressure.
            return False

        queue_ratio = self._get_queue_ratio(op)
        # Apply backpressure if queue ratio exceeds the threshold.
        return queue_ratio > self._backpressure_capacity_ratio

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add input to the operator based on
        downstream capacity.
        """
        return not self._should_apply_backpressure(op)

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """Return the maximum bytes of pending task outputs can be read for
        the given operator. None means no limit."""
        if self._should_apply_backpressure(op):
            return 0
        return None
