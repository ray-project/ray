import logging
from typing import TYPE_CHECKING, Optional

from .backpressure_policy import BackpressurePolicy
from ray._private.ray_constants import env_float
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
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
    op_usage = resource_manager.get_op_usage(op)
    op_budget = resource_manager.get_budget(op)
    if op_usage is None or op_budget is None:
        return None

    if consider_downstream_ineligible_ops:
        total_usage = resource_manager.get_op_usage_object_store_with_downstream(op)
    else:
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

    When preserve_order is enabled:
    - Limits the task submission gap (scheduling priority for earlier tasks)
    - Dynamically lowers the backpressure threshold based on active task count
    """

    # Threshold for per-Op object store budget utilization vs total
    # (utilization / total) ratio to enable downstream capacity backpressure.
    OBJECT_STORE_BUDGET_UTIL_THRESHOLD = env_float(
        "RAY_DATA_DOWNSTREAM_CAPACITY_OBJECT_STORE_BUDGET_UTIL_THRESHOLD", 0.9
    )

    # Threshold for per-Op object store budget utilization vs total
    # (utilization / total) ratio to record baseline task index gap.
    OBJECT_STORE_BUDGET_UTIL_BASELINE_THRESHOLD = env_float(
        "RAY_DATA_DOWNSTREAM_CAPACITY_OBJECT_STORE_BUDGET_UTIL_BASELINE_THRESHOLD", 0.1
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
        self._preserve_order = self._data_context.execution_options.preserve_order
        # Per-operator baseline task index gap recorded when budget threshold first hit
        self._baseline_task_index_gap: dict["PhysicalOperator", int] = {}
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
        if self._resource_manager.is_materializing_op(op):
            # Operator is materializing, so no need to perform backpressure.
            return True
        if self._resource_manager.has_materializing_downstream_op(op):
            # Downstream operator is materializing, so can't perform backpressure
            # based on downstream capacity which requires full materialization.
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

    def _get_effective_threshold(self, op: "PhysicalOperator") -> float:
        """Get the effective backpressure threshold.

        When preserve_order is enabled, dynamically lowers the threshold based on
        gap growth relative to baseline. The baseline is recorded when budget
        threshold is first hit. If gap grows beyond baseline, threshold is reduced
        exponentially.
        """
        threshold = self._backpressure_capacity_ratio
        if self._preserve_order:
            gap = self._get_task_index_gap(op)
            baseline = self._baseline_task_index_gap.get(op, 0)
            if gap > baseline and baseline >= 0:
                # Gap has grown beyond baseline: reduce threshold exponentially
                # gap_growth=1: threshold / 2, gap_growth=2: threshold / 4, etc.
                gap_growth = gap - baseline
                threshold = threshold / (2 ** gap_growth)
        return threshold

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
            self._preserve_order and
            op not in self._baseline_task_index_gap
            and utilized_budget_fraction is not None
            and utilized_budget_fraction > self.OBJECT_STORE_BUDGET_UTIL_BASELINE_THRESHOLD
        ):
            # Record baseline task index gap.
            self._baseline_task_index_gap[op] = self._get_task_index_gap(op)

        if (
            utilized_budget_fraction is not None
            and utilized_budget_fraction <= self.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        ):
            # Utilized budget fraction is below threshold, so should skip backpressure.
            return False

        queue_ratio = self._get_queue_ratio(op)
        queue_ratio_threshold = self._get_effective_threshold(op)
        # Apply backpressure if queue ratio exceeds the threshold.
        return queue_ratio > queue_ratio_threshold

    def _get_task_index_gap(self, op: "PhysicalOperator") -> int:
        """Get the gap between the earliest and latest active task.

        Returns 0 if no active tasks.
        """
        active_tasks = op.get_active_tasks()
        if not active_tasks:
            return 0
        min_idx = max_idx = active_tasks[0].task_index()
        for task in active_tasks[1:]:
            idx = task.task_index()
            if idx < min_idx:
                min_idx = idx
            elif idx > max_idx:
                max_idx = idx
        return max_idx - min_idx

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add input to the operator based on
        downstream capacity.

        When preserve_order is enabled, the dynamic threshold in
        _should_apply_backpressure scales with task gap, creating a feedback
        loop that naturally limits the gap.
        """
        return not self._should_apply_backpressure(op)

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """Return the maximum bytes of pending task outputs can be read for
        the given operator. None means no limit.

        When preserve_order is enabled and backpressure is applied, returns a
        reduced limit (not 0) to allow earlier tasks to make progress. Earlier
        tasks must complete for their outputs to be consumed, so blocking
        completely would prevent progress.
        """
        backpressure = self._should_apply_backpressure(op)
        downstream_capacity = self._get_downstream_capacity_size_bytes(op)

        if self._preserve_order:
            if backpressure:
                # Under backpressure: allow limited reading for earlier tasks.
                # Scale exponentially by gap growth (relative to baseline).
                gap = self._get_task_index_gap(op)
                baseline = self._baseline_task_index_gap.get(op, 0)
                gap_growth = max(0, gap - baseline)
                if gap_growth > 0 and downstream_capacity > 0:
                    # Reduce capacity exponentially: growth=1 → 50%, growth=2 → 25%
                    return int(max(1, downstream_capacity // (2 ** gap_growth)))
                # No gap growth or no capacity info: allow full downstream capacity
                return int(downstream_capacity) if downstream_capacity > 0 else None
            else:
                # No backpressure: still limit to downstream capacity
                if downstream_capacity > 0:
                    return int(downstream_capacity)
                return None

        # Non-preserve_order: block completely when backpressure applied
        if backpressure:
            return 0

        return None
