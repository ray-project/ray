import logging
from typing import TYPE_CHECKING, Optional

from .backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)


class DownstreamCapacityBackpressurePolicy(BackpressurePolicy):
    """Backpressure policy based on downstream processing capacity.

    This policy triggers backpressure when the output bundles size exceeds both:
    1. A ratio threshold multiplied by the number of running tasks in downstream operators
    2. An absolute threshold for the output bundles size

    The policy monitors actual downstream processing capacity by tracking the number
    of currently running tasks rather than configured parallelism. This approach
    ensures effective backpressure even when cluster resources are insufficient or
    scaling is slow, preventing memory pressure and maintaining pipeline stability.

    Key benefits:
    - Prevents memory bloat from unprocessed output objects
    - Adapts to actual cluster conditions and resource availability
    - Maintains balanced throughput across pipeline operators
    - Reduces object spilling and unnecessary rebuilds
    """

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        super().__init__(data_context, topology, resource_manager)
        self._backpressure_concurrency_ratio = (
            self._data_context.downstream_capacity_backpressure_ratio
        )
        self._backpressure_max_queued_blocks = (
            self._data_context.downstream_capacity_backpressure_max_queued_bundles
        )
        self._downstream_capacity_outputs_ratio = (
            self._data_context.downstream_capacity_outputs_ratio
        )
        self._backpressure_disabled = (
            self._backpressure_concurrency_ratio is None
            or self._backpressure_max_queued_blocks is None
        )

    def _max_concurrent_tasks(self, op: "PhysicalOperator") -> int:
        # This should return values >= 1 to ensure we do not deadlock.
        if isinstance(op, ActorPoolMapOperator):
            return sum(
                [
                    actor_pool.max_concurrent_tasks()
                    for actor_pool in op.get_autoscaling_actor_pools()
                ]
            )
        return op.num_active_tasks()

    def _estimate_max_downstream_capacity(
        self, downstream_op: "PhysicalOperator"
    ) -> Optional[int]:
        """Estimate maximum number of blocks downstream operator can consume.

        Formula:
            max_capacity = (global_limits / per_task_resources)
                         * tasks_per_actor
                         * blocks_per_task
                         * capacity_multiplier

        Args:
            downstream_op: The downstream operator to estimate capacity for.

        Returns:
            Estimated maximum number of blocks that can be queued, or None if
            estimation is not possible (e.g., no metrics yet).
        """
        avg_num_inputs_per_task = downstream_op.metrics.average_num_inputs_per_task
        if avg_num_inputs_per_task is None or avg_num_inputs_per_task == 0:
            return None

        max_concurrent_tasks = self._max_concurrent_tasks(downstream_op)

        # If there are no concurrent tasks, we should not backpressure or we
        # might deadlock.
        if max_concurrent_tasks == 0:
            return None

        return int(
            max_concurrent_tasks
            * avg_num_inputs_per_task
            * self._downstream_capacity_outputs_ratio
        )

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add input to the operator based on downstream capacity."""
        if self._backpressure_disabled:
            return True
        for output_dependency in op.output_dependencies:
            total_enqueued_blocks = self._topology[
                output_dependency
            ].total_enqueued_input_blocks()

            avg_inputs_per_task = (
                output_dependency.metrics.num_task_inputs_processed
                / max(output_dependency.metrics.num_tasks_finished, 1)
            )
            outstanding_tasks = total_enqueued_blocks / max(avg_inputs_per_task, 1)
            max_allowed_outstanding = (
                self._max_concurrent_tasks(output_dependency)
                * self._backpressure_concurrency_ratio
            )

            if (
                total_enqueued_blocks > self._backpressure_max_queued_blocks
                and outstanding_tasks > max_allowed_outstanding
            ):
                return False

        return True

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        if (
            self._downstream_capacity_outputs_ratio is None
            or self._downstream_capacity_outputs_ratio <= 0
        ):
            return None

        min_capacity_bytes = None

        for downstream_op in op.output_dependencies:
            # Get current queued blocks for this downstream op
            current_queued_blocks = self._topology[
                downstream_op
            ].total_enqueued_input_blocks()
            max_capacity_blocks = self._estimate_max_downstream_capacity(downstream_op)

            if max_capacity_blocks is None:
                # Can't estimate, don't limit
                continue

            remaining_capacity_blocks = max_capacity_blocks - current_queued_blocks

            if remaining_capacity_blocks <= 0:
                # At or over capacity, stop reading immediately
                return 0

            # Convert blocks to approximate bytes using metrics if available
            avg_block_size = op.metrics.average_bytes_per_output

            if avg_block_size is None or avg_block_size == 0:
                # No block size info, skip byte-limiting
                continue

            # Compute remaining capcity bytes and convert to an int, always be conservative
            # so use int rather than round.
            remaining_capacity_bytes = int(remaining_capacity_blocks * avg_block_size)

            # The slowest consumer across all output dependencies should determine the rate
            # of outputs to be taken so we do not overwhelm this slowest consumer. Thus we
            # take the minimum of all remaining capacity bytes for the output dependencies.
            if min_capacity_bytes is None:
                min_capacity_bytes = remaining_capacity_bytes
            else:
                min_capacity_bytes = min(min_capacity_bytes, remaining_capacity_bytes)

        return min_capacity_bytes
