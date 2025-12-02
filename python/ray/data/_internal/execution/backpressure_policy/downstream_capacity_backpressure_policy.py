import logging
from typing import TYPE_CHECKING

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
        self._backpressure_disabled = (
            self._backpressure_concurrency_ratio is None
            or self._backpressure_max_queued_blocks is None
        )

    def _max_concurrent_tasks(self, op: "PhysicalOperator") -> int:
        if isinstance(op, ActorPoolMapOperator):
            return sum(
                [
                    actor_pool.max_concurrent_tasks()
                    for actor_pool in op.get_autoscaling_actor_pools()
                ]
            )
        return op.num_active_tasks()

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
