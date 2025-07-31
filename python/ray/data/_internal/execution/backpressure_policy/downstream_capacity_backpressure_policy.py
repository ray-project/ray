import logging
import math
import sys
from typing import TYPE_CHECKING

from .backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.autoscaler.autoscaling_actor_pool import (
    AutoscalingActorPool,
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
        self._backpressure_task_slot_ratio = float(
            self._data_context.get_config("backpressure_task_slot_ratio", "inf")
        )
        self._backpressure_max_queued_bundles = int(
            self._data_context.get_config(
                "backpressure_max_queued_bundles", sys.maxsize
            )
        )
        self._backpressure_disabled = (
            math.isinf(self._backpressure_task_slot_ratio)
            or self._backpressure_max_queued_bundles == sys.maxsize
        )

    def _get_num_task_slots(self, op: "PhysicalOperator") -> int:
        if isinstance(op, AutoscalingActorPool):
            return op.num_task_slots()
        return op.num_active_tasks()

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add input to the operator based on downstream capacity."""
        if self._backpressure_disabled:
            return True
        for output_dependency in op.output_dependencies:
            num_task_slots = self._get_num_task_slots(output_dependency)
            total_enqueued_input_bundles = self._topology[
                output_dependency
            ].total_enqueued_input_bundles()

            if (
                total_enqueued_input_bundles > self._backpressure_max_queued_bundles
                and (
                    total_enqueued_input_bundles
                    > num_task_slots * self._backpressure_task_slot_ratio
                )
            ):
                return False

        return True
