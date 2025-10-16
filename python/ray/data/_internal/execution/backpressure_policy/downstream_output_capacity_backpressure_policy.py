# File: python/ray/data/_internal/execution/backpressure_policy/downstream_capacity_output_backpressure_policy.py

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


class DownstreamOutputCapacityBackpressurePolicy(BackpressurePolicy):
    """Backpressure policy that limits output reading based on downstream capacity.

    This policy prevents OOMs by estimating the maximum amount of data that can be
    queued for downstream operators based on:
    1. Global cluster resource limits
    2. Resources required per task/actor in downstream operators
    3. Average number of input blocks consumed per task

    The policy calculates an upper bound for downstream processing capacity and
    uses a multiple of that capacity as the limit for how much data should be
    queued downstream. This is done to account for the discrete scheduling steps
    in Ray Data. We primarily want to limit runaway accumulation of data, rather
    than hard stop the accumulation at the exact capacity we are estimating.
    """

    DEFAULT_CAPACITY_MULTIPLIER = 10.0

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        super().__init__(data_context, topology, resource_manager)

        # Get capacity multiplier from config, or use default
        self._capacity_multiplier = getattr(
            data_context,
            "downstream_capacity_output_backpressure_multiplier",
            self.DEFAULT_CAPACITY_MULTIPLIER,
        )

        # Disable if multiplier is None or <= 0
        self._backpressure_disabled = (
            self._capacity_multiplier is None or self._capacity_multiplier <= 0
        )

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
        # Get average blocks per task from metrics
        avg_inputs_per_task = downstream_op.metrics.average_num_inputs_per_task
        if avg_inputs_per_task is None or avg_inputs_per_task == 0:
            # No metrics yet, can't estimate - don't apply backpressure
            return None

        # Get global resource limits
        global_limits = self._resource_manager.get_global_limits()

        # Get per-task resource requirements
        per_task_resources = downstream_op.incremental_resource_usage()

        # Estimate max number of tasks/actors that can run concurrently
        # based on the most constrained resource
        max_concurrent_tasks = min(
            [
                global_limits.cpu / per_task_resources.cpu
                if per_task_resources.cpu > 0
                else float("inf"),
                global_limits.gpu / per_task_resources.gpu
                if per_task_resources.gpu > 0
                else float("inf"),
                global_limits.memory / per_task_resources.memory
                if per_task_resources.memory > 0
                else float("inf"),
            ]
        )

        if max_concurrent_tasks == float("inf"):
            # If no resource constraints, fall back to inf (no limit)
            return None

        # For ActorPoolMapOperator, multiply by max_concurrency per actor
        # For TaskPoolMapOperator, max_concurrency is already in the task count
        tasks_per_worker = 1
        if isinstance(downstream_op, ActorPoolMapOperator):
            tasks_per_worker = downstream_op._ray_remote_args.get("max_concurrency", 1)

        # Calculate total capacity
        # max_concurrent_workers * tasks_per_worker * blocks_per_task * safety_factor
        estimated_capacity = int(
            max_concurrent_tasks
            * tasks_per_worker
            * avg_inputs_per_task
            * self._capacity_multiplier
        )

        return estimated_capacity

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """Limit task output reading based on downstream capacity.

        This prevents OOMs by ensuring we don't queue more blocks than downstream
        operators can reasonably consume based on cluster resources.

        Args:
            op: The operator to get the limit for.

        Returns:
            Maximum bytes to read, or None if no limit should be applied.
        """
        if self._backpressure_disabled:
            return None

        # Check each downstream operator
        min_capacity_bytes = None

        for downstream_op in op.output_dependencies:
            # Get current queued blocks for this downstream op
            downstream_state = self._topology[downstream_op]
            current_queued_blocks = downstream_state.total_enqueued_input_bundles()

            # Estimate max capacity for downstream
            max_capacity_blocks = self._estimate_max_downstream_capacity(downstream_op)

            if max_capacity_blocks is None:
                # Can't estimate, don't limit
                continue

            # Calculate remaining capacity in blocks
            remaining_capacity_blocks = max_capacity_blocks - current_queued_blocks

            if remaining_capacity_blocks <= 0:
                # At or over capacity, stop reading immediately
                return 0

            # Convert blocks to approximate bytes
            # Use average block size from metrics if available
            avg_block_size = downstream_op.metrics.average_block_bytes
            if avg_block_size is None or avg_block_size == 0:
                # No block size info, skip byte-based limiting
                continue

            remaining_capacity_bytes = remaining_capacity_blocks * avg_block_size

            # Take the minimum across all downstream operators
            if min_capacity_bytes is None:
                min_capacity_bytes = remaining_capacity_bytes
            else:
                min_capacity_bytes = min(min_capacity_bytes, remaining_capacity_bytes)

        return min_capacity_bytes
