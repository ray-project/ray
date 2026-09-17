from typing import List, Optional, Protocol, runtime_checkable

from ray.data._internal.execution.interfaces import ExecutionResources


@runtime_checkable
class ClusterAutoscalingMetrics(Protocol):
    """Protocol that defines the metrics required for cluster autoscaling.

    Attributes:
        average_num_inputs_per_task: The average number of input blocks per task.
        average_num_outputs_per_task: The average number of output blocks per task.
        num_output_blocks_per_task_s: The number of output blocks per task per second.
    """

    average_num_inputs_per_task: Optional[float]
    average_num_outputs_per_task: Optional[float]
    num_output_blocks_per_task_s: Optional[float]


@runtime_checkable
class SupportsClusterAutoscaling(Protocol):
    """Protocol that defines the attributes and methods required for autoscaling."""

    output_dependencies: List["SupportsClusterAutoscaling"]
    metrics: ClusterAutoscalingMetrics

    def per_task_resource_allocation(
        self,
    ) -> ExecutionResources:
        """The amount of logical resources used by each task.

        For regular tasks, these are the resources required to schedule a task. For
        actor tasks, these are the resources required to schedule an actor divided by
        the number of actor threads (i.e., `max_concurrency`).

        Returns:
            The resource requirement per task.
        """
        ...

    def get_max_concurrency_limit(self) -> Optional[int]:
        """The maximum number of tasks that can be run concurrently.

        Some operators manually configure a maximum concurrency. For example, if you
        specify `concurrency` in `map_batches`.
        """
        ...

    def min_scheduling_resources(self) -> ExecutionResources:
        """The minimum resource bundle required to schedule the operator.

        For regular tasks, this is the resources required to schedule a task. For actor
        tasks, this is the resources required to schedule an actor.
        """
        ...

    def has_completed(self) -> bool:
        """Whether the operator has completed."""
        ...
