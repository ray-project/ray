"""Base operator runtime metrics."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List

from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    _METRICS,
    MetricDefinition,
    MetricsGroup,
    NodeMetrics,
    ObjectStoreUsageDetails,
    OpRuntimesMetricsMeta,
    metric_field,
    metric_property,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
    from ray.data.context import DataContext


@dataclass
class BaseOpMetrics(metaclass=OpRuntimesMetricsMeta):
    """Base runtime metrics for operators.

    Contains INPUTS, OUTPUTS, MISC, and basic RESOURCE_USAGE (cpu/gpu) metrics.
    Used by operators that don't have internal queues or tasks.

    Examples: InputDataBuffer, AggregateNumRows

    Available callbacks:
    - on_input_received(): Tracks input blocks received
    - on_output_taken(): Tracks output blocks taken by downstream operators
    """

    # === Inputs-related metrics ===
    num_inputs_received: int = metric_field(
        default=0,
        description="Number of input blocks received by operator.",
        metrics_group=MetricsGroup.INPUTS,
    )
    num_row_inputs_received: int = metric_field(
        default=0,
        description="Number of input rows received by operator.",
        metrics_group=MetricsGroup.INPUTS,
    )
    bytes_inputs_received: int = metric_field(
        default=0,
        description="Byte size of input blocks received by operator.",
        metrics_group=MetricsGroup.INPUTS,
    )

    # === Outputs-related metrics ===
    num_outputs_taken: int = metric_field(
        default=0,
        description=(
            "Number of output blocks that are already taken by downstream operators."
        ),
        metrics_group=MetricsGroup.OUTPUTS,
    )
    block_outputs_taken: int = metric_field(
        default=0,
        description="Number of blocks that are already taken by downstream operators.",
        metrics_group=MetricsGroup.OUTPUTS,
    )
    row_outputs_taken: int = metric_field(
        default=0,
        description="Number of rows that are already taken by downstream operators.",
        metrics_group=MetricsGroup.OUTPUTS,
    )
    bytes_outputs_taken: int = metric_field(
        default=0,
        description=(
            "Byte size of output blocks that are already taken by downstream operators."
        ),
        metrics_group=MetricsGroup.OUTPUTS,
    )

    # === Resource Usage-related metrics (basic) ===
    cpu_usage: float = metric_field(
        default=0,
        description="CPU usage of the operator.",
        metrics_group=MetricsGroup.RESOURCE_USAGE,
    )
    gpu_usage: float = metric_field(
        default=0,
        description="GPU usage of the operator.",
        metrics_group=MetricsGroup.RESOURCE_USAGE,
    )
    obj_store_mem_used: int = metric_field(
        default=0,
        description="Byte size of used memory in object store.",
        metrics_group=MetricsGroup.RESOURCE_USAGE,
    )

    # === Miscellaneous metrics ===

    def __init__(self, data_context: "DataContext"):
        """Initialize BaseOpMetrics.

        Args:
            data_context: DataContext instance for accessing configuration settings.
        """
        self._data_context = data_context
        self._extra_metrics: Dict[str, Any] = {}
        self._issue_detector_hanging = 0
        self._issue_detector_high_memory = 0

    @property
    def extra_metrics(self) -> Dict[str, Any]:
        """Return a dict of extra metrics."""
        return self._extra_metrics

    @classmethod
    def get_metrics(cls) -> List[MetricDefinition]:
        """Get all metrics defined across the class hierarchy."""
        return list(_METRICS)

    def as_dict(
        self,
        skip_internal_metrics: bool = False,
    ) -> Dict[str, Any]:
        """
        Return a dict representation of the metrics.

        Args:
            skip_internal_metrics: Whether to skip internal metrics.

        Returns:
            A dict representation of the metrics.
        """

        result = []
        for metric in self.get_metrics():
            if skip_internal_metrics and metric.internal_only:
                continue
            # Only include metrics that exist on this instance
            # (subclass metrics may be defined but not present on base classes)
            if hasattr(self, metric.name):
                value = getattr(self, metric.name)
                result.append((metric.name, value))

        result.extend(self._extra_metrics.items())
        return dict(result)

    # === Miscellaneous metric properties ===

    @metric_property(
        description="Indicates if the operator is hanging.",
        metrics_group=MetricsGroup.MISC,
        internal_only=True,
    )
    def issue_detector_hanging(self) -> int:
        return self._issue_detector_hanging

    @metric_property(
        description="Indicates if the operator is using high memory.",
        metrics_group=MetricsGroup.MISC,
        internal_only=True,
    )
    def issue_detector_high_memory(self) -> int:
        return self._issue_detector_high_memory

    # === External queue metrics (all operators have external queues) ===

    num_external_inqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in the external inqueue",
        metrics_group=MetricsGroup.PENDING_INPUTS,
    )
    num_external_inqueue_bytes: int = metric_field(
        default=0,
        description="Byte size of blocks in the external inqueue",
        metrics_group=MetricsGroup.PENDING_INPUTS,
    )
    num_external_outqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in the external outqueue",
        metrics_group=MetricsGroup.PENDING_OUTPUTS,
    )
    num_external_outqueue_bytes: int = metric_field(
        default=0,
        description="Byte size of blocks in the external outqueue",
        metrics_group=MetricsGroup.PENDING_OUTPUTS,
    )

    # === Callbacks (only those that BaseOpMetrics actually implements) ===

    def on_input_received(self, input: "RefBundle"):
        """Callback when the operator receives a new input."""
        self.num_inputs_received += 1
        self.num_row_inputs_received += input.num_rows() or 0
        self.bytes_inputs_received += input.size_bytes()

    def on_output_taken(self, output: "RefBundle"):
        """Callback when an output is taken from the operator."""
        self.num_outputs_taken += 1
        self.block_outputs_taken += len(output)
        self.row_outputs_taken += output.num_rows() or 0
        self.bytes_outputs_taken += output.size_bytes()

    def on_output_added_to_external_queue(self, output: "RefBundle") -> None:
        """Callback when output is added to external output queue.

        Tracks blocks and bytes in the external output queue between operators.
        All operators have external queues managed by the streaming executor.

        Args:
            output: The output bundle being added to external queue.
        """
        self.num_external_outqueue_blocks += len(output.blocks)
        self.num_external_outqueue_bytes += output.size_bytes()

    def on_input_added_from_external_queue(self, input: "RefBundle") -> None:
        """Callback when input is added from external input queue.

        Tracks blocks and bytes received from external input queue between operators.
        All operators have external queues managed by the streaming executor.

        Args:
            input: The input bundle received from external queue.
        """
        self.num_external_inqueue_blocks += len(input.blocks)
        self.num_external_inqueue_bytes += input.size_bytes()

    def on_input_removed_from_external_queue(self, input: "RefBundle") -> None:
        """Callback when input is removed/consumed from external input queue.

        Decrements external input queue metrics when input is consumed by the operator.

        Args:
            input: The input bundle being consumed from external queue.
        """
        self.num_external_inqueue_blocks -= len(input.blocks)
        self.num_external_inqueue_bytes -= input.size_bytes()

    def on_output_removed_from_external_queue(self, output: "RefBundle") -> None:
        """Callback when output is removed/taken from external output queue.

        Decrements external output queue metrics when output is taken by downstream.

        Args:
            output: The output bundle being taken from external queue.
        """
        self.num_external_outqueue_blocks -= len(output.blocks)
        self.num_external_outqueue_bytes -= output.size_bytes()

    def on_resource_usage_updated(
        self,
        cpu: float,
        gpu: float,
        object_store_memory: int,
    ) -> None:
        """Callback when resource manager updates resource usage metrics.

        Called by the resource manager to update CPU, GPU, and object store memory usage.

        Args:
            cpu: CPU usage (in cores).
            gpu: GPU usage (in GPUs).
            object_store_memory: Object store memory usage (in bytes).
        """
        self.cpu_usage = cpu
        self.gpu_usage = gpu
        self.obj_store_mem_used = object_store_memory

    def get_object_store_usage_details(self) -> ObjectStoreUsageDetails:
        """Get object store memory usage details for this operator.

        Returns a dataclass containing breakdown of object store memory usage
        by buffer type (internal queues, pending tasks, etc.). Used by the
        resource manager for memory tracking.

        Base implementation returns zeros since BaseOpMetrics doesn't track
        queues or tasks.

        Returns:
            ObjectStoreUsageDetails with all memory values set to 0 or None.
        """
        return ObjectStoreUsageDetails(
            internal_outqueue_memory=0,
            internal_inqueue_memory=0,
            pending_task_outputs_memory=None,
            pending_task_inputs_memory=0,
        )

    def get_per_node_metrics(self) -> Dict[str, NodeMetrics]:
        """Get per-node metrics for this operator.

        Returns a dictionary mapping node IDs to NodeMetrics objects containing
        per-node execution statistics (tasks run, blocks processed, etc.).

        Base implementation returns an empty dict since BaseOpMetrics doesn't
        track per-node statistics. Only TaskOpMetrics and its subclasses track
        per-node metrics.

        Returns:
            Dict mapping node IDs to NodeMetrics, or empty dict if not tracked.
        """
        return {}

    def in_task_submission_backpressure(self) -> bool:
        """Check if the operator is currently in task submission backpressure.

        Task submission backpressure occurs when the operator is prevented from
        submitting new tasks due to resource constraints or policy limits.

        Returns:
            True if in task submission backpressure, False otherwise.
        """
        return False

    def in_task_output_backpressure(self) -> bool:
        """Check if the operator is currently in task output backpressure.

        Task output backpressure occurs when the operator has too many pending
        task outputs that haven't been consumed, indicating downstream consumers
        are slow or blocked.

        Returns:
            True if in task output backpressure, False otherwise.
        """
        return False

    def notify_in_task_submission_backpressure(self, in_backpressure: bool) -> None:
        """Called from executor to update task submission backpressure. No-op in base class."""
        pass

    def notify_in_task_output_backpressure(self, in_backpressure: bool) -> None:
        """Called from executor to update task output backpressure. No-op in base class."""
        pass
