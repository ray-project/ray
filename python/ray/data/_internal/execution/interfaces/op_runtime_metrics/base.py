"""Base operator runtime metrics."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List

from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces.op_runtime_metrics.common import (
    _METRICS,
    MetricDefinition,
    MetricsGroup,
    NodeMetrics,
    ObjectStoreUsageBreakdown,
    OpRuntimesMetricsMeta,
    TaskMetrics,
    metric_field,
    metric_property,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
    from ray.data.context import DataContext


@dataclass
class BaseOpMetrics(metaclass=OpRuntimesMetricsMeta):
    """Base runtime metrics for operators.

    Contains all common metrics: INPUTS, OUTPUTS, PENDING_INPUTS, PENDING_OUTPUTS,
    MISC, and basic RESOURCE_USAGE (cpu/gpu) metrics.

    All operators use this class or TaskOpMetrics (which extends it).

    Available callbacks:
    - on_input_received(): Tracks input blocks received
    - on_output_taken(): Tracks output blocks taken by downstream operators
    - on_input_queued(): Tracks blocks added to internal input queue
    - on_input_dequeued(): Tracks blocks removed from internal input queue
    - on_output_queued(): Tracks blocks added to internal output queue
    - on_output_dequeued(): Tracks blocks removed from internal output queue
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

    issue_detector_hanging: int = metric_field(
        default=0,
        description="Indicates if the operator is hanging.",
        metrics_group=MetricsGroup.MISC,
        internal_only=True,
    )
    issue_detector_high_memory: int = metric_field(
        default=0,
        description="Indicates if the operator is using high memory.",
        metrics_group=MetricsGroup.MISC,
        internal_only=True,
    )

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

    # === Internal queue metrics (for operators with internal queues) ===

    obj_store_mem_internal_inqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in operator's internal input queue.",
        metrics_group=MetricsGroup.PENDING_INPUTS,
    )

    obj_store_mem_internal_outqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in the operator's internal output queue.",
        metrics_group=MetricsGroup.PENDING_OUTPUTS,
    )

    def __init__(self, data_context: "DataContext"):
        """Initialize BaseOpMetrics.

        Args:
            data_context: DataContext instance for accessing configuration settings.
        """
        self._data_context = data_context
        self._extra_metrics: Dict[str, Any] = {}
        # Internal queues for operators that need them
        # Will remain empty/unused for simple operators like InputDataBuffer
        self._internal_inqueue = create_bundle_queue()
        self._internal_outqueue = create_bundle_queue()

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

    def on_input_queued(self, input: "RefBundle"):
        """Callback when the operator queues an input to its internal queue."""
        self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
        self._internal_inqueue.add(input)

    def on_input_dequeued(self, input: "RefBundle"):
        """Callback when the operator dequeues an input from its internal queue."""
        self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
        input_size = input.size_bytes()
        self._internal_inqueue.remove(input)
        assert self.obj_store_mem_internal_inqueue >= 0, (
            self.obj_store_mem_internal_inqueue,
            input_size,
        )

    def on_output_queued(self, output: "RefBundle"):
        """Callback when an output is queued to the operator's internal queue."""
        self.obj_store_mem_internal_outqueue_blocks += len(output.blocks)
        self._internal_outqueue.add(output)

    def on_output_dequeued(self, output: "RefBundle"):
        """Callback when an output is dequeued from the operator's internal queue."""
        self.obj_store_mem_internal_outqueue_blocks -= len(output.blocks)
        output_size = output.size_bytes()
        self._internal_outqueue.remove(output)
        assert self.obj_store_mem_internal_outqueue >= 0, (
            self.obj_store_mem_internal_outqueue,
            output_size,
        )

    # === Internal queue metric properties ===

    @metric_property(
        description="Byte size of input blocks in the operator's internal input queue.",
        metrics_group=MetricsGroup.PENDING_INPUTS,
    )
    def obj_store_mem_internal_inqueue(self) -> int:
        return self._internal_inqueue.estimate_size_bytes()

    @metric_property(
        description=(
            "Byte size of output blocks in the operator's internal output queue."
        ),
        metrics_group=MetricsGroup.PENDING_OUTPUTS,
    )
    def obj_store_mem_internal_outqueue(self) -> int:
        return self._internal_outqueue.estimate_size_bytes()

    # === Callbacks ===

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

    def get_object_store_usage_details(self) -> ObjectStoreUsageBreakdown:
        """Get object store memory usage details for this operator.

        Returns a dataclass containing breakdown of object store memory usage
        by buffer type (internal queues, pending tasks, etc.). Used by the
        resource manager for memory tracking.

        Returns:
            ObjectStoreUsageBreakdown with internal queue memory populated.
        """
        return ObjectStoreUsageBreakdown(
            internal_outqueue_memory=self.obj_store_mem_internal_outqueue,
            internal_inqueue_memory=self.obj_store_mem_internal_inqueue,
            internal_outqueue_num_blocks=self.obj_store_mem_internal_outqueue_blocks,
            internal_inqueue_num_blocks=self.obj_store_mem_internal_inqueue_blocks,
            pending_task_outputs_memory=None,
            pending_task_inputs_memory=0,
        )

    def get_per_node_metrics(self) -> Dict[str, NodeMetrics]:
        """Get per-node metrics for this operator.

        Returns a dictionary mapping node IDs to NodeMetrics objects containing
        per-node execution statistics (tasks run, blocks processed, etc.).

        Returns:
            Dict mapping node IDs to NodeMetrics, or empty dict if not tracked.
        """
        return {}

    def task_metrics(self) -> TaskMetrics:
        """Returns the task stats for this operator.
        Used in issue detection to check if the operator is stuck."""
        return TaskMetrics()
