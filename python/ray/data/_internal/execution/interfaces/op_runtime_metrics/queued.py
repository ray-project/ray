"""Queued operator runtime metrics."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    BaseOpMetrics,
    MetricsGroup,
    ObjectStoreUsageDetails,
    metric_field,
    metric_property,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
    from ray.data.context import DataContext


@dataclass
class QueuedOpMetrics(BaseOpMetrics):
    """Metrics for operators with internal input/output queues.

    Adds PENDING_INPUTS and PENDING_OUTPUTS metrics to BaseOpMetrics.
    Used by operators that buffer data but don't execute tasks.

    Examples: UnionOperator, ZipOperator, LimitOperator

    Available callbacks (in addition to BaseOpMetrics):
    - on_input_queued(): Tracks blocks added to internal input queue
    - on_input_dequeued(): Tracks blocks removed from internal input queue
    - on_output_queued(): Tracks blocks added to internal output queue
    - on_output_dequeued(): Tracks blocks removed from internal output queue

    Note: Task-related callbacks (e.g., on_task_submitted) are not available.
    Use TaskOpMetrics for operators that execute tasks.
    """

    # === Pending Inputs-related metrics (internal queues only) ===
    obj_store_mem_internal_inqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in operator's internal input queue.",
        metrics_group=MetricsGroup.PENDING_INPUTS,
    )

    # === Pending Outputs-related metrics (internal queues only) ===
    obj_store_mem_internal_outqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in the operator's internal output queue.",
        metrics_group=MetricsGroup.PENDING_OUTPUTS,
    )

    def __init__(self, data_context: "DataContext"):
        """Initialize QueuedOpMetrics.

        Args:
            data_context: DataContext instance for accessing configuration settings.
        """
        super().__init__(data_context)
        self._internal_inqueue = create_bundle_queue()
        self._internal_outqueue = create_bundle_queue()

    # === Pending Inputs-related metric properties ===

    @metric_property(
        description="Byte size of input blocks in the operator's internal input queue.",
        metrics_group=MetricsGroup.PENDING_INPUTS,
    )
    def obj_store_mem_internal_inqueue(self) -> int:
        return self._internal_inqueue.estimate_size_bytes()

    # === Pending Outputs-related metric properties ===

    @metric_property(
        description=(
            "Byte size of output blocks in the operator's internal output queue."
        ),
        metrics_group=MetricsGroup.PENDING_OUTPUTS,
    )
    def obj_store_mem_internal_outqueue(self) -> int:
        return self._internal_outqueue.estimate_size_bytes()

    # === Callbacks (override to implement queue tracking) ===

    @override
    def on_input_queued(self, input: "RefBundle"):
        """Callback when the operator queues an input."""
        self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
        self._internal_inqueue.add(input)

    @override
    def on_input_dequeued(self, input: "RefBundle"):
        """Callback when the operator dequeues an input."""
        self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
        input_size = input.size_bytes()
        self._internal_inqueue.remove(input)
        assert self.obj_store_mem_internal_inqueue >= 0, (
            self.obj_store_mem_internal_inqueue,
            input_size,
        )

    @override
    def on_output_queued(self, output: "RefBundle"):
        """Callback when an output is queued by the operator."""
        self.obj_store_mem_internal_outqueue_blocks += len(output.blocks)
        self._internal_outqueue.add(output)

    @override
    def on_output_dequeued(self, output: "RefBundle"):
        """Callback when an output is dequeued by the operator."""
        self.obj_store_mem_internal_outqueue_blocks -= len(output.blocks)
        output_size = output.size_bytes()
        self._internal_outqueue.remove(output)
        assert self.obj_store_mem_internal_outqueue >= 0, (
            self.obj_store_mem_internal_outqueue,
            output_size,
        )

    @override
    def get_object_store_usage_details(self) -> ObjectStoreUsageDetails:
        """Get object store memory usage details for this operator.

        Returns queue memory usage. QueuedOpMetrics tracks internal queues
        but doesn't track task-related memory.

        Returns:
            ObjectStoreUsageDetails with queue memory populated.
        """
        return ObjectStoreUsageDetails(
            internal_outqueue_memory=self.obj_store_mem_internal_outqueue,
            internal_inqueue_memory=self.obj_store_mem_internal_inqueue,
            pending_task_outputs_memory=None,
            pending_task_inputs_memory=0,
        )
