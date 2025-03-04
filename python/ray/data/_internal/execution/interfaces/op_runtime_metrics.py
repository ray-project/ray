import time
from collections import defaultdict
from dataclasses import Field, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional
import math

import ray
from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_allocation
from ray.data.block import BlockMetadata

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )


# A metadata key used to mark a dataclass field as a metric.
_IS_FIELD_METRIC_KEY = "__is_metric"
# Metadata keys used to store information about a metric.
_METRIC_FIELD_DESCRIPTION_KEY = "__metric_description"
_METRIC_FIELD_METRICS_GROUP_KEY = "__metric_metrics_group"
_METRIC_FIELD_IS_MAP_ONLY_KEY = "__metric_is_map_only"

_METRICS: List["MetricDefinition"] = []

NODE_UNKNOWN = "unknown"


class MetricsGroup(Enum):
    INPUTS = "inputs"
    OUTPUTS = "outputs"
    TASKS = "tasks"
    OBJECT_STORE_MEMORY = "object_store_memory"
    MISC = "misc"


@dataclass(frozen=True)
class MetricDefinition:
    """Metadata for a metric.

    Args:
        name: The name of the metric.
        description: A human-readable description of the metric, also used as the chart
            description on the Ray Data dashboard.
        metrics_group: The group of the metric, used to organize metrics into groups in
            'StatsActor' and on the Ray Data dashboard.
        map_only: Whether the metric is only measured for 'MapOperators'.
    """

    name: str
    description: str
    metrics_group: str
    # TODO: Let's refactor this parameter so it isn't tightly coupled with a specific
    # operator type (MapOperator).
    map_only: bool = False


def metric_field(
    *,
    description: str,
    metrics_group: str,
    map_only: bool = False,
    **field_kwargs,
):
    """A dataclass field that represents a metric."""
    metadata = field_kwargs.get("metadata", {})

    metadata[_IS_FIELD_METRIC_KEY] = True

    metadata[_METRIC_FIELD_DESCRIPTION_KEY] = description
    metadata[_METRIC_FIELD_METRICS_GROUP_KEY] = metrics_group
    metadata[_METRIC_FIELD_IS_MAP_ONLY_KEY] = map_only

    return field(metadata=metadata, **field_kwargs)


def metric_property(
    *,
    description: str,
    metrics_group: str,
    map_only: bool = False,
):
    """A property that represents a metric."""

    def wrap(func):
        metric = MetricDefinition(
            name=func.__name__,
            description=description,
            metrics_group=metrics_group,
            map_only=map_only,
        )

        _METRICS.append(metric)

        return property(func)

    return wrap


@dataclass
class RunningTaskInfo:
    inputs: RefBundle
    num_outputs: int
    bytes_outputs: int
    start_time: float


@dataclass
class NodeMetrics:
    num_tasks_finished: int = 0
    bytes_outputs_of_finished_tasks: int = 0
    blocks_outputs_of_finished_tasks: int = 0


class OpRuntimesMetricsMeta(type):
    def __init__(cls, name, bases, dict):
        # NOTE: `Field.name` isn't set until the dataclass is created, so we can't
        # create the metrics in `metric_field` directly.
        super().__init__(name, bases, dict)

        # Iterate over the attributes and methods of 'OpRuntimeMetrics'.
        for name, value in dict.items():
            # If an attribute is a dataclass field and has _IS_FIELD_METRIC_KEY in its
            # metadata, then create a metric from the field metadata and add it to the
            # list of metrics. See also the 'metric_field' function.
            if isinstance(value, Field) and value.metadata.get(_IS_FIELD_METRIC_KEY):
                metric = MetricDefinition(
                    name=name,
                    description=value.metadata[_METRIC_FIELD_DESCRIPTION_KEY],
                    metrics_group=value.metadata[_METRIC_FIELD_METRICS_GROUP_KEY],
                    map_only=value.metadata[_METRIC_FIELD_IS_MAP_ONLY_KEY],
                )
                _METRICS.append(metric)


def node_id_from_block_metadata(meta: BlockMetadata) -> str:
    if meta.exec_stats is not None and meta.exec_stats.node_id is not None:
        node_id = meta.exec_stats.node_id
    else:
        node_id = NODE_UNKNOWN
    return node_id


class TaskDurationStats:
    """
    Tracks the running mean and variance incrementally with Welford's algorithm
    by updating the current mean and a measure of total squared differences.
    It allows stable updates of mean and variance in a single pass over the data
    while reducing numerical instability often found in naive computations.

    More on the algorithm: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    """

    def __init__(self):
        self._count = 0
        self._mean = 0.0
        self._m2 = 0.0  # Sum of (x - mean)^2

    def add_duration(self, duration: float) -> None:
        """Add a new sample (task duration in seconds)."""
        self._count += 1
        delta = duration - self._mean
        self._mean += delta / self._count
        delta2 = duration - self._mean
        self._m2 += delta * delta2

    def count(self) -> int:
        return self._count

    def mean(self) -> float:
        return self._mean

    def _variance(self) -> float:
        """Return the current variance of the observed durations."""
        # Variance is m2/(count-1) for sample variance
        if self._count < 2:
            return 0.0
        return self._m2 / (self._count - 1)

    def stddev(self) -> float:
        """Return the current standard deviation of the observed durations."""
        return math.sqrt(self._variance())


@dataclass
class OpRuntimeMetrics(metaclass=OpRuntimesMetricsMeta):
    """Runtime metrics for a 'PhysicalOperator'.

    Metrics are updated dynamically during the execution of the Dataset.
    This class can be used for either observablity or scheduling purposes.

    DO NOT modify the fields of this class directly. Instead, use the provided
    callback methods.
    """

    # TODO(hchen): Fields tagged with "map_only" currently only work for MapOperator.
    # We should make them work for all operators by unifying the task execution code.

    # === Inputs-related metrics ===
    num_inputs_received: int = metric_field(
        default=0,
        description="Number of input blocks received by operator.",
        metrics_group=MetricsGroup.INPUTS,
    )
    bytes_inputs_received: int = metric_field(
        default=0,
        description="Byte size of input blocks received by operator.",
        metrics_group=MetricsGroup.INPUTS,
    )
    num_task_inputs_processed: int = metric_field(
        default=0,
        description=(
            "Number of input blocks that operator's tasks have finished processing."
        ),
        metrics_group=MetricsGroup.INPUTS,
        map_only=True,
    )
    bytes_task_inputs_processed: int = metric_field(
        default=0,
        description=(
            "Byte size of input blocks that operator's tasks have finished processing."
        ),
        metrics_group=MetricsGroup.INPUTS,
        map_only=True,
    )
    bytes_inputs_of_submitted_tasks: int = metric_field(
        default=0,
        description="Byte size of input blocks passed to submitted tasks.",
        metrics_group=MetricsGroup.INPUTS,
        map_only=True,
    )

    # === Outputs-related metrics ===
    num_task_outputs_generated: int = metric_field(
        default=0,
        description="Number of output blocks generated by tasks.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    bytes_task_outputs_generated: int = metric_field(
        default=0,
        description="Byte size of output blocks generated by tasks.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    rows_task_outputs_generated: int = metric_field(
        default=0,
        description="Number of output rows generated by tasks.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    num_outputs_taken: int = metric_field(
        default=0,
        description=(
            "Number of output blocks that are already taken by downstream operators."
        ),
        metrics_group=MetricsGroup.OUTPUTS,
    )
    bytes_outputs_taken: int = metric_field(
        default=0,
        description=(
            "Byte size of output blocks that are already taken by downstream operators."
        ),
        metrics_group=MetricsGroup.OUTPUTS,
    )
    num_outputs_of_finished_tasks: int = metric_field(
        default=0,
        description="Number of generated output blocks that are from finished tasks.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    bytes_outputs_of_finished_tasks: int = metric_field(
        default=0,
        description=(
            "Byte size of generated output blocks that are from finished tasks."
        ),
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )

    # === Tasks-related metrics ===
    num_tasks_submitted: int = metric_field(
        default=0,
        description="Number of submitted tasks.",
        metrics_group=MetricsGroup.TASKS,
        map_only=True,
    )
    num_tasks_running: int = metric_field(
        default=0,
        description="Number of running tasks.",
        metrics_group=MetricsGroup.TASKS,
        map_only=True,
    )
    num_tasks_have_outputs: int = metric_field(
        default=0,
        description="Number of tasks that already have output.",
        metrics_group=MetricsGroup.TASKS,
        map_only=True,
    )
    num_tasks_finished: int = metric_field(
        default=0,
        description="Number of finished tasks.",
        metrics_group=MetricsGroup.TASKS,
        map_only=True,
    )
    num_tasks_failed: int = metric_field(
        default=0,
        description="Number of failed tasks.",
        metrics_group=MetricsGroup.TASKS,
        map_only=True,
    )
    block_generation_time: float = metric_field(
        default=0,
        description="Time spent generating blocks in tasks.",
        metrics_group=MetricsGroup.TASKS,
        map_only=True,
    )
    task_submission_backpressure_time: float = metric_field(
        default=0,
        description="Time spent in task submission backpressure.",
        metrics_group=MetricsGroup.TASKS,
    )

    # === Object store memory metrics ===
    obj_store_mem_internal_inqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in operator's internal input queue.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
    )
    obj_store_mem_internal_outqueue_blocks: int = metric_field(
        default=0,
        description="Number of blocks in the operator's internal output queue.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
    )
    obj_store_mem_freed: int = metric_field(
        default=0,
        description="Byte size of freed memory in object store.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
        map_only=True,
    )
    obj_store_mem_spilled: int = metric_field(
        default=0,
        description="Byte size of spilled memory in object store.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
        map_only=True,
    )
    obj_store_mem_used: int = metric_field(
        default=0,
        description="Byte size of used memory in object store.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
    )

    # === Miscellaneous metrics ===
    # Use "metrics_group: "misc" in the metadata for new metrics in this section.

    def __init__(self, op: "PhysicalOperator"):
        from ray.data._internal.execution.operators.map_operator import MapOperator

        self._op = op
        self._is_map = isinstance(op, MapOperator)
        self._running_tasks: Dict[int, RunningTaskInfo] = {}
        self._extra_metrics: Dict[str, Any] = {}
        # Start time of current pause due to task submission backpressure
        self._task_submission_backpressure_start_time = -1

        self._internal_inqueue = create_bundle_queue()
        self._internal_outqueue = create_bundle_queue()
        self._pending_task_inputs = create_bundle_queue()
        self._op_task_duration_stats = TaskDurationStats()

        self._per_node_metrics: Dict[str, NodeMetrics] = defaultdict(NodeMetrics)
        self._per_node_metrics_enabled: bool = op.data_context.enable_per_node_metrics

    @property
    def extra_metrics(self) -> Dict[str, Any]:
        """Return a dict of extra metrics."""
        return self._extra_metrics

    @classmethod
    def get_metrics(self) -> List[MetricDefinition]:
        return list(_METRICS)

    def as_dict(self):
        """Return a dict representation of the metrics."""
        result = []
        for metric in self.get_metrics():
            if not self._is_map and metric.map_only:
                continue
            value = getattr(self, metric.name)
            result.append((metric.name, value))

        # TODO: record resource usage in OpRuntimeMetrics,
        # avoid calling self._op.current_processor_usage()
        resource_usage = self._op.current_processor_usage()
        result.extend(
            [
                ("cpu_usage", resource_usage.cpu or 0),
                ("gpu_usage", resource_usage.gpu or 0),
            ]
        )
        result.extend(self._extra_metrics.items())
        return dict(result)

    @metric_property(
        description="Average number of blocks generated per task.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    def average_num_outputs_per_task(self) -> Optional[float]:
        """Average number of output blocks per task, or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.num_outputs_of_finished_tasks / self.num_tasks_finished

    @metric_property(
        description="Average size of task output in bytes.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    def average_bytes_per_output(self) -> Optional[float]:
        """Average size in bytes of output blocks."""
        if self.num_task_outputs_generated == 0:
            return None
        else:
            return self.bytes_task_outputs_generated / self.num_task_outputs_generated

    @metric_property(
        description="Byte size of input blocks in the operator's internal input queue.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
    )
    def obj_store_mem_internal_inqueue(self) -> int:
        return self._internal_inqueue.estimate_size_bytes()

    @metric_property(
        description=(
            "Byte size of output blocks in the operator's internal output queue."
        ),
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
    )
    def obj_store_mem_internal_outqueue(self) -> int:
        return self._internal_outqueue.estimate_size_bytes()

    @metric_property(
        description="Byte size of input blocks used by pending tasks.",
        metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
        map_only=True,
    )
    def obj_store_mem_pending_task_inputs(self) -> int:
        return self._pending_task_inputs.estimate_size_bytes()

    @property
    def obj_store_mem_pending_task_outputs(self) -> Optional[float]:
        """Estimated size in bytes of output blocks in Ray generator buffers.

        If an estimate isn't available, this property returns ``None``.
        """
        per_task_output = self.obj_store_mem_max_pending_output_per_task
        if per_task_output is None:
            return None

        # Ray Data launches multiple tasks per actor, but only one task runs at a
        # time per actor. So, the number of actually running tasks is capped by the
        # number of active actors.
        from ray.data._internal.execution.operators.actor_pool_map_operator import (
            ActorPoolMapOperator,
        )

        num_tasks_running = self.num_tasks_running
        if isinstance(self._op, ActorPoolMapOperator):
            num_tasks_running = min(
                num_tasks_running, self._op._actor_pool.num_active_actors()
            )

        return num_tasks_running * per_task_output

    @property
    def obj_store_mem_max_pending_output_per_task(self) -> Optional[float]:
        """Estimated size in bytes of output blocks in a task's generator buffer."""
        context = self._op.data_context
        if context._max_num_blocks_in_streaming_gen_buffer is None:
            return None

        bytes_per_output = self.average_bytes_per_output
        if bytes_per_output is None:
            bytes_per_output = context.target_max_block_size

        num_pending_outputs = context._max_num_blocks_in_streaming_gen_buffer
        if self.average_num_outputs_per_task is not None:
            num_pending_outputs = min(
                num_pending_outputs, self.average_num_outputs_per_task
            )
        return bytes_per_output * num_pending_outputs

    @metric_property(
        description="Average size of task inputs in bytes.",
        metrics_group=MetricsGroup.INPUTS,
        map_only=True,
    )
    def average_bytes_inputs_per_task(self) -> Optional[float]:
        """Average size in bytes of ref bundles passed to tasks, or ``None`` if no
        tasks have been submitted."""
        if self.num_tasks_submitted == 0:
            return None
        else:
            return self.bytes_inputs_of_submitted_tasks / self.num_tasks_submitted

    @metric_property(
        description="Average total output size of task in bytes.",
        metrics_group=MetricsGroup.OUTPUTS,
        map_only=True,
    )
    def average_bytes_outputs_per_task(self) -> Optional[float]:
        """Average size in bytes of output blocks per task,
        or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.bytes_outputs_of_finished_tasks / self.num_tasks_finished

    def on_input_received(self, input: RefBundle):
        """Callback when the operator receives a new input."""
        self.num_inputs_received += 1
        self.bytes_inputs_received += input.size_bytes()

    def on_input_queued(self, input: RefBundle):
        """Callback when the operator queues an input."""
        self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
        self._internal_inqueue.add(input)

    def on_input_dequeued(self, input: RefBundle):
        """Callback when the operator dequeues an input."""
        self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
        input_size = input.size_bytes()
        self._internal_inqueue.remove(input)
        assert self.obj_store_mem_internal_inqueue >= 0, (
            self._op,
            self.obj_store_mem_internal_inqueue,
            input_size,
        )

    def on_output_queued(self, output: RefBundle):
        """Callback when an output is queued by the operator."""
        self.obj_store_mem_internal_outqueue_blocks += len(output.blocks)
        self._internal_outqueue.add(output)

    def on_output_dequeued(self, output: RefBundle):
        """Callback when an output is dequeued by the operator."""
        self.obj_store_mem_internal_outqueue_blocks -= len(output.blocks)
        output_size = output.size_bytes()
        self._internal_outqueue.remove(output)
        assert self.obj_store_mem_internal_outqueue >= 0, (
            self._op,
            self.obj_store_mem_internal_outqueue,
            output_size,
        )

    def on_toggle_task_submission_backpressure(self, in_backpressure):
        if in_backpressure and self._task_submission_backpressure_start_time == -1:
            # backpressure starting, start timer
            self._task_submission_backpressure_start_time = time.perf_counter()
        elif self._task_submission_backpressure_start_time != -1:
            # backpressure stopping, stop timer
            self.task_submission_backpressure_time += (
                time.perf_counter() - self._task_submission_backpressure_start_time
            )
            self._task_submission_backpressure_start_time = -1

    def on_output_taken(self, output: RefBundle):
        """Callback when an output is taken from the operator."""
        self.num_outputs_taken += 1
        self.bytes_outputs_taken += output.size_bytes()

    def on_task_submitted(self, task_index: int, inputs: RefBundle):
        """Callback when the operator submits a task."""
        self.num_tasks_submitted += 1
        self.num_tasks_running += 1
        self.bytes_inputs_of_submitted_tasks += inputs.size_bytes()
        self._pending_task_inputs.add(inputs)
        self._running_tasks[task_index] = RunningTaskInfo(
            inputs, 0, 0, time.perf_counter()
        )

    def on_task_output_generated(self, task_index: int, output: RefBundle):
        """Callback when a new task generates an output."""
        num_outputs = len(output)
        output_bytes = output.size_bytes()

        self.num_task_outputs_generated += num_outputs
        self.bytes_task_outputs_generated += output_bytes

        task_info = self._running_tasks[task_index]
        if task_info.num_outputs == 0:
            self.num_tasks_have_outputs += 1
        task_info.num_outputs += num_outputs
        task_info.bytes_outputs += output_bytes

        for block_ref, meta in output.blocks:
            assert meta.exec_stats and meta.exec_stats.wall_time_s
            self.block_generation_time += meta.exec_stats.wall_time_s
            assert meta.num_rows is not None
            self.rows_task_outputs_generated += meta.num_rows
            trace_allocation(block_ref, "operator_output")

        # Update per node metrics
        if self._per_node_metrics_enabled:
            for _, meta in output.blocks:
                node_id = node_id_from_block_metadata(meta)
                node_metrics = self._per_node_metrics[node_id]

                node_metrics.bytes_outputs_of_finished_tasks += meta.size_bytes
                node_metrics.blocks_outputs_of_finished_tasks += 1

    def on_task_finished(self, task_index: int, exception: Optional[Exception]):
        """Callback when a task is finished."""
        self.num_tasks_running -= 1
        self.num_tasks_finished += 1
        if exception is not None:
            self.num_tasks_failed += 1

        task_info = self._running_tasks[task_index]
        self.num_outputs_of_finished_tasks += task_info.num_outputs
        self.bytes_outputs_of_finished_tasks += task_info.bytes_outputs
        self._op_task_duration_stats.add_duration(
            time.perf_counter() - task_info.start_time
        )

        inputs = self._running_tasks[task_index].inputs
        self.num_task_inputs_processed += len(inputs)
        total_input_size = inputs.size_bytes()
        self.bytes_task_inputs_processed += total_input_size
        input_size = inputs.size_bytes()
        self._pending_task_inputs.remove(inputs)
        assert self.obj_store_mem_pending_task_inputs >= 0, (
            self._op,
            self.obj_store_mem_pending_task_inputs,
            input_size,
        )

        ctx = self._op.data_context
        if ctx.enable_get_object_locations_for_metrics:
            locations = ray.experimental.get_object_locations(inputs.block_refs)
            for block, meta in inputs.blocks:
                if locations[block].get("did_spill", False):
                    assert meta.size_bytes is not None
                    self.obj_store_mem_spilled += meta.size_bytes

        self.obj_store_mem_freed += total_input_size

        # Update per node metrics
        if self._per_node_metrics_enabled:
            node_ids = set()
            for _, meta in inputs.blocks:
                node_id = node_id_from_block_metadata(meta)
                node_metrics = self._per_node_metrics[node_id]

                # Stats to update once per node id or if node id is unknown
                if node_id not in node_ids or node_id == NODE_UNKNOWN:
                    node_metrics.num_tasks_finished += 1

                # Keep track of node ids to ensure we don't double count
                node_ids.add(node_id)

        inputs.destroy_if_owned()
        del self._running_tasks[task_index]
