"""Common utilities, enums, and decorators for operator runtime metrics."""

import math
from dataclasses import Field, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from ray.data.block import BlockMetadata

# A metadata key used to mark a dataclass field as a metric.
_IS_FIELD_METRIC_KEY = "__is_metric"
# Metadata keys used to store information about a metric.
_METRIC_FIELD_DESCRIPTION_KEY = "__metric_description"
_METRIC_FIELD_METRICS_GROUP_KEY = "__metric_metrics_group"
_METRIC_FIELD_METRICS_TYPE_KEY = "__metric_metrics_type"
_METRIC_FIELD_METRICS_ARGS_KEY = "__metric_metrics_args"
_METRIC_FIELD_INTERNAL_ONLY_KEY = "__metric_internal_only"

_METRICS: List["MetricDefinition"] = []

NODE_UNKNOWN = "unknown"


@dataclass
class ObjectStoreUsageBreakdown:
    """Object store memory usage details for an operator.

    This dataclass encapsulates different types of object store memory usage
    that the resource manager needs to track. Each metrics class provides
    its own implementation based on what types of buffers it manages.

    All memory values are in bytes. None indicates the metric is not tracked.
    """

    # Internal output buffers (e.g., generator buffers, internal queues)
    internal_outqueue_memory: int = 0
    internal_outqueue_num_blocks: int = 0
    # Internal input buffers (e.g., internal input queues)
    internal_inqueue_memory: int = 0
    internal_inqueue_num_blocks: int = 0

    # Pending task outputs (e.g., in Ray generator buffers)
    pending_task_outputs_memory: Optional[int] = None

    # Pending task inputs (e.g., inputs for submitted but not finished tasks)
    pending_task_inputs_memory: int = 0


class MetricsGroup(Enum):
    """Groups for organizing operator runtime metrics.

    These groups align with the dashboard panel organization.
    """

    PENDING_INPUTS = "pending_inputs"
    INPUTS = "inputs"
    PENDING_OUTPUTS = "pending_outputs"
    OUTPUTS = "outputs"
    TASKS = "tasks"
    ACTORS = "actors"
    RESOURCE_USAGE = "resource_usage"  # Keep original string value for compatibility
    MISC = "misc"


class MetricsType(Enum):
    Counter = 0
    Gauge = 1
    Histogram = 2


@dataclass(frozen=True)
class MetricDefinition:
    """Metadata for a metric.

    Args:
        name: The name of the metric.
        description: A human-readable description of the metric, also used as the chart
            description on the Ray Data dashboard.
        metrics_group: The group of the metric, used to organize metrics into groups in
            'StatsActor' and on the Ray Data dashboard.
    """

    name: str
    description: str
    metrics_group: str
    metrics_type: MetricsType
    metrics_args: Dict[str, Any]
    internal_only: bool = False  # do not expose this metric to the user


def metric_field(
    *,
    description: str,
    metrics_group: str,
    metrics_type: MetricsType = MetricsType.Gauge,
    metrics_args: Dict[str, Any] = None,
    internal_only: bool = False,  # do not expose this metric to the user
    **field_kwargs,
):
    """A dataclass field that represents a metric."""
    from dataclasses import field

    metadata = field_kwargs.get("metadata", {})

    metadata[_IS_FIELD_METRIC_KEY] = True

    metadata[_METRIC_FIELD_DESCRIPTION_KEY] = description
    metadata[_METRIC_FIELD_METRICS_GROUP_KEY] = metrics_group
    metadata[_METRIC_FIELD_METRICS_TYPE_KEY] = metrics_type
    metadata[_METRIC_FIELD_METRICS_ARGS_KEY] = metrics_args or {}
    metadata[_METRIC_FIELD_INTERNAL_ONLY_KEY] = internal_only

    return field(metadata=metadata, **field_kwargs)


def metric_property(
    *,
    description: str,
    metrics_group: str,
    metrics_type: MetricsType = MetricsType.Gauge,
    metrics_args: Dict[str, Any] = None,
    internal_only: bool = False,  # do not expose this metric to the user
):
    """A property that represents a metric."""

    def wrap(func):
        metric = MetricDefinition(
            name=func.__name__,
            description=description,
            metrics_group=metrics_group,
            metrics_type=metrics_type,
            metrics_args=(metrics_args or {}),
            internal_only=internal_only,
        )

        _METRICS.append(metric)

        return property(func)

    return wrap


@dataclass
class RunningTaskInfo:
    """Information about a running task."""

    from ray.data._internal.execution.interfaces.ref_bundle import RefBundle

    inputs: RefBundle
    num_outputs: int
    bytes_outputs: int
    num_rows_produced: int
    start_time: float
    cum_block_gen_time: float


@dataclass
class NodeMetrics:
    """Per-node metrics for tracking task execution."""

    num_tasks_finished: int = 0
    bytes_outputs_of_finished_tasks: int = 0
    blocks_outputs_of_finished_tasks: int = 0


class OpRuntimesMetricsMeta(type):
    """Metaclass for operator runtime metrics.

    This metaclass automatically registers metric fields with the global metrics list.
    """

    def __init__(cls, name, bases, dict):
        # NOTE: `Field.name` isn't set until the dataclass is created, so we can't
        # create the metrics in `metric_field` directly.
        super().__init__(name, bases, dict)

        # Iterate over the attributes and methods of the metrics class.
        for name, value in dict.items():
            # If an attribute is a dataclass field and has _IS_FIELD_METRIC_KEY in its
            # metadata, then create a metric from the field metadata and add it to the
            # list of metrics. See also the 'metric_field' function.
            if isinstance(value, Field) and value.metadata.get(_IS_FIELD_METRIC_KEY):
                metric = MetricDefinition(
                    name=name,
                    description=value.metadata[_METRIC_FIELD_DESCRIPTION_KEY],
                    metrics_group=value.metadata[_METRIC_FIELD_METRICS_GROUP_KEY],
                    metrics_type=value.metadata[_METRIC_FIELD_METRICS_TYPE_KEY],
                    metrics_args=value.metadata[_METRIC_FIELD_METRICS_ARGS_KEY],
                    internal_only=value.metadata[_METRIC_FIELD_INTERNAL_ONLY_KEY],
                )
                _METRICS.append(metric)


def node_id_from_block_metadata(meta: BlockMetadata) -> str:
    """Extract node ID from block metadata."""
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
class TaskMetrics:
    """Task metrics for an operator."""

    num_task_outputs_generated: int = 0
    average_max_uss_per_task: float = 0.0
    op_task_duration_stats: TaskDurationStats = TaskDurationStats()
    running_tasks: Dict[int, RunningTaskInfo] = {}
