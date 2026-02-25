from enum import Enum
from typing import Callable, Dict, List

from ray._private.ray_constants import RAY_METRIC_CARDINALITY_LEVEL
from ray._private.telemetry.metric_types import MetricType

# Keep in sync with the WorkerIdKey in src/ray/stats/tag_defs.cc
WORKER_ID_TAG_KEY = "WorkerId"
# Keep in sync with the NameKey in src/ray/stats/tag_defs.cc
TASK_OR_ACTOR_NAME_TAG_KEY = "Name"
# Aggregation functions for high-cardinality gauge metrics when labels are dropped.
# Counter and Sum metrics always use sum() aggregation.
HIGH_CARDINALITY_GAUGE_AGGREGATION: Dict[str, Callable[[List[float]], float]] = {
    "tasks": sum,
    "actors": sum,
}

_CARDINALITY_LEVEL = None
_HIGH_CARDINALITY_LABELS: Dict[str, List[str]] = {}


class MetricCardinality(str, Enum):
    """Cardinality level configuration for all Ray metrics (ray_tasks, ray_actors,
    etc.). This configurtion is used to determine whether to globally drop high
    cardinality labels. This is important for high scale clusters that might consist
    thousands of workers, millions of tasks.

    - LEGACY: Keep all labels. This is the default behavior.
    - RECOMMENDED: Drop high cardinality labels. The set of high cardinality labels
    are determined internally by Ray and not exposed to users. Currently, this includes
    the following labels: WorkerId
    - LOW: Same as RECOMMENDED, but also drop the Name label for tasks and actors.
    """

    LEGACY = "legacy"
    RECOMMENDED = "recommended"
    LOW = "low"

    @staticmethod
    def get_cardinality_level() -> "MetricCardinality":
        global _CARDINALITY_LEVEL
        if _CARDINALITY_LEVEL is not None:
            return _CARDINALITY_LEVEL
        try:
            _CARDINALITY_LEVEL = MetricCardinality(RAY_METRIC_CARDINALITY_LEVEL.lower())
        except ValueError:
            _CARDINALITY_LEVEL = MetricCardinality.LEGACY
        return _CARDINALITY_LEVEL

    @staticmethod
    def get_aggregation_function(
        metric_name: str, metric_type: MetricType = MetricType.GAUGE
    ) -> Callable[[List[float]], float]:
        """Get the aggregation function for a metric when labels are dropped. This method does not currently support histogram metrics.

        Args:
            metric_name: The name of the metric.
            metric_type: The type of the metric. If provided, Counter and Sum
                metrics always use sum() aggregation.

        Returns:
            A function that takes a list of values and returns the aggregated value.
        """
        # Counter and Sum metrics always aggregate by summing
        if metric_type in (MetricType.COUNTER, MetricType.SUM):
            return sum
        # Histogram metrics are not supported by this method
        if metric_type == MetricType.HISTOGRAM:
            raise ValueError("No Aggregation function for histogram metrics.")
        # Gauge metrics use metric-specific aggregation or default to first value
        if metric_name in HIGH_CARDINALITY_GAUGE_AGGREGATION:
            return HIGH_CARDINALITY_GAUGE_AGGREGATION[metric_name]
        return lambda values: values[0]

    @staticmethod
    def get_high_cardinality_metrics() -> List[str]:
        return list(HIGH_CARDINALITY_GAUGE_AGGREGATION.keys())

    @staticmethod
    def get_high_cardinality_labels_to_drop(metric_name: str) -> List[str]:
        """
        Get the high cardinality labels of the metric.
        """
        if metric_name in _HIGH_CARDINALITY_LABELS:
            return _HIGH_CARDINALITY_LABELS[metric_name]

        cardinality_level = MetricCardinality.get_cardinality_level()
        if (
            cardinality_level == MetricCardinality.LEGACY
            or metric_name not in MetricCardinality.get_high_cardinality_metrics()
        ):
            _HIGH_CARDINALITY_LABELS[metric_name] = []
            return []

        _HIGH_CARDINALITY_LABELS[metric_name] = [WORKER_ID_TAG_KEY]
        if cardinality_level == MetricCardinality.LOW and metric_name in [
            "tasks",
            "actors",
        ]:
            _HIGH_CARDINALITY_LABELS[metric_name].append(TASK_OR_ACTOR_NAME_TAG_KEY)
        return _HIGH_CARDINALITY_LABELS[metric_name]
