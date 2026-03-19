from enum import Enum
from typing import Callable, Dict, List

from ray._private.ray_constants import RAY_METRIC_CARDINALITY_LEVEL
from ray._private.telemetry.metric_types import MetricType

# Keep in sync with the WorkerIdKey in src/ray/stats/tag_defs.cc
WORKER_ID_TAG_KEY = "WorkerId"
# Keep in sync with the NameKey in src/ray/stats/tag_defs.cc
TASK_OR_ACTOR_NAME_TAG_KEY = "Name"
# Keep in sync with REPLICA_TAG in python/ray/serve/metrics.py
SERVE_REPLICA_TAG_KEY = "replica"
# actor_id is a per-handle-instance label used in Serve router metrics
SERVE_ACTOR_ID_TAG_KEY = "actor_id"

# Aggregation functions for high-cardinality gauge metrics when labels are dropped.
# Counter and Sum metrics always use sum() aggregation.
HIGH_CARDINALITY_GAUGE_AGGREGATION: Dict[str, Callable[[List[float]], float]] = {
    "tasks": sum,
    "actors": sum,
}

# Per-instance labels that cause unbounded cardinality growth in cumulative metrics
# (Counter, Sum). Unlike gauges which clear after each scrape, cumulative metrics
# retain one entry per unique tag combination forever. These labels are unique per
# actor/replica/worker instance, so they must be dropped at write time.
# Dropped when RAY_metric_cardinality_level >= "recommended" (the default).
_CUMULATIVE_METRIC_HIGH_CARDINALITY_LABELS = (
    WORKER_ID_TAG_KEY,
    SERVE_REPLICA_TAG_KEY,
    SERVE_ACTOR_ID_TAG_KEY,
)

_CARDINALITY_LEVEL = None
_HIGH_CARDINALITY_LABELS: Dict[str, List[str]] = {}
_CUMULATIVE_HIGH_CARDINALITY_LABELS_CACHE: Dict[str, List[str]] = {}


class MetricCardinality(str, Enum):
    """Cardinality level configuration for all Ray metrics (ray_tasks, ray_actors,
    etc.). This configurtion is used to determine whether to globally drop high
    cardinality labels. This is important for high scale clusters that might consist
    thousands of workers, millions of tasks.

    - LEGACY: Keep all labels. This is the default behavior.
    - RECOMMENDED: Drop high cardinality labels. The set of high cardinality labels
    are determined internally by Ray and not exposed to users. For gauge metrics,
    this currently includes WorkerId. For cumulative metrics (Counter, Sum), this
    includes WorkerId, replica, and actor_id to prevent unbounded memory growth.
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

    @staticmethod
    def get_cumulative_metric_labels_to_drop(metric_name: str) -> List[str]:
        """Get per-instance labels to drop at write time for cumulative metrics.

        Cumulative metrics (Counter, Sum) are never cleared, so every unique tag
        combination creates a permanent entry. This returns labels to strip so
        observations collapse to a bounded number of entries.

        Returns an empty list at LEGACY cardinality level.
        """
        if metric_name in _CUMULATIVE_HIGH_CARDINALITY_LABELS_CACHE:
            return _CUMULATIVE_HIGH_CARDINALITY_LABELS_CACHE[metric_name]

        cardinality_level = MetricCardinality.get_cardinality_level()
        if cardinality_level == MetricCardinality.LEGACY:
            _CUMULATIVE_HIGH_CARDINALITY_LABELS_CACHE[metric_name] = []
            return []

        _CUMULATIVE_HIGH_CARDINALITY_LABELS_CACHE[metric_name] = list(
            _CUMULATIVE_METRIC_HIGH_CARDINALITY_LABELS
        )
        return _CUMULATIVE_HIGH_CARDINALITY_LABELS_CACHE[metric_name]
