from enum import Enum
from typing import Callable, Collection, Dict, List, Optional

from ray._private.ray_constants import RAY_METRIC_CARDINALITY_LEVEL
from ray._private.telemetry.metric_types import MetricType

# Keep in sync with the WorkerIdKey in src/ray/stats/tag_defs.cc
WORKER_ID_TAG_KEY = "WorkerId"
# Keep in sync with the NameKey in src/ray/stats/tag_defs.cc
TASK_OR_ACTOR_NAME_TAG_KEY = "Name"
# Serve attaches this to every metric emitted inside a replica. It is 1:1 with
# WorkerId, so a metric that carries it is a Serve metric and drives per-replica
# cardinality. Its presence is what scopes the reduction to Serve metrics.
REPLICA_ID_TAG_KEY = "ReplicaId"
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
    - RECOMMENDED: Drop WorkerId from the metrics Ray marks as high cardinality
    (tasks, actors). Serve metrics and other metrics are untouched.
    - LOW: Same as RECOMMENDED, and additionally drop the Name label for tasks
    and actors, and drop WorkerId and ReplicaId from Serve metrics (any series
    that carries a ReplicaId tag), collapsing per-replica series to the node
    level.
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
        # Gauge metrics use metric-specific aggregation, or sum by default so
        # that additive per-replica gauges (running, waiting, ...) collapse to a
        # correct node total when WorkerId and ReplicaId are dropped. A ratio
        # gauge summed this way overcounts; aggregate those at the query layer
        # until the metric owner can declare its aggregation.
        if metric_name in HIGH_CARDINALITY_GAUGE_AGGREGATION:
            return HIGH_CARDINALITY_GAUGE_AGGREGATION[metric_name]
        return sum

    @staticmethod
    def get_high_cardinality_metrics() -> List[str]:
        return list(HIGH_CARDINALITY_GAUGE_AGGREGATION.keys())

    @staticmethod
    def get_high_cardinality_labels_to_drop(
        metric_name: str, tag_keys: Optional[Collection[str]] = None
    ) -> List[str]:
        """Get the high cardinality labels to drop for one metric.

        LEGACY drops nothing. Otherwise:
        - The metrics Ray marks as high cardinality (tasks, actors) drop
          WorkerId, and additionally Name at the LOW level.
        - At the LOW level only, a Serve metric (a series that carries the
          ReplicaId tag) drops both WorkerId and ReplicaId, so per-replica
          series collapse to the node level.
        Every other metric is left untouched.

        Args:
            metric_name: The name of the metric.
            tag_keys: The tag keys present on the metric's series. Required to
                detect a Serve metric. When None, only the name-based rules
                apply and the result is not cached.

        Returns:
            The label keys to drop from the metric before export.
        """
        if metric_name in _HIGH_CARDINALITY_LABELS:
            return _HIGH_CARDINALITY_LABELS[metric_name]

        level = MetricCardinality.get_cardinality_level()
        labels: List[str] = []
        if metric_name in MetricCardinality.get_high_cardinality_metrics():
            if level != MetricCardinality.LEGACY:
                labels = [WORKER_ID_TAG_KEY]
                if level == MetricCardinality.LOW:
                    labels.append(TASK_OR_ACTOR_NAME_TAG_KEY)
        elif level == MetricCardinality.LOW and (
            tag_keys is not None and REPLICA_ID_TAG_KEY in tag_keys
        ):
            labels = [WORKER_ID_TAG_KEY, REPLICA_ID_TAG_KEY]

        # Skip caching when tag_keys is unknown so a name-only call cannot poison
        # the entry for a Serve metric whose ReplicaId tag was not yet visible.
        if tag_keys is not None:
            _HIGH_CARDINALITY_LABELS[metric_name] = labels
        return labels
