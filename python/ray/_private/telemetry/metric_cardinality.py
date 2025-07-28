from enum import Enum
from typing import List

from ray._private.ray_constants import RAY_METRIC_CARDINALITY_LEVEL

# Keep in sync with the WorkerIdKey in src/ray/stats/tag_defs.cc
WORKER_ID_TAG_KEY = "WorkerId"

_CARDINALITY_LEVEL = None


class MetricCardinality(str, Enum):
    """Cardinality level configuration for all Ray metrics (ray_tasks, ray_actors,
    etc.). This configurtion is used to determine whether to globally drop high
    cardinality labels. This is important for high scale clusters that might consist
    thousands of workers, millions of tasks.

    - LEGACY: Keep all labels. This is the default behavior.
    - RECOMMENDED: Drop high cardinality labels. The set of high cardinality labels
    are determined internally by Ray and not exposed to users. Currently, this includes
    the following labels: WorkerId
    """

    LEGACY = "legacy"
    RECOMMENDED = "recommended"

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
    def get_high_cardinality_labels_to_drop() -> List[str]:
        """
        Get the high cardinality labels of the metric.
        """
        return [WORKER_ID_TAG_KEY]
