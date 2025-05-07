from typing import Dict

from ray.train.v2._internal.metrics.base import RUN_NAME_TAG_KEY, Metric


class WorkerMetrics:
    """Factory for creating worker-specific metrics.

    This class defines all metrics used to track the state and performance of the
    training workers. Each metric is defined with its name, type, default value,
    description, and required tags.
    """

    # ===== Tag Keys =====
    WORKER_WORLD_RANK_TAG_KEY = "ray_train_worker_world_rank"
    TAG_KEYS = [RUN_NAME_TAG_KEY, WORKER_WORLD_RANK_TAG_KEY]

    # ===== Metric Names =====
    TRAIN_REPORT_TOTAL_BLOCKED_TIME_S = "train_report_total_blocked_time_s"

    @classmethod
    def _create_time_metric(cls, name: str, description: str) -> Metric:
        """Create a time-based metric."""
        return Metric(
            name=name,
            type=float,
            default=0.0,
            description=description,
            tag_keys=cls.TAG_KEYS,
        )

    @classmethod
    def get_worker_metrics(cls) -> Dict[str, Metric]:
        """Get all worker metrics."""
        return {
            cls.TRAIN_REPORT_TOTAL_BLOCKED_TIME_S: cls._create_time_metric(
                cls.TRAIN_REPORT_TOTAL_BLOCKED_TIME_S,
                "Cumulative time in seconds to report a checkpoint to the storage.",
            ),
        }
