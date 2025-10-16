from typing import Dict

from ray.train.v2._internal.metrics.base import (
    RUN_ID_TAG_KEY,
    RUN_NAME_TAG_KEY,
    TimeMetric,
)

WORKER_WORLD_RANK_TAG_KEY = "ray_train_worker_world_rank"
WORKER_ACTOR_ID_TAG_KEY = "ray_train_worker_actor_id"


class WorkerMetrics:
    """Factory for creating worker-specific metrics.

    This class defines all metrics used to track the state and performance of the
    training workers. Each metric is defined with its name, type, default value,
    description, and required tags.
    """

    # ===== Metric Names =====
    REPORT_TOTAL_BLOCKED_TIME_S = "train_report_total_blocked_time_s"

    @classmethod
    def _create_time_metric(
        cls, name: str, description: str, base_tags: Dict[str, str]
    ) -> TimeMetric:
        """Create a time-based metric."""
        return TimeMetric(
            name=name,
            description=description,
            base_tags=base_tags,
        )

    @classmethod
    def get_worker_metrics(
        cls, run_name: str, run_id: str, world_rank: int, worker_actor_id: str
    ) -> Dict[str, TimeMetric]:
        """Get all worker metrics."""
        base_tags = {
            RUN_NAME_TAG_KEY: run_name,
            RUN_ID_TAG_KEY: run_id,
            WORKER_WORLD_RANK_TAG_KEY: str(world_rank),
            WORKER_ACTOR_ID_TAG_KEY: worker_actor_id,
        }
        return {
            cls.REPORT_TOTAL_BLOCKED_TIME_S: cls._create_time_metric(
                cls.REPORT_TOTAL_BLOCKED_TIME_S,
                "Cumulative time in seconds to report a checkpoint to the storage.",
                base_tags,
            ),
        }
