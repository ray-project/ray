from dataclasses import dataclass
from typing import List

from ray.train.v2._internal.metrics.base import RUN_NAME_TAG_KEY, Metric

WORKER_WORLD_RANK_TAG_KEY = "ray_train_worker_world_rank"
WORKER_TAG_KEYS = (RUN_NAME_TAG_KEY, WORKER_WORLD_RANK_TAG_KEY)


@dataclass
class WorkerMetric(Metric):
    tag_keys: List[str] = WORKER_TAG_KEYS


TRAIN_REPORT_TOTAL_BLOCKED_TIME_S = WorkerMetric(
    name="train_report_total_blocked_time_s",
    type=float,
    default=0.0,
    description="Cumulative time in seconds to report a checkpoint to the storage.",
)

WORKER_METRICS = [
    TRAIN_REPORT_TOTAL_BLOCKED_TIME_S,
]
