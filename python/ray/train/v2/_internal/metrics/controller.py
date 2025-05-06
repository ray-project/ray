from dataclasses import dataclass
from typing import List

from ray.train.v2._internal.metrics.base import RUN_NAME_TAG_KEY, Metric

CONTROLLER_TAG_KEYS = (RUN_NAME_TAG_KEY,)
CONTROLLER_STATE_TAG_KEY = "ray_train_controller_state"


@dataclass
class ControllerMetric(Metric):
    tag_keys: List[str] = CONTROLLER_TAG_KEYS


TRAIN_WORKER_GROUP_START_TOTAL_TIME_S = ControllerMetric(
    name="train_worker_group_start_total_time_s",
    type=float,
    default=0.0,
    description="Cumulative time in seconds to start worker groups in the Train job.",
)

TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S = ControllerMetric(
    name="train_worker_group_shutdown_total_time_s",
    type=float,
    default=0.0,
    description="Cumulative time in seconds to shutdown worker groups in the Train job.",
)

TRAIN_CONTROLLER_STATE = ControllerMetric(
    name="train_controller_state",
    type=int,
    default=0,
    description="The current state of the controller",
    tag_keys=CONTROLLER_TAG_KEYS + (CONTROLLER_STATE_TAG_KEY,),
)

CONTROLLER_METRICS = [
    TRAIN_WORKER_GROUP_START_TOTAL_TIME_S,
    TRAIN_WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S,
    TRAIN_CONTROLLER_STATE,
]
