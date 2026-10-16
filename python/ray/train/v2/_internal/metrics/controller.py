from enum import IntEnum
from typing import Dict, Union

from ray.train.v2._internal.execution.controller.state import TrainControllerStateType
from ray.train.v2._internal.metrics.base import (
    RUN_ID_TAG_KEY,
    RUN_NAME_TAG_KEY,
    EnumMetric,
    GaugeMetric,
    TimeMetric,
)


class ControllerMetrics:
    """Factory for creating controller-specific metrics.

    This class defines all metrics used to track the state and performance of the
    training controller. Each metric is defined with its name, type, default value,
    description, and required tags.
    """

    # ===== Metric Names =====
    CONTROLLER_STATE = "train_controller_state"
    WORKER_GROUP_START_TOTAL_TIME_S = "train_worker_group_start_total_time_s"
    WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S = "train_worker_group_shutdown_total_time_s"

    # ===== Tag Keys =====
    CONTROLLER_STATE_TAG_KEY = "ray_train_controller_state"

    @classmethod
    def _create_time_metric(
        cls, name: str, description: str, base_tags: Dict[str, str]
    ) -> TimeMetric:
        return TimeMetric(
            name=name,
            description=description,
            base_tags=base_tags,
        )

    @classmethod
    def _create_controller_state_metric(
        cls, base_tags: Dict[str, str]
    ) -> EnumMetric[TrainControllerStateType]:
        return EnumMetric[TrainControllerStateType](
            name=cls.CONTROLLER_STATE,
            description="Current state of the Ray Train controller",
            base_tags=base_tags,
            enum_tag_key=cls.CONTROLLER_STATE_TAG_KEY,
        )

    @classmethod
    def get_controller_metrics(
        cls, run_name: str, run_id: str
    ) -> Dict[str, Union[TimeMetric, EnumMetric[TrainControllerStateType]]]:
        base_tags = {RUN_NAME_TAG_KEY: run_name, RUN_ID_TAG_KEY: run_id}
        return {
            cls.WORKER_GROUP_START_TOTAL_TIME_S: cls._create_time_metric(
                cls.WORKER_GROUP_START_TOTAL_TIME_S,
                "Total time taken to start the worker group",
                base_tags,
            ),
            cls.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S: cls._create_time_metric(
                cls.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S,
                "Total time taken to shutdown the worker group",
                base_tags,
            ),
            cls.CONTROLLER_STATE: cls._create_controller_state_metric(base_tags),
        }


class NcclHangDetectorState(IntEnum):
    """The NCCL hang detector's view of a run, as a gauge value."""

    HEALTHY = 0
    SUSPECTED = 1
    CONFIRMED = 2


class NcclHangDetectorMetrics:
    """Factory for the NCCL hang detector's run-level metrics.

    Labelled by run only, never by attempt or communicator: attempt ids and
    communicator hashes change on every restart and a job can have many, and
    Ray metrics can't drop a label set, so each would grow the exported series
    for the life of the controller. Each value is the worst across the
    communicators of the current attempt.
    """

    # ===== Metric Names =====
    DETECTOR_STATE = "train_nccl_hang_detector_state"
    STALL_DURATION_S = "train_nccl_hang_stall_duration_s"

    @classmethod
    def get_nccl_hang_detector_metrics(
        cls, run_name: str, run_id: str
    ) -> Dict[str, GaugeMetric]:
        base_tags = {RUN_NAME_TAG_KEY: run_name, RUN_ID_TAG_KEY: run_id}
        return {
            cls.DETECTOR_STATE: GaugeMetric(
                name=cls.DETECTOR_STATE,
                description="State of the Ray Train NCCL hang detector: "
                "0 healthy, 1 hang suspected, 2 hang confirmed",
                base_tags=base_tags,
                default=float(NcclHangDetectorState.HEALTHY),
            ),
            cls.STALL_DURATION_S: GaugeMetric(
                name=cls.STALL_DURATION_S,
                description="Seconds the Ray Train NCCL hang detector believes "
                "a hang has been in progress",
                base_tags=base_tags,
            ),
        }
