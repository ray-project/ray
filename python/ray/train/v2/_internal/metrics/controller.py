from typing import Dict, Union

from ray.train.v2._internal.execution.controller.state import TrainControllerStateType
from ray.train.v2._internal.metrics.base import (
    RUN_ID_TAG_KEY,
    RUN_NAME_TAG_KEY,
    EnumMetric,
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

    # ===== Controller State Codes =====
    # Value recorded by the controller state gauge for each state. The Train Grafana
    # dashboard's state timeline panel maps these codes back to state names, so keep
    # them in sync with `CONTROLLER_STATE_PANEL` in
    # `ray/dashboard/modules/metrics/dashboards/train_dashboard_panels.py`.
    # 0 is reserved for "no state recorded".
    CONTROLLER_STATE_CODES: Dict[TrainControllerStateType, int] = {
        TrainControllerStateType.INITIALIZING: 1,
        TrainControllerStateType.SCHEDULING: 2,
        TrainControllerStateType.RESCHEDULING: 3,
        TrainControllerStateType.RUNNING: 4,
        TrainControllerStateType.PREEMPTING: 5,
        TrainControllerStateType.RESTARTING: 6,
        TrainControllerStateType.RESIZING: 7,
        TrainControllerStateType.SHUTTING_DOWN: 8,
        TrainControllerStateType.ERRORED: 9,
        TrainControllerStateType.FINISHED: 10,
        TrainControllerStateType.ABORTED: 11,
    }

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
            enum_codes=cls.CONTROLLER_STATE_CODES,
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
