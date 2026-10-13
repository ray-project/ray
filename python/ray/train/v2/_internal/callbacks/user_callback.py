from typing import TYPE_CHECKING, Any, Dict, List

from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import WorkerGroupPollStatus
from ray.train.v2.api.callback import UserCallback

if TYPE_CHECKING:
    from ray.train.health import HealthDecision


class UserCallbackHandler(WorkerGroupCallback, ReportCallback, ControllerCallback):
    """Responsible for calling methods of subscribers implementing
    the `UserCallback` interface.
    """

    def __init__(
        self, user_callbacks: List[UserCallback], train_run_context: TrainRunContext
    ):
        self._user_callbacks = user_callbacks
        self._train_run_context = train_run_context

    # --------------------------
    # ReportCallback
    # --------------------------

    def after_report(
        self,
        training_report: _TrainingReport,
        metrics: List[Dict[str, Any]],
    ):
        for user_callback in self._user_callbacks:
            user_callback.after_report(
                run_context=self._train_run_context,
                metrics=metrics,
                checkpoint=training_report.checkpoint,
            )

    # --------------------------
    # ControllerCallback
    # --------------------------

    def after_health_decision(self, health_decision: "HealthDecision"):
        for user_callback in self._user_callbacks:
            user_callback.after_health_decision(
                run_context=self._train_run_context, health_decision=health_decision
            )

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def after_worker_group_poll_status(
        self, worker_group_status: WorkerGroupPollStatus
    ):
        if not worker_group_status.errors:
            return

        for user_callback in self._user_callbacks:
            user_callback.after_exception(
                run_context=self._train_run_context,
                worker_exceptions=worker_group_status.errors,
            )
