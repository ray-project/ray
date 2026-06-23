from typing import Any, Dict, List

from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import WorkerGroupPollStatus
from ray.train.v2.api.callback import UserCallback
from ray.train.v2.api.result import Result


class UserCallbackHandler(WorkerGroupCallback, ReportCallback, ControllerCallback):
    """Responsible for calling methods of subscribers implementing
    the `UserCallback` interface.

    Bridges internal callback interfaces (``ControllerCallback``,
    ``ReportCallback``, ``WorkerGroupCallback``) to the public
    ``UserCallback`` interface.
    """

    def __init__(
        self, user_callbacks: List[UserCallback], train_run_context: TrainRunContext
    ):
        self._user_callbacks = user_callbacks
        self._train_run_context = train_run_context

    # --------------------------
    # ControllerCallback
    # --------------------------

    def after_controller_start(self, train_run_context: TrainRunContext):
        for user_callback in self._user_callbacks:
            user_callback.before_run(run_context=train_run_context)

    def after_controller_finish(self, result: Result):
        for user_callback in self._user_callbacks:
            user_callback.after_run(
                run_context=self._train_run_context,
                result=result,
            )

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
