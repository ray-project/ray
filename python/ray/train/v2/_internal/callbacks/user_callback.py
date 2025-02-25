from typing import Any, Dict, List, Optional

from ray.train import Checkpoint
from ray.train.v2._internal.execution.callback import (
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group import WorkerGroupPollStatus
from ray.train.v2.api.callback import UserCallback


class UserCallbackHandler(WorkerGroupCallback, ReportCallback):
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
        self, metrics: List[Dict[str, Any]], checkpoint: Optional[Checkpoint]
    ):
        for user_callback in self._user_callbacks:
            user_callback.after_report(
                run_context=self._train_run_context,
                metrics=metrics,
                checkpoint=checkpoint,
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
