from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2.api.callback import RayTrainCallback
from ray.train.v2.api.config import ScalingConfig
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.controller import (
        TrainControllerState,
    )
    from ray.train.v2._internal.execution.failure_handling import FailureDecision
    from ray.train.v2._internal.execution.scaling_policy import ResizeDecision
    from ray.train.v2._internal.execution.worker_group import (
        Worker,
        WorkerGroup,
        WorkerGroupContext,
        WorkerGroupPollStatus,
    )
    from ray.train.v2.api.result import Result


@DeveloperAPI
class WorkerGroupCallback(RayTrainCallback):
    def before_init_train_context(
        self, workers: List["Worker"]
    ) -> Dict[str, List[Any]]:
        """Called before initializing the TrainContext for the worker_group.

        Return:
            A dictionary of additional arguments for TrainContext.
            The key is the argument name and the value is a list of argument values
            to pass to the TrainContext constructor of each worker in the worker group.
        """
        return {}

    @contextmanager
    def on_worker_group_start(self):
        yield

    def before_worker_group_start(self, worker_group_context: "WorkerGroupContext"):
        """Called before the worker group actors are initialized."""
        pass

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        """Called after the worker group actors are initialized.
        All workers should be ready to execute tasks."""
        pass

    def after_worker_group_training_start(self, worker_group: "WorkerGroup"):
        pass

    @contextmanager
    def on_worker_group_shutdown(self):
        yield

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup"):
        """Called before the worker group is shut down.
        Workers may be dead at this point due to actor failures, so this method
        should catch and handle exceptions if attempting to execute tasks."""
        pass

    def after_worker_group_shutdown(self, worker_group_context: "WorkerGroupContext"):
        """Called after the worker group is shut down."""
        pass

    def after_worker_group_poll_status(
        self, worker_group_status: "WorkerGroupPollStatus"
    ):
        pass

    def before_worker_group_abort(self, worker_group_context: "WorkerGroupContext"):
        """Called before the worker group is aborted."""
        pass

    def after_worker_group_abort(self, worker_group_context: "WorkerGroupContext"):
        """Called after the worker group is aborted."""
        pass


@DeveloperAPI
class ControllerCallback(RayTrainCallback):
    def after_controller_start(self, train_run_context: "TrainRunContext"):
        """Called immediately after `TrainController.run` is called,
        before the control loop starts executing."""
        pass

    # TODO(matthewdeng): Revisit this callback interface for better extensibility.
    # This hook was added for the specific use case of setting a `label_selector`
    # for new worker groups (e.g., for TPU reservations). The current interface is
    # tightly coupled to this purpose and limits its reuse for other use-cases.
    def on_controller_start_worker_group(
        self, *, scaling_config: ScalingConfig, num_workers: int
    ) -> Optional[Dict[str, str]]:
        """Called by the TrainController before the worker group is started.

        This hook can be used to perform setup that modifies the worker group's
        placement, such as reserving an accelerator slice.

        Args:
            scaling_config: The scaling configuration for the run.
            num_workers: The number of workers to be started.

        Returns:
            An optional dictionary defining a `label_selector`
            to gang schedule the worker group on the reserved TPU slice.
        """
        return None

    def before_controller_shutdown(self):
        """Called before `TrainController.run` exits,
        after the control loop has exited."""
        pass

    def after_controller_state_update(
        self,
        previous_state: "TrainControllerState",
        current_state: "TrainControllerState",
    ):
        """Called whenever the controller state is updated."""
        pass

    def before_controller_execute_failure_decision(
        self,
        failure_decision: "FailureDecision",
    ):
        """Called before the controller executes a failure decision."""
        pass

    def before_controller_execute_resize_decision(
        self,
        resize_decision: "ResizeDecision",
    ):
        """Called before the controller executes a resize decision."""
        pass

    def after_controller_finish(self, result: "Result"):
        """Called after the training run completes, providing access to the final result.

        Args:
            result: The final training result containing metrics and checkpoint.
        """
        pass


# TODO: consider consolidating all metrics into one dict, possibly with UDF
@DeveloperAPI
class ReportCallback(RayTrainCallback):
    def after_report(
        self,
        training_report: _TrainingReport,
        metrics: List[Dict[str, Any]],
    ):
        """Called after all workers have reported a training result.

        Note that this differs from `after_worker_group_poll_status`,
        which may only contain a subset of workers that have reported.
        For example, if only rank 0 is performing checkpointing, then
        rank 0 would report a training result the slowest.
        """
        pass


@DeveloperAPI
class WorkerCallback(RayTrainCallback):
    """
    Callbacks that are hooked to the worker event.

    These callbacks are created on the train driver process and then
    copied and passed to all the workers.
    The execution of these callbacks happens on each of the workers,
    not on the train driver process.
    """

    def after_init_train_context(self):
        pass

    def before_worker_shutdown(self):
        pass


@DeveloperAPI
class TrainContextCallback(RayTrainCallback):
    """
    Callbacks that are hooked to the train context event.

    These callbacks are created on the train driver process and then
    copied and passed to all the workers.
    The execution of these callbacks happens on the train context of the workers.
    """

    @contextmanager
    def on_report(self):
        yield
