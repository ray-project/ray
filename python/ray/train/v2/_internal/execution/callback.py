from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, List

from ray.train import Checkpoint
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.controller import TrainControllerState
    from ray.train.v2._internal.execution.failure_handling import FailureDecision
    from ray.train.v2._internal.execution.scaling_policy import ScalingDecision
    from ray.train.v2._internal.execution.worker_group import (
        WorkerGroup,
        WorkerGroupStatus,
    )


class Callback:
    """Hooks that subscribe to a some event and get run as a callback."""

    pass


@DeveloperAPI
class WorkerGroupCallback(Callback):
    def before_init_train_context(
        self, worker_group: "WorkerGroup"
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

    def after_worker_group_poll_status(self, worker_group_status: "WorkerGroupStatus"):
        pass


@DeveloperAPI
class ControllerCallback(Callback):
    def after_controller_start(self):
        """Called immediately after `TrainController.run` is called,
        before the control loop starts executing."""
        pass

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
        worker_group_status: "WorkerGroupStatus",
    ):
        """Called before the controller executes a failure decision."""
        pass

    def before_controller_execute_scaling_decision(
        self,
        scaling_decision: "ScalingDecision",
        worker_group_status: "WorkerGroupStatus",
    ):
        """Called before the controller executes a scaling decision."""
        pass


# TODO: Call the CheckpointCallback in the checkpoint manager.
@DeveloperAPI
class CheckpointCallback(Callback):
    def after_checkpoint_register(self, checkpoint: Checkpoint):
        pass

    def after_checkpoint_delete(self, checkpoint: Checkpoint):
        pass


@DeveloperAPI
class WorkerCallback(Callback):
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
class TrainContextCallback(Callback):
    """
    Callbacks that are hooked to the train context event.

    These callbacks are created on the train driver process and then
    copied and passed to all the workers.
    The execution of these callbacks happens on the train context of the workers.
    """

    @contextmanager
    def on_report(self):
        yield
