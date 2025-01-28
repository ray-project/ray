from enum import Enum
from typing import Optional

from ray.train.v2._internal.exceptions import TrainingFailedError
from ray.train.v2._internal.execution.failure_handling import FailureDecision
from ray.train.v2._internal.execution.scaling_policy.scaling_policy import (
    ScalingDecision,
)
from ray.train.v2._internal.execution.worker_group.worker_group import WorkerGroupStatus


class TrainControllerStateType(Enum):
    """Enum representing different states of the train controller.
    Each enum value is a tuple of (name, is_active, needs_new_run_attempt).

    States:
       INITIALIZING: The train controller is starting up. This is always the initial
           state of the controller.
       SCHEDULING: The training controller is in the process of scheduling a new worker
           group.
       RESCHEDULING: The train controller is in the process of rescheduling the worker
           group.
       RUNNING: The train controller is actively running training tasks.
       RESTARTING: The train controller is in the process of recovering from an error.
       RESIZING: The train controller is in the process of resizing a running worker
           group.
       ERRORED: A terminal state indicating that training has encountered an error and
           cannot continue.
       FINISHED: A terminal state indicating that training has completed.
    """

    INITIALIZING = ("INITIALIZING", True, True)
    SCHEDULING = ("SCHEDULING", True, False)
    RESCHEDULING = ("RESCHEDULING", True, False)
    RUNNING = ("RUNNING", True, False)
    RESTARTING = ("RESTARTING", True, True)
    RESIZING = ("RESIZING", True, True)
    ERRORED = ("ERRORED", False, False)
    FINISHED = ("FINISHED", False, False)

    def __init__(self, state_name: str, is_active: bool, needs_new_run_attempt: bool):
        self.state_name = state_name
        self.is_active = is_active
        self.needs_new_run_attempt = needs_new_run_attempt


class TrainControllerState:
    """Base class for all train controller states.

    Methods:
        get_type() -> TrainControllerStateType: Returns the type of the state.
        is_active() -> bool: Returns whether the state is active.
        needs_new_run_attempt() -> bool: Returns whether a new run attempt is needed.
    """

    def __init__(self, state_type: TrainControllerStateType):
        self._state_type = state_type

    def __repr__(self) -> str:
        attrs = {
            "type": self._state_type.name,
            "is_active": self._state_type.is_active,
            "needs_new_run_attempt": self._state_type.needs_new_run_attempt,
            **{k: v for k, v in vars(self).items() if not k.startswith("_")},
        }
        attrs_str = "\n    ".join(f"{k}={v}" for k, v in attrs.items())
        return f"{self.__class__.__name__}(\n    {attrs_str}\n)"

    def is_active(self) -> bool:
        return self._state_type.is_active

    def needs_new_run_attempt(self) -> bool:
        return self._state_type.needs_new_run_attempt


class InitializingState(TrainControllerState):
    def __init__(self):
        super().__init__(state_type=TrainControllerStateType.INITIALIZING)


class SchedulingState(TrainControllerState):
    def __init__(self, scaling_decision: ScalingDecision):
        super().__init__(state_type=TrainControllerStateType.SCHEDULING)
        self.scaling_decision = scaling_decision


class ReschedulingState(TrainControllerState):
    def __init__(self):
        super().__init__(state_type=TrainControllerStateType.RESCHEDULING)


class RunningState(TrainControllerState):
    # TODO: Split into multiple states where each state has exact non-optional fields.
    def __init__(
        self,
        worker_group_status: Optional[WorkerGroupStatus] = None,
        failure_decision: Optional[FailureDecision] = None,
    ):
        super().__init__(state_type=TrainControllerStateType.RUNNING)
        self.worker_group_status = worker_group_status
        self.failure_decision = failure_decision


class RestartingState(TrainControllerState):
    def __init__(
        self,
        worker_group_status: WorkerGroupStatus,
        failure_decision: FailureDecision,
        training_failed_error: TrainingFailedError,
    ):
        super().__init__(state_type=TrainControllerStateType.RESTARTING)
        self.worker_group_status = worker_group_status
        self.failure_decision = failure_decision
        self.training_failed_error = training_failed_error


class ResizingState(TrainControllerState):
    def __init__(
        self, worker_group_status: WorkerGroupStatus, scaling_decision: ScalingDecision
    ):
        super().__init__(state_type=TrainControllerStateType.RESIZING)
        self.worker_group_status = worker_group_status
        self.scaling_decision = scaling_decision


class ErroredState(TrainControllerState):
    def __init__(
        self,
        worker_group_status: WorkerGroupStatus,
        failure_decision: FailureDecision,
        training_failed_error: TrainingFailedError,
    ):
        super().__init__(state_type=TrainControllerStateType.ERRORED)
        self.worker_group_status = worker_group_status
        self.failure_decision = failure_decision
        self.training_failed_error = training_failed_error


class FinishedState(TrainControllerState):
    def __init__(self, worker_group_status: WorkerGroupStatus):
        super().__init__(state_type=TrainControllerStateType.FINISHED)
        self.worker_group_status = worker_group_status
