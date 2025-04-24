from enum import Enum

from ray.train.v2._internal.execution.scaling_policy.scaling_policy import (
    ScalingDecision,
)
from ray.train.v2.api.exceptions import TrainingFailedError


class TrainControllerStateType(Enum):
    """Enum representing different states of the train controller.

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

    Args:
        state_name: The name of the state.
        is_terminal: Whether this is a terminal state that should not be further processed.
        needs_new_run_attempt: Whether this state requires starting a new run attempt, where
            a run attempt is a logical unit that encompasses both scheduling workers and
            executing training on those workers.
    """

    INITIALIZING = ("INITIALIZING", False, True)
    SCHEDULING = ("SCHEDULING", False, False)
    RESCHEDULING = ("RESCHEDULING", False, False)
    RUNNING = ("RUNNING", False, False)
    RESTARTING = ("RESTARTING", False, True)
    RESIZING = ("RESIZING", False, True)
    ERRORED = ("ERRORED", True, False)
    FINISHED = ("FINISHED", True, False)

    def __init__(self, state_name: str, is_terminal: bool, needs_new_run_attempt: bool):
        self.state_name = state_name
        self.is_terminal = is_terminal
        self.needs_new_run_attempt = needs_new_run_attempt


class TrainControllerState:
    """Base class for all train controller states.

    Methods:
        get_type() -> TrainControllerStateType: Returns the type of the state.
        is_terminal() -> bool: Returns whether the state is terminal.
        needs_new_run_attempt() -> bool: Returns whether a new run attempt is needed.
    """

    def __init__(self, state_type: TrainControllerStateType):
        self._state_type = state_type

    def __repr__(self) -> str:
        attrs = {
            "type": self._state_type.name,
            "is_terminal": self._state_type.is_terminal,
            "needs_new_run_attempt": self._state_type.needs_new_run_attempt,
            **{k: v for k, v in vars(self).items() if not k.startswith("_")},
        }
        attrs_str = "\n    ".join(f"{k}={v}" for k, v in attrs.items())
        return f"{self.__class__.__name__}(\n    {attrs_str}\n)"

    def is_terminal(self) -> bool:
        return self._state_type.is_terminal

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
    # TODO: Split into multiple more granular states, or add more fields.
    # For example, we may want to indicate if any health checks failed.
    def __init__(self):
        super().__init__(state_type=TrainControllerStateType.RUNNING)


class RestartingState(TrainControllerState):
    def __init__(
        self,
        training_failed_error: TrainingFailedError,
    ):
        super().__init__(state_type=TrainControllerStateType.RESTARTING)
        self.training_failed_error = training_failed_error


class ResizingState(TrainControllerState):
    def __init__(
        self,
        scaling_decision: ScalingDecision,
    ):
        super().__init__(state_type=TrainControllerStateType.RESIZING)
        self.scaling_decision = scaling_decision


class ErroredState(TrainControllerState):
    def __init__(
        self,
        training_failed_error: TrainingFailedError,
    ):
        super().__init__(state_type=TrainControllerStateType.ERRORED)
        self.training_failed_error = training_failed_error


class FinishedState(TrainControllerState):
    def __init__(self):
        super().__init__(state_type=TrainControllerStateType.FINISHED)
