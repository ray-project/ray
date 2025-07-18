import abc
from enum import Enum
from typing import Optional

from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.exceptions import ControllerError, TrainingFailedError


class FailureDecision(Enum):
    RESTART = "RESTART"
    RESCHEDULE = "RESCHEDULE"
    RAISE = "RAISE"
    NOOP = "NOOP"
    ABORT = "ABORT"


class FailurePolicy(abc.ABC):
    """A policy that determines how to handle user and system failures.
    FailurePolicy will handle the scheduling failure and worker group poll failure.

    This can be used to implement fault tolerance and error recovery.
    """

    def __init__(self, failure_config: FailureConfig):
        self.failure_config = failure_config

    @abc.abstractmethod
    def make_decision(
        self,
        training_failed_error: Optional[TrainingFailedError] = None,
        controller_failed_error: Optional[ControllerError] = None,
    ) -> FailureDecision:
        raise NotImplementedError
