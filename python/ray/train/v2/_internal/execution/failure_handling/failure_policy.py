import abc
from enum import Enum

from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.exceptions import TrainingFailedError


class FailureDecision(Enum):
    RETRY = "RETRY"
    RAISE = "RAISE"
    NOOP = "NOOP"


class FailurePolicy(abc.ABC):
    """A policy that determines how to handle user and system failures.
    FailurePolicy will handle the controller failure and worker errors during training.

    This can be used to implement fault tolerance and error recovery.
    """

    def __init__(self, failure_config: FailureConfig):
        self.failure_config = failure_config

    @abc.abstractmethod
    def make_decision(
        self,
        training_failed_error: TrainingFailedError,
    ) -> FailureDecision:
        raise NotImplementedError
