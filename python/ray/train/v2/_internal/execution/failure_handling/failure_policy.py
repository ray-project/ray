import abc
from enum import Enum
from typing import Dict, Union

from ray.train.v2.api.config import FailureConfig


class FailureDecision(Enum):
    RETRY = "RETRY"
    RAISE = "RAISE"
    NOOP = "NOOP"


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
        scheduling_or_poll_error: Union[Exception, Dict[int, Exception]],
    ) -> FailureDecision:
        raise NotImplementedError
