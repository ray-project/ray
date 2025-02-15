import abc
from enum import Enum

from ray.train.v2._internal.execution.worker_group import WorkerGroupPollStatus
from ray.train.v2.api.config import FailureConfig


class FailureDecision(Enum):
    RESTART = "RESTART"
    RAISE = "RAISE"
    NOOP = "NOOP"


class FailurePolicy(abc.ABC):
    """A policy that determines how to handle user and system failures.

    This can be used to implement fault tolerance and error recovery.
    """

    def __init__(self, failure_config: FailureConfig):
        self.failure_config = failure_config

    @abc.abstractmethod
    def make_decision(
        self, worker_group_status: WorkerGroupPollStatus
    ) -> FailureDecision:
        raise NotImplementedError
