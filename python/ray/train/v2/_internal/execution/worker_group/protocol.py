from abc import ABC, abstractmethod
from typing import Dict


class WorkerGroupStatus(ABC):
    """Protocol for status objects that can be handled by failure policies.

    This provides a common interface for both runtime worker failures
    (WorkerGroupPollStatus) and scheduling failures (WorkerGroupSchedulingStatus).
    """

    @property
    @abstractmethod
    def errors(self) -> Dict[int, Exception]:
        ...

    @property
    @abstractmethod
    def finished(self) -> bool:
        """For WorkerGroupPollStatus, this is True if all workers are finished.
        For WorkerGroupSchedulingStatus, this is always True because the scheduling operation
        is synchronous.
        """
        ...

    @abstractmethod
    def get_error_string(self) -> str:
        ...

    @abstractmethod
    def get_restart_error_string(self) -> str:
        ...

    @abstractmethod
    def get_raise_error_string(self) -> str:
        ...
