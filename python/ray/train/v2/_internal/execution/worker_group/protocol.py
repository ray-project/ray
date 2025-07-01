from abc import abstractmethod
from typing import Dict, Protocol


class PolicyHandledStatus(Protocol):
    """Protocol for status objects that can be handled by failure policies.

    This provides a common interface for both runtime worker failures
    (WorkerGroupPollStatus) and startup failures (WorkerGroupResizeStatus).
    """

    @property
    @abstractmethod
    def errors(self) -> Dict[int, Exception]:
        ...

    @property
    @abstractmethod
    def finished(self) -> bool:
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
