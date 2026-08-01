from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor


class IssueType(str, Enum):
    HANGING = "hanging"
    HIGH_MEMORY = "high memory"


@dataclass
class Issue:
    dataset_name: str
    operator_id: str
    message: str
    issue_type: IssueType


class IssueDetector(ABC):
    @classmethod
    @abstractmethod
    def from_executor(cls, executor: "StreamingExecutor") -> "IssueDetector":
        """Factory method to create an issue detector from a StreamingExecutor.

        Args:
            executor: The StreamingExecutor instance to extract dependencies from.

        Returns:
            An instance of the issue detector.
        """
        pass

    @abstractmethod
    def detect(self) -> List[Issue]:
        pass

    @abstractmethod
    def detection_time_interval_s(self) -> float:
        """Time interval between detections, or -1 if not enabled."""
        pass
