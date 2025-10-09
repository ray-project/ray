from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.context import DataContext


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
    def __init__(self, executor: "StreamingExecutor", ctx: "DataContext"):
        self._executor = executor
        self._ctx = ctx

    @abstractmethod
    def detect(self) -> List[Issue]:
        pass

    @abstractmethod
    def detection_time_interval_s(self) -> float:
        """Time interval between detections, or -1 if not enabled."""
        pass
