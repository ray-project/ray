from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.context import DataContext


@dataclass
class Issue:
    message: str


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
