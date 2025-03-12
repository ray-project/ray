from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List
from dataclasses import dataclass

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
