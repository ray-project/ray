from typing import Any, List
from abc import ABC, abstractmethod
from ray.serve.experimental.llm.types import (
    GenerationRequest,
)


class SchedulingPolicy(ABC):
    @abstractmethod
    def select_requests(self) -> List[GenerationRequest]:
        pass
