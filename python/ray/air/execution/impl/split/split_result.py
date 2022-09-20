from typing import Any

from dataclasses import dataclass

from ray.air.execution.event import FutureResult


@dataclass
class SplitResult(FutureResult):
    return_value: Any
