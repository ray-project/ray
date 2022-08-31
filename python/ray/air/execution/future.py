import ray
from typing import Type

from dataclasses import dataclass
from ray.air.execution.result import ExecutionResult


@dataclass
class TypedFuture:
    future: ray.ObjectRef
    cls: Type[ExecutionResult]
    n_args: int = 1

    def __hash__(self):
        return self.future.__hash__()

    def convert_result(self, result):
        if self.n_args == 0:
            return self.cls()
        elif self.n_args == 1:
            return self.cls(result)
        else:
            return self.cls(*result)
