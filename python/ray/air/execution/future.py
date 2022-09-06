import ray
from typing import Type

from dataclasses import dataclass
from ray.air.execution.event import ExecutionEvent


@dataclass
class TypedFuture:
    future: ray.ObjectRef
    cls: Type[ExecutionEvent]

    def __hash__(self):
        return self.future.__hash__()

    def convert_result(self, result):
        n_args = len(getattr(self.cls, "__dataclass_fields__", 1))
        if n_args == 0:
            return self.cls()
        elif n_args == 1:
            return self.cls(result)
        else:
            return self.cls(*result)
