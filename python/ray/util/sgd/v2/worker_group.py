from dataclasses import dataclass
from typing import Callable, Any


class WorkerGroup:
    pass


class BaseWorker:
    def execute(self, func: Callable) -> Any:
        """Executes the input function.
        Args:
            func(Callable): A function that does not take any arguments.
        """
        return func()


@dataclass
class WorkerConfig:
    pass
