from typing import Callable, Any, List

import ray
from ray.types import ObjectRef


class WorkerGroup:
    def __init__(self,
                 num_workers: int = 1,
                 num_cpus_per_worker: int = 1,
                 num_gpus_per_worker: int = 0):
        self.workers = self._start_workers()

    def _start_workers(self):
        pass

    def shutdown(self):
        pass

    def execute_async(self, func: Callable) -> List[ObjectRef]:
        return [w.execute(func) for w in self.workers]

    def execute(self, func: Callable) -> List[Any]:
        return ray.get(self.execute_async(func))


class BaseWorker:
    def execute(self, func: Callable) -> Any:
        """Executes the input function.
        Args:
            func(Callable): A function that does not take any arguments.
        """
        return func()
