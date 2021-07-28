from typing import Union, Callable, List, TypeVar, Optional

from ray.util.sgd.v2.backends.backend import BackendConfig
from ray.util.sgd.v2.callbacks.callback import Callback

R = TypeVar("R")


class Trainer:
    def __init__(self,
                 backend: Union[str, BackendConfig],
                 num_workers: int = 1,
                 num_cpus_per_worker: int = 1,
                 num_gpus_per_worker: int = 0,
                 callbacks: Optional[List[Callback]] = None):
        self._callbacks = [] if callbacks is None else callbacks

    def run(self, train_func: Callable[[], R]) -> List[R]:
        pass
