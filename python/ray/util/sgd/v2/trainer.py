import logging
from typing import Optional

from ray.util.sgd.v2.backend_config import BackendConfig
from ray.util.sgd.v2.torch import TorchConfig, TorchExecutor
from ray.util.sgd.v2.worker_config import WorkerConfig

logger = logging.getLogger(__name__)


class Trainer:
    def __init__(self, worker_config: WorkerConfig, backend: str,
                 backend_config: Optional[BackendConfig]):
        executor_cls = self._get_executor_cls(backend_config)
        self.executor = executor_cls(worker_config, backend_config)
        if backend_config:
            backend_config.validate(backend)

    def _get_executor_cls(self, backend_config: BackendConfig):
        if isinstance(backend_config, TorchConfig):
            return TorchExecutor
        raise TypeError("Unable to find valid config type for backend_config "
                        "argument.")

    def run(self, train_func, config):
        result_generator = self.run_generator(train_func, config)
        results = []
        for result in result_generator:
            # handle callbacks
            results.append(result)
        return results

    def run_generator(self, train_func, config):
        result_generator = self.executor.run(train_func, config)
        yield from result_generator
