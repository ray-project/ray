from typing import Callable

from ray.util.sgd.v2.worker_group import WorkerGroup


class BackendConfig:
    @property
    def backend_name(self):
        raise NotImplementedError

    def validate(self, name):
        assert name == self.backend_name


class BackendExecutor:
    def __init__(self,
                 backend_config: BackendConfig,
                 num_workers: int = 1,
                 num_cpus_per_worker: int = 1,
                 num_gpus_per_worker: int = 0):
        self._backend_config = backend_config
        self._num_workers = num_workers
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker

    def start(self):
        self.worker_group = WorkerGroup(self._num_workers,
                                        self._num_cpus_per_worker,
                                        self._num_gpus_per_worker)

    def execute(self, train_func: Callable):
        pass

    def shutdown(self):
        self.worker_group.shutdown()

    def run(self, train_func: Callable):
        """ Runs the training function.

                1. Starts the executor.
                2. Executes the function.
                3. Shuts down the executor.
                4. Returns results of the function.
        """
        pass
