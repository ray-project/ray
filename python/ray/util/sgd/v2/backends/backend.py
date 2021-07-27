from typing import Callable

from ray.util.sgd.v2.worker_group import WorkerGroup


class BackendConfig:
    """Parent class for configurations of backends (torch, horovod, etc.)"""
    @property
    def backend_name(self):
        raise NotImplementedError

    def validate(self, name):
        assert name == self.backend_name


class BackendExecutor:
    """Main execution class for SGD backends (torch, tensorflow, etc.).

    This class holds a worker group and is responsible for executing the
    training function on the workers, and collecting intermediate results
    from ``sgd.report()``.

    Args:
        backend_config (BackendConfig): The configurations for this
            specific backend.
        num_workers (int): Number of workers to use for training.
        num_cpus_per_worker (float): Number of CPUs to use per worker.
        num_gpus_per_worker (float): Number of GPUs to use per worker.
    """
    def __init__(self,
                 backend_config: BackendConfig,
                 num_workers: int = 1,
                 num_cpus_per_worker: float = 1,
                 num_gpus_per_worker: float = 0):
        self._backend_config = backend_config
        self._num_workers = num_workers
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker

        self.worker_group = None

    def start(self):
        """Starts the worker group."""
        self.worker_group = WorkerGroup(self._num_workers,
                                        self._num_cpus_per_worker,
                                        self._num_gpus_per_worker)

    def execute(self, train_func: Callable, config: Dict):
        """Executes the provided training function on each of the workers.

        The provided function must accept ``config`` as an argument.

        Args:
            train_func (Callable): The training function to run on each
                worker. It must accept ``config`` as an argument.

        Returns:


        """
        if
        def wrapped_train_func():
            return train_func(config)
        if not self.worker_group:


    def shutdown(self):
        """Shuts down the workers in the worker group."""
        self.worker_group.shutdown()
        self.worker_group = Deac

    def run(self, train_func: Callable):
        """Run full start/execute/shutdown flow.

        Args:
            train_func (Callable): The training function to run on each
                worker. It must accept ``config`` as an argument.
        """
        self.start()
        self.execute(train_func)
        self.shutdown()
