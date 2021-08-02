import logging
from typing import Callable, TypeVar, List, Optional

import ray
from ray.exceptions import RayActorError
from ray.util.sgd.v2.worker_group import WorkerGroup

T = TypeVar("T")

logger = logging.getLogger(__name__)


class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        raise NotImplementedError


class BackendExecutor:
    """Main execution class for training backends.

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
        self._backend = self._backend_config.backend_cls()
        self._num_workers = num_workers
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker

        self.worker_group = InactiveWorkerGroup()

    def start(self, initialization_hook: Optional[Callable[[], None]] = None):
        """Starts the worker group."""
        self.worker_group = WorkerGroup(self._num_workers,
                                        self._num_cpus_per_worker,
                                        self._num_gpus_per_worker)
        if initialization_hook:
            self.worker_group.execute(initialization_hook)
        self._backend.on_start(self.worker_group, self._backend_config)

    def run(self, train_func: Callable[[], T]) -> List[T]:
        """Executes a training function on all workers.

        Args:
            train_func (Callable): The training function to run on each worker.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """
        # Run the training function asynchronously.
        training_futures = self.worker_group.execute_async(train_func)

        return self.get_with_failure_handling(training_futures)

    def get_with_failure_handling(self, remote_values):
        """Gets the remote values while handling for worker failures.

        Args:
            remote_values (list): List of object refs representing functions
                that may fail in the middle of execution. For example, running
                a SGD training loop in multiple parallel actor calls.

        Returns:
            The resolved objects represented by the passed in ObjectRefs.
        """
        unfinished = remote_values
        try:
            while len(unfinished) > 0:
                finished, unfinished = ray.wait(unfinished)
                # If a failure occurs the ObjectRef will be marked as finished.
                # Calling ray.get will expose the failure as a RayActorError.
                ray.get(finished)
        except RayActorError as exc:
            logger.exception(str(exc))
            self.handle_failure()
            return
        return ray.get(remote_values)

    def handle_failure(self):
        # TODO: Fault-tolerance/elastic training here.
        self.shutdown()
        raise RuntimeError("Worker crashed during training. "
                           "Training unsuccessful.")

    def shutdown(self):
        """Shuts down the workers in the worker group."""
        try:
            self._backend.on_shutdown(self.worker_group, self._backend_config)
        except RayActorError:
            logger.warning("Graceful shutdown of backend failed. This is "
                           "expected if one of the workers has crashed.")
        self.worker_group.shutdown()
        self.worker_group = InactiveWorkerGroup()


class BackendInterface:
    def on_start(self, worker_group: WorkerGroup,
                 backend_config: BackendConfig):
        raise NotImplementedError

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: BackendConfig):
        raise NotImplementedError


class InactiveWorkerGroupError(Exception):
    """Raised when underlying worker group is inactive."""


class InactiveWorkerGroup():
    # TODO: fix inheritence. perhaps create WorkerGroupInterface.
    def __getattribute__(self, *args, **kwargs):
        raise InactiveWorkerGroupError()
