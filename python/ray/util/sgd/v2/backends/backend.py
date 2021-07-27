import logging
from typing import Callable, Iterator, TypeVar, List, Any

import ray
from ray.exceptions import RayActorError
from ray.util.sgd.v2.worker_group import WorkerGroup

T = TypeVar("T")

logger = logging.getLogger(__name__)


def fetch_next():
    """Fetch next item from result queue on each worker."""
    # TODO: Implement along with sgd.report()
    return None


def fetch_all():
    """Fetch all items from result queue on each worker."""
    # TODO: Implement along with sgd.report()
    return [None]


class BackendConfig:
    """Parent class for configurations of backends (torch, horovod, etc.)"""

    @property
    def backend_name(self):
        raise NotImplementedError

    def validate(self, name):
        assert name == self.backend_name(), (name, self.backend_name)


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

        self.worker_group = DeactivatedWorkerGroup()

    def start(self):
        """Starts the worker group."""
        self.worker_group = WorkerGroup(self._num_workers,
                                        self._num_cpus_per_worker,
                                        self._num_gpus_per_worker)

    def execute(self, train_func: Callable[[], T]) -> Iterator[Any]:
        """Executes training function on all workers and yield results.

        The provided function must have any required arguments.

        Args:
            train_func (Callable): The training function to run on each
                worker. It must not have any required arguments.

        Yields:
            A list of values passed to ``sgd.report()`` from each worker.
        """
        # Run the training function asynchronously.
        training_futures = self.worker_group.execute_async(train_func)

        not_ready = True
        # While the training function has not yet finished.
        while not_ready:
            # Attempt to get intermediate results.
            result_futures = self.worker_group.execute_async(fetch_next)

            result_ready = []
            result_not_ready = True
            while result_not_ready:
                # Check every second to see if results are ready.
                result_ready, result_not_ready = ray.wait(
                    result_futures,
                    num_returns=len(self.worker_group),
                    timeout=1)

                # Also check if training function has finished.
                ready, not_ready = ray.wait(
                    training_futures,
                    num_returns=len(self.worker_group),
                    timeout=0)
                # Break if all workers have finished the training function.
                if len(ready) == len(self.worker_group):
                    break

            # If some workers already have results ready, then get the results.
            if result_ready:
                yield self.get_handle_failure(result_futures)

        # Training function has finished on all workers.
        # Get remainder of results from queue.
        all_result_futures = self.worker_group.execute_async(fetch_all)
        all_results = self.get_handle_failure(all_result_futures)

        if all_results:
            if not all(len(r) == len(all_results[0]) for r in all_results):
                raise RuntimeError("There is a mismatch in results returned "
                                   "by the training function. Make sure "
                                   "``sgd.report`` is called the same number "
                                   "of times on all workers.")
            for i in range(len(all_results[0])):
                yield [r[i] for r in all_results]

        # Store the return values as an attribute so caller can access.
        self.return_values = self.get_handle_failure(training_futures)

    def get_handle_failure(self, remote_values):
        """Gets the remote values while handling for worker failures.

        Args:
            remote_values (list): List of object refs representing functions
                that may fail in the middle of execution. For example, running
                a SGD training loop in multiple parallel actor calls.

        Returns:
            The objects represented the passed in ObjectRefs.
        """
        unfinished = remote_values
        try:
            while len(unfinished) > 0:
                finished, unfinished = ray.wait(unfinished)
                finished = ray.get(finished)
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
        self.worker_group.shutdown()
        self.worker_group = DeactivatedWorkerGroup()

    def run(self, train_func: Callable[[], T]) -> List[T]:
        """Run full start/execute/shutdown flow.

        Args:
            train_func (Callable): The training function to run on each
                worker. It must accept ``config`` as an argument.

        Yields:
            A list of values passed to ``sgd.report()`` from each worker.
        """
        self.start()
        self.execute(train_func)
        self.shutdown()


class DeactivatedWorkerGroup:
    def __getattr__(self, *args, **kwargs):
        raise RuntimeError(
            "This Trainer is not active. It is either shutdown already or "
            "never started in the first place. Either create a new Trainer "
            "or start this one.")
