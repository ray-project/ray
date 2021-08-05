import logging
from typing import Callable, TypeVar, List, Optional, Dict

import ray
from ray.exceptions import RayActorError
from ray.util.sgd.v2.worker_group import WorkerGroup
from ray.util.sgd.v2.session import init_session, get_session, shutdown_session

T = TypeVar("T")

logger = logging.getLogger(__name__)


class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        raise NotImplementedError


class SGDBackendError(Exception):
    """Errors with BackendExecutor that should not be exposed to user."""


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

    def start_training(self, train_func: Callable[[], T]) -> None:
        """Executes a training function on all workers in a separate thread.

        ``finish_training`` should be called after this.

        Args:
            train_func (Callable): The training function to run on each worker.
        """

        # First initialize the session.
        def initialize_session(world_rank, train_func):
            try:
                init_session(training_func=train_func, world_rank=world_rank)
            except ValueError:
                raise SGDBackendError(
                    "Attempting to start training but a "
                    "previous training run is still ongoing. "
                    "You must call `finish_training` before "
                    "calling `start_training` again.")

        futures = []
        for world_rank in range(len(self.worker_group)):
            futures.append(
                self.worker_group.execute_single_async(
                    world_rank,
                    initialize_session,
                    world_rank=world_rank,
                    train_func=train_func))

        ray.get(futures)

        # Run the training function asynchronously in its own thread.
        def train_async():
            session = get_session()
            session.start()

        self.worker_group.execute_async(train_async)

    def fetch_next_result(self) -> Optional[List[Dict]]:
        """Fetch next results produced by ``sgd.report()`` from each worker.

        Assumes ``start_training`` has already been called.

        Returns:
            A list of dictionaries of values passed to ``sgd.report()`` from
                each worker. Each item corresponds to an intermediate result
                a single worker. If there are no more items to fetch,
                returns None.
        """

        def get_next():
            # Get the session for this worker.
            try:
                session = get_session()
            except ValueError:
                # Session is not initialized yet.
                raise SGDBackendError("`fetch_next_result` has been called "
                                      "before `start_training`. Please call "
                                      "`start_training` before "
                                      "`fetch_next_result`.")

            try:
                result = session.get_next()
            except RuntimeError:
                # Training thread has not been started yet.
                raise SGDBackendError("`fetch_next_result` has been called "
                                      "before `start_training`. Please call "
                                      "`start_training` before "
                                      "`fetch_next_result`.")

            return result

        futures = self.worker_group.execute_async(get_next)
        results = self.get_with_failure_handling(futures)

        # Check if any worker returned None.
        if any(r is None for r in results):
            # Either all workers have results or none of them do.
            if not all(r is None for r in results):
                raise RuntimeError("Some workers returned results while "
                                   "others didn't. Make sure that "
                                   "`sgd.report()` is called the same number "
                                   "of times on all workers.")
            else:
                results = None

        return results

    def finish_training(self) -> List[T]:
        """Finish training and return final results. Propagate any exceptions.

        Blocks until training is finished on all workers.

        Assumes `start_training` has already been called.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """

        def end_training():
            # Get the session for this worker.
            try:
                session = get_session()
            except ValueError:
                # Session is not initialized yet.
                raise SGDBackendError("`finish_training` has been called "
                                      "before `start_training`. Please call "
                                      "`start_training` before "
                                      "`finish_training`.")

            try:
                # session.finish raises any Exceptions from training.
                output = session.finish()
            finally:
                # Shutdown session even if session.finish() raises an
                # Exception.
                shutdown_session()

            return output

        futures = self.worker_group.execute_async(end_training)
        return self.get_with_failure_handling(futures)

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

    def __len__(self):
        raise InactiveWorkerGroupError()
