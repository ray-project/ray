import logging
import queue
from typing import Callable, TypeVar, List, Optional, Dict

import ray
from ray.exceptions import RayActorError
from ray.util.sgd.v2.constants import RESULT_FETCH_TIMEOUT
from ray.util.sgd.v2.worker_group import WorkerGroup
from ray.util.sgd.v2.session import init_session, get_session, shutdown_session
from ray.util.sgd.v2.utils import PropagatingThread

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

    def start_training(self, train_func: Callable[[], T]) -> None:
        """Executes a training function on all workers in a separate thread.

        Args:
            train_func (Callable): The training function to run on each worker.
        """

        # First initialize the session.
        def initialize_session(world_rank, train_func):
            thread = PropagatingThread(target=train_func, daemon=True)
            try:
                init_session(training_thread=thread, world_rank=world_rank)
            except ValueError:
                raise RuntimeError("Attempting to start training but a "
                                   "previous training run is still ongoing. "
                                   "You must call `finish_training` before "
                                   "calling `start_training` again.") from None

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
            session.training_thread.start()

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
                raise RuntimeError("`fetch_next_result` has been called "
                                   "before `start_training`. Please call "
                                   "`start_training` before "
                                   "`fetch_next_result`.")

            # Release the lock to trigger training to continue.
            session.continue_lock.release()

            result = None
            # While training is still ongoing, attempt to get the result.
            while result is None and session.training_thread.is_alive():
                try:
                    result = session.result_queue.get(
                        block=True, timeout=RESULT_FETCH_TIMEOUT)
                except queue.Empty:
                    pass

            # If no result was found, then the runner must no longer be alive.
            if result is None:
                # Try one last time to fetch results in case results were
                # reported in between the time of the last check and the
                # termination of the thread runner.
                try:
                    result = session.result_queue.get(
                        block=True, timeout=RESULT_FETCH_TIMEOUT)
                except queue.Empty:
                    pass

            # Return None if there are no more results to fetch.
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
                raise RuntimeError("`finish_training` has been called "
                                   "before `start_training`. Please call "
                                   "`start_training` before "
                                   "`finish_training`.") from None

            shutdown_session()

            training_thread = session.training_thread

            # Wait for training to finish.
            # This will raise any errors that occur during training, including
            # SystemError
            func_output = training_thread.join()

            return func_output

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
