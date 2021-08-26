import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, TypeVar, List, Optional, Dict, Union

import ray
from ray import cloudpickle
from ray.exceptions import RayActorError
from ray.ray_constants import env_integer
from ray.util.sgd.v2.checkpoint import CheckpointStrategy
from ray.util.sgd.v2.constants import ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, \
    DEFAULT_RESULTS_DIR
from ray.util.sgd.v2.session import TrainingResultType, TrainingResult
from ray.util.sgd.v2.session import init_session, get_session, shutdown_session
from ray.util.sgd.v2.utils import construct_path
from ray.util.sgd.v2.worker_group import WorkerGroup

T = TypeVar("T")

logger = logging.getLogger(__name__)


def check_for_failure(remote_values):
    """Gets the remote values while handling for worker failures.

    Args:
        remote_values (list): List of object refs representing functions
            that may fail in the middle of execution. For example, running
            a SGD training loop in multiple parallel actor calls.

    Returns:
        Returns Tuple of success boolean and list of workers indexes that fail.
    """
    unfinished = remote_values.copy()
    dead_worker_indexes = []  # Store the indexes of the failed workers.
    while len(unfinished) > 0:
        finished, unfinished = ray.wait(unfinished)
        # If a failure occurs the ObjectRef will be marked as finished.
        # Calling ray.get will expose the failure as a RayActorError.
        for object_ref in finished:
            try:
                ray.get(object_ref)
            except RayActorError as exc:
                logger.exception(str(exc))
                failed_actor_rank = remote_values.index(object_ref)
                logger.info(f"Worker {failed_actor_rank} has failed.")
                dead_worker_indexes.append(failed_actor_rank)
    if len(dead_worker_indexes) > 0:
        return False, dead_worker_indexes
    else:
        return True, []


class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        raise NotImplementedError


class SGDBackendError(Exception):
    """Errors with BackendExecutor that should not be exposed to user."""


class TrainingWorkerError(Exception):
    """Raised if a worker fails during training."""


class BackendExecutor:
    """Main execution class for training backends.

    This class holds a worker group and is responsible for executing the
    training function on the workers, and collecting intermediate results
    from ``sgd.report()`` and ``sgd.checkpoint()``.

    Args:
        backend_config (BackendConfig): The configurations for this
            specific backend.
        num_workers (int): Number of workers to use for training.
        num_cpus_per_worker (float): Number of CPUs to use per worker.
        num_gpus_per_worker (float): Number of GPUs to use per worker.
        log_dir (Optional[str|Path]): Path to the file directory where logs
            should be persisted. If this is not specified, one will be
            generated.

    Attributes:
        logdir (Path): Path to the file directory where logs will be persisted.
        latest_run_dir (Optional[Path]): Path to the file directory for the
            latest run. Configured through ``start_training``.
        latest_checkpoint_dir (Optional[Path]): Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_path (Optional[Path]): Path to the latest persisted
            checkpoint from the latest run.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def __init__(self,
                 backend_config: BackendConfig,
                 num_workers: int = 1,
                 num_cpus_per_worker: float = 1,
                 num_gpus_per_worker: float = 0,
                 log_dir: Optional[Union[str, Path]] = None):
        self._backend_config = backend_config
        self._backend = self._backend_config.backend_cls()
        self._num_workers = num_workers
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker

        self.worker_group = InactiveWorkerGroup()
        self.latest_checkpoint = None

        # Create directory for logs.
        log_dir = Path(log_dir) if log_dir else None
        self.logdir = self._construct_logdir(log_dir)
        self.logdir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Trainer logs will be logged in: {self.logdir}")

        # Incremental unique run ID of this Trainer.
        self._run_id = 0
        # Incremental unique checkpoint ID of this run.
        self._latest_checkpoint_id = 0

    def _construct_logdir(self, logdir: Optional[Path]) -> Path:
        """Path to the log directory."""
        if not logdir:
            # Initialize timestamp for identifying this SGD training execution.
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            logdir = Path(f"sgd_{timestr}")

        return construct_path(logdir, DEFAULT_RESULTS_DIR)

    def start(self, initialization_hook: Optional[Callable[[], None]] = None):
        """Starts the worker group."""
        self.worker_group = WorkerGroup(self._num_workers,
                                        self._num_cpus_per_worker,
                                        self._num_gpus_per_worker)
        if initialization_hook:
            self.worker_group.execute(initialization_hook)
        self._backend.on_start(self.worker_group, self._backend_config)

    def start_training(
            self,
            train_func: Callable[[], T],
            checkpoint: Optional[Union[Dict, str, Path]] = None,
            checkpoint_strategy: Optional[CheckpointStrategy] = None) -> None:
        """Executes a training function on all workers in a separate thread.

        ``finish_training`` should be called after this.

        Args:
            train_func (Callable): The training function to run on each worker.
            run_dir (Optional[str|Path]): The absolute path or path relative
                to ``Trainer.logdir`` for this run's logs.
            checkpoint (Optional[Dict|str|Path]): The checkpoint data that
                should be loaded onto each worker and accessed by the
                training function via ``sgd.load_checkpoint()``. If this is a
                ``str`` or ``Path`` then the value is expected to be a path
                to a file that contains a serialized checkpoint dict. If this
                is ``None`` then no checkpoint will be loaded.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.
        """
        # Create new log directory for this run.
        self._run_id += 1
        self.latest_run_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Run results will be logged in: {self.latest_run_dir}")

        # Restart checkpointing.
        self._checkpoint_id = 0
        self._checkpoint_strategy = CheckpointStrategy() if \
            checkpoint_strategy is None else checkpoint_strategy

        use_detailed_autofilled_metrics = env_integer(
            ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, 0)

        # First initialize the session.
        def initialize_session(world_rank, train_func, checkpoint):
            try:
                init_session(
                    training_func=train_func,
                    world_rank=world_rank,
                    checkpoint=checkpoint,
                    detailed_autofilled_metrics=use_detailed_autofilled_metrics
                )
            except ValueError:
                raise SGDBackendError(
                    "Attempting to start training but a "
                    "previous training run is still ongoing. "
                    "You must call `finish_training` before "
                    "calling `start_training` again.")

        checkpoint_dict = self._load_checkpoint(checkpoint)

        futures = []
        for world_rank in range(len(self.worker_group)):
            futures.append(
                self.worker_group.execute_single_async(
                    world_rank,
                    initialize_session,
                    world_rank=world_rank,
                    train_func=train_func,
                    checkpoint=checkpoint_dict))

        self.get_with_failure_handling(futures)

        # Run the training function asynchronously in its own thread.
        def train_async():
            session = get_session()
            session.start()

        self.worker_group.execute_async(train_async)

    def _get_next_results(self) -> Optional[List[TrainingResult]]:
        """Fetches the next ``TrainingResult`` from each worker.

        Each ``TrainingResult`` is expected to correspond to the same step from
        each worker (e.g. the same call to ``sgd.report()`` or
        ``sgd.checkpoint()``).

        Returns:
            A list of ``TrainingResult``s with the same
            ``TrainingResultType``, or ``None`` if there are no more results.
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

        # Get next result from each worker.
        futures = self.worker_group.execute_async(get_next)
        results = self.get_with_failure_handling(futures)

        # Check if any worker returned None.
        if any(r is None for r in results):
            # Either all workers have results or none of them do.
            if not all(r is None for r in results):
                raise RuntimeError("Some workers returned results while "
                                   "others didn't. Make sure that "
                                   "`sgd.report()` and `sgd.checkpoint()` are "
                                   "called the same number of times on all "
                                   "workers.")
            else:
                # Return None if all results are None.
                return None
        first_result = results[0]
        result_type = first_result.type
        if any(r.type != result_type for r in results):
            raise RuntimeError("Some workers returned results with "
                               "different types. Make sure `sgd.report()` and "
                               "`sgd.save_checkpoint()` are called the same "
                               "number of times and in the same order on each "
                               "worker.")
        return results

    def _process_checkpoint(self,
                            checkpoint_results: List[TrainingResult]) -> None:
        """ Perform all processing for a checkpoint. """

        # Get checkpoint from first worker.
        checkpoint = checkpoint_results[0].data
        # Store checkpoint in memory.
        self.latest_checkpoint = checkpoint
        # Increment checkpoint id.
        self._latest_checkpoint_id += 1
        if self._checkpoint_strategy.num_to_keep == 0:
            # Checkpoints should not be persisted to disk.
            return

        # TODO(matt): Implement additional checkpoint strategy functionality.
        # Get or create checkpoint dir.
        self.latest_checkpoint_dir.mkdir(parents=True, exist_ok=True)
        # Write checkpoint to disk.
        with self.latest_checkpoint_path.open("wb") as f:
            cloudpickle.dump(checkpoint, f)
            logger.debug(f"Checkpoint successfully written to: "
                         f"{self.latest_checkpoint_path}")

    def _load_checkpoint(self,
                         checkpoint_to_load: Optional[Union[Dict, str, Path]]
                         ) -> Optional[Dict]:
        """Load the checkpoint dictionary from the input dict or path."""
        if checkpoint_to_load is None:
            return None
        if isinstance(checkpoint_to_load, Dict):
            return checkpoint_to_load
        else:
            # Load checkpoint from path.
            checkpoint_path = Path(checkpoint_to_load).expanduser()
            if not checkpoint_path.exists():
                raise ValueError(f"Checkpoint path {checkpoint_path} "
                                 f"does not exist.")
            with checkpoint_path.open("rb") as f:
                return cloudpickle.load(f)

    def fetch_next_result(self) -> Optional[List[Dict]]:
        """Fetch next results produced by ``sgd.report()`` from each worker.

        Assumes ``start_training`` has already been called.

        Returns:
            A list of dictionaries of values passed to ``sgd.report()`` from
                each worker. Each item corresponds to an intermediate result
                a single worker. If there are no more items to fetch,
                returns None.
        """

        while True:
            results = self._get_next_results()
            if results is None:
                return None
            first_result = results[0]
            result_type = first_result.type
            if result_type is TrainingResultType.REPORT:
                result_data = [r.data for r in results]
                return result_data
            elif result_type is TrainingResultType.CHECKPOINT:
                self._process_checkpoint(results)
                # Iterate until next REPORT call or training has finished.
            else:
                raise SGDBackendError(f"Unexpected result type: "
                                      f"{result_type}. "
                                      f"Expected one of "
                                      f"{[type in TrainingResultType]}")

    def finish_training(self) -> List[T]:
        """Finish training and return final results. Propagate any exceptions.

        Blocks until training is finished on all workers.

        Assumes `start_training` has already been called.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """

        def pause_reporting():
            # Get the session for this worker.
            try:
                session = get_session()
            except ValueError:
                # Session is not initialized yet.
                raise SGDBackendError("`finish_training` has been called "
                                      "before `start_training`. Please call "
                                      "`start_training` before "
                                      "`finish_training`.")

            return session.pause_reporting()

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

        # Disable workers from enqueuing results from `sgd.report()`.
        # Results will not be processed during the execution of `finish`.
        # Note: Reported results may still be enqueued at this point,
        #       and should be handled appropriately.
        futures = self.worker_group.execute_async(pause_reporting)
        self.get_with_failure_handling(futures)

        # Finish up processing checkpoints. Reporting has been disabled.
        while True:
            results = self._get_next_results()
            if results is None:
                break
            result_type = results[0].type
            # Process checkpoints and ignore other result types.
            if result_type is TrainingResultType.CHECKPOINT:
                self._process_checkpoint(results)

        futures = self.worker_group.execute_async(end_training)
        results = self.get_with_failure_handling(futures)
        return results

    def get_with_failure_handling(self, remote_values):
        """Gets the remote values while handling for worker failures.

        Args:
            remote_values (list): List of object refs representing functions
                that may fail in the middle of execution. For example, running
                a SGD training loop in multiple parallel actor calls.
        Returns:
            The resolved objects represented by the passed in ObjectRefs.
        """
        success, failed_worker_indexes = check_for_failure(remote_values)
        if success:
            return ray.get(remote_values)
        else:
            self._backend.handle_failure(
                self.worker_group, failed_worker_indexes, self._backend_config)
            raise TrainingWorkerError

    def shutdown(self):
        """Shuts down the workers in the worker group."""
        try:
            self._backend.on_shutdown(self.worker_group, self._backend_config)
        except RayActorError:
            logger.warning("Graceful shutdown of backend failed. This is "
                           "expected if one of the workers has crashed.")
        self.worker_group.shutdown()
        self.worker_group = InactiveWorkerGroup()

    @property
    def is_started(self):
        return not isinstance(self.worker_group, InactiveWorkerGroup)

    @property
    def latest_run_dir(self) -> Optional[Path]:
        """Path to the latest run directory."""
        if self._run_id > 0:
            run_dir = Path(f"run_{self._run_id:03d}")
            return construct_path(run_dir, self.logdir)
        else:
            return None

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        checkpoint_dir = Path("checkpoints")
        return construct_path(checkpoint_dir, self.latest_run_dir)

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Path to the latest persisted checkpoint."""
        if self._latest_checkpoint_id > 0:
            checkpoint_file = f"checkpoint_{self._latest_checkpoint_id:06d}"
            return self.latest_checkpoint_dir.joinpath(checkpoint_file)
        else:
            return None


class BackendInterface:
    def on_start(self, worker_group: WorkerGroup,
                 backend_config: BackendConfig):
        raise NotImplementedError

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: BackendConfig):
        raise NotImplementedError

    def handle_failure(self, worker_group: WorkerGroup,
                       failed_worker_indexes: List[int],
                       backend_config: BackendConfig):
        # TODO: Handle failures during handling of another failure.
        worker_group.remove_workers(failed_worker_indexes)
        if len(worker_group) > 0:
            self.on_shutdown(worker_group, backend_config)
            worker_group.execute(shutdown_session)
        worker_group.add_workers(len(failed_worker_indexes))
        self.on_start(worker_group, backend_config)


class InactiveWorkerGroupError(Exception):
    """Raised when underlying worker group is inactive."""


class InactiveWorkerGroup():
    # TODO: fix inheritence. perhaps create WorkerGroupInterface.

    # Need to define getstate and setstate so that getattr does not screwup
    # pickling. See https://stackoverflow.com/a/50888571/11249691
    def __getstate__(self):
        return vars(self)

    def __setstate__(self, state):
        vars(self).update(state)

    def __getattr__(self, name):
        raise InactiveWorkerGroupError()

    def __len__(self):
        raise InactiveWorkerGroupError()
