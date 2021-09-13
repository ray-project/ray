import abc
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Callable, TypeVar, List, Optional, Dict, Union

import ray
from ray import cloudpickle
from ray.exceptions import RayActorError
from ray.ray_constants import env_integer
from ray.util.sgd.v2.checkpoint import CheckpointStrategy
from ray.util.sgd.v2.constants import ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, \
    TUNE_INSTALLED, TUNE_CHECKPOINT_FILE_NAME, \
    TUNE_CHECKPOINT_ID
from ray.util.sgd.v2.session import TrainingResultType, TrainingResult
from ray.util.sgd.v2.session import init_session, get_session, shutdown_session
from ray.util.sgd.v2.utils import construct_path, get_node_id, get_gpu_ids, \
    check_for_failure
from ray.util.sgd.v2.worker_group import WorkerGroup

if TUNE_INSTALLED:
    from ray import tune
else:
    tune = None

T = TypeVar("T")

logger = logging.getLogger(__name__)


class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        raise NotImplementedError


class SGDBackendError(Exception):
    """Errors with BackendExecutor that should not be exposed to user."""


class CheckpointManager:
    """Manages checkpoint processing, writing, and loading.


    - A ``checkpoints`` directory is created in the ``run_dir`` and contains
    all the checkpoint files.

    The full default path will be:

    ~/ray_results/sgd_<datestring>/run_<run_id>/checkpoints/
    checkpoint_<checkpoint_id>

    Attributes:
        latest_checkpoint_dir (Optional[Path]): Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_filename (Optional[str]): Filename for the latest
            checkpoint.
        latest_checkpoint_path (Optional[Path]): Path to the latest persisted
            checkpoint from the latest run.
        latest_checkpoint_id (Optional[int]): The id of the most recently
            saved checkpoint.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def on_init(self):
        """Checkpoint code executed during BackendExecutor init."""
        self.latest_checkpoint = None

        # Incremental unique checkpoint ID of this run.
        self._latest_checkpoint_id = 0

    def on_start_training(
            self,
            checkpoint_strategy: Optional[CheckpointStrategy],
            run_dir: Path,
            latest_checkpoint_id: Optional[int] = None,
    ):
        """Checkpoint code executed during BackendExecutor start_training."""
        # Restart checkpointing.
        self._latest_checkpoint_id = latest_checkpoint_id if \
            latest_checkpoint_id else 0
        self._checkpoint_strategy = CheckpointStrategy() if \
            checkpoint_strategy is None else checkpoint_strategy
        self.run_dir = run_dir

    def _process_checkpoint(self,
                            checkpoint_results: List[TrainingResult]) -> None:
        """Perform all processing for a checkpoint. """

        # Get checkpoint from first worker.
        checkpoint = checkpoint_results[0].data

        # Increment checkpoint id.
        self._latest_checkpoint_id += 1

        # Store checkpoint in memory.
        self.latest_checkpoint = checkpoint

        self.write_checkpoint(checkpoint)

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

    def write_checkpoint(self, checkpoint: Dict):
        """Writes checkpoint to disk."""
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

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        checkpoint_dir = Path("checkpoints")
        return construct_path(checkpoint_dir, self.run_dir)

    @property
    def latest_checkpoint_file_name(self) -> Optional[str]:
        """Filename to use for the latest checkpoint."""
        if self._latest_checkpoint_id > 0:
            return f"checkpoint_{self._latest_checkpoint_id:06d}"
        else:
            return None

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Path to the latest persisted checkpoint."""
        if self._latest_checkpoint_id > 0:
            checkpoint_file = self.latest_checkpoint_file_name
            return self.latest_checkpoint_dir.joinpath(checkpoint_file)
        else:
            return None


class TuneCheckpointManager(CheckpointManager):
    def create_logdir(self, log_dir: Optional[Union[str, Path]]):
        # Don't create logdir when using with Tune.
        pass

    def create_run_dir(self):
        # Don't create run_dir when using with Tune.
        pass

    def _load_checkpoint(self,
                         checkpoint_to_load: Optional[Union[Dict, str, Path]]
                         ) -> Optional[Dict]:
        loaded_checkpoint = super()._load_checkpoint(checkpoint_to_load)
        if loaded_checkpoint is not None:
            # If the Tune trial is restarted, a new Trainer is instantiated.
            # However, we want the checkpoint_id to continue incrementing
            # from the previous run.
            self._latest_checkpoint_id = loaded_checkpoint[TUNE_CHECKPOINT_ID]
        return loaded_checkpoint

    def write_checkpoint(self, checkpoint: Dict):
        # Store the checkpoint_id in the file so that the Tune trial can be
        # resumed after failure or cancellation.
        checkpoint[TUNE_CHECKPOINT_ID] = self._latest_checkpoint_id
        # If inside a Tune Trainable, then checkpoint with Tune.
        with tune.checkpoint_dir(step=self._latest_checkpoint_id) as \
                checkpoint_dir:
            path = Path(checkpoint_dir)
            # Use a standard file name so that we know which file to load
            # the checkpoint from.
            file_path = path.joinpath(TUNE_CHECKPOINT_FILE_NAME)
            with file_path.open("wb") as f:
                cloudpickle.dump(checkpoint, f)


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
        additional_resources_per_worker (Optional[Dict[str, float]]):
            Dictionary specifying the extra resources that will be
            requested for each worker in addition to ``num_cpus_per_worker``
            and ``num_gpus_per_worker``.
        max_retries (int): Number of retries when Ray actors fail.
            Defaults to 3. Set to -1 for unlimited retries.

    Attributes:
        latest_checkpoint_dir (Optional[Path]): Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_path (Optional[Path]): Path to the latest persisted
            checkpoint from the latest run.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def __init__(
            self,
            backend_config: BackendConfig,
            num_workers: int = 1,
            num_cpus_per_worker: float = 1,
            num_gpus_per_worker: float = 0,
            additional_resources_per_worker: Optional[Dict[str, float]] = None,
            max_retries: int = 3):
        self._backend_config = backend_config
        self._backend = self._backend_config.backend_cls()
        self._num_workers = num_workers
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker
        self._additional_resources_per_worker = additional_resources_per_worker
        self._max_failures = max_retries
        if self._max_failures < 0:
            self._max_failures = float("inf")
        self._num_failures = 0
        self._initialization_hook = None

        if tune is not None and tune.is_session_enabled():
            self.checkpoint_manager = TuneCheckpointManager()
        else:
            self.checkpoint_manager = CheckpointManager()

        self.worker_group = InactiveWorkerGroup()

        self.checkpoint_manager.on_init()

    def start(self, initialization_hook: Optional[Callable[[], None]] = None):
        """Starts the worker group."""
        self.worker_group = WorkerGroup(
            self._num_workers, self._num_cpus_per_worker,
            self._num_gpus_per_worker, self._additional_resources_per_worker)
        try:
            if initialization_hook:
                self._initialization_hook = initialization_hook
                self.worker_group.execute(initialization_hook)
            if self._num_gpus_per_worker > 0:
                self._setup_gpus()
            self._backend.on_start(self.worker_group, self._backend_config)
        except RayActorError as exc:
            logger.exception(str(exc))
            self._increment_failures()
            self._restart()

    def _setup_gpus(self):
        """Sets CUDA_VISIBLE_DEVICES on all workers.

        For each worker, CUDA_VISIBLE_DEVICES will be set to the GPU IDs
        visible to all workers on that worker's node.

        This allows GPU workers on the same node to communicate with one
        another.

        Example:

            Setup:
            - Node1:
                - Worker1: {0, 1}
                - Worker2: {2, 3}
            - Node2:
                - Worker3: {0, 1}

            CUDA_VISIBLE_DEVICES:
            - Worker1: "0,1,2,3"
            - Worker2: "0,1,2,3"
            - Worker2: "0,1"

        """

        def get_node_id_and_gpu():
            node_id = get_node_id()
            gpu_ids = get_gpu_ids()
            return node_id, gpu_ids

        node_ids_and_gpu_ids = self.worker_group.execute(get_node_id_and_gpu)

        node_id_to_worker_id = defaultdict(set)
        node_id_to_gpu_ids = defaultdict(set)

        for worker_id, (node_id, gpu_ids) in enumerate(node_ids_and_gpu_ids):
            node_id_to_worker_id[node_id].add(worker_id)
            node_id_to_gpu_ids[node_id].update(gpu_ids)

        futures = []
        for node_id, gpu_ids in node_id_to_gpu_ids.items():
            all_gpu_ids = ",".join([str(gpu_id) for gpu_id in gpu_ids])

            def set_gpu_ids():
                os.environ["CUDA_VISIBLE_DEVICES"] = all_gpu_ids

            for worker_id in node_id_to_worker_id[node_id]:
                futures.append(
                    self.worker_group.execute_single_async(
                        worker_id, set_gpu_ids))
        ray.get(futures)

    def start_training(
            self,
            train_func: Callable[[], T],
            run_dir: Path,
            checkpoint: Optional[Union[Dict, str, Path]] = None,
            checkpoint_strategy: Optional[CheckpointStrategy] = None,
            latest_checkpoint_id: Optional[int] = None,
    ) -> None:
        """Executes a training function on all workers in a separate thread.

        ``finish_training`` should be called after this.

        Args:
            train_func (Callable): The training function to run on each worker.
            run_dir (Path): The directory to use for this run.
            checkpoint (Optional[Dict|str|Path]): The checkpoint data that
                should be loaded onto each worker and accessed by the
                training function via ``sgd.load_checkpoint()``. If this is a
                ``str`` or ``Path`` then the value is expected to be a path
                to a file that contains a serialized checkpoint dict. If this
                is ``None`` then no checkpoint will be loaded.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.
            latest_checkpoint_id (Optional[int]): The checkpoint id of the
                most recently saved checkpoint.
        """
        self.checkpoint_manager.on_start_training(
            checkpoint_strategy=checkpoint_strategy,
            run_dir=run_dir,
            latest_checkpoint_id=latest_checkpoint_id)

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

        checkpoint_dict = self.checkpoint_manager._load_checkpoint(checkpoint)

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
                self.checkpoint_manager._process_checkpoint(results)
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
                self.checkpoint_manager._process_checkpoint(results)

        futures = self.worker_group.execute_async(end_training)
        results = self.get_with_failure_handling(futures)
        return results

    def get_with_failure_handling(self, remote_values):
        """Gets the remote values while handling for worker failures.

        This method should be called instead of ``ray.get()`` directly in
        order to handle worker failures.

        If a worker failure is identified, backend specific failure handling
        is executed and a ``TrainingWorkerError`` is raised.

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
            self._increment_failures()
            try:
                self._backend.handle_failure(self.worker_group,
                                             failed_worker_indexes,
                                             self._backend_config)
            except RayActorError as exc:
                logger.exception(str(exc))
                self._restart()
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
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        return self.checkpoint_manager.latest_checkpoint_dir

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Path to the latest persisted checkpoint."""
        return self.checkpoint_manager.latest_checkpoint_path

    @property
    def latest_checkpoint_id(self) -> Optional[int]:
        """The checkpoint id of most recently saved checkpoint.

        If no checkpoint has been saved yet, then return None.
        """
        checkpoint_id = self.checkpoint_manager._latest_checkpoint_id
        if checkpoint_id == 0:
            return None
        else:
            return checkpoint_id

    @property
    def latest_checkpoint(self) -> Optional[Dict]:
        """Latest checkpoint object."""
        return self.checkpoint_manager.latest_checkpoint

    def _restart(self):
        self.worker_group.shutdown()
        if self._initialization_hook is not None:
            initialization_hook = self._initialization_hook
        else:
            initialization_hook = None
        self.start(initialization_hook=initialization_hook)

    def _increment_failures(self):
        self._num_failures += 1
        if self._num_failures >= self._max_failures:
            raise RuntimeError("Training has failed even after "
                               f"{self._num_failures} "
                               "attempts. You can change the number of max "
                               "failure attempts by setting the "
                               "`max_retries` arg in your `Trainer`.") \
                from None


class Backend(metaclass=abc.ABCMeta):
    def on_start(self, worker_group: WorkerGroup,
                 backend_config: BackendConfig):
        """Logic for starting this backend."""
        pass

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: BackendConfig):
        """Logic for shutting down the backend."""
        pass

    def handle_failure(self, worker_group: WorkerGroup,
                       failed_worker_indexes: List[int],
                       backend_config: BackendConfig):
        """Logic for handling failures.

        By default, restart all workers.
        """
        worker_group.shutdown()
        worker_group.start()
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
