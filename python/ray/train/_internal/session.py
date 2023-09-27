import os
import logging
import platform
import queue
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
import functools
from pathlib import Path
import shutil
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set, Type, Union
import warnings

import ray
from ray.air._internal.session import _get_session
from ray.air._internal.util import StartTraceback, RunnerThread
from ray.air.constants import (
    _RESULT_FETCH_TIMEOUT,
    _ERROR_FETCH_TIMEOUT,
    SESSION_MISUSE_LOG_ONCE_KEY,
    TIMESTAMP,
    TIME_THIS_ITER_S,
)
from ray.data import Dataset, DatasetPipeline
from ray.train import Checkpoint
from ray.train._internal.accelerator import Accelerator
from ray.train._internal.storage import _use_storage_context, StorageContext
from ray.train.constants import (
    CHECKPOINT_METADATA_KEY,
    CHECKPOINT_RANK_KEY,
    DETAILED_AUTOFILLED_KEYS,
    WORKER_HOSTNAME,
    WORKER_NODE_IP,
    WORKER_PID,
    TIME_TOTAL_S,
    LAZY_CHECKPOINT_MARKER_FILE,
    RAY_CHDIR_TO_TRIAL_DIR,
)
from ray.train.error import SessionMisuseError
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.debug import log_once
from ray.util.placement_group import _valid_resource_shape
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)


if TYPE_CHECKING:
    from ray.data import DataIterator
    from ray.tune.execution.placement_groups import PlacementGroupFactory


_INDEX_FILE_EXTENSION = ".files"
_INDEX_FILE = ".RANK_{0}" + _INDEX_FILE_EXTENSION


class TrainingResultType(Enum):
    REPORT = auto()
    CHECKPOINT = auto()


logger = logging.getLogger(__name__)


@dataclass
class TrialInfo:
    """The trial information to propagate to TrainSession."""

    name: str
    id: str
    resources: Dict[str, float]
    logdir: str
    driver_ip: str
    experiment_name: Optional[str] = None


# TODO(justinvyu): [code_removal]
@dataclass
class TrainingResult:
    type: TrainingResultType
    data: Union[Dict, Checkpoint, str]
    metadata: Optional[Dict] = None


class _FutureTrainingResult:
    """A future that will be resolved to a `_TrainingResult`.

    This is needed for specific schedulers such as PBT that schedule saves.

    This wrapper should be removed after refactoring PBT to not schedule saves anymore.
    """

    def __init__(self, future: ray.ObjectRef):
        self.future = future

    def resolve(self, block: bool = True) -> Optional["_TrainingResult"]:
        """Resolve into ``_TrainingResult``.

        This will return None for function trainables if no checkpoint has been
        saved before.
        """
        if block:
            timeout = None
        else:
            timeout = 1e-9
        try:
            return ray.get(self.future, timeout=timeout)
        except TimeoutError:
            # Not ready, yet
            pass
        except Exception as exc:
            logger.error(f"Error resolving result: {exc}")


class _TrainingResult:
    """A (checkpoint, metrics) result reported by the user."""

    def __init__(self, checkpoint: Optional[Checkpoint], metrics: Dict[str, Any]):
        self.checkpoint = checkpoint
        self.metrics = metrics

    def __repr__(self) -> str:
        return f"TrainingResult(checkpoint={self.checkpoint}, metrics={self.metrics})"


# TODO(xwjiang): This needs a better name.
@DeveloperAPI
class _TrainSession:
    """Holds information for training on each worker."""

    def __init__(
        self,
        training_func: Callable,
        world_rank: int,
        local_rank: int,
        node_rank: int,
        local_world_size: int,
        world_size: int,
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        trial_info: Optional[TrialInfo] = None,
        dataset_shard: Optional[Union[Dataset, DatasetPipeline]] = None,
        metadata: Dict[str, Any] = None,
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        checkpoint: Optional[Checkpoint] = None,
        detailed_autofilled_metrics: bool = False,
        # Deprecated
        # If True and the worker is on the same node as driver,
        # will send over checkpoint path and metadata instead of
        # the whole checkpoint to avoid unnecessary serialization.
        enable_lazy_checkpointing: bool = True,
        checkpoint_keep_all_ranks: bool = False,
        checkpoint_upload_from_workers: bool = False,
        storage: Optional[StorageContext] = None,
        synchronous_result_reporting: bool = False,
    ):
        # `synchronous_result_reporting` refers to whether or not the
        # training function is immediately unblocked to continue running
        # after the main thread receives its result.
        # Ex 1: For 2 Ray Train workers with synchronous_result_reporting=True,
        # the worker that produces a result first will immediately will continue
        # onto the next iteration.
        # Ex 2: For a Tune function Trainable with `synchronous_result_reporting=False`,
        # training will only continue with an explicit call to `session.get_next`.
        # Synchronous reporting in example 2 is needed for Tune schedulers to
        # be able to stop the execution of the training function at will,
        # for advanced pausing schedulers (PBT, BOHB) and actor reuse.
        self.synchronous_result_reporting = synchronous_result_reporting

        # Ray Train worker properties
        self.dataset_shard = dataset_shard
        self.metadata = metadata

        self.world_rank = world_rank
        self.local_rank = local_rank
        self.node_rank = node_rank
        self.local_world_size = local_world_size
        self.world_size = world_size

        # Checkpoint configurations
        # TODO(justinvyu): These should all be removed.
        self.enable_lazy_checkpointing = enable_lazy_checkpointing
        self.checkpoint_keep_all_ranks = checkpoint_keep_all_ranks
        self.checkpoint_upload_from_workers = checkpoint_upload_from_workers
        # Only used if checkpoint_upload_from_workers is True.
        self.legacy_checkpoint_uri = None

        if _use_storage_context():
            assert storage
            logger.debug(f"StorageContext on SESSION (rank={world_rank}):\n{storage}")

        # NOTE: `reset` will initialize many properties needed to start running the
        # training_func as a thread.
        self.reset(
            training_func=training_func,
            trial_info=trial_info,
            storage=storage,
            loaded_checkpoint=checkpoint,
        )

        # Autofilled metrics attributes.
        self.detailed_autofilled_metrics = detailed_autofilled_metrics
        self.last_report_time = time.time()
        self.iteration = 0
        self.time_total = 0.0
        self.local_ip = self.get_current_ip()

        self.accelerator = None

    def get_current_ip(self):
        self.local_ip = ray.util.get_node_ip_address()
        return self.local_ip

    def start(self):
        """Starts the training thread."""
        self.training_started = True
        self.training_thread.start()

    def reset(
        self,
        training_func: Callable,
        trial_info: TrialInfo,
        storage: StorageContext,
        loaded_checkpoint=None,
    ):
        # This lock is used to control the execution of the training thread.
        self.continue_lock = threading.Semaphore(0)

        # This event is used to signal the training thread to stop.
        self.stop_event = threading.Event()

        # Queue for sending results across threads.
        self.result_queue = queue.Queue(1)

        # Queue for raising exceptions from runner thread to main thread.
        # The error queue has a max size of one to prevent stacking error and force
        # error reporting to block until finished.
        self.error_queue = queue.Queue(1)

        # The Thread object that is running the training function.
        self.training_thread = RunnerThread(
            target=training_func, daemon=True, error_queue=self.error_queue
        )

        # Possibly override with new state
        self.trial_info = trial_info
        self.storage = storage
        self.loaded_checkpoint = loaded_checkpoint

        # Reset state
        self.ignore_report = False
        self.training_started = False
        self._first_report = True

        if _use_storage_context():
            # Change the working directory to the local trial directory.
            # -> All workers on the same node share a working directory.
            os.makedirs(storage.trial_local_path, exist_ok=True)
            if bool(int(os.environ.get(RAY_CHDIR_TO_TRIAL_DIR, "1"))):
                logger.debug(
                    "Switching the working directory to the trial directory: "
                    f"{storage.trial_local_path}"
                )
                os.chdir(storage.trial_local_path)
        else:
            if trial_info:
                # Change the working directory to `logdir`.
                logdir = os.path.join(trial_info.logdir, f"rank_{self.world_rank}")
                os.makedirs(logdir, exist_ok=True)
                os.chdir(logdir)

    def pause_reporting(self):
        """Ignore all future ``session.report()`` calls."""
        self.ignore_report = True

    def finish(self, timeout: Optional[float] = None):
        """Finishes the training thread.

        Either returns the output from training or raises any Exception from
        training.
        """
        # Set the stop event for the training thread to gracefully exit.
        self.stop_event.set()

        # Release the lock so that training thread can process this event.
        self.continue_lock.release()

        # Force a final (blocking) sync of artifacts in the trial path to storage.
        if _use_storage_context():
            self.storage.persist_artifacts(force=True)

        # Wait for training to finish.
        # This will raise any errors that occur during training, including
        # SystemError
        func_output = self.training_thread.join(timeout=timeout)
        # If training finished successfully, then return results.
        return func_output

    def get_next(self) -> Optional[TrainingResult]:
        """Gets the next ``TrainingResult`` from the result queue.

        If the result queue is empty, then this function returns ``None``.
        """
        if not self.training_started:
            raise RuntimeError("Please call start before calling get_next.")

        if self.synchronous_result_reporting:
            # There's no need to release the lock on the first report
            # since `start` already started the training thread.
            if not self._first_report:
                # Release the lock to trigger training to continue,
                # until the next call to report.
                self.continue_lock.release()
            self._first_report = False

        result = None
        # While training is still ongoing, attempt to get the result.
        while result is None and self.training_thread.is_alive():
            try:
                result = self.result_queue.get(
                    block=True, timeout=_RESULT_FETCH_TIMEOUT
                )
            except queue.Empty:
                pass

        # If no result was found, then the runner must no longer be alive.
        if result is None:
            # Try one last time to fetch results in case results were
            # reported in between the time of the last check and the
            # termination of the thread runner.
            try:
                result = self.result_queue.get(
                    block=False, timeout=_RESULT_FETCH_TIMEOUT
                )
            except queue.Empty:
                pass

        # check if error occurred inside the thread runner.
        if result is None:
            # only raise an error from the runner if all results are consumed
            self._report_thread_runner_error(block=True)
        else:
            if not self.error_queue.empty():
                logger.debug(
                    (
                        "Runner error waiting to be raised in main thread. "
                        "Logging all available results first."
                    )
                )

        if not self.synchronous_result_reporting:
            # At this point, the training thread has reached
            # the `train.report` and is blocked there.
            # If performing asynchronous result reporting,
            # release the lock to allow each worker to keep training
            # immediately after the coordinator fetches their result.
            self.continue_lock.release()

        # Return None if there are no more results to fetch.
        return result

    def _auto_fill_metrics(self, result: dict) -> dict:
        """Add autofilled metrics and update attributes."""
        current_time = time.time()
        current_datetime = datetime.now()
        if TIME_THIS_ITER_S in result:
            time_this_iter = result[TIME_THIS_ITER_S]
        else:
            time_this_iter = current_time - self.last_report_time
        self.iteration += 1
        self.time_total += time_this_iter
        self.last_report_time = current_time

        auto_filled_metrics = {
            TIMESTAMP: int(time.mktime(current_datetime.timetuple())),
            TIME_TOTAL_S: self.time_total,
            WORKER_PID: os.getpid(),
            WORKER_HOSTNAME: platform.node(),
            WORKER_NODE_IP: self.local_ip,
        }

        if not self.detailed_autofilled_metrics:
            auto_filled_metrics = {
                k: v
                for k, v in auto_filled_metrics.items()
                if k not in DETAILED_AUTOFILLED_KEYS
            }

        result = result.copy()
        result.update(auto_filled_metrics)
        return result

    def _report_legacy(self, **kwargs):
        """Adds kwargs to the queue to be consumed by main thread."""
        if self.ignore_report:
            return

        kwargs = self._auto_fill_metrics(kwargs)

        result = TrainingResult(type=TrainingResultType.REPORT, data=kwargs)

        # Add result to a thread-safe queue.
        self.result_queue.put(result, block=True)

        # Acquire lock to stop the training thread until main thread
        # triggers resume.
        self.continue_lock.acquire()

    def _auto_fill_checkpoint_metrics(self, result: dict) -> dict:
        """Add autofilled metrics and update attributes."""
        current_datetime = datetime.now()

        auto_filled_metrics = {
            TIMESTAMP: int(time.mktime(current_datetime.timetuple()))
        }
        result = result.copy()
        result.update(auto_filled_metrics)
        return result

    def _report_thread_runner_error(self, block=False):
        try:
            e = self.error_queue.get(block=block, timeout=_ERROR_FETCH_TIMEOUT)
            raise StartTraceback from e
        except queue.Empty:
            pass

    def _create_checkpoint_file_list(self, checkpoint: Checkpoint):
        """Create an index of the folder contents

        So we know which files belong to which rank.
        """
        root = checkpoint._local_path
        ckpt_files = []
        for dir, _, files in os.walk(root):
            # Strip the root path from the path though, since
            # we are only interested in the part relative to
            # the root of this checkpoint.
            dir = dir[len(root) :]
            for fn in files:
                ckpt_files.append(os.path.join(dir, fn))
        # Write these files into the index file.
        with open(os.path.join(root, _INDEX_FILE.format(self.world_rank)), "w") as f:
            for fn in ckpt_files:
                f.write(f"{fn}\n")

    def _remove_uploaded_checkpoint_files(self, checkpoint: Checkpoint):
        """Get rid of already uploaded large checkpoint files.

        This is so they don't get shipped to the driver node.
        """
        root = checkpoint._local_path
        for f in os.listdir(root):
            if f.endswith(_INDEX_FILE_EXTENSION):
                # We will leave the index file in there so local
                # checkpoint has knowledge about the cloud files.
                continue
            fp = os.path.join(root, f)
            if os.path.isfile(fp):
                os.unlink(fp)
            elif os.path.isdir(fp):
                shutil.rmtree(fp)

    def checkpoint(self, checkpoint: Checkpoint):
        """Adds kwargs to the queue to be consumed by main thread.

        Also stores the checkpoint in ``self.loaded_checkpoint``.
        """
        checkpoint_type, _ = checkpoint.get_internal_representation()

        if checkpoint_type == "data_dict" and self.checkpoint_keep_all_ranks:
            if log_once("keep_all_ranks_dict_checkpoint"):
                logger.warning(
                    "Saving checkpoints from all ranks does not work with "
                    "dictionary checkpoints. Set `ray.train.CheckpointConfig"
                    "(_checkpoint_keep_all_ranks=False)`, or write checkpoints "
                    "to a directory and report directory checkpoints that "
                    "contain unique files per worker rank. For example, "
                    "use filenames that contain the unique rank. You can "
                    "retrieve the rank with `session.get_world_rank()` within "
                    "your training loop per worker."
                )

        upload_from_workers = (
            checkpoint_type == "local_path"
            and self.checkpoint_upload_from_workers
            and self.legacy_checkpoint_uri
        )
        if upload_from_workers:
            self._create_checkpoint_file_list(checkpoint)
            logger.info(
                f"Uploading checkpoint files from worker rank {self.world_rank} "
                f"to cloud URI {self.legacy_checkpoint_uri}."
            )
            # We want to upload the files directly to cloud storage,
            # so that they won't need to be shipped to the driver node
            # via object store.
            checkpoint.to_uri(self.legacy_checkpoint_uri)
            logger.info("Done uploading checkpoint files.")
            self._remove_uploaded_checkpoint_files(checkpoint)

        # Update session checkpoint to latest checkpoint.
        self.loaded_checkpoint = checkpoint

        # Only store checkpoints on worker with rank 0.
        if self.world_rank != 0 and not self.checkpoint_keep_all_ranks:
            checkpoint = None

        metadata = self._auto_fill_checkpoint_metrics({})

        if (
            checkpoint
            and self.enable_lazy_checkpointing
            and checkpoint._local_path
            and (Path(self.trial_info.logdir) / LAZY_CHECKPOINT_MARKER_FILE).exists()
        ):
            metadata.update({CHECKPOINT_METADATA_KEY: checkpoint._metadata})
            checkpoint = str(checkpoint._local_path)

        # Save the rank of the worker that created this checkpoint.
        metadata.update({CHECKPOINT_RANK_KEY: self.world_rank})

        result = TrainingResult(
            type=TrainingResultType.CHECKPOINT,
            data=checkpoint,
            metadata=metadata,
        )

        # Add result to a thread-safe queue.
        self.result_queue.put(result, block=True)

        # Acquire lock to stop the training thread until
        # checkpoint has been processed.
        self.continue_lock.acquire()

    def _set_legacy_checkpoint_uri(self, uri: str):
        """Tell session where to save the next directory checkpoint on the cloud.

        Args:
            uri: URI to the location where next checkpoint should be saved.
        """
        self.legacy_checkpoint_uri = uri

    def _report_training_result(self, training_result: _TrainingResult) -> None:
        """Place a training result on the result queue for the main thread to process,
        then block until the main thread signals that training should continue.

        NOTE: This is used internally to report results from Train to Tune
        without persisting checkpoints to storage 2 times.
        `report` is the public API that directly persists to storage, which
        should only be called by user code.
        """
        if training_result.checkpoint:
            # NOTE: This populates `train.get_checkpoint`
            self.loaded_checkpoint = training_result.checkpoint

            # NOTE: This is where the coordinator AND workers increment their
            # checkpoint index.
            self.storage.current_checkpoint_index += 1

        # Add result to a thread-safe queue.
        self.result_queue.put(training_result, block=True)

        # Acquire lock to stop the training thread until main thread
        # triggers resume.
        self.continue_lock.acquire()

        # If the trial should be terminated, exit gracefully.
        # NOTE: This is only really useful if `synchronous_result_reporting=True`.
        # Otherwise, the lock is immediately released on reporting, and this
        # check is skipped before the main thread decides to set the stop event.
        if self.stop_event.is_set():
            self.stop_event.clear()
            sys.exit(0)

    def new_report(
        self, metrics: Dict, checkpoint: Optional[Checkpoint] = None
    ) -> None:
        if self.ignore_report:
            return

        persisted_checkpoint = None
        if checkpoint:
            # Persist the reported checkpoint files to storage.
            persisted_checkpoint = self.storage.persist_current_checkpoint(checkpoint)

        # Persist trial artifacts to storage.
        force_artifact_sync = (
            persisted_checkpoint
            and self.storage.sync_config.sync_artifacts_on_checkpoint
        )
        self.storage.persist_artifacts(force=force_artifact_sync)

        metrics = self._auto_fill_metrics(metrics)

        # Set additional user metadata from the Trainer.
        if persisted_checkpoint and self.metadata:
            user_metadata = persisted_checkpoint.get_metadata()
            for k, v in self.metadata.items():
                # Update keys not already set by the user. This gives user-set keys
                # precedence over keys set at the Trainer level.
                if k not in user_metadata:
                    user_metadata[k] = v
            persisted_checkpoint.set_metadata(user_metadata)

        result = _TrainingResult(checkpoint=persisted_checkpoint, metrics=metrics)

        self._report_training_result(result)

    def report(self, metrics: Dict, checkpoint: Optional[Checkpoint] = None) -> None:
        # TODO(xwjiang): tons of optimizations.

        # Special case: early fail for Torch tensors
        if "torch" in sys.modules:
            from ray.air._internal.torch_utils import contains_tensor

            if contains_tensor(metrics):
                raise ValueError(
                    "Passing objects containg Torch tensors as metrics "
                    "is not supported as it will throw an exception on "
                    "deserialization. You can either convert the tensors "
                    "to Python objects or report a `train.Checkpoint` "
                    "with `ray.train.report` to store your Torch objects."
                )

        if _use_storage_context():
            return self.new_report(metrics, checkpoint=checkpoint)

        if checkpoint:
            self.checkpoint(checkpoint)
        self._report_legacy(**metrics)

    @property
    def experiment_name(self) -> str:
        return self.trial_info.experiment_name

    @property
    def trial_name(self) -> str:
        return self.trial_info.name

    @property
    def trial_id(self) -> str:
        return self.trial_info.id

    @property
    def trial_resources(self) -> "PlacementGroupFactory":
        return self.trial_info.resources

    @property
    def trial_dir(self) -> str:
        return self.trial_info.logdir

    def get_dataset_shard(
        self,
        dataset_name: Optional[str] = None,
    ) -> Optional["DataIterator"]:
        shard = self.dataset_shard
        if shard is None:
            warnings.warn(
                "No dataset passed in. Returning None. Make sure to "
                "pass in a Dataset to Trainer.run to use this "
                "function."
            )
        elif isinstance(shard, dict):
            if not dataset_name:
                raise RuntimeError(
                    "Multiple datasets were passed into ``Trainer``, "
                    "but no ``dataset_name`` is passed into "
                    "``get_dataset_shard``. Please specify which "
                    "dataset shard to retrieve."
                )
            return shard.get(dataset_name)
        return shard


# Cache of resource dicts that have been checked by the launch hook already.
_checked_resources: Set[frozenset] = set()
_session: Optional[_TrainSession] = None


def _tune_task_and_actor_launch_hook(
    fn, resources: Dict[str, float], strategy: Optional[SchedulingStrategyT]
):
    """Launch hook to catch nested tasks that can't fit in the placement group.

    This gives users a nice warning in case they launch a nested task in a Tune trial
    without reserving resources in the trial placement group to fit it.
    """

    # Already checked, skip for performance reasons.
    key = frozenset({(k, v) for k, v in resources.items() if v > 0})
    if not key or key in _checked_resources:
        return

    # No need to check if placement group is None.
    if (
        not isinstance(strategy, PlacementGroupSchedulingStrategy)
        or strategy.placement_group is None
    ):
        return

    # Check if the resource request is targeting the current placement group.
    cur_pg = ray.util.get_current_placement_group()
    if not cur_pg or strategy.placement_group.id != cur_pg.id:
        return

    _checked_resources.add(key)

    # Check if the request can be fulfilled by the current placement group.
    pgf = get_trial_resources()

    if pgf.head_bundle_is_empty:
        available_bundles = cur_pg.bundle_specs[0:]
    else:
        available_bundles = cur_pg.bundle_specs[1:]

    # Check if the request can be fulfilled by the current placement group.
    if _valid_resource_shape(resources, available_bundles):
        return

    if fn.class_name:
        submitted = "actor"
        name = fn.module_name + "." + fn.class_name + "." + fn.function_name
    else:
        submitted = "task"
        name = fn.module_name + "." + fn.function_name

    # Normalize the resource spec so it looks the same as the placement group bundle.
    main_resources = cur_pg.bundle_specs[0]
    resources = {k: float(v) for k, v in resources.items() if v > 0}

    raise RuntimeError(
        f"No trial resources are available for launching the {submitted} `{name}`. "
        "To resolve this, specify the Tune option:\n\n"
        ">  resources_per_trial=tune.PlacementGroupFactory(\n"
        f">    [{main_resources}] + [{resources}] * N\n"
        ">  )\n\n"
        f"Where `N` is the number of slots to reserve for trial {submitted}s. "
        "If you are using a Ray training library, there might be a utility function "
        "to set this automatically for you. For more information, refer to "
        "https://docs.ray.io/en/latest/tune/tutorials/tune-resources.html"
    )


def init_session(*args, **kwargs) -> None:
    global _session
    if _session:
        raise ValueError(
            "A Train session is already in use. Do not call "
            "`init_session()` manually."
        )

    # Setup hooks for generating placement group resource deadlock warnings.
    from ray import actor, remote_function

    if "TUNE_DISABLE_RESOURCE_CHECKS" not in os.environ:
        actor._actor_launch_hook = _tune_task_and_actor_launch_hook
        remote_function._task_launch_hook = _tune_task_and_actor_launch_hook

    _session = _TrainSession(*args, **kwargs)


def get_session() -> Optional[_TrainSession]:
    return _session


def shutdown_session():
    """Shuts down the initialized session."""
    global _session
    _session = None


def _raise_accelerator_session_misuse():
    """Raises a SessionMisuseError because a utility function was used improperly."""
    raise SessionMisuseError(
        "prepare/accelerate utility functions should be called inside a training "
        "function executed by `Trainer.run`"
    )


def get_accelerator(default_accelerator_cls: Type[Accelerator]) -> Accelerator:
    """The accelerator for this training session.

    If an accelerator has not been set, then this method will construct an
    accelerator using the provided accelerator class.

    Raises:
        SessionMisuseError: if the session is uninitialized.
    """
    session = get_session()
    if session is None:
        _raise_accelerator_session_misuse()
    if session.accelerator is None:
        session.accelerator = default_accelerator_cls()
    return session.accelerator


def set_accelerator(accelerator: Accelerator) -> None:
    """Sets the accelerator for this training session.

    Args:
        accelerator: The accelerator to use for training.

    Raises:
        SessionMisuseError: if the session is unitialized.
        RuntimeError: if the accelerator has already been set.
    """
    session = get_session()
    if session is None:
        _raise_accelerator_session_misuse()
    if session.accelerator is not None:
        raise RuntimeError("Cannot change accelerator once set.")
    session.accelerator = accelerator


def _warn_session_misuse(default_value: Any = None):
    """Warns if fn is being used outside of session and returns ``default_value``."""

    def inner(fn: Callable):
        fn_name = fn.__name__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            session = _get_session()
            if not session:
                if log_once(f"{SESSION_MISUSE_LOG_ONCE_KEY}-{fn_name}"):
                    warnings.warn(
                        f"`{fn_name}` is meant to only be "
                        "called inside a function that is executed by a Tuner"
                        f" or Trainer. Returning `{default_value}`."
                    )
                return default_value
            return fn(*args, **kwargs)

        return wrapper

    return inner


@PublicAPI(stability="stable")
@_warn_session_misuse()
def report(metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
    """Report metrics and optionally save a checkpoint.

    Each invocation of this method will automatically increment the underlying
    ``training_iteration`` number. The physical meaning of this "iteration" is
    defined by user depending on how often they call ``report``.
    It does not necessarily map to one epoch.

    This method acts as a synchronous barrier for all distributed training workers.
    All workers must call `ray.train.report` the same number of times.

    If a checkpoint is provided, it will be
    :ref:`persisted to storage <persistent-storage-guide>`.

    Example:

        .. testcode::

            import tempfile

            from ray import train
            from ray.train import Checkpoint
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0
                checkpoint = train.get_checkpoint()
                if checkpoint:
                    with checkpoint.as_directory() as checkpoint_dir:
                        # Load back training state
                        ...

                for epoch in range(start_epoch, config.get("num_epochs", 10)):
                    # Do training...

                    metrics = {"loss": ...}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...

                        checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)
                        train.report(metrics, checkpoint=checkpoint)

            trainer = TorchTrainer(
                train_func, scaling_config=train.ScalingConfig(num_workers=2)
            )

    Args:
        metrics: The metrics you want to report.
        checkpoint: The optional checkpoint you want to report.
    """

    _get_session().report(metrics, checkpoint=checkpoint)


@PublicAPI(stability="stable")
@_warn_session_misuse()
def get_checkpoint() -> Optional[Checkpoint]:
    """Access the session's last checkpoint to resume from if applicable.

    Example:

        .. testcode::

            import tempfile

            from ray import train
            from ray.train import Checkpoint
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0
                checkpoint = train.get_checkpoint()
                if checkpoint:
                    with checkpoint.as_directory() as checkpoint_dir:
                        # Load back training state
                        ...

                for epoch in range(start_epoch, config.get("num_epochs", 10)):
                    # Do training...

                    metrics = {"loss": ...}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...

                        checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)
                        train.report(metrics, checkpoint=checkpoint)

            trainer = TorchTrainer(
                train_func, scaling_config=train.ScalingConfig(num_workers=2)
            )

    Returns:
        Checkpoint object if the session is currently being resumed.
            Otherwise, return None.
    """

    return _get_session().loaded_checkpoint


@PublicAPI(stability="beta")
@_warn_session_misuse()
def get_metadata() -> Dict[str, Any]:
    """User metadata dict passed to the Trainer constructor."""
    return _get_session().metadata


@PublicAPI(stability="beta")
@_warn_session_misuse()
def get_experiment_name() -> str:
    """Experiment name for the corresponding trial."""
    return _get_session().experiment_name


@PublicAPI(stability="beta")
@_warn_session_misuse()
def get_trial_name() -> str:
    """Trial name for the corresponding trial."""
    return _get_session().trial_name


@PublicAPI(stability="beta")
@_warn_session_misuse()
def get_trial_id() -> str:
    """Trial id for the corresponding trial."""
    return _get_session().trial_id


@PublicAPI(stability="beta")
@_warn_session_misuse()
def get_trial_resources() -> "PlacementGroupFactory":
    """Trial resources for the corresponding trial."""
    return _get_session().trial_resources


@PublicAPI(stability="beta")
@_warn_session_misuse()
def get_trial_dir() -> str:
    """Log directory corresponding to the trial directory for a Tune session.
    If calling from a Train session, this will give the trial directory of its parent
    Tune session.

    .. testcode::

        from ray import train, tune

        def train_func(config):
            print(train.get_context().get_trial_dir())

        tuner = tune.Tuner(train_func)
        tuner.fit()

    .. testoutput::
        :options: +MOCK

        /Users/root/ray_results/train_func_2023-07-19_15-01-37/train_func_d620c_00000_0_2023-07-19_15-01-40
    """
    return _get_session().trial_dir


@PublicAPI(stability="beta")
@_warn_session_misuse(default_value=1)
def get_world_size() -> int:
    """Get the current world size (i.e. total number of workers) for this run.

    .. testcode::

        import ray
        from ray import train
        from ray.train import ScalingConfig
        from ray.train.tensorflow import TensorflowTrainer

        NUM_WORKERS = 2

        def train_loop_per_worker(config):
            assert train.get_context().get_world_size() == NUM_WORKERS

        train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        trainer = TensorflowTrainer(
            train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=NUM_WORKERS),
            datasets={"train": train_dataset}
        )
        trainer.fit()

    .. testoutput::
        :hide:

        ...
    """
    session = _get_session()
    if not hasattr(session, "world_size"):
        raise RuntimeError(
            "`get_world_size` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.world_size


@PublicAPI(stability="beta")
@_warn_session_misuse(default_value=0)
def get_world_rank() -> int:
    """Get the world rank of this worker.

    .. testcode::

        import ray
        from ray import train
        from ray.train import ScalingConfig
        from ray.train.tensorflow import TensorflowTrainer

        def train_loop_per_worker(config):
            if train.get_context().get_world_rank() == 0:
                print("Worker 0")

        train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        trainer = TensorflowTrainer(
            train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": train_dataset}
        )
        trainer.fit()

    .. testoutput::
        :hide:

        ...
    """
    session = _get_session()
    if not hasattr(session, "world_rank"):
        raise RuntimeError(
            "`get_world_rank` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.world_rank


@PublicAPI(stability="beta")
@_warn_session_misuse(default_value=0)
def get_local_rank() -> int:
    """Get the local rank of this worker (rank of the worker on its node).

    .. testcode::

        import torch

        import ray
        from ray import train
        from ray.train import ScalingConfig
        from ray.train.torch import TorchTrainer

        def train_loop_per_worker(config):
            if torch.cuda.is_available():
                torch.cuda.set_device(train.get_context().get_local_rank())
            ...

        train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        trainer = TorchTrainer(
            train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
            datasets={"train": train_dataset}
        )
        trainer.fit()

    .. testoutput::
        :hide:

        ...
    """
    session = _get_session()
    if not hasattr(session, "local_rank"):
        raise RuntimeError(
            "`get_local_rank` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.local_rank


@PublicAPI(stability="beta")
@_warn_session_misuse(default_value=0)
def get_local_world_size() -> int:
    """Get the local world size of this node (i.e. number of workers on this node).

    Example:

        .. testcode::

            import ray
            from ray import train
            from ray.train import ScalingConfig
            from ray.train.torch import TorchTrainer

            def train_loop_per_worker():
                print(train.get_context().get_local_world_size())

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = TorchTrainer(train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=1),
                datasets={"train": train_dataset})
            trainer.fit()

        .. testoutput::
            :hide:

            ...
    """
    session = _get_session()
    if not hasattr(session, "local_world_size"):
        raise RuntimeError(
            "`get_local_world_size` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.local_world_size


@PublicAPI(stability="beta")
@_warn_session_misuse(default_value=0)
def get_node_rank() -> int:
    """Get the rank of this node.

    Example:

        .. testcode::

            import ray
            from ray import train
            from ray.train import ScalingConfig
            from ray.train.torch import TorchTrainer

            def train_loop_per_worker():
                print(train.get_context().get_node_rank())

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = TorchTrainer(train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=1),
                datasets={"train": train_dataset})
            trainer.fit()

        .. testoutput::
            :hide:

            ...
    """
    session = _get_session()
    if not hasattr(session, "node_rank"):
        raise RuntimeError(
            "`get_node_rank` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.node_rank


@PublicAPI(stability="stable")
@_warn_session_misuse()
def get_dataset_shard(
    dataset_name: Optional[str] = None,
) -> Optional["DataIterator"]:
    """Returns the :class:`ray.data.DataIterator` shard for this worker.

    Call :meth:`~ray.data.DataIterator.iter_torch_batches` or
    :meth:`~ray.data.DataIterator.to_tf` on this shard to convert it to the
    appropriate framework-specific data type.

    .. testcode::

        import ray
        from ray import train
        from ray.train import ScalingConfig
        from ray.train.torch import TorchTrainer

        def train_loop_per_worker(config):
            ...
            for epoch in range(2):
                # Trainer will automatically handle sharding.
                data_shard = train.get_dataset_shard("train")
                for batch in data_shard.iter_torch_batches():
                    ...

        train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        trainer = TorchTrainer(
            train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": train_dataset}
        )
        trainer.fit()

    .. testoutput::
        :hide:

        ...

    Args:
        dataset_name: If a Dictionary of Datasets was passed to ``Trainer``, then
            specifies which dataset shard to return.

    Returns:
        The ``DataIterator`` shard to use for this worker.
        If no dataset is passed into Trainer, then return None.
    """
    session = _get_session()
    if not hasattr(session, "get_dataset_shard"):
        raise RuntimeError(
            "`get_dataset_shard` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.get_dataset_shard(dataset_name)


@DeveloperAPI
@_warn_session_misuse()
def get_storage() -> StorageContext:
    """Returns the :class:`~ray.train._internal.storage.StorageContext` storage
    context which gives advanced access to the filesystem and paths
    configured through `RunConfig`.

    NOTE: This is a developer API, and the `StorageContext` interface may change
    without notice between minor versions.
    """
    return get_session().storage
