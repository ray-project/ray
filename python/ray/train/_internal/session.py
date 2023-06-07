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
from pathlib import Path
import shutil
from typing import Callable, Dict, Optional, Type, Union

import ray
from ray.air._internal.util import StartTraceback, RunnerThread
from ray.air.checkpoint import Checkpoint
from ray.air.constants import (
    _RESULT_FETCH_TIMEOUT,
    _ERROR_FETCH_TIMEOUT,
    TIMESTAMP,
    TIME_THIS_ITER_S,
)
from ray.data import Dataset, DatasetPipeline
from ray.train._internal.accelerator import Accelerator
from ray.train.constants import (
    CHECKPOINT_METADATA_KEY,
    CHECKPOINT_RANK_KEY,
    DETAILED_AUTOFILLED_KEYS,
    WORKER_HOSTNAME,
    WORKER_NODE_IP,
    WORKER_PID,
    TIME_TOTAL_S,
    LAZY_CHECKPOINT_MARKER_FILE,
)

from ray.train.error import SessionMisuseError
from ray.train.session import _TrainSessionImpl
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once


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


@dataclass
class TrainingResult:
    type: TrainingResultType
    data: Union[Dict, Checkpoint, str]
    metadata: Optional[Dict] = None


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
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        checkpoint: Optional[Checkpoint] = None,
        # Deprecated
        encode_data_fn: Optional[Callable] = None,
        detailed_autofilled_metrics: bool = False,
        # If True and the worker is on the same node as driver,
        # will send over checkpoint path and metadata instead of
        # the whole checkpoint to avoid unnecessary serialization.
        enable_lazy_checkpointing: bool = True,
        checkpoint_keep_all_ranks: bool = False,
        checkpoint_upload_from_workers: bool = False,
    ):

        self.dataset_shard = dataset_shard

        self.world_rank = world_rank
        self.local_rank = local_rank
        self.node_rank = node_rank
        self.local_world_size = local_world_size
        self.world_size = world_size
        self.trial_info = trial_info
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        self.loaded_checkpoint = checkpoint
        self.enable_lazy_checkpointing = enable_lazy_checkpointing
        self.checkpoint_keep_all_ranks = checkpoint_keep_all_ranks
        self.checkpoint_upload_from_workers = checkpoint_upload_from_workers
        # Only used if checkpoint_upload_from_workers is True.
        self.checkpoint_uri = None

        # Function to encode checkpoint dict before sending to the driver.
        if not encode_data_fn:

            def noop(x):
                return x

            encode_data_fn = noop
        self._encode_data_fn = encode_data_fn

        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        if trial_info:
            # Change the working directory to `logdir`.
            logdir = os.path.join(trial_info.logdir, f"rank_{self.world_rank}")
            os.makedirs(logdir, exist_ok=True)
            os.chdir(logdir)

        # This lock is used to control the execution of the training thread.
        self.continue_lock = threading.Semaphore(0)

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

        # Autofilled metrics attributes.
        self.detailed_autofilled_metrics = detailed_autofilled_metrics
        self.last_report_time = time.time()
        self.iteration = 0
        self.time_total = 0.0
        self.local_ip = self.get_current_ip()

        self.ignore_report = False
        self.training_started = False

        self.accelerator = None

    def get_current_ip(self):
        self.local_ip = ray.util.get_node_ip_address()
        return self.local_ip

    def start(self):
        """Starts the training thread."""
        self.training_started = True
        self.training_thread.start()

    def pause_reporting(self):
        """Ignore all future ``session.report()`` calls."""
        self.ignore_report = True

    def finish(self):
        """Finishes the training thread.

        Either returns the output from training or raises any Exception from
        training.
        """

        # Wait for training to finish.
        # This will raise any errors that occur during training, including
        # SystemError
        func_output = self.training_thread.join()
        # If training finished successfully, then return results.
        return func_output

    def get_next(self) -> Optional[TrainingResult]:
        """Gets the next ``TrainingResult`` from the result queue.

        If the result queue is empty, then this function returns ``None``.
        """
        if not self.training_started:
            raise RuntimeError("Please call start before calling get_next.")
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

        # Release the lock to trigger training to continue.
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
                    "dictionary checkpoints. Set `ray.air.CheckpointConfig"
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
            and self.checkpoint_uri
        )
        if upload_from_workers:
            self._create_checkpoint_file_list(checkpoint)
            logger.info(
                f"Uploading checkpoint files from worker rank {self.world_rank} "
                f"to cloud URI {self.checkpoint_uri}."
            )
            # We want to upload the files directly to cloud storage,
            # so that they won't need to be shipped to the driver node
            # via object store.
            checkpoint.to_uri(self.checkpoint_uri)
            logger.info("Done uploading checkpoint files.")
            self._remove_uploaded_checkpoint_files(checkpoint)

        # Update session checkpoint to latest checkpoint.
        self.loaded_checkpoint = checkpoint

        # Only store checkpoints on worker with rank 0.
        if self.world_rank != 0 and not self.checkpoint_keep_all_ranks:
            checkpoint = None
        elif checkpoint:
            checkpoint = self._encode_data_fn(checkpoint)

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

    def _set_checkpoint_uri(self, uri: str):
        """Tell session where to save the next directory checkpoint on the cloud.

        Args:
            uri: URI to the location where next checkpoint should be saved.
        """
        self.checkpoint_uri = uri

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
                    "to Python objects or use a `TorchCheckpoint` as the "
                    "`checkpoint` argument of `ray.air.session.report` to "
                    "store your Torch objects."
                )

        if checkpoint:
            self.checkpoint(checkpoint)
        self._report_legacy(**metrics)


_session: Optional[_TrainSession] = None
# V2 Session API
_session_v2: Optional[_TrainSessionImpl] = None


def init_session(*args, **kwargs) -> None:
    global _session
    global _session_v2
    if _session:
        raise ValueError(
            "A Train session is already in use. Do not call "
            "`init_session()` manually."
        )
    _session = _TrainSession(*args, **kwargs)
    _session_v2 = _TrainSessionImpl(session=_session)


def get_session() -> Optional[_TrainSession]:
    return _session


def shutdown_session():
    """Shuts down the initialized session."""
    global _session
    global _session_v2
    _session = None
    _session_v2 = None


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
