import os
import platform
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import Callable, Dict, Optional, Type, Union

import ray
from ray.air.checkpoint import Checkpoint
from ray.data import Dataset, DatasetPipeline
from ray.train._internal.accelerator import Accelerator
from ray.train._internal.utils import PropagatingThread
from ray.train.constants import (
    DATE,
    DETAILED_AUTOFILLED_KEYS,
    HOSTNAME,
    NODE_IP,
    PID,
    RESULT_FETCH_TIMEOUT,
    TIME_THIS_ITER_S,
    TIME_TOTAL_S,
    TIMESTAMP,
    TRAINING_ITERATION,
)
from ray.train.error import SessionMisuseError
from ray.train.session import _TrainSessionImpl


class TrainingResultType(Enum):
    REPORT = auto()
    CHECKPOINT = auto()


@dataclass
class TrialInfo:
    """The trial information to propagate to TrainSession."""

    name: str
    id: str
    resources: Dict[str, float]
    logdir: str


@dataclass
class TrainingResult:
    type: TrainingResultType
    data: Dict


# TODO(xwjiang): This needs a better name.
class _TrainSession:
    """Holds information for training on each worker."""

    def __init__(
        self,
        training_func: Callable,
        world_rank: int,
        local_rank: int,
        world_size: int,
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        trial_info: Optional[TrialInfo] = None,
        dataset_shard: Optional[Union[Dataset, DatasetPipeline]] = None,
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        checkpoint: Optional[Union[Dict, Checkpoint]] = None,
        encode_data_fn: Callable = None,
        detailed_autofilled_metrics: bool = False,
    ):

        self.dataset_shard = dataset_shard

        # The Thread object that is running the training function.
        self.training_thread = PropagatingThread(target=training_func, daemon=True)
        self.world_rank = world_rank
        self.local_rank = local_rank
        self.world_size = world_size
        self.trial_info = trial_info
        # TODO(xwjiang): Legacy Ray Train trainer clean up!
        self.loaded_checkpoint: Optional[Union[Dict, Checkpoint]] = checkpoint

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
                result = self.result_queue.get(block=True, timeout=RESULT_FETCH_TIMEOUT)
            except queue.Empty:
                pass

        # If no result was found, then the runner must no longer be alive.
        if result is None:
            # Try one last time to fetch results in case results were
            # reported in between the time of the last check and the
            # termination of the thread runner.
            try:
                result = self.result_queue.get(
                    block=False, timeout=RESULT_FETCH_TIMEOUT
                )
            except queue.Empty:
                pass

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
            DATE: current_datetime.strftime("%Y-%m-%d_%H-%M-%S"),
            TIMESTAMP: int(time.mktime(current_datetime.timetuple())),
            TIME_THIS_ITER_S: time_this_iter,
            TIME_TOTAL_S: self.time_total,
            PID: os.getpid(),
            HOSTNAME: platform.node(),
            NODE_IP: self.local_ip,
            TRAINING_ITERATION: self.iteration,
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

        kwargs = self._encode_data_fn(self._auto_fill_metrics(kwargs))

        result = TrainingResult(TrainingResultType.REPORT, kwargs)

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

    def checkpoint(self, **kwargs):
        """Adds kwargs to the queue to be consumed by main thread.

        Also stores the checkpoint in ``self.loaded_checkpoint``.
        """

        # Update session checkpoint to latest checkpoint.
        self.loaded_checkpoint = kwargs

        # Only store checkpoints on worker with rank 0.
        if self.world_rank != 0:
            kwargs = {}
        else:
            kwargs = self._encode_data_fn(self._auto_fill_checkpoint_metrics(kwargs))

        result = TrainingResult(TrainingResultType.CHECKPOINT, kwargs)
        # Add result to a thread-safe queue.
        self.result_queue.put(result, block=True)

        # Acquire lock to stop the training thread until
        # checkpoint has been processed.
        self.continue_lock.acquire()

    def report(self, metrics: Dict, checkpoint: Optional[Checkpoint] = None) -> None:
        # TODO(xwjiang): tons of optimizations.
        if checkpoint:
            checkpoint_dict = checkpoint.to_dict()
            self.checkpoint(**checkpoint_dict)
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
