import os
import platform
import queue
import threading
import time
from datetime import datetime
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable
from typing import Optional, Dict, Type
import warnings

import ray
from ray.train.accelerator import Accelerator
from ray.train.constants import (
    DETAILED_AUTOFILLED_KEYS,
    TIME_THIS_ITER_S,
    PID,
    TIMESTAMP,
    TIME_TOTAL_S,
    NODE_IP,
    TRAINING_ITERATION,
    HOSTNAME,
    DATE,
    RESULT_FETCH_TIMEOUT,
    SESSION_MISUSE_LOG_ONCE_KEY,
)
from ray.train.utils import PropagatingThread
from ray.train.impl.dataset_spec import RayDataset
from ray.util import PublicAPI, log_once


class TrainingResultType(Enum):
    REPORT = auto()
    CHECKPOINT = auto()


@dataclass
class TrainingResult:
    type: TrainingResultType
    data: Dict


class Session:
    """Holds information for training on each worker."""

    def __init__(
        self,
        training_func: Callable,
        world_rank: int,
        local_rank: int,
        world_size: int,
        dataset_shard: Optional[RayDataset] = None,
        checkpoint: Optional[Dict] = None,
        encode_data_fn: Callable = None,
        detailed_autofilled_metrics: bool = False,
    ):

        self.dataset_shard = dataset_shard

        # The Thread object that is running the training function.
        self.training_thread = PropagatingThread(target=training_func, daemon=True)
        self.world_rank = world_rank
        self.local_rank = local_rank
        self.world_size = world_size
        self.loaded_checkpoint = checkpoint

        # Function to encode checkpoint dict before sending to the driver.
        if not encode_data_fn:

            def noop(x):
                return x

            encode_data_fn = noop
        self._encode_data_fn = encode_data_fn

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
        """Ignore all future ``train.report()`` calls."""
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

    def report(self, **kwargs):
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


_session = None


def _warn_session_misuse(fn_name: str):
    """Logs warning message on provided fn being used outside of session.

    Args:
        fn_name (str): The name of the function to warn about.
    """

    if log_once(f"{SESSION_MISUSE_LOG_ONCE_KEY}-{fn_name}"):
        warnings.warn(
            f"`train.{fn_name}()` is meant to only be "
            f"called "
            "inside a training function that is executed by "
            "`Trainer.run`. Returning None."
        )


def init_session(*args, **kwargs) -> None:
    global _session
    if _session:
        raise ValueError(
            "A Train session is already in use. Do not call "
            "`init_session()` manually."
        )
    _session = Session(*args, **kwargs)


def get_session() -> Optional[Session]:
    global _session
    return _session


def shutdown_session():
    """Shuts down the initialized session."""
    global _session
    _session = None


@PublicAPI(stability="beta")
def get_dataset_shard(dataset_name: Optional[str] = None) -> Optional[RayDataset]:
    """Returns the Ray Dataset or DatasetPipeline shard for this worker.

    You should call ``to_torch()`` or ``to_tf()`` on this shard to convert
    it to the appropriate framework-specific Dataset.

    .. code-block:: python

        import ray
        from ray import train

        def train_func():
            model = Net()
            for iter in range(100):
                data_shard = train.get_dataset_shard().to_torch()
                model.train(data_shard)
            return model

        dataset = ray.data.read_csv("train.csv")
        dataset.filter(...).repeat().random_shuffle()

        trainer = Trainer(backend="torch")
        trainer.start()
        # Trainer will automatically handle sharding.
        train_model = trainer.run(train_func, dataset=dataset)
        trainer.shutdown()

    Args:
        dataset_name (Optional[str]): If a Dictionary of Datasets was passed to
            ``Trainer``, then specifies which dataset shard to return.


    Returns:
        The ``Dataset`` or ``DatasetPipeline`` shard to use for this worker.
        If no dataset is passed into Trainer, then return None.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(get_dataset_shard.__name__)
        return
    shard = session.dataset_shard
    if shard is None:
        warnings.warn(
            "No dataset passed in. Returning None. Make sure to "
            "pass in a Ray Dataset to Trainer.run to use this "
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
        return shard[dataset_name]
    return shard


@PublicAPI(stability="beta")
def report(**kwargs) -> None:
    """Reports all keyword arguments to Train as intermediate results.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            for iter in range(100):
                time.sleep(1)
                train.report(hello="world")

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be reported by Train.
            If callbacks are provided, they are executed on these
            intermediate results.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(report.__name__)
        return
    session.report(**kwargs)


@PublicAPI(stability="beta")
def world_rank() -> int:
    """Get the world rank of this worker.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            for iter in range(100):
                time.sleep(1)
                if train.world_rank() == 0:
                    print("Worker 0")

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    """
    session = get_session()
    if session is None:
        return 0
    return session.world_rank


@PublicAPI(stability="beta")
def local_rank() -> int:
    """Get the local rank of this worker (rank of the worker on its node).

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            if torch.cuda.is_available():
                torch.cuda.set_device(train.local_rank())
            ...

        trainer = Trainer(backend="torch", use_gpu=True)
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    """
    session = get_session()
    if session is None:
        return 0
    return session.local_rank


@PublicAPI(stability="beta")
def load_checkpoint() -> Optional[Dict]:
    """Loads checkpoint data onto the worker.

    .. code-block:: python

        from ray import train

        def train_func():
            checkpoint = train.load_checkpoint()
            for iter in range(checkpoint["epoch"], 5):
                print(iter)

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func, checkpoint={"epoch": 3})
        # 3
        # 4
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be checkpointed by Train.
    Returns:
        The most recently saved checkpoint if ``train.save_checkpoint()``
        has been called. Otherwise, the checkpoint that the session was
        originally initialized with. ``None`` if neither exist.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(load_checkpoint.__name__)
        return
    return session.loaded_checkpoint


@PublicAPI(stability="beta")
def save_checkpoint(**kwargs) -> None:
    """Checkpoints all keyword arguments to Train as restorable state.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            for iter in range(100):
                time.sleep(1)
                train.save_checkpoint(epoch=iter)

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be checkpointed by Train.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(save_checkpoint.__name__)
        return
    session.checkpoint(**kwargs)


@PublicAPI(stability="beta")
def world_size() -> int:
    """Get the current world size (i.e. total number of workers) for this run.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            assert train.world_size() == 4

        trainer = Trainer(backend="torch", num_workers=4)
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()
    """
    session = get_session()
    if session is None:
        return 1
    return session.world_size


class SessionMisuseError(Exception):
    """Method or function was used outside of a session."""


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
        accelerator (Accelerator): The accelerator to use for training.

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
