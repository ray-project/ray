from typing import Iterable, List, Optional, Dict, Set, Tuple, Union
import abc
import warnings
import logging
import numpy as np
import json
from pathlib import Path

from ray.util.debug import log_once
from ray.util.ml_utils.dict import flatten_dict
from ray.util.ml_utils.json import SafeFallbackEncoder
from ray.train.callbacks import TrainingCallback
from ray.train.constants import (RESULT_FILE_JSON, TRAINING_ITERATION,
                                 TIME_TOTAL_S, TIMESTAMP, PID)

logger = logging.getLogger(__name__)


class TrainingLogdirMixin:
    def start_training(self, logdir: str, **info):
        if self._logdir:
            logdir_path = Path(self._logdir)
        else:
            logdir_path = Path(logdir)

        if not logdir_path.is_dir():
            raise ValueError(f"logdir '{logdir_path}' must be a directory.")

        self._logdir_path = logdir_path

    @property
    def logdir(self) -> Optional[Path]:
        """Path to currently used logging directory."""
        if not hasattr(self, "_logdir_path"):
            return Path(self._logdir)
        return Path(self._logdir_path)


class TrainingSingleFileLoggingCallback(
        TrainingLogdirMixin, TrainingCallback, metaclass=abc.ABCMeta):
    """Abstract Train logging callback class.

    Args:
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer.
        filename (Optional[str]): Filename in logdir to save results to.
        workers_to_log (int|List[int]|None): Worker indices to log.
            If None, will log all workers. By default, will log the
            worker with index 0.
    """

    # Defining it like this ensures it will be overwritten
    # in a subclass - otherwise an exception will be raised
    _default_filename: Union[str, Path]

    def __init__(self,
                 logdir: Optional[str] = None,
                 filename: Optional[str] = None,
                 workers_to_log: Optional[Union[int, List[int]]] = 0) -> None:
        self._logdir = logdir
        self._filename = filename
        self._workers_to_log = self._validate_workers_to_log(workers_to_log)
        self._log_path = None

    def _validate_workers_to_log(self, workers_to_log) -> List[int]:
        if isinstance(workers_to_log, int):
            workers_to_log = [workers_to_log]

        if workers_to_log is not None:
            if not isinstance(workers_to_log, Iterable):
                raise TypeError("workers_to_log must be an Iterable, got "
                                f"{type(workers_to_log)}.")
            if not all(isinstance(worker, int) for worker in workers_to_log):
                raise TypeError(
                    "All elements of workers_to_log must be integers.")
            if len(workers_to_log) < 1:
                raise ValueError(
                    "At least one worker must be specified in workers_to_log.")
        return workers_to_log

    def _create_log_path(self, logdir_path: Path, filename: Path) -> Path:
        if not filename:
            raise ValueError("filename cannot be None or empty.")
        return logdir_path.joinpath(Path(filename))

    def start_training(self, logdir: str, **info):
        super().start_training(logdir, **info)

        if not self._filename:
            filename = self._default_filename
        else:
            filename = self._filename

        self._log_path = self._create_log_path(self.logdir, filename)

    @property
    def log_path(self) -> Optional[Path]:
        """Path to the log file.

        Will be None before `start_training` is called for the first time.
        """
        return self._log_path


class JsonLoggerCallback(TrainingSingleFileLoggingCallback):
    """Logs Train results in json format.

    Args:
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer.
        filename (Optional[str]): Filename in logdir to save results to.
        workers_to_log (int|List[int]|None): Worker indices to log.
            If None, will log all workers. By default, will log the
            worker with index 0.
    """

    _default_filename: Union[str, Path] = RESULT_FILE_JSON

    def start_training(self, logdir: str, **info):
        super().start_training(logdir, **info)

        # Create a JSON file with an empty list
        # that will be latter appended to
        with open(self._log_path, "w") as f:
            json.dump([], f, cls=SafeFallbackEncoder)

    def handle_result(self, results: List[Dict], **info):
        if self._workers_to_log is None or results is None:
            results_to_log = results
        else:
            results_to_log = [
                result for i, result in enumerate(results)
                if i in self._workers_to_log
            ]
        with open(self._log_path, "r+") as f:
            loaded_results = json.load(f)
            f.seek(0)
            json.dump(
                loaded_results + [results_to_log], f, cls=SafeFallbackEncoder)


class TrainingSingleWorkerLoggingCallback(
        TrainingLogdirMixin, TrainingCallback, metaclass=abc.ABCMeta):
    """Abstract Train logging callback class.

    Allows only for single-worker logging.

    Args:
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer.
        worker_to_log (int): Worker index to log. By default, will log the
            worker with index 0.
    """

    def __init__(self, logdir: Optional[str] = None,
                 worker_to_log: int = 0) -> None:
        self._logdir = logdir
        self._workers_to_log = self._validate_worker_to_log(worker_to_log)
        self._log_path = None

    def _validate_worker_to_log(self, worker_to_log) -> int:
        if isinstance(worker_to_log, Iterable):
            worker_to_log = list(worker_to_log)
            if len(worker_to_log) > 1:
                raise ValueError(
                    f"{self.__class__.__name__} only supports logging "
                    "from a single worker.")
            elif len(worker_to_log) < 1:
                raise ValueError(
                    "At least one worker must be specified in workers_to_log.")
            worker_to_log = worker_to_log[0]
        if not isinstance(worker_to_log, int):
            raise TypeError("workers_to_log must be an integer.")
        return worker_to_log


class TBXLoggerCallback(TrainingSingleWorkerLoggingCallback):
    """Logs Train results in TensorboardX format.

    Args:
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer.
        worker_to_log (int): Worker index to log. By default, will log the
            worker with index 0.
    """

    VALID_SUMMARY_TYPES: Tuple[type] = (int, float, np.float32, np.float64,
                                        np.int32, np.int64)
    IGNORE_KEYS: Set[str] = {PID, TIMESTAMP, TIME_TOTAL_S, TRAINING_ITERATION}

    def start_training(self, logdir: str, **info):
        super().start_training(logdir, **info)

        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                warnings.warn(
                    "pip install 'tensorboardX' to see TensorBoard files.")
            raise

        self._file_writer = SummaryWriter(self.logdir, flush_secs=30)

    def handle_result(self, results: List[Dict], **info):
        result = results[self._workers_to_log]
        step = result[TRAINING_ITERATION]
        result = {k: v for k, v in result.items() if k not in self.IGNORE_KEYS}
        flat_result = flatten_dict(result, delimiter="/")
        path = ["ray", "train"]

        # same logic as in ray.tune.logger.TBXLogger
        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if (isinstance(value, self.VALID_SUMMARY_TYPES)
                    and not np.isnan(value)):
                self._file_writer.add_scalar(
                    full_attr, value, global_step=step)
            elif ((isinstance(value, list) and len(value) > 0)
                  or (isinstance(value, np.ndarray) and value.size > 0)):

                # Must be video
                if isinstance(value, np.ndarray) and value.ndim == 5:
                    self._file_writer.add_video(
                        full_attr, value, global_step=step, fps=20)
                    continue

                try:
                    self._file_writer.add_histogram(
                        full_attr, value, global_step=step)
                # In case TensorboardX still doesn't think it's a valid value
                # (e.g. `[[]]`), warn and move on.
                except (ValueError, TypeError):
                    if log_once("invalid_tbx_value"):
                        warnings.warn(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value,
                                             type(self).__name__))
        self._file_writer.flush()

    def finish_training(self, error: bool = False, **info):
        self._file_writer.close()
