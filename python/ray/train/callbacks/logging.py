import abc
import json
import logging
import os
import warnings
from pathlib import Path
from typing import List, Optional, Dict, Set, Tuple, Union

import numpy as np

from ray.train.callbacks import TrainingCallback
from ray.train.callbacks.results_prepocessors import WorkersResultsPreprocessor, \
    SingleWorkerResultsPreprocessor, KeysResultsPreprocessor
from ray.train.constants import (RESULT_FILE_JSON, TRAINING_ITERATION,
                                 TIME_TOTAL_S, TIMESTAMP, PID,
                                 TRAIN_CHECKPOINT_SUBDIR)
from ray.util.debug import log_once
from ray.util.ml_utils.dict import flatten_dict
from ray.util.ml_utils.json import SafeFallbackEncoder
from ray.util.ml_utils.mlflow import MLflowLoggerUtil

logger = logging.getLogger(__name__)


class TrainingLogdirCallback(TrainingCallback, abc.ABC):
    """Abstract Train callback class with logging directory."""

    def start_training(self, logdir: str, config: Dict, **info):
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


class TrainingSingleFileLoggingCallback(TrainingLogdirCallback, abc.ABC):
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
        self._log_path = None
        self._results_preprocessors = [
            WorkersResultsPreprocessor(workers_to_log=workers_to_log)
        ]

    def _create_log_path(self, logdir_path: Path, filename: Path) -> Path:
        if not filename:
            raise ValueError("filename cannot be None or empty.")
        return logdir_path.joinpath(Path(filename))

    def start_training(self, logdir: str, **info):
        super().start_training(logdir=logdir, **info)

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
        super().start_training(logdir=logdir, **info)

        # Create a JSON file with an empty list
        # that will be latter appended to
        with open(self._log_path, "w") as f:
            json.dump([], f, cls=SafeFallbackEncoder)

    def handle_result(self, results: List[Dict], **info):
        with open(self._log_path, "r+") as f:
            loaded_results = json.load(f)
            f.seek(0)
            json.dump(loaded_results + [results], f, cls=SafeFallbackEncoder)


class MLflowLoggerCallback(TrainingLogdirCallback, TrainingCallback):
    """MLflow Logger to automatically log Train results and config to MLflow.

    MLflow (https://mlflow.org) Tracking is an open source library for
    recording and querying experiments. This Ray Train callback
    sends information (config parameters, training results & metrics,
    and artifacts) to MLflow for automatic experiment tracking.

    Args:
        tracking_uri (Optional[str]): The tracking URI for where to manage
            experiments and runs. This can either be a local file path or a
            remote server. If None is passed in, the logdir of the trainer
            will be used as the tracking URI.
            This arg gets passed directly to mlflow initialization.
        registry_uri (Optional[str]): The registry URI that gets passed
            directly to mlflow initialization. If None is passed in, the
            logdir of the trainer will be used as the registry URI.
        experiment_id (Optional[str]): The experiment id of an already
            existing experiment. If not
            passed in, experiment_name will be used.
        experiment_name (Optional[str]): The experiment name to use for this
            Train run.
            If the experiment with the name already exists with MLflow,
            it will be used. If not, a new experiment will be created with
            this name. At least one of ``experiment_id`` or
            ``experiment_name`` must be passed in.
        tags (Optional[Dict]):  An optional dictionary of string keys and
            values to set as tags on the run
        save_artifact (bool): If set to True, automatically save the entire
            contents of the Train local_dir as an artifact to the
            corresponding run in MlFlow.
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer. If no tracking
            uri or registry uri are passed in, the logdir will be used for
            both.
        worker_to_log (int): Worker index to log. By default, will log the
            worker with index 0.
    """

    def __init__(self,
                 tracking_uri: Optional[str] = None,
                 registry_uri: Optional[str] = None,
                 experiment_id: Optional[str] = None,
                 experiment_name: Optional[str] = None,
                 tags: Optional[Dict] = None,
                 save_artifact: bool = False,
                 logdir: Optional[str] = None,
                 worker_to_log: int = 0):
        super().__init__(logdir=logdir)
        self._results_preprocessors = [
            SingleWorkerResultsPreprocessor(workers_to_log=worker_to_log)
        ]

        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri
        self.experiment_id = experiment_id
        self.experiment_name = experiment_name
        self.tags = tags

        self.save_artifact = save_artifact
        self.mlflow_util = MLflowLoggerUtil()

    def start_training(self, logdir: str, config: Dict, **info):
        super().start_training(logdir=logdir, config=config, **info)

        tracking_uri = self.tracking_uri or os.path.join(
            str(self.logdir), "mlruns")
        registry_uri = self.registry_uri or os.path.join(
            str(self.logdir), "mlruns")

        self.mlflow_util.setup_mlflow(
            tracking_uri=tracking_uri,
            registry_uri=registry_uri,
            experiment_id=self.experiment_id,
            experiment_name=self.experiment_name,
            create_experiment_if_not_exists=True)

        self.mlflow_util.start_run(tags=self.tags, set_active=True)
        self.mlflow_util.log_params(params_to_log=config)

    def handle_result(self, results: List[Dict], **info):
        result = results[0]

        self.mlflow_util.log_metrics(
            metrics_to_log=result, step=result[TRAINING_ITERATION])

    def finish_training(self, error: bool = False, **info):
        checkpoint_dir = self.logdir.joinpath(TRAIN_CHECKPOINT_SUBDIR)
        if self.save_artifact and checkpoint_dir.exists():
            self.mlflow_util.save_artifacts(dir=str(checkpoint_dir))
        self.mlflow_util.end_run(status="FAILED" if error else "FINISHED")


class TBXLoggerCallback(TrainingLogdirCallback, TrainingCallback):
    """Logs Train results in TensorboardX format.

    Args:
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer.
        worker_to_log (int): Worker index to log. By default, will log the
            worker with index 0.
    """

    VALID_SUMMARY_TYPES: Tuple[type] = (int, float, np.float32, np.float64,
                                        np.int32, np.int64)
    IGNORE_KEYS: Set[str] = {PID, TIMESTAMP, TIME_TOTAL_S}

    def __init__(self, logdir: Optional[str] = None,
                 worker_to_log: int = 0) -> None:
        self._logdir = logdir
        self._log_path = None

        self._results_preprocessors = [
            SingleWorkerResultsPreprocessor(workers_to_log=worker_to_log),
            KeysResultsPreprocessor(excluded_keys=self.IGNORE_KEYS)
        ]

    def start_training(self, logdir: str, **info):
        super().start_training(logdir=logdir, **info)

        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                warnings.warn(
                    "pip install 'tensorboardX' to see TensorBoard files.")
            raise

        self._file_writer = SummaryWriter(self.logdir, flush_secs=30)

    def handle_result(self, results: List[Dict], **info):
        result = results[0]
        # Use TRAINING_ITERATION for step but remove it so it is not logged.
        step = result.pop(TRAINING_ITERATION)
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
