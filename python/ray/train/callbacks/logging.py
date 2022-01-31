import json
import logging
import os
import warnings
from pathlib import Path
from typing import List, Optional, Dict, Set, Tuple, Union

import numpy as np

from ray.train.callbacks import TrainingCallback
from ray.train.callbacks.results_preprocessors import (
    IndexedResultsPreprocessor,
    ExcludedKeysResultsPreprocessor,
)
from ray.train.callbacks.results_preprocessors.preprocessor import (
    SequentialResultsPreprocessor,
)
from ray.train.constants import (
    RESULT_FILE_JSON,
    TRAINING_ITERATION,
    TIME_TOTAL_S,
    TIMESTAMP,
    PID,
    TRAIN_CHECKPOINT_SUBDIR,
)
from ray.util.debug import log_once
from ray.util.ml_utils.dict import flatten_dict
from ray.util.ml_utils.json import SafeFallbackEncoder
from ray.util.ml_utils.mlflow import MLflowLoggerUtil

logger = logging.getLogger(__name__)


class TrainCallbackLogdirManager:
    """Sets up a logging directory for a callback.

    The path of the ``logdir`` can be set during initialization. Otherwise, the
    ``default_logdir`` will be used when ``setup_logdir`` is called.

    ``setup_logdir`` is expected to be called from
    ``TrainingCallback.start_training``, with a default ``logdir``.

    Args:
        logdir (Optional[str|Path]): The path of the logdir to use.
            If None is passed in, the logdir will use the ``default_logdir``
            when ``setup_logdir`` is called.
        create_logdir (bool): Whether to create the logdir if it does not
            already exist.

    Attributes:
        logdir_path (Path): The path of the logdir. The default logdir will
            not be available until ``setup_logdir`` is called.
    """

    def __init__(
        self, logdir: Optional[Union[str, Path]] = None, create_logdir: bool = True
    ):
        self._default_logdir = None
        self._logdir = Path(logdir) if logdir else None
        self._create_logdir = create_logdir

    def setup_logdir(self, default_logdir: str) -> Path:
        """Sets up the logdir.

        The directory will be created if it does not exist and
         ``create_logdir`` is set to True.

        Args:
            default_logdir (str): The default logdir to use, only if the
            ``TrainCallbackLogdirManager`` was not initialized with a ``logdir``.

        Returns:
            The path of the logdir.
        """
        self._default_logdir = Path(default_logdir)

        if self._create_logdir:
            self.logdir_path.mkdir(parents=True, exist_ok=True)

        if not self.logdir_path.is_dir():
            raise ValueError(f"logdir '{self.logdir_path}' must be a directory.")

        return self.logdir_path

    @property
    def logdir_path(self) -> Path:
        if self._logdir:
            return self._logdir
        if self._default_logdir:
            return self._default_logdir
        raise RuntimeError("Logdir must be set in init or setup_logdir.")


class JsonLoggerCallback(TrainingCallback):
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

    def __init__(
        self,
        logdir: Optional[str] = None,
        filename: Optional[str] = None,
        workers_to_log: Optional[Union[int, List[int]]] = 0,
    ):
        self._filename = filename
        self._logdir_manager = TrainCallbackLogdirManager(logdir=logdir)
        self.results_preprocessor = IndexedResultsPreprocessor(indices=workers_to_log)

    def start_training(self, logdir: str, **info):
        self._logdir_manager.setup_logdir(default_logdir=logdir)

        # Create a JSON file with an empty list
        # that will be latter appended to
        with open(self.log_path, "w") as f:
            json.dump([], f, cls=SafeFallbackEncoder)

    def handle_result(self, results: List[Dict], **info):
        with open(self.log_path, "r+") as f:
            loaded_results = json.load(f)
            f.seek(0)
            json.dump(loaded_results + [results], f, cls=SafeFallbackEncoder)

    @property
    def logdir(self) -> Path:
        return self._logdir_manager.logdir_path

    @property
    def log_path(self) -> Path:
        filename = self._filename or JsonLoggerCallback._default_filename
        return self.logdir.joinpath(filename)


class MLflowLoggerCallback(TrainingCallback):
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

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        registry_uri: Optional[str] = None,
        experiment_id: Optional[str] = None,
        experiment_name: Optional[str] = None,
        tags: Optional[Dict] = None,
        save_artifact: bool = False,
        logdir: Optional[str] = None,
        worker_to_log: int = 0,
    ):
        self._logdir_manager = TrainCallbackLogdirManager(logdir=logdir)
        self.results_preprocessor = IndexedResultsPreprocessor(indices=worker_to_log)

        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri
        self.experiment_id = experiment_id
        self.experiment_name = experiment_name
        self.tags = tags

        self.save_artifact = save_artifact
        self.mlflow_util = MLflowLoggerUtil()

    def start_training(self, logdir: str, config: Dict, **info):
        self._logdir_manager.setup_logdir(default_logdir=logdir)

        tracking_uri = self.tracking_uri or os.path.join(str(self.logdir), "mlruns")
        registry_uri = self.registry_uri or os.path.join(str(self.logdir), "mlruns")

        self.mlflow_util.setup_mlflow(
            tracking_uri=tracking_uri,
            registry_uri=registry_uri,
            experiment_id=self.experiment_id,
            experiment_name=self.experiment_name,
            create_experiment_if_not_exists=True,
        )

        self.mlflow_util.start_run(tags=self.tags, set_active=True)
        self.mlflow_util.log_params(params_to_log=config)

    def handle_result(self, results: List[Dict], **info):
        result = results[0]

        self.mlflow_util.log_metrics(
            metrics_to_log=result, step=result[TRAINING_ITERATION]
        )

    def finish_training(self, error: bool = False, **info):
        checkpoint_dir = self.logdir.joinpath(TRAIN_CHECKPOINT_SUBDIR)
        if self.save_artifact and checkpoint_dir.exists():
            self.mlflow_util.save_artifacts(dir=str(checkpoint_dir))
        self.mlflow_util.end_run(status="FAILED" if error else "FINISHED")

    @property
    def logdir(self) -> Path:
        return self._logdir_manager.logdir_path


class TBXLoggerCallback(TrainingCallback):
    """Logs Train results in TensorboardX format.

    Args:
        logdir (Optional[str]): Path to directory where the results file
            should be. If None, will be set by the Trainer.
        worker_to_log (int): Worker index to log. By default, will log the
            worker with index 0.
    """

    VALID_SUMMARY_TYPES: Tuple[type] = (
        int,
        float,
        np.float32,
        np.float64,
        np.int32,
        np.int64,
    )
    IGNORE_KEYS: Set[str] = {PID, TIMESTAMP, TIME_TOTAL_S}

    def __init__(self, logdir: Optional[str] = None, worker_to_log: int = 0) -> None:
        self._logdir_manager = TrainCallbackLogdirManager(logdir=logdir)

        results_preprocessors = [
            IndexedResultsPreprocessor(indices=worker_to_log),
            ExcludedKeysResultsPreprocessor(excluded_keys=self.IGNORE_KEYS),
        ]
        self.results_preprocessor = SequentialResultsPreprocessor(results_preprocessors)

    def start_training(self, logdir: str, **info):
        self._logdir_manager.setup_logdir(default_logdir=logdir)

        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                warnings.warn("pip install 'tensorboardX' to see TensorBoard files.")
            raise

        self._file_writer = SummaryWriter(str(self.logdir), flush_secs=30)

    def handle_result(self, results: List[Dict], **info):
        result = results[0]
        # Use TRAINING_ITERATION for step but remove it so it is not logged.
        step = result.pop(TRAINING_ITERATION)
        flat_result = flatten_dict(result, delimiter="/")
        path = ["ray", "train"]

        # same logic as in ray.tune.logger.TBXLogger
        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if isinstance(value, self.VALID_SUMMARY_TYPES) and not np.isnan(value):
                self._file_writer.add_scalar(full_attr, value, global_step=step)
            elif (isinstance(value, list) and len(value) > 0) or (
                isinstance(value, np.ndarray) and value.size > 0
            ):

                # Must be video
                if isinstance(value, np.ndarray) and value.ndim == 5:
                    self._file_writer.add_video(
                        full_attr, value, global_step=step, fps=20
                    )
                    continue

                try:
                    self._file_writer.add_histogram(full_attr, value, global_step=step)
                # In case TensorboardX still doesn't think it's a valid value
                # (e.g. `[[]]`), warn and move on.
                except (ValueError, TypeError):
                    if log_once("invalid_tbx_value"):
                        warnings.warn(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value, type(self).__name__)
                        )
        self._file_writer.flush()

    def finish_training(self, error: bool = False, **info):
        self._file_writer.close()

    @property
    def logdir(self) -> Path:
        return self._logdir_manager.logdir_path
