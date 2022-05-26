import csv
import json
import logging
import numpy as np
import os
import yaml

from typing import Iterable, TYPE_CHECKING, Dict, List, Optional, TextIO, Type

import ray.cloudpickle as cloudpickle

from ray.tune.callback import Callback
from ray.tune.utils.util import SafeFallbackEncoder
from ray.util.debug import log_once
from ray.tune.result import (
    TRAINING_ITERATION,
    TIME_TOTAL_S,
    TIMESTEPS_TOTAL,
    EXPR_PARAM_FILE,
    EXPR_PARAM_PICKLE_FILE,
    EXPR_PROGRESS_FILE,
    EXPR_RESULT_FILE,
)
from ray.tune.utils import flatten_dict
from ray.util.annotations import PublicAPI, DeveloperAPI

if TYPE_CHECKING:
    from ray.tune.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)

tf = None
VALID_SUMMARY_TYPES = [int, float, np.float32, np.float64, np.int32, np.int64]


@DeveloperAPI
class Logger:
    """Logging interface for ray.tune.

    By default, the UnifiedLogger implementation is used which logs results in
    multiple formats (TensorBoard, rllab/viskit, plain json, custom loggers)
    at once.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        trial: Trial object for the logger to access.
    """

    def __init__(self, config: Dict, logdir: str, trial: Optional["Trial"] = None):
        self.config = config
        self.logdir = logdir
        self.trial = trial
        self._init()

    def _init(self):
        pass

    def on_result(self, result):
        """Given a result, appends it to the existing log."""

        raise NotImplementedError

    def update_config(self, config):
        """Updates the config for logger."""

        pass

    def close(self):
        """Releases all resources used by this logger."""

        pass

    def flush(self):
        """Flushes all disk writes to storage."""

        pass


@PublicAPI
class NoopLogger(Logger):
    def on_result(self, result):
        pass


@PublicAPI
class JsonLogger(Logger):
    """Logs trial results in json format.

    Also writes to a results file and param.json file when results or
    configurations are updated. Experiments must be executed with the
    JsonLogger to be compatible with the ExperimentAnalysis tool.
    """

    def _init(self):
        self.update_config(self.config)
        local_file = os.path.join(self.logdir, EXPR_RESULT_FILE)
        self.local_out = open(local_file, "a")

    def on_result(self, result: Dict):
        json.dump(result, self, cls=SafeFallbackEncoder)
        self.write("\n")
        self.local_out.flush()

    def write(self, b):
        self.local_out.write(b)

    def flush(self):
        if not self.local_out.closed:
            self.local_out.flush()

    def close(self):
        self.local_out.close()

    def update_config(self, config: Dict):
        self.config = config
        config_out = os.path.join(self.logdir, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(self.config, f, indent=2, sort_keys=True, cls=SafeFallbackEncoder)
        config_pkl = os.path.join(self.logdir, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self.config, f)


@PublicAPI
class CSVLogger(Logger):
    """Logs results to progress.csv under the trial directory.

    Automatically flattens nested dicts in the result dict before writing
    to csv:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    """

    def _init(self):
        self._initialized = False

    def _maybe_init(self):
        """CSV outputted with Headers as first set of results."""
        if not self._initialized:
            progress_file = os.path.join(self.logdir, EXPR_PROGRESS_FILE)
            self._continuing = (
                os.path.exists(progress_file) and os.path.getsize(progress_file) > 0
            )
            self._file = open(progress_file, "a")
            self._csv_out = None
            self._initialized = True

    def on_result(self, result: Dict):
        self._maybe_init()

        tmp = result.copy()
        if "config" in tmp:
            del tmp["config"]
        result = flatten_dict(tmp, delimiter="/")
        if self._csv_out is None:
            self._csv_out = csv.DictWriter(self._file, result.keys())
            if not self._continuing:
                self._csv_out.writeheader()
        self._csv_out.writerow(
            {k: v for k, v in result.items() if k in self._csv_out.fieldnames}
        )
        self._file.flush()

    def flush(self):
        if self._initialized and not self._file.closed:
            self._file.flush()

    def close(self):
        if self._initialized:
            self._file.close()


@PublicAPI
class TBXLogger(Logger):
    """TensorBoardX Logger.

    Note that hparams will be written only after a trial has terminated.
    This logger automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}
    """

    VALID_HPARAMS = (str, bool, int, float, list, type(None))
    VALID_NP_HPARAMS = (np.bool8, np.float32, np.float64, np.int32, np.int64)

    def _init(self):
        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                logger.info('pip install "ray[tune]" to see TensorBoard files.')
            raise
        self._file_writer = SummaryWriter(self.logdir, flush_secs=30)
        self.last_result = None

    def on_result(self, result: Dict):
        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        tmp = result.copy()
        for k in ["config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION]:
            if k in tmp:
                del tmp[k]  # not useful to log these

        flat_result = flatten_dict(tmp, delimiter="/")
        path = ["ray", "tune"]
        valid_result = {}

        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if isinstance(value, tuple(VALID_SUMMARY_TYPES)) and not np.isnan(value):
                valid_result[full_attr] = value
                self._file_writer.add_scalar(full_attr, value, global_step=step)
            elif (isinstance(value, list) and len(value) > 0) or (
                isinstance(value, np.ndarray) and value.size > 0
            ):
                valid_result[full_attr] = value

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
                        logger.warning(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value, type(self).__name__)
                        )

        self.last_result = valid_result
        self._file_writer.flush()

    def flush(self):
        if self._file_writer is not None:
            self._file_writer.flush()

    def close(self):
        if self._file_writer is not None:
            if self.trial and self.trial.evaluated_params and self.last_result:
                flat_result = flatten_dict(self.last_result, delimiter="/")
                scrubbed_result = {
                    k: value
                    for k, value in flat_result.items()
                    if isinstance(value, tuple(VALID_SUMMARY_TYPES))
                }
                self._try_log_hparams(scrubbed_result)
            self._file_writer.close()

    def _try_log_hparams(self, result):
        # TBX currently errors if the hparams value is None.
        flat_params = flatten_dict(self.trial.evaluated_params)
        scrubbed_params = {
            k: v for k, v in flat_params.items() if isinstance(v, self.VALID_HPARAMS)
        }

        np_params = {
            k: v.tolist()
            for k, v in flat_params.items()
            if isinstance(v, self.VALID_NP_HPARAMS)
        }

        scrubbed_params.update(np_params)

        removed = {
            k: v
            for k, v in flat_params.items()
            if not isinstance(v, self.VALID_HPARAMS + self.VALID_NP_HPARAMS)
        }
        if removed:
            logger.info(
                "Removed the following hyperparameter values when "
                "logging to tensorboard: %s",
                str(removed),
            )

        from tensorboardX.summary import hparams

        try:
            experiment_tag, session_start_tag, session_end_tag = hparams(
                hparam_dict=scrubbed_params, metric_dict=result
            )
            self._file_writer.file_writer.add_summary(experiment_tag)
            self._file_writer.file_writer.add_summary(session_start_tag)
            self._file_writer.file_writer.add_summary(session_end_tag)
        except Exception:
            logger.exception(
                "TensorboardX failed to log hparams. "
                "This may be due to an unsupported type "
                "in the hyperparameter values."
            )


DEFAULT_LOGGERS = (JsonLogger, CSVLogger, TBXLogger)


@PublicAPI
class UnifiedLogger(Logger):
    """Unified result logger for TensorBoard, rllab/viskit, plain json.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        loggers: List of logger creators. Defaults to CSV, Tensorboard,
            and JSON loggers.
    """

    def __init__(
        self,
        config: Dict,
        logdir: str,
        trial: Optional["Trial"] = None,
        loggers: Optional[List[Type[Logger]]] = None,
    ):
        if loggers is None:
            self._logger_cls_list = DEFAULT_LOGGERS
        else:
            self._logger_cls_list = loggers
        if JsonLogger not in self._logger_cls_list:
            if log_once("JsonLogger"):
                logger.warning(
                    "JsonLogger not provided. The ExperimentAnalysis tool is "
                    "disabled."
                )

        super(UnifiedLogger, self).__init__(config, logdir, trial)

    def _init(self):
        self._loggers = []
        for cls in self._logger_cls_list:
            try:
                self._loggers.append(cls(self.config, self.logdir, self.trial))
            except Exception as exc:
                if log_once(f"instantiate:{cls.__name__}"):
                    logger.warning(
                        "Could not instantiate %s: %s.", cls.__name__, str(exc)
                    )

    def on_result(self, result):
        for _logger in self._loggers:
            _logger.on_result(result)

    def update_config(self, config):
        for _logger in self._loggers:
            _logger.update_config(config)

    def close(self):
        for _logger in self._loggers:
            _logger.close()

    def flush(self):
        for _logger in self._loggers:
            _logger.flush()


@PublicAPI
class LoggerCallback(Callback):
    """Base class for experiment-level logger callbacks

    This base class defines a general interface for logging events,
    like trial starts, restores, ends, checkpoint saves, and receiving
    trial results.

    Callbacks implementing this interface should make sure that logging
    utilities are cleaned up properly on trial termination, i.e. when
    ``log_trial_end`` is received. This includes e.g. closing files.
    """

    def log_trial_start(self, trial: "Trial"):
        """Handle logging when a trial starts.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_restore(self, trial: "Trial"):
        """Handle logging when a trial restores.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_save(self, trial: "Trial"):
        """Handle logging when a trial saves a checkpoint.

        Args:
            trial: Trial object.
        """
        pass

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        """Handle logging when a trial reports a result.

        Args:
            trial: Trial object.
            result: Result dictionary.
        """
        pass

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        """Handle logging when a trial ends.

        Args:
            trial: Trial object.
            failed: True if the Trial finished gracefully, False if
                it failed (e.g. when it raised an exception).
        """
        pass

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        self.log_trial_result(iteration, trial, result)

    def on_trial_start(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_start(trial)

    def on_trial_restore(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_restore(trial)

    def on_trial_save(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_save(trial)

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_end(trial, failed=False)

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_trial_end(trial, failed=True)


@DeveloperAPI
class LegacyLoggerCallback(LoggerCallback):
    """Supports logging to trial-specific `Logger` classes.

    Previously, Ray Tune logging was handled via `Logger` classes that have
    been instantiated per-trial. This callback is a fallback to these
    `Logger`-classes, instantiating each `Logger` class for each trial
    and logging to them.

    Args:
        logger_classes: Logger classes that should
            be instantiated for each trial.

    """

    def __init__(self, logger_classes: Iterable[Type[Logger]]):
        self.logger_classes = list(logger_classes)
        self._class_trial_loggers: Dict[Type[Logger], Dict["Trial", Logger]] = {}

    def log_trial_start(self, trial: "Trial"):
        trial.init_logdir()

        for logger_class in self.logger_classes:
            trial_loggers = self._class_trial_loggers.get(logger_class, {})
            if trial not in trial_loggers:
                logger = logger_class(trial.config, trial.logdir, trial)
                trial_loggers[trial] = logger
            self._class_trial_loggers[logger_class] = trial_loggers

    def log_trial_restore(self, trial: "Trial"):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].flush()

    def log_trial_save(self, trial: "Trial"):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].flush()

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].on_result(result)

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        for logger_class, trial_loggers in self._class_trial_loggers.items():
            if trial in trial_loggers:
                trial_loggers[trial].close()


@PublicAPI
class JsonLoggerCallback(LoggerCallback):
    """Logs trial results in json format.

    Also writes to a results file and param.json file when results or
    configurations are updated. Experiments must be executed with the
    JsonLoggerCallback to be compatible with the ExperimentAnalysis tool.
    """

    def __init__(self):
        self._trial_configs: Dict["Trial", Dict] = {}
        self._trial_files: Dict["Trial", TextIO] = {}

    def log_trial_start(self, trial: "Trial"):
        if trial in self._trial_files:
            self._trial_files[trial].close()

        # Update config
        self.update_config(trial, trial.config)

        # Make sure logdir exists
        trial.init_logdir()
        local_file = os.path.join(trial.logdir, EXPR_RESULT_FILE)
        self._trial_files[trial] = open(local_file, "at")

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_files:
            self.log_trial_start(trial)
        json.dump(result, self._trial_files[trial], cls=SafeFallbackEncoder)
        self._trial_files[trial].write("\n")
        self._trial_files[trial].flush()

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial not in self._trial_files:
            return

        self._trial_files[trial].close()
        del self._trial_files[trial]

    def update_config(self, trial: "Trial", config: Dict):
        self._trial_configs[trial] = config

        config_out = os.path.join(trial.logdir, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(
                self._trial_configs[trial],
                f,
                indent=2,
                sort_keys=True,
                cls=SafeFallbackEncoder,
            )

        config_pkl = os.path.join(trial.logdir, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self._trial_configs[trial], f)


@PublicAPI
class CSVLoggerCallback(LoggerCallback):
    """Logs results to progress.csv under the trial directory.

    Automatically flattens nested dicts in the result dict before writing
    to csv:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    """

    def __init__(self):
        self._trial_continue: Dict["Trial", bool] = {}
        self._trial_files: Dict["Trial", TextIO] = {}
        self._trial_csv: Dict["Trial", csv.DictWriter] = {}

    def _setup_trial(self, trial: "Trial"):
        if trial in self._trial_files:
            self._trial_files[trial].close()

        # Make sure logdir exists
        trial.init_logdir()
        local_file = os.path.join(trial.logdir, EXPR_PROGRESS_FILE)
        self._trial_continue[trial] = (
            os.path.exists(local_file) and os.path.getsize(local_file) > 0
        )
        self._trial_files[trial] = open(local_file, "at")
        self._trial_csv[trial] = None

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_files:
            self._setup_trial(trial)

        tmp = result.copy()
        tmp.pop("config", None)
        result = flatten_dict(tmp, delimiter="/")

        if not self._trial_csv[trial]:
            self._trial_csv[trial] = csv.DictWriter(
                self._trial_files[trial], result.keys()
            )
            if not self._trial_continue[trial]:
                self._trial_csv[trial].writeheader()

        self._trial_csv[trial].writerow(
            {k: v for k, v in result.items() if k in self._trial_csv[trial].fieldnames}
        )
        self._trial_files[trial].flush()

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial not in self._trial_files:
            return

        del self._trial_csv[trial]
        self._trial_files[trial].close()
        del self._trial_files[trial]


@PublicAPI
class TBXLoggerCallback(LoggerCallback):
    """TensorBoardX Logger.

    Note that hparams will be written only after a trial has terminated.
    This logger automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}
    """

    VALID_HPARAMS = (str, bool, int, float, list, type(None))
    VALID_NP_HPARAMS = (np.bool8, np.float32, np.float64, np.int32, np.int64)

    def __init__(self):
        try:
            from tensorboardX import SummaryWriter

            self._summary_writer_cls = SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                logger.info('pip install "ray[tune]" to see TensorBoard files.')
            raise
        self._trial_writer: Dict["Trial", SummaryWriter] = {}
        self._trial_result: Dict["Trial", Dict] = {}

    def log_trial_start(self, trial: "Trial"):
        if trial in self._trial_writer:
            self._trial_writer[trial].close()
        trial.init_logdir()
        self._trial_writer[trial] = self._summary_writer_cls(
            trial.logdir, flush_secs=30
        )
        self._trial_result[trial] = {}

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_writer:
            self.log_trial_start(trial)

        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        tmp = result.copy()
        for k in ["config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION]:
            if k in tmp:
                del tmp[k]  # not useful to log these

        flat_result = flatten_dict(tmp, delimiter="/")
        path = ["ray", "tune"]
        valid_result = {}

        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if isinstance(value, tuple(VALID_SUMMARY_TYPES)) and not np.isnan(value):
                valid_result[full_attr] = value
                self._trial_writer[trial].add_scalar(full_attr, value, global_step=step)
            elif (isinstance(value, list) and len(value) > 0) or (
                isinstance(value, np.ndarray) and value.size > 0
            ):
                valid_result[full_attr] = value

                # Must be video
                if isinstance(value, np.ndarray) and value.ndim == 5:
                    self._trial_writer[trial].add_video(
                        full_attr, value, global_step=step, fps=20
                    )
                    continue

                try:
                    self._trial_writer[trial].add_histogram(
                        full_attr, value, global_step=step
                    )
                # In case TensorboardX still doesn't think it's a valid value
                # (e.g. `[[]]`), warn and move on.
                except (ValueError, TypeError):
                    if log_once("invalid_tbx_value"):
                        logger.warning(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value, type(self).__name__)
                        )

        self._trial_result[trial] = valid_result
        self._trial_writer[trial].flush()

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial in self._trial_writer:
            if trial and trial.evaluated_params and self._trial_result[trial]:
                flat_result = flatten_dict(self._trial_result[trial], delimiter="/")
                scrubbed_result = {
                    k: value
                    for k, value in flat_result.items()
                    if isinstance(value, tuple(VALID_SUMMARY_TYPES))
                }
                self._try_log_hparams(trial, scrubbed_result)
            self._trial_writer[trial].close()
            del self._trial_writer[trial]
            del self._trial_result[trial]

    def _try_log_hparams(self, trial: "Trial", result: Dict):
        # TBX currently errors if the hparams value is None.
        flat_params = flatten_dict(trial.evaluated_params)
        scrubbed_params = {
            k: v for k, v in flat_params.items() if isinstance(v, self.VALID_HPARAMS)
        }

        np_params = {
            k: v.tolist()
            for k, v in flat_params.items()
            if isinstance(v, self.VALID_NP_HPARAMS)
        }

        scrubbed_params.update(np_params)

        removed = {
            k: v
            for k, v in flat_params.items()
            if not isinstance(v, self.VALID_HPARAMS + self.VALID_NP_HPARAMS)
        }
        if removed:
            logger.info(
                "Removed the following hyperparameter values when "
                "logging to tensorboard: %s",
                str(removed),
            )

        from tensorboardX.summary import hparams

        try:
            experiment_tag, session_start_tag, session_end_tag = hparams(
                hparam_dict=scrubbed_params, metric_dict=result
            )
            self._trial_writer[trial].file_writer.add_summary(experiment_tag)
            self._trial_writer[trial].file_writer.add_summary(session_start_tag)
            self._trial_writer[trial].file_writer.add_summary(session_end_tag)
        except Exception:
            logger.exception(
                "TensorboardX failed to log hparams. "
                "This may be due to an unsupported type "
                "in the hyperparameter values."
            )


def pretty_print(result):
    result = result.copy()
    result.update(config=None)  # drop config from pretty print
    result.update(hist_stats=None)  # drop hist_stats from pretty print
    out = {}
    for k, v in result.items():
        if v is not None:
            out[k] = v

    cleaned = json.dumps(out, cls=SafeFallbackEncoder)
    return yaml.safe_dump(json.loads(cleaned), default_flow_style=False)
