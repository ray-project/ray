import csv
import json
import logging
import numpy as np
import os
import yaml

from typing import TYPE_CHECKING, Dict, List, Optional, Type

import ray.cloudpickle as cloudpickle

from ray.tune.utils.util import SafeFallbackEncoder
from ray.util.debug import log_once
from ray.tune.result import (TRAINING_ITERATION, TIME_TOTAL_S, TIMESTEPS_TOTAL,
                             EXPR_PARAM_FILE, EXPR_PARAM_PICKLE_FILE,
                             EXPR_PROGRESS_FILE, EXPR_RESULT_FILE)
from ray.tune.utils import flatten_dict

if TYPE_CHECKING:
    from ray.tune.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)

tf = None
VALID_SUMMARY_TYPES = [int, float, np.float32, np.float64, np.int32, np.int64]


class Logger:
    """Logging interface for ray.tune.

    By default, the UnifiedLogger implementation is used which logs results in
    multiple formats (TensorBoard, rllab/viskit, plain json, custom loggers)
    at once.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        trial (Trial): Trial object for the logger to access.
    """

    def __init__(self,
                 config: Dict,
                 logdir: str,
                 trial: Optional["Trial"] = None):
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


class NoopLogger(Logger):
    def on_result(self, result):
        pass


class MLFLowLogger(Logger):
    """MLFlow logger.

    Requires the experiment configuration to have a MLFlow Experiment ID
    or manually set the proper environment variables.

    """

    def _init(self):
        logger_config = self.config.get("logger_config", {})
        from mlflow.tracking import MlflowClient
        client = MlflowClient(
            tracking_uri=logger_config.get("mlflow_tracking_uri"),
            registry_uri=logger_config.get("mlflow_registry_uri"))
        run = client.create_run(logger_config.get("mlflow_experiment_id"))
        self._run_id = run.info.run_id
        for key, value in self.config.items():
            client.log_param(self._run_id, key, value)
        self.client = client

    def on_result(self, result: Dict):
        for key, value in result.items():
            if not isinstance(value, float):
                continue
            self.client.log_metric(
                self._run_id, key, value, step=result.get(TRAINING_ITERATION))

    def close(self):
        self.client.set_terminated(self._run_id)


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
        self.local_out.flush()

    def close(self):
        self.local_out.close()

    def update_config(self, config: Dict):
        self.config = config
        config_out = os.path.join(self.logdir, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(
                self.config,
                f,
                indent=2,
                sort_keys=True,
                cls=SafeFallbackEncoder)
        config_pkl = os.path.join(self.logdir, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self.config, f)


class CSVLogger(Logger):
    """Logs results to progress.csv under the trial directory.

    Automatically flattens nested dicts in the result dict before writing
    to csv:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    """

    def _init(self):
        """CSV outputted with Headers as first set of results."""
        progress_file = os.path.join(self.logdir, EXPR_PROGRESS_FILE)
        self._continuing = os.path.exists(progress_file)
        self._file = open(progress_file, "a")
        self._csv_out = None

    def on_result(self, result: Dict):
        tmp = result.copy()
        if "config" in tmp:
            del tmp["config"]
        result = flatten_dict(tmp, delimiter="/")
        if self._csv_out is None:
            self._csv_out = csv.DictWriter(self._file, result.keys())
            if not self._continuing:
                self._csv_out.writeheader()
        self._csv_out.writerow(
            {k: v
             for k, v in result.items() if k in self._csv_out.fieldnames})
        self._file.flush()

    def flush(self):
        self._file.flush()

    def close(self):
        self._file.close()


class TBXLogger(Logger):
    """TensorBoardX Logger.

    Note that hparams will be written only after a trial has terminated.
    This logger automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}
    """

    # NoneType is not supported on the last TBX release yet.
    VALID_HPARAMS = (str, bool, np.bool8, int, np.integer, float, list)

    def _init(self):
        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                logger.info(
                    "pip install 'ray[tune]' to see TensorBoard files.")
            raise
        self._file_writer = SummaryWriter(self.logdir, flush_secs=30)
        self.last_result = None

    def on_result(self, result: Dict):
        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        tmp = result.copy()
        for k in [
                "config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION
        ]:
            if k in tmp:
                del tmp[k]  # not useful to log these

        flat_result = flatten_dict(tmp, delimiter="/")
        path = ["ray", "tune"]
        valid_result = {}

        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if (isinstance(value, tuple(VALID_SUMMARY_TYPES))
                    and not np.isnan(value)):
                valid_result[full_attr] = value
                self._file_writer.add_scalar(
                    full_attr, value, global_step=step)
            elif ((isinstance(value, list) and len(value) > 0)
                  or (isinstance(value, np.ndarray) and value.size > 0)):
                valid_result[full_attr] = value

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
                        logger.warning(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value,
                                             type(self).__name__))

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
            k: v
            for k, v in flat_params.items()
            if isinstance(v, self.VALID_HPARAMS)
        }

        removed = {
            k: v
            for k, v in flat_params.items()
            if not isinstance(v, self.VALID_HPARAMS)
        }
        if removed:
            logger.info(
                "Removed the following hyperparameter values when "
                "logging to tensorboard: %s", str(removed))

        from tensorboardX.summary import hparams
        try:
            experiment_tag, session_start_tag, session_end_tag = hparams(
                hparam_dict=scrubbed_params, metric_dict=result)
            self._file_writer.file_writer.add_summary(experiment_tag)
            self._file_writer.file_writer.add_summary(session_start_tag)
            self._file_writer.file_writer.add_summary(session_end_tag)
        except Exception:
            logger.exception("TensorboardX failed to log hparams. "
                             "This may be due to an unsupported type "
                             "in the hyperparameter values.")


DEFAULT_LOGGERS = (JsonLogger, CSVLogger, TBXLogger)


class UnifiedLogger(Logger):
    """Unified result logger for TensorBoard, rllab/viskit, plain json.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        loggers (list): List of logger creators. Defaults to CSV, Tensorboard,
            and JSON loggers.
    """

    def __init__(self,
                 config: Dict,
                 logdir: str,
                 trial: Optional["Trial"] = None,
                 loggers: Optional[List[Type[Logger]]] = None):
        if loggers is None:
            self._logger_cls_list = DEFAULT_LOGGERS
        else:
            self._logger_cls_list = loggers
        if JsonLogger not in self._logger_cls_list:
            if log_once("JsonLogger"):
                logger.warning(
                    "JsonLogger not provided. The ExperimentAnalysis tool is "
                    "disabled.")

        super(UnifiedLogger, self).__init__(config, logdir, trial)

    def _init(self):
        self._loggers = []
        for cls in self._logger_cls_list:
            try:
                self._loggers.append(cls(self.config, self.logdir, self.trial))
            except Exception as exc:
                if log_once(f"instantiate:{cls.__name__}"):
                    logger.warning("Could not instantiate %s: %s.",
                                   cls.__name__, str(exc))

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
