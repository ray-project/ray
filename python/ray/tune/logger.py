from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import csv
import json
import logging
import os
import yaml
import distutils.version
import numbers

import numpy as np

import ray.cloudpickle as cloudpickle
from ray.tune.util import flatten_dict
from ray.tune.syncer import get_log_syncer
from ray.tune.result import (NODE_IP, TRAINING_ITERATION, TIME_TOTAL_S,
                             TIMESTEPS_TOTAL, EXPR_PARAM_FILE,
                             EXPR_PARAM_PICKLE_FILE, EXPR_PROGRESS_FILE,
                             EXPR_RESULT_FILE)

logger = logging.getLogger(__name__)

tf = None
VALID_SUMMARY_TYPES = [int, float, np.float32, np.float64, np.int32]


class Logger(object):
    """Logging interface for ray.tune.

    By default, the UnifiedLogger implementation is used which logs results in
    multiple formats (TensorBoard, rllab/viskit, plain json, custom loggers)
    at once.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
    """

    def __init__(self, config, logdir, trial=None):
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
        from mlflow.tracking import MlflowClient
        client = MlflowClient()
        run = client.create_run(self.config.get("mlflow_experiment_id"))
        self._run_id = run.info.run_id
        for key, value in self.config.items():
            client.log_param(self._run_id, key, value)
        self.client = client

    def on_result(self, result):
        for key, value in result.items():
            if not isinstance(value, float):
                continue
            self.client.log_metric(
                self._run_id, key, value, step=result.get(TRAINING_ITERATION))

    def close(self):
        self.client.set_terminated(self._run_id)


class JsonLogger(Logger):
    def _init(self):
        self.update_config(self.config)
        local_file = os.path.join(self.logdir, EXPR_RESULT_FILE)
        self.local_out = open(local_file, "a")

    def on_result(self, result):
        json.dump(result, self, cls=_SafeFallbackEncoder)
        self.write("\n")
        self.local_out.flush()

    def write(self, b):
        self.local_out.write(b)

    def flush(self):
        self.local_out.flush()

    def close(self):
        self.local_out.close()

    def update_config(self, config):
        self.config = config
        config_out = os.path.join(self.logdir, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(
                self.config,
                f,
                indent=2,
                sort_keys=True,
                cls=_SafeFallbackEncoder)
        config_pkl = os.path.join(self.logdir, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self.config, f)


def tf2_compat_logger(config, logdir, trial=None):
    """Chooses TensorBoard logger depending on imported TF version."""
    global tf
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow for test purposes")
        tf = None
        raise RuntimeError("Not importing TensorFlow for test purposes")
    else:
        import tensorflow as tf
        use_tf2_api = (distutils.version.LooseVersion(tf.__version__) >=
                       distutils.version.LooseVersion("2.0.0"))
        if use_tf2_api:
            tf = tf.compat.v2  # setting this for TF2.0
            return TF2Logger(config, logdir, trial)
        else:
            return TFLogger(config, logdir, trial)


class TF2Logger(Logger):
    """TensorBoard Logger for TF version >= 2.0.0.

    Automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    If you need to do more advanced logging, it is recommended
    to use a Summary Writer in the Trainable yourself.
    """

    def _init(self):
        self._file_writer = None
        self._hp_logged = False

    def on_result(self, result):
        if self._file_writer is None:
            from tensorflow.python.eager import context
            from tensorboard.plugins.hparams import api as hp
            self._context = context
            self._file_writer = tf.summary.create_file_writer(self.logdir)
        with tf.device("/CPU:0"):
            with tf.summary.record_if(True), self._file_writer.as_default():
                step = result.get(
                    TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

                tmp = result.copy()
                if not self._hp_logged:
                    if self.trial and self.trial.evaluated_params:
                        try:
                            hp.hparams(
                                self.trial.evaluated_params,
                                trial_id=self.trial.trial_id)
                        except Exception as exc:
                            logger.error("HParams failed with %s", exc)
                    self._hp_logged = True

                for k in [
                        "config", "pid", "timestamp", TIME_TOTAL_S,
                        TRAINING_ITERATION
                ]:
                    if k in tmp:
                        del tmp[k]  # not useful to log these

                flat_result = flatten_dict(tmp, delimiter="/")
                path = ["ray", "tune"]
                for attr, value in flat_result.items():
                    if type(value) in VALID_SUMMARY_TYPES:
                        tf.summary.scalar(
                            "/".join(path + [attr]), value, step=step)
        self._file_writer.flush()

    def flush(self):
        if self._file_writer is not None:
            self._file_writer.flush()

    def close(self):
        if self._file_writer is not None:
            self._file_writer.close()


def to_tf_values(result, path):
    flat_result = flatten_dict(result, delimiter="/")
    values = [
        tf.Summary.Value(tag="/".join(path + [attr]), simple_value=value)
        for attr, value in flat_result.items()
        if type(value) in VALID_SUMMARY_TYPES
    ]
    return values


class TFLogger(Logger):
    """TensorBoard Logger for TF version < 2.0.0.

    Automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    If you need to do more advanced logging, it is recommended
    to use a Summary Writer in the Trainable yourself.
    """

    def _init(self):
        logger.debug("Initializing TFLogger instead of TF2Logger.")
        self._file_writer = tf.compat.v1.summary.FileWriter(self.logdir)

    def on_result(self, result):
        tmp = result.copy()
        for k in [
                "config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION
        ]:
            if k in tmp:
                del tmp[k]  # not useful to tf log these
        values = to_tf_values(tmp, ["ray", "tune"])
        train_stats = tf.Summary(value=values)
        t = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]
        self._file_writer.add_summary(train_stats, t)
        iteration_value = to_tf_values({
            TRAINING_ITERATION: result[TRAINING_ITERATION]
        }, ["ray", "tune"])
        iteration_stats = tf.Summary(value=iteration_value)
        self._file_writer.add_summary(iteration_stats, t)
        self._file_writer.flush()

    def flush(self):
        self._file_writer.flush()

    def close(self):
        self._file_writer.close()


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

    def on_result(self, result):
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


DEFAULT_LOGGERS = (JsonLogger, CSVLogger, tf2_compat_logger)


class UnifiedLogger(Logger):
    """Unified result logger for TensorBoard, rllab/viskit, plain json.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        loggers (list): List of logger creators. Defaults to CSV, Tensorboard,
            and JSON loggers.
        sync_function (func|str): Optional function for syncer to run.
            See ray/python/ray/tune/log_sync.py
    """

    def __init__(self,
                 config,
                 logdir,
                 trial=None,
                 loggers=None,
                 sync_function=None):
        if loggers is None:
            self._logger_cls_list = DEFAULT_LOGGERS
        else:
            self._logger_cls_list = loggers
        self._sync_function = sync_function
        self._log_syncer = None

        super(UnifiedLogger, self).__init__(config, logdir, trial)

    def _init(self):
        self._loggers = []
        for cls in self._logger_cls_list:
            try:
                self._loggers.append(cls(self.config, self.logdir, self.trial))
            except Exception as exc:
                logger.warning("Could not instantiate {}: {}.".format(
                    cls.__name__, str(exc)))
        self._log_syncer = get_log_syncer(
            self.logdir,
            remote_dir=self.logdir,
            sync_function=self._sync_function)

    def on_result(self, result):
        for _logger in self._loggers:
            _logger.on_result(result)
        self._log_syncer.set_worker_ip(result.get(NODE_IP))
        self._log_syncer.sync_down_if_needed()

    def update_config(self, config):
        for _logger in self._loggers:
            _logger.update_config(config)

    def close(self):
        for _logger in self._loggers:
            _logger.close()
        self._log_syncer.sync_down()

    def flush(self):
        for _logger in self._loggers:
            _logger.flush()
        self._log_syncer.sync_down()

    def sync_results_to_new_location(self, worker_ip):
        """Sends the current log directory to the remote node.

        Syncing will not occur if the cluster is not started
        with the Ray autoscaler.
        """
        if worker_ip != self._log_syncer.worker_ip:
            logger.info("Syncing (blocking) results to {}".format(worker_ip))
            self._log_syncer.reset()
            self._log_syncer.set_worker_ip(worker_ip)
            self._log_syncer.sync_up()
            # TODO: change this because this is blocking. But failures
            # are rare, so maybe this is OK?
            self._log_syncer.wait()


class _SafeFallbackEncoder(json.JSONEncoder):
    def __init__(self, nan_str="null", **kwargs):
        super(_SafeFallbackEncoder, self).__init__(**kwargs)
        self.nan_str = nan_str

    def default(self, value):
        try:
            if np.isnan(value):
                return self.nan_str

            if (type(value).__module__ == np.__name__
                    and isinstance(value, np.ndarray)):
                return value.tolist()

            if issubclass(type(value), numbers.Integral):
                return int(value)
            if issubclass(type(value), numbers.Number):
                return float(value)

            return super(_SafeFallbackEncoder, self).default(value)

        except Exception:
            return str(value)  # give up, just stringify it (ok for logs)


def pretty_print(result):
    result = result.copy()
    result.update(config=None)  # drop config from pretty print
    out = {}
    for k, v in result.items():
        if v is not None:
            out[k] = v

    cleaned = json.dumps(out, cls=_SafeFallbackEncoder)
    return yaml.safe_dump(json.loads(cleaned), default_flow_style=False)
