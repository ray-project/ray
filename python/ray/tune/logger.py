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
use_tf150_api = True


class Logger(object):
    """Logging interface for ray.tune.

    By default, the UnifiedLogger implementation is used which logs results in
    multiple formats (TensorBoard, rllab/viskit, plain json, custom loggers)
    at once.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
    """

    def __init__(self, config, logdir):
        self.config = config
        self.logdir = logdir
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


def to_tf_values(result, path):
    if use_tf150_api:
        type_list = [int, float, np.float32, np.float64, np.int32]
    else:
        type_list = [int, float]
    flat_result = flatten_dict(result, delimiter="/")
    values = [
        tf.Summary.Value(tag="/".join(path + [attr]), simple_value=value)
        for attr, value in flat_result.items() if type(value) in type_list
    ]
    return values


class TFLogger(Logger):
    def _init(self):
        try:
            global tf, use_tf150_api
            if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
                logger.warning("Not importing TensorFlow for test purposes")
                tf = None
            else:
                import tensorflow
                tf = tensorflow
                use_tf150_api = (distutils.version.LooseVersion(tf.VERSION) >=
                                 distutils.version.LooseVersion("1.5.0"))
        except ImportError:
            logger.warning("Couldn't import TensorFlow - "
                           "disabling TensorBoard logging.")
        self._file_writer = tf.summary.FileWriter(self.logdir)

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
    def _init(self):
        """CSV outputted with Headers as first set of results."""
        # Note that we assume params.json was already created by JsonLogger
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


DEFAULT_LOGGERS = (JsonLogger, CSVLogger, TFLogger)


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

    def __init__(self, config, logdir, loggers=None, sync_function=None):
        if loggers is None:
            self._logger_cls_list = DEFAULT_LOGGERS
        else:
            self._logger_cls_list = loggers
        self._sync_function = sync_function
        self._log_syncer = None

        super(UnifiedLogger, self).__init__(config, logdir)

    def _init(self):
        self._loggers = []
        for cls in self._logger_cls_list:
            try:
                self._loggers.append(cls(self.config, self.logdir))
            except Exception:
                logger.warning("Could not instantiate {} - skipping.".format(
                    str(cls)))
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
