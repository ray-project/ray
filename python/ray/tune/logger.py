from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import csv
import json
import numpy as np
import os
import sys
import tensorflow as tf

from ray.tune.result import TrainingResult

if sys.version_info[0] == 2:
    import cStringIO as StringIO
elif sys.version_info[0] == 3:
    import io as StringIO


class Logger(object):
    """Logging interface for ray.tune; specialized implementations follow.

    By default, the UnifiedLogger implementation is used which logs results in
    multiple formats (TensorBoard, rllab/viskit, plain json) at once.
    """

    _attrs_to_log = [
        "time_this_iter_s", "mean_loss", "mean_accuracy",
        "episode_reward_mean", "episode_len_mean"]

    def __init__(self, config, logdir, upload_uri=None):
        self.config = config
        self.logdir = logdir
        self.uri = upload_uri
        self._init()

    def _init(self):
        pass

    def on_result(self, result):
        """Given a result, appends it to the existing log."""

        raise NotImplementedError

    def close(self):
        """Releases all resources used by this logger."""

        pass


class UnifiedLogger(Logger):
    """Unified result logger for TensorBoard, rllab/viskit, plain json."""

    def _init(self):
        self._loggers = []
        for cls in [_JsonLogger, _TFLogger, _VisKitLogger]:
            self._loggers.append(cls(self.config, self.logdir, self.uri))
        print("Unified logger created with logdir '{}'".format(self.logdir))

    def on_result(self, result):
        for logger in self._loggers:
            logger.on_result(result)

    def close(self):
        for logger in self._loggers:
            logger.close()


class NoopLogger(Logger):
    def on_result(self, result):
        pass


class _JsonLogger(Logger):
    def _init(self):
        config_out = os.path.join(self.logdir, "params.json")
        with open(config_out, "w") as f:
            json.dump(self.config, f, sort_keys=True, cls=_CustomEncoder)
        local_file = os.path.join(self.logdir, "result.json")
        self.local_out = open(local_file, "w")
        if self.uri:
            self.result_buffer = StringIO.StringIO()
            import smart_open
            self.smart_open = smart_open.smart_open

    def on_result(self, result):
        json.dump(result._asdict(), self, cls=_CustomEncoder)
        self.write("\n")

    def write(self, b):
        self.local_out.write(b)
        self.local_out.flush()
        # TODO(pcm): At the moment we are writing the whole results output from
        # the beginning in each iteration. This will write O(n^2) bytes where n
        # is the number of bytes printed so far. Fix this! This should at least
        # only write the last 5MBs (S3 chunksize).
        if self.uri:
            with self.smart_open(self.uri, "w") as f:
                self.result_buffer.write(b)
                f.write(self.result_buffer.getvalue())

    def close(self):
        self.local_out.close()


class _TFLogger(Logger):
    def _init(self):
        self._file_writer = tf.summary.FileWriter(self.logdir)

    def on_result(self, result):
        values = []
        for attr in Logger._attrs_to_log:
            if getattr(result, attr) is not None:
                values.append(tf.Summary.Value(
                    tag="ray/tune/{}".format(attr),
                    simple_value=getattr(result, attr)))
        train_stats = tf.Summary(value=values)
        self._file_writer.add_summary(train_stats, result.training_iteration)

    def close(self):
        self._file_writer.close()


class _VisKitLogger(Logger):
    def _init(self):
        # Note that we assume params.json was already created by JsonLogger
        self._file = open(os.path.join(self.logdir, "progress.csv"), "w")
        self._csv_out = csv.DictWriter(self._file, TrainingResult._fields)
        self._csv_out.writeheader()

    def on_result(self, result):
        self._csv_out.writerow(result._asdict())

    def close(self):
        self._file.close()


class _CustomEncoder(json.JSONEncoder):
    def __init__(self, nan_str="null", **kwargs):
        super(_CustomEncoder, self).__init__(**kwargs)
        self.nan_str = nan_str

    def iterencode(self, o, _one_shot=False):
        if self.ensure_ascii:
            _encoder = json.encoder.encode_basestring_ascii
        else:
            _encoder = json.encoder.encode_basestring

        def floatstr(o, allow_nan=self.allow_nan, nan_str=self.nan_str):
            return repr(o) if not np.isnan(o) else nan_str

        _iterencode = json.encoder._make_iterencode(
                None, self.default, _encoder, self.indent, floatstr,
                self.key_separator, self.item_separator, self.sort_keys,
                self.skipkeys, _one_shot)
        return _iterencode(o, 0)

    def default(self, value):
        if np.isnan(value):
            return None
        if np.issubdtype(value, float):
            return float(value)
        if np.issubdtype(value, int):
            return int(value)
