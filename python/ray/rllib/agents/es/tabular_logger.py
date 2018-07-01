# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import os
import sys
import time

import tensorflow as tf
from tensorflow.core.util import event_pb2
from tensorflow.python import pywrap_tensorflow
from tensorflow.python.util import compat

DEBUG = 10
INFO = 20
WARN = 30
ERROR = 40

DISABLED = 50


class TbWriter(object):
    """Based on SummaryWriter, but changed to allow for a different prefix."""
    def __init__(self, dir, prefix):
        self.dir = dir
        # Start at 1, because EvWriter automatically generates an object with
        # step = 0.
        self.step = 1
        self.evwriter = pywrap_tensorflow.EventsWriter(
            compat.as_bytes(os.path.join(dir, prefix)))

    def write_values(self, key2val):
        summary = tf.Summary(value=[tf.Summary.Value(tag=k,
                                                     simple_value=float(v))
                                    for (k, v) in key2val.items()])
        event = event_pb2.Event(wall_time=time.time(), summary=summary)
        event.step = self.step
        self.evwriter.WriteEvent(event)
        self.evwriter.Flush()
        self.step += 1

    def close(self):
        self.evwriter.Close()

# API


def start(dir):
    if _Logger.CURRENT is not _Logger.DEFAULT:
        sys.stderr.write("WARNING: You asked to start logging (dir=%s), but "
                         "you never stopped the previous logger (dir=%s)."
                         "\n" % (dir, _Logger.CURRENT.dir))
    _Logger.CURRENT = _Logger(dir=dir)


def stop():
    if _Logger.CURRENT is _Logger.DEFAULT:
        sys.stderr.write("WARNING: You asked to stop logging, but you never "
                         "started any previous logger."
                         "\n" % (dir, _Logger.CURRENT.dir))
        return
    _Logger.CURRENT.close()
    _Logger.CURRENT = _Logger.DEFAULT


def record_tabular(key, val):
    """Log a value of some diagnostic.

    Call this once for each diagnostic quantity, each iteration.
    """
    _Logger.CURRENT.record_tabular(key, val)


def dump_tabular():
    """Write all of the diagnostics from the current iteration."""
    _Logger.CURRENT.dump_tabular()


def log(*args, **kwargs):
    """Write the sequence of args, with no separators.

    This is written to the console and output files (if you've configured an
    output file).
    """
    level = kwargs['level'] if 'level' in kwargs else INFO
    _Logger.CURRENT.log(*args, level=level)


def debug(*args):
    log(*args, level=DEBUG)


def info(*args):
    log(*args, level=INFO)


def warn(*args):
    log(*args, level=WARN)


def error(*args):
    log(*args, level=ERROR)


def set_level(level):
    """
    Set logging threshold on current logger.
    """
    _Logger.CURRENT.set_level(level)


def get_dir():
    """
    Get directory that log files are being written to.
    will be None if there is no output directory (i.e., if you didn't call
    start)
    """
    return _Logger.CURRENT.get_dir()


def get_expt_dir():
    sys.stderr.write("get_expt_dir() is Deprecated. Switch to get_dir()\n")
    return get_dir()

# Backend


class _Logger(object):
    # A logger with no output files. (See right below class definition) so that
    # you can still log to the terminal without setting up any output files.
    DEFAULT = None
    # Current logger being used by the free functions above.
    CURRENT = None

    def __init__(self, dir=None):
        self.name2val = OrderedDict()  # Values this iteration.
        self.level = INFO
        self.dir = dir
        self.text_outputs = [sys.stdout]
        if dir is not None:
            os.makedirs(dir, exist_ok=True)
            self.text_outputs.append(open(os.path.join(dir, "log.txt"), "w"))
            self.tbwriter = TbWriter(dir=dir, prefix="events")
        else:
            self.tbwriter = None

    # Logging API, forwarded

    def record_tabular(self, key, val):
        self.name2val[key] = val

    def dump_tabular(self):
        # Create strings for printing.
        key2str = OrderedDict()
        for (key, val) in self.name2val.items():
            if hasattr(val, "__float__"):
                valstr = "%-8.3g" % val
            else:
                valstr = val
            key2str[self._truncate(key)] = self._truncate(valstr)
        keywidth = max(map(len, key2str.keys()))
        valwidth = max(map(len, key2str.values()))
        # Write to all text outputs
        self._write_text("-" * (keywidth + valwidth + 7), "\n")
        for (key, val) in key2str.items():
            self._write_text("| ", key, " " * (keywidth - len(key)),
                             " | ", val, " " * (valwidth - len(val)), " |\n")
        self._write_text("-" * (keywidth + valwidth + 7), "\n")
        for f in self.text_outputs:
            try:
                f.flush()
            except OSError:
                sys.stderr.write('Warning! OSError when flushing.\n')
        # Write to tensorboard
        if self.tbwriter is not None:
            self.tbwriter.write_values(self.name2val)
            self.name2val.clear()

    def log(self, *args, **kwargs):
        level = kwargs['level'] if 'level' in kwargs else INFO
        if self.level <= level:
            self._do_log(*args)

    # Configuration

    def set_level(self, level):
        self.level = level

    def get_dir(self):
        return self.dir

    def close(self):
        for f in self.text_outputs[1:]:
            f.close()
        if self.tbwriter:
            self.tbwriter.close()

    # Misc

    def _do_log(self, *args):
        self._write_text(*args + ('\n',))
        for f in self.text_outputs:
            try:
                f.flush()
            except OSError:
                print('Warning! OSError when flushing.')

    def _write_text(self, *strings):
        for f in self.text_outputs:
            for string in strings:
                f.write(string)

    def _truncate(self, s):
        if len(s) > 33:
            return s[:30] + "..."
        else:
            return s


_Logger.DEFAULT = _Logger()
_Logger.CURRENT = _Logger.DEFAULT
