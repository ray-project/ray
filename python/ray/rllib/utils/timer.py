from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import numpy as np


import time
import logging, sys
import datetime as dt

class MyFormatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)[:-3]
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)[:-3]
        return s

def setup_custom_logger(name):
    handler = logging.FileHandler('{fn}.txt'.format(fn=name), mode='w')
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)

    formatter = MyFormatter(fmt='[%(asctime)s %(filename)s] %(levelname)-8s %(message)s',datefmt='%Y-%m-%d,%H:%M:%S.%f')
    clean_formatter = MyFormatter(fmt='%(message)s')
    handler.setFormatter(clean_formatter)
    screen_handler.setFormatter(formatter)
    return logger

class TimerStat(object):
    """A running stat for conveniently logging the duration of a code block.

    Example:
        wait_timer = TimerStat()
        with wait_timer:
            ray.wait(...)

    Note that this class is *not* thread-safe.
    """

    def __init__(self, window_size=10):
        self._window_size = window_size
        self._samples = []
        self._units_processed = []
        self._start_time = None
        self._total_time = 0.0
        self.count = 0

    def __enter__(self):
        assert self._start_time is None, "concurrent updates not supported"
        self._start_time = time.time()

    def __exit__(self, type, value, tb):
        assert self._start_time is not None
        time_delta = time.time() - self._start_time
        self.push(time_delta)
        self._start_time = None

    def push(self, time_delta):
        self._samples.append(time_delta)
        if len(self._samples) > self._window_size:
            self._samples.pop(0)
        self.count += 1
        self._total_time += time_delta

    def push_units_processed(self, n):
        self._units_processed.append(n)
        if len(self._units_processed) > self._window_size:
            self._units_processed.pop(0)

    @property
    def mean(self):
        return np.mean(self._samples)

    @property
    def mean_units_processed(self):
        return float(np.mean(self._units_processed))

    @property
    def mean_throughput(self):
        time_total = sum(self._samples)
        if not time_total:
            return 0.0
        return sum(self._units_processed) / time_total
