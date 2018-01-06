from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib.utils.filter import RunningStat


class TimerStat(RunningStat):
    """A running stat for conveniently logging the duration of a code block.

    Example:
        wait_timer = TimeStat()
        with wait_timer:
            ray.wait(...)

    Note that this class is *not* thread-safe.
    """

    def __init__(self):
        RunningStat.__init__(self, ())
        self._start_time = None
        self._total_time = 0.0
        self._units_processed = RunningStat(())

    def __enter__(self):
        assert self._start_time is None, "concurrent updates not supported"
        self._start_time = time.time()

    def __exit__(self, type, value, tb):
        assert self._start_time is not None
        time_delta = time.time() - self._start_time
        self.push(time_delta)
        self._total_time += time_delta
        self._start_time = None

    def push_units_processed(self, n):
        self._units_processed.push(n)

    @property
    def mean_throughput(self):
        return float(self._units_processed.mean / self.mean)

    @property
    def mean_units_processed(self):
        return float(self._units_processed.mean)
