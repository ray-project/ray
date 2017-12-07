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

    def __enter__(self):
        assert self._start_time is None, "concurrent updates not supported"
        self._start_time = time.time()

    def __exit__(self, type, value, tb):
        assert self._start_time is not None
        self.push(time.time() - self._start_time)
        self._start_time = None
