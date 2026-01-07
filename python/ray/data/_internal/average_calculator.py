import time
from collections import deque
from typing import Deque, Tuple


class TimeWindowAverageCalculator:
    """A utility class to calculate the average of values reported in a time window."""

    def __init__(
        self,
        window_s: float,
    ):
        assert window_s > 0
        # Time window in seconds.
        self._window_s = window_s
        # Buffer the values reported in the time window, each value is a
        # tuple of (time, value).
        self._values: Deque[Tuple[float, float]] = deque()
        # Sum of all values in the time window.
        self._sum: float = 0

    def report(self, value: float):
        """Report a value to the calculator."""
        now = time.time()
        self._values.append((now, value))
        self._sum += value
        self._trim(now)

    def get_average(self):
        """Get the average of values reported in the time window,
        or None if no values reported in the last time window.
        """
        self._trim(time.time())
        if len(self._values) == 0:
            return None
        return self._sum / len(self._values)

    def _trim(self, now):
        """Remove the values reported outside of the time window."""
        while len(self._values) > 0 and now - self._values[0][0] > self._window_s:
            _, value = self._values.popleft()
            self._sum -= value
