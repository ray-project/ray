"""Window bookkeeping for cumulative GPU activity metrics.

Each successful read credits activity x seconds since the previous successful
read. Failed reads leave the window open; invalid values restart it.
"""

import math
import time
from typing import Callable, Optional


class WindowAccountant:
    """Tracks the window between two successful reads of one source."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._window_start: Optional[float] = None

    def window_seconds(self) -> Optional[float]:
        """Seconds since the window opened, or None if no window is open."""
        if self._window_start is None:
            return None
        return max(0.0, self._clock() - self._window_start)

    def credit(self, percent: Optional[float]) -> Optional[float]:
        """Return seconds of activity for the window ending now, or None if none."""
        now = self._clock()
        if percent is None or math.isnan(percent):
            # The window can't be trusted; start over from now.
            self._window_start = now
            return None
        if self._window_start is None:
            self._window_start = now
            return None
        span = max(0.0, now - self._window_start)
        self._window_start = now
        return min(max(percent, 0.0), 100.0) / 100.0 * span

    def fail(self) -> None:
        """Record a failed read: nothing is credited and the window stays open."""
