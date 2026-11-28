"""Windowed signals the sizer and optimizer decide on.

Both decide on windows, not instants: a pool qualifies as idle, saturated or
a donor only if the signal held for a full window, so one noisy sample never
moves actors around.
"""

from collections import deque
from typing import Deque, Optional


class WindowedSignal:
    """A fixed-length window of samples; callers decide when to record.

    Queries return None until the window has filled once, so nothing is
    decided on partial history.
    """

    def __init__(self, window_length: int):
        assert window_length > 0
        self._window_length = window_length
        self._samples: Deque[float] = deque(maxlen=window_length)

    def record(self, value: float) -> None:
        self._samples.append(value)

    def covered(self) -> bool:
        return len(self._samples) == self._window_length

    def peak(self) -> Optional[float]:
        return max(self._samples) if self.covered() else None

    def minimum(self) -> Optional[float]:
        return min(self._samples) if self.covered() else None


class BoolStreak:
    """Fixed-window streak: ``is_streak()`` is True iff every record in the
    window is True and the window has filled."""

    def __init__(self, window_length: int):
        assert window_length > 0
        self._window_length = window_length
        # Consecutive trailing True records, saturated at window_length.
        self._run = 0

    def record(self, value: bool) -> None:
        self._run = min(self._run + 1, self._window_length) if value else 0

    def is_streak(self) -> bool:
        return self._run >= self._window_length

    @property
    def run_length(self) -> int:
        """Consecutive trailing True records (saturated at the window length)."""
        return self._run

    def reset(self) -> None:
        self._run = 0
