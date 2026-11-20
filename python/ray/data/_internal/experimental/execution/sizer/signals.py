"""Streaks the sizer decides on: a pool qualifies as idle only if it held
for a full window, so one noisy sample never moves actors around."""


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
