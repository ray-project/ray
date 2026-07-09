import warnings

import numpy as np
import pytest

from ray.rllib.utils.metrics.window_stat import WindowStat


def test_empty_window_stat_does_not_warn():
    """An empty WindowStat must not emit numpy empty-slice RuntimeWarnings.

    Regression test for issue #45659: `mean()`/`std()` used to call
    `np.nanmean`/`np.nanstd` on an empty slice when no items had been pushed
    yet, emitting "Mean of empty slice" and "Degrees of freedom <= 0"
    RuntimeWarnings (e.g. when an env step is slow enough that a reporting
    interval elapses before the first sample arrives).
    """
    win_stats = WindowStat("level", 3)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        # Neither of these may raise a RuntimeWarning on an empty window.
        assert np.isnan(win_stats.mean())
        assert np.isnan(win_stats.std())
        # `stats()` calls `mean()` and `std()` internally.
        stats = win_stats.stats()

    assert stats["level_count"] == 0


def test_window_stat_after_push():
    """Sanity check that stats are still correct once items are pushed."""
    win_stats = WindowStat("level", 3)
    for value in (5.0, 7.0, 7.0, 10.0):
        win_stats.push(value)

    # Mean of the last 3 items: (7 + 7 + 10) / 3 == 8.0.
    assert win_stats.mean() == pytest.approx(8.0)
    assert win_stats.std() >= 0.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
