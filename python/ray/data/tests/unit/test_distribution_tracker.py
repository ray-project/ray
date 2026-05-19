import numpy as np
import pytest

try:
    import datasketches
except ImportError:
    datasketches = None

from ray.data._internal.execution.interfaces.distribution_tracker import (
    DistributionTracker,
)


def test_empty_tracker_has_zero_moments_and_no_extremes():
    tracker = DistributionTracker()

    assert tracker.num_samples == 0
    assert tracker.mean == 0.0
    assert tracker.variance == 0.0
    assert tracker.min is None
    assert tracker.max is None


def test_moments_match_numpy_after_adding_samples():
    tracker = DistributionTracker()
    samples = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    for s in samples:
        tracker.add_sample(s)

    assert tracker.num_samples == len(samples)
    assert pytest.approx(tracker.mean) == np.mean(samples)
    assert pytest.approx(tracker.variance) == np.var(samples, ddof=1)
    assert pytest.approx(tracker.stddev) == np.std(samples, ddof=1)


def test_extremes_track_min_and_max():
    tracker = DistributionTracker()
    samples = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    for s in samples:
        tracker.add_sample(s)

    assert tracker.min == 2.0
    assert tracker.max == 9.0


def test_as_dict_contains_all_fields():
    tracker = DistributionTracker()
    tracker.add_sample(1.0)

    d = tracker.as_dict()
    assert set(d.keys()) == {
        "num_samples",
        "mean",
        "variance",
        "min",
        "max",
        "p50",
        "p90",
        "p95",
        "p99",
    }


@pytest.mark.skipif(datasketches is None, reason="datasketches not installed")
def test_percentiles_approximate_expected_quantiles():
    tracker = DistributionTracker()
    for i in range(1, 101):
        tracker.add_sample(float(i))

    assert tracker.p50 is not None and 45 <= tracker.p50 <= 55
    assert tracker.p90 is not None and 85 <= tracker.p90 <= 95
    assert tracker.p99 is not None and 95 <= tracker.p99 <= 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
