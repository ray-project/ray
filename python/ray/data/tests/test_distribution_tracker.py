import numpy as np
import pytest

try:
    import datasketches
except ImportError:
    datasketches = None

from ray.data._internal.execution.interfaces.distribution_tracker import (
    DistributionTracker,
)


def test_moments_and_extremes():
    tracker = DistributionTracker()

    assert tracker.num_samples == 0
    assert tracker.mean == 0.0
    assert tracker.variance == 0.0
    assert tracker.min is None
    assert tracker.max is None

    samples = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    for s in samples:
        tracker.add_sample(s)

    assert tracker.num_samples == len(samples)
    assert pytest.approx(tracker.mean) == np.mean(samples)
    assert pytest.approx(tracker.variance) == np.var(samples, ddof=1)
    assert pytest.approx(tracker.stddev) == np.std(samples, ddof=1)
    assert tracker.min == 2.0
    assert tracker.max == 9.0

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
def test_approximate_percentiles():
    tracker = DistributionTracker()
    for i in range(1, 101):
        tracker.add_sample(float(i))

    assert 45 <= tracker.p50 <= 55
    assert 85 <= tracker.p90 <= 95
    assert 95 <= tracker.p99 <= 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
