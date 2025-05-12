import pytest
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.test_utils import check


def test_reduce_per_index_on_merge():
    stats_default = Stats(reduce="mean", window=3, reduce_per_index_on_merge=False)
    stats1 = Stats(reduce="mean", window=3)
    stats1.push(10)
    stats1.push(20)
    stats1.push(30)

    stats2 = Stats(reduce="mean", window=3)
    stats2.push(100)
    stats2.push(200)
    stats2.push(300)

    stats_default.merge_in_parallel(stats1, stats2)
    # Default behavior repeats the mean at each index:
    # Last index: mean([30, 300]) = 165 -> [165, 165]
    # Second to last: mean([20, 200]) = 110 -> [165, 165, 110, 110]
    # Take only last 3: [110, 165, 165]
    check(stats_default.values, [110, 165, 165])
    check(stats_default.peek(), (110 + 165 + 165) / 3)

    stats_per_index = Stats(reduce="mean", window=3, reduce_per_index_on_merge=True)
    stats1 = Stats(reduce="mean", window=3)
    stats1.push(10)
    stats1.push(20)
    stats1.push(30)

    stats2 = Stats(reduce="mean", window=3)
    stats2.push(100)
    stats2.push(200)
    stats2.push(300)

    stats_per_index.merge_in_parallel(stats1, stats2)
    # Per-index behavior:
    # Last index: mean([30, 300]) = 165 -> [165]
    # Second to last: mean([20, 200]) = 110 -> [165, 110]
    # First index: mean([10, 100]) = 55 -> [165, 110, 55]
    # Reversed: [55, 110, 165]
    check(stats_per_index.values, [55, 110, 165])
    check(stats_per_index.peek(), (55 + 110 + 165) / 3)


def test_percentiles():
    """Test that the percentiles reduce method works correctly."""
    # Test basic functionality with single stats
    stats = Stats(reduce="percentiles", window=5)
    stats.push(5)
    stats.push(2)
    stats.push(8)
    stats.push(1)
    stats.push(9)

    # Values should be sorted when peeking
    check(stats.peek(), [1, 2, 5, 8, 9])

    # Test with window constraint
    stats.push(3)

    # Window is 5, so the oldest value (5) should be dropped
    check(stats.peek(), [1, 2, 3, 8, 9])

    # Test reduce
    reduced_stats = stats.reduce()
    check(reduced_stats.values, [1, 2, 3, 8, 9])

    # Test merge_in_parallel
    stats1 = Stats(reduce="percentiles", window=10)
    stats1.push(10)
    stats1.push(30)
    stats1.push(20)
    check(stats1.peek(), [10, 20, 30])
    stats1.reduce()
    check(stats1.values, [10, 20, 30])

    stats2 = Stats(reduce="percentiles", window=10)
    stats2.push(15)
    stats2.push(5)
    stats2.push(25)
    check(stats2.peek(), [5, 15, 25])
    stats2.reduce()
    check(stats2.values, [5, 15, 25])

    merged_stats = Stats(reduce="percentiles", window=10)
    merged_stats.merge_in_parallel(stats1, stats2)
    # Should merge and sort values from both stats
    # Merged values should be sorted, as incoming values are sorted
    check(merged_stats.values, [5, 10, 15, 20, 25, 30])
    check(merged_stats.peek(), [5, 10, 15, 20, 25, 30])

    # Test validation - window required
    with pytest.raises(ValueError, match="A window must be specified"):
        Stats(reduce="percentiles", window=None)

    # Test validation - percentiles must be a list
    with pytest.raises(ValueError, match="must be a list"):
        Stats(reduce="percentiles", window=5, percentiles=0.5)

    # Test validation - percentiles must contain numbers
    with pytest.raises(ValueError, match="must contain only ints or floats"):
        Stats(reduce="percentiles", window=5, percentiles=["invalid"])

    # Test validation - percentiles must be between 0 and 100
    with pytest.raises(ValueError, match="must contain only values between 0 and 100"):
        Stats(reduce="percentiles", window=5, percentiles=[-1, 50, 101])

    # Test validation - percentiles must be None for other reduce methods
    with pytest.raises(
        ValueError, match="must be None when `reduce` is not 'percentiles'"
    ):
        Stats(reduce="mean", window=5, percentiles=[50])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
