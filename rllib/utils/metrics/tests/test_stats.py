import numpy as np
import pytest
import time
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.test_utils import check

# Default values used throughout the tests
DEFAULT_EMA_COEFF = 0.01
DEFAULT_THROUGHPUT_EMA_COEFF = 0.05
DEFAULT_CLEAR_ON_REDUCE = False
DEFAULT_THROUGHPUT = False


@pytest.fixture
def basic_stats():
    return Stats(
        init_values=None,
        reduce="mean",
        ema_coeff=DEFAULT_EMA_COEFF,
        window=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )


@pytest.mark.parametrize(
    "init_values,expected_len,expected_peek",
    [(1.0, 1, 1.0), (None, 0, np.nan), ([1, 2, 3], 3, 2)],
)
def test_init_with_values(init_values, expected_len, expected_peek):
    """Test initialization with different initial values."""
    stats = Stats(
        init_values=init_values,
        reduce="mean",
        ema_coeff=None,
        window=3,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    check(len(stats), expected_len)
    if expected_len > 0:
        check(stats.peek(), expected_peek)
        check(stats.peek(compile=True), [expected_peek])
    else:
        check(np.isnan(stats.peek()), True)


def test_invalid_init_params():
    """Test initialization with invalid parameters."""
    # Invalid reduce method
    with pytest.raises(ValueError):
        Stats(
            init_values=None,
            reduce="invalid",
            window=None,
            ema_coeff=DEFAULT_EMA_COEFF,
            clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
            throughput=DEFAULT_THROUGHPUT,
            throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
        )

    # Cannot have both window and ema_coeff
    with pytest.raises(ValueError):
        Stats(
            init_values=None,
            window=3,
            ema_coeff=0.1,
            reduce="mean",
            clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
            throughput=DEFAULT_THROUGHPUT,
            throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
        )

    # Cannot have ema_coeff with non-mean reduction
    with pytest.raises(ValueError):
        Stats(
            init_values=None,
            reduce="sum",
            ema_coeff=0.1,
            window=None,
            clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
            throughput=DEFAULT_THROUGHPUT,
            throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
        )


def test_push_with_ema():
    """Test pushing values with EMA reduction."""
    stats = Stats(
        init_values=None,
        reduce="mean",
        ema_coeff=DEFAULT_EMA_COEFF,
        window=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1.0)
    stats.push(2.0)

    # EMA formula: new_val = (1.0 - ema_coeff) * old_val + ema_coeff * val
    expected = 1.0 * (1.0 - DEFAULT_EMA_COEFF) + 2.0 * DEFAULT_EMA_COEFF
    check(abs(stats.peek() - expected) < 1e-6, True)


def test_window():
    window_size = 3
    stats = Stats(
        init_values=None,
        window=window_size,
        reduce="mean",
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    # Push values and check window behavior
    for i in range(1, 5):  # Push values 1, 2, 3, 4
        stats.push(i)

        # Check that the window size is respected
        expected_window_size = min(i, window_size)
        check(len(stats.values), expected_window_size)

        # Check that the window contains the most recent values
        if i <= window_size:
            expected_values = list(range(1, i + 1))
        else:
            expected_values = list(range(i - window_size + 1, i + 1))

        check(list(stats.peek(compile=False)), expected_values)

    # After pushing 4 values with window size 3, we should have [2, 3, 4]
    # and the mean should be (2 + 3 + 4) / 3 = 3
    check(stats.peek(), 3)

    # Test reduce behavior
    reduced_value = stats.reduce()
    check(reduced_value, 3)


@pytest.mark.parametrize(
    "reduce_method,values,expected",
    [
        ("sum", [1, 2, 3], 6),
        ("min", [10, 20, 5, 100], 5),
        ("max", [1, 3, 2, 4], 4),
    ],
)
def test_reduce_methods(reduce_method, values, expected):
    """Test different reduce methods."""
    stats = Stats(
        init_values=None,
        reduce=reduce_method,
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    for val in values:
        stats.push(val)
    check(stats.peek(), expected)


def test_reduce_with_clear():
    """Test reduce with clear_on_reduce=True."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=True,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1)
    stats.push(2)

    reduced_value = stats.reduce()
    check(reduced_value, 3)
    check(len(stats), 0)  # Stats should be cleared


def test_merge_on_time_axis():
    """Test merging stats on time axis."""
    stats1 = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats1.push(1)
    stats1.push(2)

    stats2 = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats2.push(3)
    stats2.push(4)

    stats1.merge_on_time_axis(stats2)
    check(stats1.peek(), 10)  # sum of [1, 2, 3, 4]


def test_merge_in_parallel():
    """Test merging stats in parallel."""
    window_size = 3

    stats1 = Stats(
        init_values=None,
        reduce="mean",
        window=window_size,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    for i in range(1, 4):  # [1, 2, 3]
        stats1.push(i)

    stats2 = Stats(
        init_values=None,
        reduce="mean",
        window=window_size,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    for i in range(4, 7):  # [4, 5, 6]
        stats2.push(i)

    result = Stats(
        init_values=None,
        reduce="mean",
        window=window_size,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    result.merge_in_parallel(stats1, stats2)

    check(abs(result.peek() - 4.167) < 1e-3, True)


@pytest.mark.parametrize(
    "op,expected",
    [
        (lambda s: float(s), 2.0),
        (lambda s: int(s), 2),
        (lambda s: s + 1, 3.0),
        (lambda s: s - 1, 1.0),
        (lambda s: s * 2, 4.0),
        (lambda s: s == 2.0, True),
        (lambda s: s <= 3.0, True),
        (lambda s: s >= 1.0, True),
        (lambda s: s < 3.0, True),
        (lambda s: s > 1.0, True),
    ],
)
def test_numeric_operations(op, expected):
    """Test numeric operations on Stats objects."""
    stats = Stats(
        init_values=None,
        reduce="mean",
        ema_coeff=DEFAULT_EMA_COEFF,
        window=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(2.0)

    check(op(stats), expected)


def test_state_serialization():
    """Test saving and loading Stats state."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=3,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    for i in range(1, 4):
        stats.push(i)

    state = stats.get_state()
    loaded_stats = Stats.from_state(state)

    check(loaded_stats._reduce_method, stats._reduce_method)
    check(loaded_stats._window, stats._window)
    check(loaded_stats.peek(), stats.peek())
    check(len(loaded_stats), len(stats))


def test_similar_to():
    """Test creating similar Stats objects."""
    original = Stats(
        init_values=None,
        reduce="sum",
        window=3,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    original.push(1)
    original.push(2)

    original.reduce()

    # Similar stats without initial values
    similar = Stats.similar_to(original)
    check(similar._reduce_method, original._reduce_method)
    check(similar._window, original._window)
    check(len(similar), 0)  # Should start empty

    # Similar stats with initial values
    similar_with_value = Stats.similar_to(original, init_values=[3, 4])
    check(len(similar_with_value), 2)
    check(similar_with_value.peek(), 7)

    # Test that adding to the similar stats does not affect the original stats
    similar.push(10)
    check(original.peek(), 3)
    check(original.get_reduce_history(), [[np.nan], [np.nan], [3]])


def test_reduce_history():
    """Test basic reduce history functionality."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )

    # Initially history should contain NaN values
    check(stats.get_reduce_history(), [[np.nan], [np.nan], [np.nan]])

    # Push values and reduce
    stats.push(1)
    stats.push(2)
    check(stats.reduce(), 3)
    check(stats.get_reduce_history(), [[np.nan], [np.nan], [3]])

    # Push more values and reduce
    stats.push(3)
    stats.push(4)
    check(stats.reduce(), 10)
    check(stats.get_reduce_history(), [[np.nan], [3], [10]])


def test_reduce_history_with_clear():
    """Test reduce history with clear_on_reduce=True."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=True,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )

    # Push and reduce multiple times
    stats.push(1)
    stats.push(2)
    check(stats.reduce(), 3)
    check(stats.get_reduce_history(), [[np.nan], [np.nan], [3]])
    check(len(stats), 0)  # Values should be cleared

    stats.push(3)
    stats.push(4)
    check(stats.reduce(), 7)
    check(stats.get_reduce_history(), [[np.nan], [3], [7]])
    check(len(stats), 0)


def test_basic_throughput():
    """Test basic throughput tracking."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=True,
        throughput_ema_coeff=None,
    )

    # First push - throughput should be 0 initially
    stats.push(1)
    check(stats.peek(), 1)
    check(stats.throughput, np.nan)

    # Wait and push again to measure throughput
    time.sleep(0.1)
    stats.push(1)
    check(stats.peek(), 2)
    check(stats.throughput, 10, rtol=0.1)

    # Wait and push again to measure throughput
    time.sleep(0.1)
    stats.push(2)
    check(stats.peek(), 4)
    check(
        stats.throughput, 10.1, rtol=0.1
    )  # default EMA coefficient for throughput is 0.01


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
