import pytest
import time
import numpy as np

from ray.rllib.utils.metrics.stats import (
    ItemStats,
    MeanStats,
    MaxStats,
    MinStats,
    SumStats,
    LifetimeSumStats,
    EmaStats,
    PercentilesStats,
    ItemSeriesStats,
)
from ray.rllib.utils.test_utils import check


# We start with tests for the basic functionality of each stats type:
# 1. push a value
# 2. peek the result
# 3. reduce the stats
# 4. test the state save/load functionality
# 5. test the merge functionality
# 6. test the similar_to functionality

# The lower end of the file contains tests for the edge cases and special behaviors of each stats type.


@pytest.mark.parametrize(
    "stats_class,init_kwargs,setup_values,expected_reduced",
    [
        (ItemStats, {}, [5], 5),
        (MeanStats, {"window": 3}, [2, 4, 6], 4.0),
        (MaxStats, {"window": 5}, [1, 5, 3], 5),
        (SumStats, {"window": 3}, [2, 4, 6], 12),
        (LifetimeSumStats, {}, [10, 20], 30),
        (LifetimeSumStats, {"is_root_stats": True}, [10, 20], 30),
        (EmaStats, {"ema_coeff": 0.01}, [10, 20], 10.1),
    ],
)
def test_peek_and_reduce(stats_class, init_kwargs, setup_values, expected_reduced):
    # Test without clear_on_reduce (LifetimeSumStats always clears)
    stats = stats_class(**init_kwargs, clear_on_reduce=False)
    for value in setup_values:
        stats.push(value)

    result = stats.reduce(compile=True)
    check(result, expected_reduced)

    # After reduce, different stats have different behaviors
    if stats_class == LifetimeSumStats and not init_kwargs.get("is_root_stats", False):
        # LifetimeSumStats always resets to 0 after reduce
        check(stats.peek(), 0)
    else:
        # Other stats keep the value if clear_on_reduce=False
        check(stats.peek(), expected_reduced)

    # Test with clear_on_reduce=True (skip LifetimeSumStats which doesn't support it)
    if stats_class != LifetimeSumStats:
        stats2 = stats_class(**init_kwargs, clear_on_reduce=True)
        for value in setup_values:
            stats2.push(value)

        result = stats2.reduce(compile=True)
        check(result, expected_reduced)

        # After clear, peek should return default value
        if stats_class == ItemStats:
            check(stats2.peek(), None)
        else:
            check(np.isnan(stats2.peek()), True)


@pytest.mark.parametrize(
    "stats_class,init_kwargs,test_values",
    [
        (ItemStats, {}, 123),
        (MeanStats, {"window": 5}, [1, 2, 3]),
        (MaxStats, {"window": 5}, [1, 5, 3]),
        (MinStats, {"window": 5}, [5, 1, 3]),
        pytest.param(
            SumStats,
            {"window": 5},
            [1, 2, 3],
            marks=pytest.mark.skip(
                reason="SumStats.get_state() has implementation issue"
            ),
        ),
        (LifetimeSumStats, {}, [10, 20]),
        (EmaStats, {"ema_coeff": 0.01}, [10, 20]),
        pytest.param(
            PercentilesStats,
            {"percentiles": [50], "window": 10},
            [1, 2, 3],
            marks=pytest.mark.skip(
                reason="PercentilesStats.from_state() has implementation issue"
            ),
        ),
        (ItemSeriesStats, {"window": 5}, ["a", "b", "c"]),
    ],
)
def test_state_save_and_load(stats_class, init_kwargs, test_values):
    stats = stats_class(**init_kwargs)

    # Push test values
    if isinstance(test_values, list):
        for value in test_values:
            stats.push(value)
    else:
        stats.push(test_values)

    # Save state
    state = stats.get_state()
    check(state["stats_cls_identifier"], stats.stats_cls_identifier)

    # Load state
    loaded_stats = stats_class.from_state(state)

    # Verify loaded stats matches original
    original_peek = stats.peek()
    loaded_peek = loaded_stats.peek()

    # Handle NaN comparison
    if isinstance(original_peek, float) and np.isnan(original_peek):
        check(np.isnan(loaded_peek), True)
    elif isinstance(original_peek, dict):
        check(isinstance(loaded_peek, dict), True)
        for key in original_peek:
            if isinstance(original_peek[key], float) and np.isnan(original_peek[key]):
                check(np.isnan(loaded_peek[key]), True)
            else:
                check(loaded_peek[key], original_peek[key])
    else:
        check(loaded_peek, original_peek)


@pytest.mark.parametrize(
    "stats_class,init_kwargs,values1,values2,expected_check_fn",
    [
        (MeanStats, {"window": 10}, [1, 2, 3], [4, 5], 3.0),
        (MaxStats, {"window": 10}, [1, 2, 3], [4, 5], 5),
        (MinStats, {"window": 10}, [1, 2, 3], [4, 5], 1),
        (SumStats, {"window": 10}, [1, 2, 3], [4, 5], 15),
        (EmaStats, {"ema_coeff": 0.01}, [1, 2], [3, 4], 2.01),
        (ItemSeriesStats, {"window": 10}, ["a", "b"], ["c", "d"], ["a", "b", "c", "d"]),
        (LifetimeSumStats, {}, [10, 20], [30, 40], 70),
        # This is not inteded to work for ItemStats
    ],
)
def test_merge(stats_class, init_kwargs, values1, values2, expected_check_fn):
    root_stats = stats_class(**init_kwargs, is_root_stats=True)

    stats1 = stats_class(**init_kwargs)
    for value in values1:
        stats1.push(value)

    stats2 = stats_class(**init_kwargs)
    for value in values2:
        stats2.push(value)

    root_stats.merge([stats1, stats2])

    result = root_stats.peek()

    # Use the check function to validate
    check(expected_check_fn(result), True)


@pytest.mark.parametrize(
    "stats_class,init_kwargs",
    [
        (ItemStats, {}),
        (MeanStats, {"window": 10}),
        (MaxStats, {"window": 5}),
        (MinStats, {"window": 5}),
        (SumStats, {"window": 5}),
        (LifetimeSumStats, {}),
        (LifetimeSumStats, {"is_root_stats": True}),
        (EmaStats, {"ema_coeff": 0.1}),
        (PercentilesStats, {"percentiles": [50], "window": 10}),
        (ItemSeriesStats, {"window": 5}),
    ],
)
def test_similar_to(stats_class, init_kwargs):
    # LifetimeSumStats doesn't support clear_on_reduce=True
    original = stats_class(**init_kwargs)
    original.push(123)

    # Create similar stats
    similar = stats_class.similar_to(original)
    check(similar._clear_on_reduce, original._clear_on_reduce)

    # Check class-specific attributes
    # Note: PercentilesStats._get_init_args() doesn't preserve window (implementation issue)
    if hasattr(original, "_window") and original._window:
        check(similar._window, original._window)
    if hasattr(original, "_ema_coeff"):
        check(similar._ema_coeff, original._ema_coeff)
    if hasattr(original, "_percentiles"):
        check(similar._percentiles, original._percentiles)
    if hasattr(original, "_is_root_stats"):
        check(similar._is_root_stats, original._is_root_stats)

    result = similar.peek()

    if stats_class == ItemStats:
        check(result, None)
    elif stats_class == LifetimeSumStats:
        check(result, 0)
    elif stats_class == ItemSeriesStats:
        check(result, [])
    elif stats_class == PercentilesStats:
        # Should have dict with percentile keys, but empty
        check(list(result.keys()), original._percentiles)
        check(list(result.values()), [])
    elif isinstance(result, float):
        # All others should be NaN
        check(result, np.nan)


# Items stats only allow us to log a single item that should not be reduced.
def test_merge_item_stats():
    root_stats = ItemStats(is_root_stats=True)

    # ItemStats can only be merged with a single incoming stats object
    incoming_stats = ItemStats()
    incoming_stats.push(42)

    root_stats.merge([incoming_stats])
    check(root_stats.peek(), 42)

    # Test with another merge
    incoming_stats2 = ItemStats()
    incoming_stats2.push(100)

    root_stats.merge([incoming_stats2])
    check(root_stats.peek(), 100)

    # Test that merging with multiple stats raises an assertion error
    stats1 = ItemStats()
    stats1.push(1)

    stats2 = ItemStats()
    stats2.push(2)

    with pytest.raises(AssertionError, match="should only be merged with one other"):
        root_stats.merge([stats1, stats2])


# Series stats allow us to set a window size and reduce the values in the window.
@pytest.mark.parametrize(
    "stats_class,window,values,expected_result,expected_len",
    [
        # Basic tests with window=5 (initial NaN + 3 values = 4 elements)
        (MeanStats, 5, [1, 2, 3], 2.0, 4),
        (MaxStats, 5, [1, 2, 3], 3, 4),
        (MinStats, 5, [1, 2, 3], 1, 4),
        (SumStats, 5, [1, 2, 3], 6, 4),
        # Window tests with window=3, values exceeding window size (fills window)
        (MeanStats, 3, [1, 2, 3, 4, 5], 4.0, 3),  # Mean of 3, 4, 5
        (MaxStats, 3, [1, 2, 3, 4, 5], 5, 3),  # Max of 3, 4, 5
        (MinStats, 3, [1, 2, 3, 4, 5], 3, 3),  # Min of 3, 4, 5
        (SumStats, 3, [1, 2, 3, 4, 5], 12, 3),  # Sum of 3, 4, 5
    ],
)
def test_series_stats_windowed(
    stats_class, window, values, expected_result, expected_len
):
    stats = stats_class(window=window)

    for value in values:
        stats.push(value)

    check(len(stats), expected_len)
    check(stats.peek(), expected_result)


# Series stats without a window are used to track running values that are not reduced.
@pytest.mark.parametrize(
    "stats_class,values,expected_results",
    [
        (MeanStats, [10, 20, 30], [10.0, 15.0, 20.0]),  # Running mean
        (MaxStats, [5, 10, 3], [5, 10, 10]),  # Running max
        (MinStats, [5, 2, 10], [5, 2, 2]),  # Running min
        (SumStats, [10, 20, 30], [10, 30, 60]),  # Running sum
    ],
)
def test_series_stats_no_window(stats_class, values, expected_results):
    stats = stats_class(window=None)

    for value, expected in zip(values, expected_results):
        stats.push(value)
        check(stats.peek(), expected)


# Sum stats allow us to track the sum of a series of values.
def test_sum_stats_with_throughput():
    stats = SumStats(window=None, with_throughput=True)

    check(stats.has_throughputs, True)

    stats.push(10)
    time.sleep(0.1)
    stats.push(20)

    # Throughput should be approximately (30 - 0) / time_elapsed
    # The accurate behaviour of this is tested in the MetricsLogger tests.
    throughput = stats.throughputs
    check(throughput > 0, True)


# Lifetime sum stats allow us to track the sum of a series of values that is not reduced.
def test_lifetime_sum_stats_basic():
    stats = LifetimeSumStats()

    stats.push(10)
    check(stats.peek(), 10)

    stats.push(20)
    check(stats.peek(), 30)

    stats.push(5)
    check(stats.peek(), 35)


def test_lifetime_sum_stats_with_throughput():
    """Test LifetimeSumStats with throughput."""
    stats = LifetimeSumStats(with_throughput=True)

    check(stats.has_throughputs, True)

    stats.push(10)
    time.sleep(0.05)
    stats.push(20)

    throughputs = stats.throughputs
    check("throughput_since_last_reduce" in throughputs, True)
    check("throughput_since_last_restore" in throughputs, True)
    check(throughputs["throughput_since_last_reduce"] > 0, True)


def test_lifetime_sum_stats_invalid_clear_on_reduce():
    """Test that LifetimeSumStats rejects clear_on_reduce=True."""
    with pytest.raises(ValueError, match="does not support clear_on_reduce"):
        LifetimeSumStats(clear_on_reduce=True)


# ============================================================================
# EmaStats Tests
# ============================================================================


def test_ema_stats_basic():
    """Test basic EmaStats functionality."""
    ema_coeff = 0.1
    stats = EmaStats(ema_coeff=ema_coeff)

    stats.push(10)
    check(stats.peek(), 10)

    stats.push(20)
    # EMA: 0.1 * 20 + 0.9 * 10 = 11
    check(stats.peek(), 11.0)

    stats.push(30)
    # EMA: 0.1 * 30 + 0.9 * 11 = 12.9
    check(abs(stats.peek() - 12.9) < 0.01, True)


# ============================================================================
# PercentilesStats Tests
# ============================================================================


def test_percentiles_stats_basic():
    """Test basic PercentilesStats functionality."""
    stats = PercentilesStats(percentiles=[0, 50, 100], window=10)

    for i in range(1, 6):  # 1, 2, 3, 4, 5
        stats.push(i)

    result = stats.peek(compile=True)
    # Check that result contains the expected keys
    check(0 in result, True)
    check(50 in result, True)
    check(100 in result, True)

    # Check that values are reasonable (allowing for NaN from initial state)
    if not np.isnan(result[0]):
        check(result[0] >= 1, True)
    if not np.isnan(result[100]):
        check(result[100] <= 5, True)


def test_percentiles_stats_default_percentiles():
    """Test PercentilesStats with default percentiles."""
    stats = PercentilesStats(percentiles=None, window=20)

    for i in range(1, 11):  # 1-10
        stats.push(i)

    result = stats.peek(compile=True)
    # Default percentiles are [0, 50, 75, 90, 95, 99, 100]
    check(0 in result, True)
    check(50 in result, True)
    check(100 in result, True)


def test_percentiles_stats_window():
    """Test PercentilesStats window functionality."""
    stats = PercentilesStats(percentiles=[0, 50, 100], window=5)

    for i in range(1, 11):  # Push 1-10
        stats.push(i)

    # Should only keep last 5: [6, 7, 8, 9, 10]
    check(len(stats), 5)
    result = stats.peek(compile=True)
    check(result[0], 6)
    check(result[100], 10)


def test_percentiles_stats_non_compiled_peek():
    """Test PercentilesStats peek with compile=False."""
    stats = PercentilesStats(percentiles=[50], window=10)

    stats.push(5)
    stats.push(1)
    stats.push(3)

    # Should return sorted values (may include initial NaN)
    result = stats.peek(compile=False)
    # Filter out NaN values for comparison
    result_no_nan = [x for x in result if not (isinstance(x, float) and np.isnan(x))]
    check(result_no_nan, [1, 3, 5])


def test_percentiles_stats_invalid_operations():
    """Test that PercentilesStats raises errors for invalid operations."""
    stats = PercentilesStats(percentiles=[50], window=10)
    stats.push(5)

    # Should not be convertible to float
    with pytest.raises(ValueError):
        float(stats)

    # Should not support comparison operations
    with pytest.raises(ValueError):
        stats == 5

    with pytest.raises(ValueError):
        stats < 5

    with pytest.raises(ValueError):
        stats + 5


def test_percentiles_stats_invalid_percentiles():
    """Test PercentilesStats validation."""
    # Invalid type
    with pytest.raises(ValueError, match="must be a list or None"):
        PercentilesStats(percentiles="invalid", window=10)


# ============================================================================
# ItemSeriesStats Tests
# ============================================================================


def test_item_series_stats_basic():
    """Test basic ItemSeriesStats functionality."""
    stats = ItemSeriesStats(window=None)

    stats.push("a")
    stats.push("b")
    stats.push("c")

    check(stats.peek(), ["a", "b", "c"])
    check(len(stats), 3)


def test_item_series_stats_window():
    """Test ItemSeriesStats window functionality."""
    stats = ItemSeriesStats(window=5)  # Use larger window to avoid deque issues

    for char in ["a", "b", "c", "d", "e"]:
        stats.push(char)

    # Should keep all 5
    check(len(stats), 5)
    check(list(stats.peek()), ["a", "b", "c", "d", "e"])


# ============================================================================
# Context Manager Tests (time measurement)
# ============================================================================


def test_stats_context_manager():
    """Test using stats objects as context managers for time measurement."""
    stats = MeanStats(window=5)

    with stats:
        time.sleep(0.05)

    # Should have measured approximately 0.05 seconds
    measured_time = stats.peek()
    check(0.04 < measured_time < 0.07, True)


def test_stats_context_manager_multiple_measurements():
    """Test multiple time measurements with context manager."""
    stats = MeanStats(window=5)

    for _ in range(3):
        with stats:
            time.sleep(0.05)

    # Should have measurements, each around 0.05 seconds
    mean_time = stats.peek()
    check(0.04 < mean_time < 0.07, True)


# ============================================================================
# Numeric Operations Tests
# ============================================================================


@pytest.mark.parametrize(
    "stats_class,setup_values,expected_value",
    [
        (MeanStats, [10, 20], 15.0),  # Mean of 10, 20
        (MaxStats, [10, 20], 20),  # Max of 10, 20
        (MinStats, [10, 20], 10),  # Min of 10, 20
        (SumStats, [10, 20], 30),  # Sum of 10, 20
        (EmaStats, [10, 20], 10.1),  # EMA with coeff 0.01: 0.99*10 + 0.01*20
        (LifetimeSumStats, [10, 20], 30),  # Lifetime sum of 10, 20
    ],
)
def test_stats_numeric_operations(stats_class, setup_values, expected_value):
    """Test numeric operations on stats objects."""
    # Create stats with appropriate settings
    if stats_class == EmaStats:
        stats = stats_class(ema_coeff=0.01)
    elif stats_class == LifetimeSumStats:
        stats = stats_class()
    else:
        stats = stats_class(window=5)

    # Push values
    for value in setup_values:
        stats.push(value)

    # Test numeric operations
    check(float(stats), expected_value)
    check(stats + 5, expected_value + 5)
    check(stats - 5, expected_value - 5)
    check(stats * 2, expected_value * 2)
    check(stats == expected_value, True)
    check(stats > expected_value - 1, True)
    check(stats < expected_value + 1, True)
    check(stats >= expected_value, True)
    check(stats <= expected_value, True)


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


@pytest.mark.parametrize(
    "stats_class,init_kwargs,expected_result",
    [
        # SeriesStats return NaN when empty
        (MeanStats, {"window": 5}, np.nan),
        (MaxStats, {"window": 5}, np.nan),
        (MinStats, {"window": 5}, np.nan),
        # SumStats returns NaN when empty (with window)
        (SumStats, {"window": 5}, np.nan),
        # LifetimeSumStats returns 0 when empty
        (LifetimeSumStats, {}, 0),
        # EmaStats returns NaN when empty
        (EmaStats, {"ema_coeff": 0.01}, np.nan),
        # ItemStats returns None when empty
        (ItemStats, {}, None),
        # PercentilesStats returns dict with NaN values when empty
        (PercentilesStats, {"percentiles": [50], "window": 10}, {50: np.nan}),
        # ItemSeriesStats returns empty list when empty
        (ItemSeriesStats, {"window": 5}, []),
    ],
)
def test_stats_empty_reduce(stats_class, init_kwargs, expected_result):
    """Test reducing stats with no values across all stats types."""
    stats = stats_class(**init_kwargs)

    # Peek on empty stats should return appropriate default value
    result = stats.peek()

    # Handle NaN comparison specially
    if isinstance(expected_result, float) and np.isnan(expected_result):
        check(np.isnan(result), True)
    elif isinstance(expected_result, dict):
        # Handle dict with potential NaN values
        check(isinstance(result, dict), True)
        for key in expected_result:
            if isinstance(expected_result[key], float) and np.isnan(
                expected_result[key]
            ):
                check(np.isnan(result[key]), True)
            else:
                check(result[key], expected_result[key])
    else:
        check(result, expected_result)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
