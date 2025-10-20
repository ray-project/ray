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
        (EmaStats, {"ema_coeff": 0.01}, [10, 20], 10.1),
    ],
)
def test_peek_and_reduce(stats_class, init_kwargs, setup_values, expected_reduced):
    # Test without clear_on_reduce (LifetimeSumStats always clears)
    stats = stats_class(**init_kwargs)
    for value in setup_values:
        stats.push(value)

    check(stats.peek(), expected_reduced)
    result = stats.reduce(compile=True)
    check(result, expected_reduced)

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


def test_peek_and_reduce_percentiles_stats():
    stats = PercentilesStats(percentiles=[0, 50, 100], window=10)
    for value in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        stats.push(value)

    check(stats.peek(compile=False), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    check(stats.peek(compile=True), {0: 1, 50: 5.5, 100: 10})
    result = stats.reduce(compile=True)
    check(result, {0: 1, 50: 5.5, 100: 10})


def test_peek_and_reduce_item_series_stats():
    stats = ItemSeriesStats(window=10)
    for value in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
        stats.push(value)

    assert stats.peek(compile=False) == [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
    ]
    assert stats.peek(compile=True) == [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
    ]
    result = stats.reduce(compile=True)
    assert result == ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]


@pytest.mark.parametrize(
    "stats_class,init_kwargs,test_values",
    [
        (ItemStats, {}, 123),
        (MeanStats, {"window": 5}, [1, 2, 3]),
        (MaxStats, {"window": 5}, [1, 5, 3]),
        (MinStats, {"window": 5}, [5, 1, 3]),
        (SumStats, {"window": 5}, [1, 2, 3]),
        (LifetimeSumStats, {}, [10, 20]),
        (EmaStats, {"ema_coeff": 0.01}, [10, 20]),
        (PercentilesStats, {"percentiles": [50], "window": 10}, [1, 2, 3]),
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
    "stats_class,init_kwargs,values1,values2,expected_result",
    [
        (MeanStats, {"window": 10}, [1, 2, 3], [4, 5], 3.0),
        (MaxStats, {"window": 10}, [1, 2, 3], [4, 5], 5),
        (MinStats, {"window": 10}, [1, 2, 3], [4, 5], 1),
        (SumStats, {"window": 10}, [1, 2, 3], [4, 5], 15),
        (EmaStats, {"ema_coeff": 0.01}, [1, 2], [3, 4], 2.01),
        (ItemSeriesStats, {"window": 10}, [1, 2], [3, 4], [1, 2, 3, 4]),
        (LifetimeSumStats, {}, [10, 20], [30, 40], 100),
        # This is not inteded to work for ItemStats
    ],
)
def test_merge(stats_class, init_kwargs, values1, values2, expected_result):
    root_stats = stats_class(**init_kwargs, is_root_stats=True)

    stats1 = stats_class(**init_kwargs)
    for value in values1:
        stats1.push(value)

    stats2 = stats_class(**init_kwargs)
    for value in values2:
        stats2.push(value)

    root_stats.merge([stats1, stats2])

    result = root_stats.peek()

    check(result, expected_result)


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
        check(list(result.values()), [None])
    elif isinstance(result, float):
        # All others should be NaN
        check(result, np.nan)


# Series stats allow us to set a window size and reduce the values in the window.
@pytest.mark.parametrize(
    "stats_class,window,values,expected_result",
    [
        # Basic tests with window=5
        (MeanStats, 5, [1, 2, 3], 2.0),
        (MaxStats, 5, [1, 2, 3], 3),
        (MinStats, 5, [1, 2, 3], 1),
        (SumStats, 5, [1, 2, 3], 6),
        # Window tests with window=3, values exceeding window size (fills window)
        (MeanStats, 3, [1, 2, 3, 4, 5], 4.0),  # Mean of 3, 4, 5
        (MaxStats, 3, [1, 2, 3, 4, 5], 5),  # Max of 3, 4, 5
        (MinStats, 3, [1, 2, 3, 4, 5], 3),  # Min of 3, 4, 5
        (SumStats, 3, [1, 2, 3, 4, 5], 12),  # Sum of 3, 4, 5
    ],
)
def test_series_stats_windowed(stats_class, window, values, expected_result):
    # All examples chosen such that we should end up with a length of three
    expected_len = 3
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
    time.sleep(0.1)

    # Throughput should be approximately (30 - 0) / 0.2 = 150
    # The accurate behaviour of this is tested in the MetricsLogger tests.
    throughput = stats.throughputs
    check(throughput, 150, atol=30)


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


def test_ema_stats():
    """Test basic EmaStats functionality."""
    ema_coeff = 0.1
    stats = EmaStats(ema_coeff=ema_coeff)

    stats.push(10)
    check(stats.peek(), 10)

    stats.push(20)
    check(stats.peek(), 11.0)

    stats.push(30)
    check(stats.peek(), 12.9)


def test_percentiles_stats():
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


def test_percentiles_stats_windowed_default_percentiles():
    """Test PercentilesStats window functionality."""
    stats = PercentilesStats(percentiles=None, window=101)

    for i in range(1, 201):
        stats.push(i)

    # Should only keep last 100: [100, 101, ..., 199, 200]
    check(len(stats), 101)
    result = stats.peek(compile=True)
    check(result[0], 100)
    check(result[50], 150)
    check(result[75], 175)
    check(result[90], 190)
    check(result[95], 195)
    check(result[99], 199)
    check(result[100], 200)


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
        (PercentilesStats, {"percentiles": [50], "window": 10}, {50: None}),
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
        assert isinstance(stats, PercentilesStats)
        assert isinstance(result, dict)
        check(list(result.keys()), list(expected_result.keys()))
        check(list(result.values()), list(expected_result.values()))
    else:
        check(result, expected_result)


@pytest.mark.parametrize(
    "stats_class, result_value",
    [(MeanStats, 2), (SumStats, 6), (MinStats, 1), (MaxStats, 3)],
)
def test_stats_reduce_at_root(stats_class, result_value):
    """Test reducing stats at root level."""
    stats = stats_class(window=5, reduce_at_root=True, is_root_stats=False)

    stats.push(1)
    stats.push(2)
    stats.push(3)

    with pytest.raises(ValueError, match="Can not compile"):
        stats.reduce(compile=True)

    stats_values_before_reduce = stats.values.copy()

    reduced_stats = stats.reduce(compile=False)

    check(list(reduced_stats.values), list(stats_values_before_reduce))

    root_stats = stats.similar_to(stats)
    root_stats._is_root_stats = True

    root_stats.merge([reduced_stats])

    reduced_root_stats = root_stats.reduce(compile=False)

    check(len(reduced_root_stats), 1)

    check(reduced_root_stats, result_value)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
