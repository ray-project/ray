import numpy as np
from collections import deque
import pytest
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.test_utils import check

# Default values used throughout the tests, these can be the ones that are used by the MetricsLogger
DEFAULT_EMA_COEFF = 0.01
DEFAULT_THROUGHPUT_EMA_COEFF = 0.05
DEFAULT_CLEAR_ON_REDUCE = False
DEFAULT_THROUGHPUT = False


def test_init():
    """Test initialization of Stats objects with different parameters."""
    # Test initialization with initial value
    stats = Stats(
        init_values=1.0,
        reduce="mean",
        ema_coeff=DEFAULT_EMA_COEFF,
        window=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    assert len(stats) == 1
    assert stats.peek() == 1.0
    assert stats.peek(compile=False) == [1.0]
    assert stats._ema_coeff == DEFAULT_EMA_COEFF

    # Test initialization with window
    stats = Stats(
        init_values=None,
        window=3,
        reduce="mean",
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    assert stats._window == 3
    assert isinstance(stats.values, deque)

    # Test initialization with different reduce methods
    for reduce_method in ["mean", "min", "max", "sum", None]:
        stats = Stats(
            init_values=None,
            reduce=reduce_method,
            window=None,
            ema_coeff=DEFAULT_EMA_COEFF if reduce_method == "mean" else None,
            clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
            throughput=DEFAULT_THROUGHPUT,
            throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
        )
        assert stats._reduce_method == reduce_method

    # Test invalid initialization parameters
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


def test_push_and_peek():
    """Test pushing values and peeking at results."""
    # Test with mean reduction
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
    # EMA formula: t1 = (1.0 - ema_coeff) * t0 + ema_coeff * new_val
    expected = 1.0 * (1.0 - DEFAULT_EMA_COEFF) + 2.0 * DEFAULT_EMA_COEFF
    assert abs(stats.peek() - expected) < 1e-6

    # Test with window
    stats = Stats(
        init_values=None,
        window=2,
        reduce="mean",
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1.0)
    stats.push(2.0)
    stats.push(3.0)
    assert stats.peek() == 2.5

    # Test with sum reduction
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1)
    stats.push(2)
    stats.push(3)
    assert stats.peek() == 6

    # Test with min reduction
    stats = Stats(
        init_values=None,
        reduce="min",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(10)
    stats.push(20)
    stats.push(5)
    stats.push(100)
    assert stats.peek() == 5

    # Test with max reduction
    stats = Stats(
        init_values=None,
        reduce="max",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1)
    stats.push(3)
    stats.push(2)
    stats.push(4)
    assert stats.peek() == 4


def test_window_behavior():
    """Test behavior with different window sizes."""
    # Test with finite window
    stats = Stats(
        init_values=None,
        window=2,
        reduce="mean",
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1.0)
    stats.push(2.0)
    stats.push(3.0)
    assert len(stats) == 2  # Only keeps last 2 values
    assert stats.peek() == 2.5

    # Test with infinite window
    stats = Stats(
        init_values=None,
        window=None,
        reduce="mean",
        ema_coeff=DEFAULT_EMA_COEFF,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1.0)
    stats.push(2.0)
    assert len(stats) == 1  # We reduce for every push
    assert stats.peek() == 1.01

    # Test with max reduction and window
    stats = Stats(
        init_values=None,
        reduce="max",
        window=2,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(2)
    stats.push(3)
    stats.push(1)
    stats.push(-1)
    assert stats.peek() == 1


def test_reduce():
    """Test the reduce method."""
    # Test with sum reduction
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1)
    stats.push(2)
    stats.push(3)
    reduced_value = stats.reduce()
    assert reduced_value == 6
    assert len(stats) == 1  # Should keep only the reduced value

    # Test with clear_on_reduce
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
    assert reduced_value == 3
    assert len(stats) == 0  # Original stats should be cleared


def test_merge_operations():
    """Test merging operations between Stats objects."""
    # Test merge_on_time_axis
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
    assert stats1.peek() == 10

    # Test merge_in_parallel
    stats1 = Stats(
        init_values=None,
        reduce="mean",
        window=3,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats1.push(1)
    stats1.push(2)
    stats1.push(3)
    stats2 = Stats(
        init_values=None,
        reduce="mean",
        window=3,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats2.push(4)
    stats2.push(5)
    stats2.push(6)
    stats = Stats(
        init_values=None,
        reduce="mean",
        window=3,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.merge_in_parallel(stats1, stats2)
    assert abs(stats.peek() - 4.1666667) < 1e-6  # mean of last values


def test_numeric_operations():
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

    # Test basic arithmetic operations
    assert float(stats) == 2.0
    assert int(stats) == 2
    assert stats + 1 == 3.0
    assert stats - 1 == 1.0
    assert stats * 2 == 4.0

    # Test comparison operations
    assert stats == 2.0
    assert stats <= 3.0
    assert stats >= 1.0
    assert stats < 3.0
    assert stats > 1.0


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
    stats.push(1)
    stats.push(2)
    stats.push(3)

    state = stats.get_state()
    loaded_stats = Stats.from_state(state)

    assert loaded_stats._reduce_method == stats._reduce_method
    assert loaded_stats._window == stats._window
    assert loaded_stats.peek() == stats.peek()
    assert len(loaded_stats) == len(stats)


def test_state_with_throughput_serialization():
    """Test saving and loading Stats state with throughput tracking."""
    # Create a Stats object with throughput tracking
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=True,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )

    # Push some values with time delays to generate throughput data
    import time

    stats.push(1)
    time.sleep(0.1)  # 100ms delay
    stats.push(2)
    time.sleep(0.1)  # 100ms delay
    stats.push(3)

    # Get the current state
    state = stats.get_state()

    # Create a new Stats object from the state
    loaded_stats = Stats.from_state(state)

    # Verify that throughput tracking is preserved
    assert loaded_stats._throughput_stats is not None
    assert loaded_stats._throughput_ema_coeff == stats._throughput_ema_coeff
    assert loaded_stats._last_push_time >= 0  # Should be set after loading

    # Verify that the current throughput is preserved
    assert abs(loaded_stats.throughput - stats.throughput) < 1e-6

    # Verify that throughput tracking continues to work after loading
    time.sleep(0.1)  # 100ms delay
    loaded_stats.push(4)
    assert loaded_stats.peek() == 10  # sum of all values
    assert loaded_stats.throughput > 0  # Should have some throughput

    # Test that throughput tracking is preserved even after multiple reduce calls
    loaded_stats.reduce()
    time.sleep(0.1)  # 100ms delay
    loaded_stats.push(5)
    loaded_stats.reduce()
    assert loaded_stats.throughput > 0  # Should still have throughput tracking

    # Test that throughput tracking is preserved when creating similar stats
    similar_stats = Stats.similar_to(loaded_stats)
    assert similar_stats._throughput_stats is not None
    assert similar_stats._throughput_ema_coeff == loaded_stats._throughput_ema_coeff
    assert similar_stats._last_push_time == -1  # Should be reset for new instance


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

    similar = Stats.similar_to(original)
    assert similar._reduce_method == original._reduce_method
    assert similar._window == original._window
    assert len(similar) == 0  # Should start empty

    similar_with_value = Stats.similar_to(original, init_values=[1, 2])
    assert len(similar_with_value) == 2
    assert similar_with_value.peek() == 3  # sum of [1, 2]


def test_reduce_history():
    """Test the reduce history functionality."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )

    # Initially history should contain zeros
    assert stats.get_reduce_history() == [[np.nan], [np.nan], [np.nan]]

    # Push some values and reduce
    stats.push(1)
    stats.push(2)
    reduced_value = stats.reduce()
    assert reduced_value == 3  # sum of [1, 2]
    check(stats.get_reduce_history(), [[np.nan], [np.nan], [3]])

    # Push more values and reduce again
    stats.push(3)
    stats.push(4)
    reduced_value = stats.reduce()
    assert reduced_value == 10  # sum of [1, 2, 3, 4]
    check(stats.get_reduce_history(), [[np.nan], [3], [10]])

    # Push and reduce one more time
    stats.push(5)
    stats.push(6)
    reduced_value = stats.reduce()
    assert reduced_value == 21  # sum of [1, 2, 3, 4, 5, 6]
    check(stats.get_reduce_history(), [[3], [10], [21]])

    # Test that history is preserved when creating similar stats
    similar = Stats.similar_to(stats)
    check(similar.get_reduce_history(), [[3], [10], [21]])

    # Test that history is preserved when loading from state
    state = stats.get_state()
    loaded = Stats.from_state(state)
    check(loaded.get_reduce_history(), [[3], [10], [21]])

    # Push and reduce one more time
    stats.push(7)
    stats.push(8)
    reduced_value = stats.reduce()
    assert reduced_value == 36  # sum of [1, 2, 3, 4, 5, 6, 7, 8]
    check(stats.get_reduce_history(), [[10], [21], [36]])


def test_reduce_history_with_clear_on_reduce():
    """Test the reduce history functionality with clear_on_reduce=True."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=True,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )

    # Initially history should contain zeros
    assert stats.get_reduce_history() == [[np.nan], [np.nan], [np.nan]]

    # Push some values and reduce
    stats.push(1)
    stats.push(2)
    reduced_value = stats.reduce()
    assert reduced_value == 3  # sum of [1, 2]
    assert stats.get_reduce_history() == [[np.nan], [np.nan], [3]]
    assert len(stats) == 0  # Values should be cleared

    # Push more values and reduce again
    stats.push(3)
    stats.push(4)
    reduced_value = stats.reduce()
    assert reduced_value == 7  # sum of [3, 4]
    assert stats.get_reduce_history() == [[np.nan], [3], [7]]
    assert len(stats) == 0  # Values should be cleared

    # Push and reduce one more time
    stats.push(5)
    stats.push(6)
    reduced_value = stats.reduce()
    assert reduced_value == 11  # sum of [5, 6]
    assert stats.get_reduce_history() == [[3], [7], [11]]
    assert len(stats) == 0  # Values should be cleared

    # Test that history is preserved when creating similar stats
    similar = Stats.similar_to(stats)
    assert similar.get_reduce_history() == [[3], [7], [11]]
    assert similar._clear_on_reduce

    # Test that history is preserved when loading from state
    state = stats.get_state()
    loaded = Stats.from_state(state)
    assert loaded.get_reduce_history() == [[3], [7], [11]]
    assert loaded._clear_on_reduce

    # Push and reduce one more time
    loaded.push(7)
    loaded.push(8)
    reduced_value = loaded.reduce()
    assert reduced_value == 15  # sum of [7, 8]
    assert loaded.get_reduce_history() == [[7], [11], [15]]
    assert len(loaded) == 0  # Values should be cleared


def test_throughput_without_reduce():
    """Test that throughput is tracked correctly without explicit reduce() calls."""
    # Create a Stats object that tracks throughput
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=True,
        throughput_ema_coeff=1,
    )

    # Push some values with time delays to simulate real usage
    import time

    # First push - throughput should be 0 initially
    stats.push(1)
    assert stats.peek() == 1
    assert stats.throughput == 0.0  # No throughput yet as we only have one value

    # Push second value after a delay
    time.sleep(0.1)  # 100ms delay
    stats.push(2)
    assert stats.peek() == 3  # sum of 1 + 2
    assert 15 < stats.throughput < 25  # 2/0.1 = 20 values per second

    # Push third value after another delay
    time.sleep(0.2)  # 200ms delay
    stats.push(3)
    assert stats.peek() == 6  # sum of 1 + 2 + 3
    assert 10 < stats.throughput < 20  # 3/0.3 = 10 values per second

    # Test that throughput is only available when requested
    assert stats.peek() == 6  # Regular peek returns just the value
    assert (
        stats.throughput == stats.throughput
    )  # Throughput property returns throughput

    # Test that throughput is 0 when no values are pushed
    empty_stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=True,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    check(empty_stats.peek(), np.nan)
    check(empty_stats.throughput, 0.0)

    # Test that throughput tracking requires sum reduction
    with pytest.raises(ValueError):
        Stats(
            init_values=None,
            reduce="mean",
            window=None,
            ema_coeff=DEFAULT_EMA_COEFF,
            clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
            throughput=True,
            throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
        )

    # Test that throughput tracking requires infinite window
    with pytest.raises(ValueError):
        Stats(
            init_values=None,
            reduce="sum",
            window=10,
            ema_coeff=None,
            clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
            throughput=True,
            throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
        )

    # Test that _last_push_time is properly initialized to -1
    assert stats._last_push_time >= 0  # Should be set after pushes
    new_stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=True,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    assert new_stats._last_push_time == -1  # Should be -1 for new instances

    # Test throughput tracking after loading stats
    state = stats.get_state()
    loaded_stats = Stats.from_state(state)
    assert loaded_stats._last_push_time >= 0  # Should be set after loading
    assert loaded_stats.peek() == 6  # Value should be preserved
    assert loaded_stats.throughput == stats.throughput  # Throughput should be preserved

    # Test that throughput tracking works after loading
    loaded_stats.reduce()
    loaded_stats.push(2)
    loaded_stats.reduce()
    time.sleep(0.1)  # 100ms delay
    loaded_stats.push(4)
    assert loaded_stats.peek() == 12  # sum of 6 + 4
    assert 30 < loaded_stats.throughput < 50  # 4/0.1 = 40 values per second

    # Test that accessing throughput on non-throughput stats raises error
    non_throughput_stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=False,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    with pytest.raises(ValueError):
        non_throughput_stats.throughput  # noqa: B018


def test_reduce_history_without_new_values():
    """Test that multiple reduce calls without new values maintain consistent history."""
    # Test with sum reduction
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )

    # Push some initial values
    stats.push(1)
    stats.push(2)

    # First reduce call
    first_reduce = stats.reduce()
    first_history = stats.get_reduce_history()
    assert first_reduce == 3  # sum of [1, 2]
    check(first_history, [[np.nan], [np.nan], [3]])

    # Second reduce call without new values
    second_reduce = stats.reduce()
    second_history = stats.get_reduce_history()
    assert second_reduce == 3  # should still be 3
    check(second_history, [[np.nan], [np.nan], [3]])

    # Third reduce call without new values
    third_reduce = stats.reduce()
    third_history = stats.get_reduce_history()
    assert third_reduce == 3  # should still be 3
    check(third_history, [[np.nan], [np.nan], [3]])

    # Test with window-based reduction
    stats = Stats(
        init_values=None,
        reduce="mean",
        window=2,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    stats.push(1.0)
    stats.push(2.0)

    # First reduce call
    first_reduce = stats.reduce()
    first_history = stats.get_reduce_history()
    assert first_reduce == 1.5  # mean of [1.0, 2.0]
    check(first_history, [[np.nan], [np.nan], [1.5]])

    # Second reduce call without new values
    second_reduce = stats.reduce()
    second_history = stats.get_reduce_history()
    assert second_reduce == 1.5  # should still be 1.5
    check(second_history, [[np.nan], [np.nan], [1.5]])

    # Test with EMA reduction
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

    # First reduce call
    first_reduce = stats.reduce()
    first_history = stats.get_reduce_history()
    assert abs(first_reduce - 1.01) < 1e-6
    check(first_history, [[np.nan], [np.nan], [1.01]])

    # Second reduce call without new values
    second_reduce = stats.reduce()
    second_history = stats.get_reduce_history()
    assert abs(second_reduce - 1.01) < 1e-6
    check(second_history, [[np.nan], [np.nan], [1.01]])

    # Test with clear_on_reduce=True
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

    # First reduce call
    first_reduce = stats.reduce()
    first_history = stats.get_reduce_history()
    assert first_reduce == 3  # sum of [1, 2]
    check(first_history, [[np.nan], [np.nan], [3]])
    assert len(stats) == 0  # values should be cleared

    # Second reduce call without new values
    second_reduce = stats.reduce()
    second_history = stats.get_reduce_history()
    # should be [nan] as values are cleared
    assert np.isnan(second_reduce)
    check(second_history, [[np.nan], [3], [np.nan]])
    assert len(stats) == 0  # values should still be cleared


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
