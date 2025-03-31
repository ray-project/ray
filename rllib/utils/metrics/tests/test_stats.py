import numpy as np
import pytest
from collections import deque
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
    [(1.0, 1, 1.0), (None, 0, np.nan)]
)
def test_init_with_values(init_values, expected_len, expected_peek):
    """Test initialization with different initial values."""
    stats = Stats(
        init_values=init_values,
        reduce="mean",
        ema_coeff=DEFAULT_EMA_COEFF,
        window=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=DEFAULT_THROUGHPUT,
        throughput_ema_coeff=DEFAULT_THROUGHPUT_EMA_COEFF,
    )
    assert len(stats) == expected_len
    if expected_len > 0:
        assert stats.peek() == expected_peek
        assert stats.peek(compile=False) == [expected_peek]
    else:
        assert np.isnan(stats.peek())

def test_init_with_window():
    """Test initialization with a window."""
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
    assert stats._window == window_size
    assert isinstance(stats.values, deque)

@pytest.mark.parametrize(
    "reduce_method", 
    ["mean", "min", "max", "sum", None]
)
def test_init_with_reduce_methods(reduce_method):
    """Test initialization with different reduce methods."""
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
    assert abs(stats.peek() - expected) < 1e-6

def test_push_with_window():
    """Test pushing values with a window."""
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
    stats.push(3.0)  # This should push out 1.0
    assert stats.peek() == 2.5  # Mean of [2.0, 3.0]
    assert len(stats) == 2  # Window size is 2

@pytest.mark.parametrize(
    "reduce_method,values,expected", 
    [
        ("sum", [1, 2, 3], 6),
        ("min", [10, 20, 5, 100], 5),
        ("max", [1, 3, 2, 4], 4),
    ]
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
    assert stats.peek() == expected

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
    assert reduced_value == 3
    assert len(stats) == 0  # Stats should be cleared

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
    assert stats1.peek() == 10  # sum of [1, 2, 3, 4]

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
    
    assert abs(result.peek() - 4.167) < 1e-3

@pytest.mark.parametrize(
    "op,value,expected", 
    [
        (lambda s: float(s), None, 2.0),
        (lambda s: int(s), None, 2),
        (lambda s: s + 1, None, 3.0),
        (lambda s: s - 1, None, 1.0),
        (lambda s: s * 2, None, 4.0),
        (lambda s: s == 2.0, None, True),
        (lambda s: s <= 3.0, None, True),
        (lambda s: s >= 1.0, None, True),
        (lambda s: s < 3.0, None, True),
        (lambda s: s > 1.0, None, True),
    ]
)
def test_numeric_operations(op, value, expected):
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
    
    assert op(stats) == expected

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

    assert loaded_stats._reduce_method == stats._reduce_method
    assert loaded_stats._window == stats._window
    assert loaded_stats.peek() == stats.peek()
    assert len(loaded_stats) == len(stats)

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

    # Similar stats without initial values
    similar = Stats.similar_to(original)
    assert similar._reduce_method == original._reduce_method
    assert similar._window == original._window
    assert len(similar) == 0  # Should start empty

    # Similar stats with initial values
    similar_with_value = Stats.similar_to(original, init_values=[1, 2])
    assert len(similar_with_value) == 2
    assert similar_with_value.peek() == 3  # sum of [1, 2]

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
    assert stats.reduce() == 3
    check(stats.get_reduce_history(), [[np.nan], [np.nan], [3]])

    # Push more values and reduce
    stats.push(3)
    stats.push(4)
    assert stats.reduce() == 10
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
    assert stats.reduce() == 3
    check(stats.get_reduce_history(), [[np.nan], [np.nan], [3]])
    assert len(stats) == 0  # Values should be cleared

    stats.push(3)
    stats.push(4)
    assert stats.reduce() == 7
    check(stats.get_reduce_history(), [[np.nan], [3], [7]])
    assert len(stats) == 0

def test_basic_throughput():
    """Test basic throughput tracking."""
    stats = Stats(
        init_values=None,
        reduce="sum",
        window=None,
        ema_coeff=None,
        clear_on_reduce=DEFAULT_CLEAR_ON_REDUCE,
        throughput=True,
        throughput_ema_coeff=1.0,  # Use 1.0 for simple testing
    )

    # First push - throughput should be 0 initially
    stats.push(1)
    assert stats.peek() == 1
    assert stats.throughput == 0.0

    # Wait and push again to measure throughput
    time.sleep(0.1)
    stats.push(2)
    assert stats.peek() == 3
    assert 5 < stats.throughput < 30  # Roughly 10 items/second

def test_throughput_error_conditions():
    """Test throughput error conditions."""
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
        non_throughput_stats.throughput

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
