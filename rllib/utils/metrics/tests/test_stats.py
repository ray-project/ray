import pytest
import time
import numpy as np
import re

from ray.rllib.utils.metrics.stats import Stats, merge_stats
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
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


def test_basic_merge_on_time_axis():
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


def test_basic_merge_in_parallel():
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
        reduce_per_index_on_aggregate=True,
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
    check(original._last_reduced, [3])


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
    check(stats._last_reduced, [np.nan])

    # Push values and reduce
    stats.push(1)
    stats.push(2)
    check(stats.reduce(), 3)
    check(stats._last_reduced, [3])

    # Push more values and reduce
    stats.push(3)
    stats.push(4)
    check(stats.reduce(), 10)
    check(stats._last_reduced, [10])


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
    check(stats._last_reduced, [3])
    check(len(stats), 0)  # Values should be cleared

    stats.push(3)
    stats.push(4)
    check(stats.reduce(), 7)
    check(stats._last_reduced, [7])
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


@pytest.mark.parametrize(
    "reduce_method,"
    "reduce_per_index,"
    "clear_on_reduce,"
    "window,"
    "expected_first_round_values,"
    "expected_first_round_peek,"
    "expected_second_round_values,"
    "expected_second_round_peek,"
    "expected_third_round_values,"
    "expected_third_round_peek",
    [
        # In the following, we carry out some calculations by hand to verify that the math yields expected results.
        # To keep things readable, we round the results to 2 decimal places. Since we don't aggregate many times,
        # the rounding errors are negligible.
        (
            "mean",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            # With window=None and ema_coeff=0.01, the values list
            # contains a single value. For mean with reduce_per_index=True,
            # the first merged values are [55, 110, 165]
            # EMA calculation:
            # 1. Start with 55
            # 2. Update with 110: 0.99*55 + 0.01*110 = 55.55
            # 3. Update with 165: 0.99*55.55 + 0.01*165 = 56.65
            [56.65],  # expected_first_round_values - final EMA value
            56.65,  # expected_first_round_peek - same as the EMA value
            # Second round, merged values are [220, 275, 330]
            # Starting fresh after clear_on_reduce:
            # 1. Start with 220
            # 2. Update with 275: 0.99*220 + 0.01*275 = 220.55
            # 3. Update with 330: 0.99*220.55 + 0.01*330 = 221.65
            [221.65],  # expected_second_round_values - final EMA value
            221.65,  # expected_second_round_peek - same as the EMA value
            # Third round, merged values contain [385]
            [700],  # expected_third_round_values - final EMA value
            700,  # expected_third_round_peek - final EMA value
        ),
        (
            "mean",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            # Three values that we reduce per index from the two incoming stats.
            # [(10 + 100) / 2, (20 + 200) / 2, (30 + 300) / 2] = [55, 110, 165]
            [55, 110, 165],  # expected_first_round_values
            (55 + 110 + 165) / 3,  # expected_first_round_peek
            # Since we clear on reduce, the second round starts fresh.
            # The values are the three values that we reduce per index from the two incoming stats.
            # [(40 + 400) / 2, (50 + 500) / 2, (60 + 600) / 2] = [220, 275, 330]
            [220, 275, 330],  # expected_second_round_values
            (220 + 275 + 330) / 3,  # expected_second_round_peek
            # Since we clear on reduce, the third round starts fresh.
            # We only add the new value from the second Stats object.
            [
                700
            ],  # expected_third_round_values - clear_on_reduce makes this just the new merged value
            700,  # expected_third_round_peek
        ),
        (
            "mean",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            # With window=None and ema_coeff=0.01, the values list
            # contains a single value. For mean with reduce_per_index=True,
            # For the first Stats object, the values are [10, 20, 30]
            # EMA calculation:
            # 1. Start with 10
            # 2. Update with 20: 0.99*10 + 0.01*20 = 10.1
            # 3. Update with 30: 0.99*10.1 + 0.01*30 = 10.299
            # For the second Stats object, the values are [100, 200, 300]
            # EMA calculation:
            # 1. Start with 100
            # 2. Update with 200: 0.99*100 + 0.01*200 = 101
            # 3. Update with 300: 0.99*101 + 0.01*300 = 102.99
            # Finally, the we reduce over the single index:
            # 0.5*10.299 + 0.5*102.99 = 56.64
            [56.64],  # expected_first_round_values - final EMA value
            56.64,  # expected_first_round_peek - same as the EMA value
            # Second round, for the first object, the values are [40, 50, 60]
            # Starting from 10.299 (because we don't clear on reduce)
            # 1. Update with 40: 0.99*10.299 + 0.01*40 = 10.6
            # 2. Update with 50: 0.99*10.6 + 0.01*50 = 10.994
            # 3. Update with 60: 0.99*10.994 + 0.01*60 = 11.48
            # For the second object, the values are [400, 500, 600]
            # 1. Start from 102.99 (because we don't clear on reduce)
            # 2. Update with 400: 0.99*102.99 + 0.01*400 = 105.96
            # 3. Update with 500: 0.99*105.96 + 0.01*500 = 109.9
            # 4. Update with 600: 0.99*109.9 + 0.01*600 = 114.8
            # Finally, the we reduce over the single index:
            # 0.5*11.48 + 0.5*114.8 = 63.14
            [63.14],  # expected_second_round_values - final EMA value
            63.14,  # expected_second_round_peek - same as the EMA value
            # Third round, for the first object, there are no new values
            # For the second object, the values are [700]
            # 1. Start from 114.8 (because we don't clear on reduce)
            # 2. Update with 700: 0.99*114.8 + 0.01*700 = 120.65
            # Finally, the we reduce over the single index:
            # 0.5*11.48 + 0.5*120.65 = 66.07
            [66.07],  # expected_third_round_values - final EMA value
            66.07,  # expected_third_round_peek - final EMA value
        ),
        (
            "mean",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            # The first round values are the three values that we reduce per index from the two incoming stats.
            # [(10 + 100) / 2, (20 + 200) / 2, (30 + 300) / 2] = [55, 110, 165]
            [55, 110, 165],  # expected_first_round_values
            (55 + 110 + 165) / 3,  # expected_first_round_peek
            # Since we don't clear on reduce, the second round includes the latest value from the first round.
            # [(30 + 300) / 2, (40 + 400) / 2, (50 + 500) / 2, (60 + 600) / 2] = [165, 220, 275, 330]
            [
                165,
                220,
                275,
                330,
            ],  # expected_second_round_values - includes values from previous round
            (165 + 220 + 275 + 330)
            / 4,  # expected_second_round_peek - average of all 4 values
            # Since we don't clear on reduce, the third round includes the latest value from the second round.
            # [(30 + 400) / 2, (40 + 500) / 2, (50 + 600) / 2, (60 + 700) / 2] = [215, 270, 325, 380]
            [
                215,
                270,
                325,
                380,
            ],  # expected_third_round_values - matches actual values in test
            (215 + 270 + 325 + 380)
            / 4,  # expected_third_round_peek - average of all 4 values
        ),
        (
            "sum",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            [660],  # expected_first_round_values
            110 + 220 + 330,  # expected_first_round_peek
            [1650],  # expected_second_round_values
            440 + 550 + 660,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [110, 220, 330],  # expected_first_round_values
            110 + 220 + 330,  # expected_first_round_peek
            [440, 550, 660],  # expected_second_round_values
            440 + 550 + 660,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            [660],  # expected_first_round_values
            110 + 220 + 330,  # expected_first_round_peek
            # The leading zero in this list is an artifact of how we merge lifetime sums.
            # We merge them by substracting the previously reduced values from their history from the sum.
            [0.0, 660 + 1650],  # expected_second_round_values
            660 + 440 + 550 + 660,  # expected_second_round_peek
            [0.0, 660 + 1650 + 700],  # expected_third_round_values
            660 + 1650 + 700,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            [110, 220, 330],  # expected_first_round_values
            110 + 220 + 330,  # expected_first_round_peek
            [330, 440, 550, 660],  # expected_second_round_values
            330 + 440 + 550 + 660,  # expected_second_round_peek
            [430, 540, 650, 760],  # expected_third_round_values
            430 + 540 + 650 + 760,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            [10],  # expected_first_round_values
            10,  # expected_first_round_peek
            [40],  # expected_second_round_values
            40,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [10, 20, 30],  # expected_first_round_values
            10,  # expected_first_round_peek
            [40, 50, 60],  # expected_second_round_values
            40,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            [10],  # expected_first_round_values
            10,  # expected_first_round_peek
            [10, 10],  # expected_second_round_values
            10,  # expected_second_round_peek
            [10, 10],  # expected_third_round_values
            10,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            # Minima of [(10, 100), (20, 200), (30, 300)] = [10, 20, 30]
            [10, 20, 30],  # expected_first_round_values
            10,  # expected_first_round_peek
            # Minima of [(30, 300), (40, 400), (50, 500), (60, 600)] = [30, 40, 50, 60]
            [30, 40, 50, 60],  # expected_second_round_values
            30,  # expected_second_round_peek
            # Minimum of [(30, 400), (40, 500), (50, 600), (60, 700)] = [30, 40, 50, 60]
            [30, 40, 50, 60],  # expected_third_round_values
            30,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            [300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            True,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [100, 200, 300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [400, 500, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            [300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [300, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [600, 700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            True,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            [100, 200, 300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [300, 400, 500, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [400, 500, 600, 700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "mean",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            # With window=None and ema_coeff=0.01, the values list
            # contains a single value. For mean with reduce_per_index=True,
            # For the first Stats object, the values are [10, 20, 30]
            # EMA calculation:
            # 1. Start with 10
            # 2. Update with 20: 0.99*10 + 0.01*20 = 10.1
            # 3. Update with 30: 0.99*10.1 + 0.01*30 = 10.299
            # For the second Stats object, the values are [100, 200, 300]
            # EMA calculation:
            # 1. Start with 100
            # 2. Update with 200: 0.99*100 + 0.01*200 = 101
            # 3. Update with 300: 0.99*101 + 0.01*300 = 102.99
            # Finally, the we reduce over the single index:
            # 0.5*10.299 + 0.5*102.99 = 56.64
            [56.64, 56.64],  # expected_first_round_values - final EMA value
            56.64,  # expected_first_round_peek - same as the EMA value
            # Second round, for the first object, the values are [40, 50, 60]
            # Start with 40 (because we clear on reduce)
            # 1. Update with 40: 0.99*40 + 0.01*40 = 40.0
            # 2. Update with 50: 0.99*40.0 + 0.01*50 = 40.1
            # 3. Update with 60: 0.99*40.1 + 0.01*60 = 40.3
            # For the second object, the values are [400, 500, 600]
            # Start with 400 (because we clear on reduce)
            # 1. Update with 400: 0.99*400 + 0.01*400 = 400.0
            # 2. Update with 500: 0.99*400.0 + 0.01*500 = 401.0
            # 3. Update with 600: 0.99*401.0 + 0.01*600 = 403.0
            # Finally, the we reduce over the two indices:
            # 0.5*40.3 + 0.5*403.0 = 221.65
            [221.65, 221.65],  # expected_second_round_values - final EMA value
            221.65,  # expected_second_round_peek - same as the EMA value
            # Third round, for the first object, there are no new values
            # For the second object, the values are [700]
            [700],  # expected_third_round_values - final EMA value
            700,  # expected_third_round_peek - final EMA value
        ),
        (
            "mean",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [110, 110, 165, 165],  # expected_first_round_values
            (110 + 110 + 165 + 165) / 4,  # expected_first_round_peek
            [275, 275, 330, 330],  # expected_second_round_values
            (275 + 275 + 330 + 330) / 4,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "mean",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            # With window=None and ema_coeff=0.01, the values list
            # contains a single value. For mean with reduce_per_index=True,
            # For the first Stats object, the values are [10, 20, 30]
            # EMA calculation:
            # 1. Start with 10
            # 2. Update with 20: 0.99*10 + 0.01*20 = 10.1
            # 3. Update with 30: 0.99*10.1 + 0.01*30 = 10.299
            # For the second Stats object, the values are [100, 200, 300]
            # EMA calculation:
            # 1. Start with 100
            # 2. Update with 200: 0.99*100 + 0.01*200 = 101
            # 3. Update with 300: 0.99*101 + 0.01*300 = 102.99
            # Finally, the we reduce over the single index:
            # 0.5*10.299 + 0.5*102.99 = 56.64
            [56.64, 56.64],  # expected_first_round_values
            56.64,  # expected_first_round_peek
            # Second round, for the first object, the values are [40, 50, 60]
            # Starting from 10.299 (because we don't clear on reduce)
            # 1. Update with 40: 0.99*10.299 + 0.01*40 = 10.6
            # 2. Update with 50: 0.99*10.6 + 0.01*50 = 10.994
            # 3. Update with 60: 0.99*10.994 + 0.01*60 = 11.48
            # For the second object, the values are [400, 500, 600]
            # 1. Start from 102.99 (because we don't clear on reduce)
            # 2. Update with 400: 0.99*102.99 + 0.01*400 = 105.96
            # 3. Update with 500: 0.99*105.96 + 0.01*500 = 109.9
            # 4. Update with 600: 0.99*109.9 + 0.01*600 = 114.8
            # Finally, the we reduce over the single index:
            # 0.5*11.48 + 0.5*114.8 = 63.14
            [63.14, 63.14],  # expected_second_round_values
            63.14,  # expected_second_round_peek
            # Third round, for the first object, there are no new values
            # For the second object, the values are [700]
            # 1. Start from 114.8 (because we don't clear on reduce)
            # 2. Update with 700: 0.99*114.8 + 0.01*700 = 120.65
            # Finally, the we reduce over the single index:
            # 0.5*11.48 + 0.5*120.65 = 66.07
            [66.07, 66.07],  # expected_third_round_values
            66.07,  # expected_third_round_peek
        ),
        (
            "mean",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            [110, 110, 165, 165],  # expected_first_round_values
            (110 + 110 + 165 + 165) / 4,  # expected_first_round_peek
            [275, 275, 330, 330],  # expected_second_round_values
            (275 + 275 + 330 + 330) / 4,  # expected_second_round_peek
            [325, 325, 380, 380],  # expected_third_round_values
            (325 + 325 + 380 + 380) / 4,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            [660 / 2, 660 / 2],  # expected_first_round_values
            # 10 + 20 + 30 + 100 + 200 + 300
            660,  # expected_first_round_peek
            [1650 / 2, 1650 / 2],  # expected_second_round_values
            # 40 + 50 + 60 + 400 + 500 + 600
            1650,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [110, 110, 165, 165],  # expected_first_round_values
            110 + 110 + 165 + 165,  # expected_first_round_peek
            [275, 275, 330, 330],  # expected_second_round_values
            275 + 275 + 330 + 330,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            [330.0, 330.0],  # expected_first_round_values
            660.0,  # expected_first_round_peek
            [0, 1155.0, 1155.0],  # expected_second_round_values
            2310.0,  # expected_second_round_peek
            [0, 1505.0, 1505.0],  # expected_third_round_values
            3010.0,  # expected_third_round_peek
        ),
        (
            "sum",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            [110, 110, 165, 165],  # expected_first_round_values
            110 + 110 + 165 + 165,  # expected_first_round_peek
            [275, 275, 330, 330],  # expected_second_round_values
            275 + 275 + 330 + 330,  # expected_second_round_peek
            [325, 325, 380, 380],  # expected_third_round_values
            325 + 325 + 380 + 380,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            [10, 10],  # expected_first_round_values
            10,  # expected_first_round_peek
            [40, 40],  # expected_second_round_values
            40,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [20, 20, 30, 30],  # expected_first_round_values
            20,  # expected_first_round_peek
            [50, 50, 60, 60],  # expected_second_round_values
            50,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            [10, 10],  # expected_first_round_values
            10,  # expected_first_round_peek
            [10, 10, 10],  # expected_second_round_values
            10,  # expected_second_round_peek
            [10, 10, 10],  # expected_third_round_values
            10,  # expected_third_round_peek
        ),
        (
            "min",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            [20, 20, 30, 30],  # expected_first_round_values
            20,  # expected_first_round_peek
            [50, 50, 60, 60],  # expected_second_round_values
            50,  # expected_second_round_peek
            [50, 50, 60, 60],  # expected_third_round_values
            50,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            None,  # window
            [300, 300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [600, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            False,  # reduce_per_index
            True,  # clear_on_reduce
            4,  # window
            [200, 200, 300, 300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [500, 500, 600, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            None,  # window
            [300, 300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [300, 600, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [600, 700, 700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
        (
            "max",  # reduce_method
            False,  # reduce_per_index
            False,  # clear_on_reduce
            4,  # window
            [200, 200, 300, 300],  # expected_first_round_values
            300,  # expected_first_round_peek
            [500, 500, 600, 600],  # expected_second_round_values
            600,  # expected_second_round_peek
            [600, 600, 700, 700],  # expected_third_round_values
            700,  # expected_third_round_peek
        ),
    ],
)
def test_aggregation_multiple_rounds(
    reduce_method,
    reduce_per_index,
    clear_on_reduce,
    window,
    expected_first_round_values,
    expected_first_round_peek,
    expected_second_round_values,
    expected_second_round_peek,
    expected_third_round_values,
    expected_third_round_peek,
):
    """Test reduce_per_index_on_aggregate with different reduction methods, clear_on_reduce,  setting."""
    # First round: Create and fill two stats objects
    incoming_stats1 = Stats(
        reduce=reduce_method,
        window=window,
        clear_on_reduce=clear_on_reduce,
        reduce_per_index_on_aggregate=reduce_per_index,
    )
    incoming_stats1.push(10)
    incoming_stats1.push(20)
    incoming_stats1.push(30)

    incoming_stats2 = Stats(
        reduce=reduce_method,
        window=window,
        clear_on_reduce=clear_on_reduce,
        reduce_per_index_on_aggregate=reduce_per_index,
    )
    incoming_stats2.push(100)
    incoming_stats2.push(200)
    incoming_stats2.push(300)

    # First merge
    # Use compile=False to simulate how we use stats in the MetricsLogger
    incoming_stats1_reduced = incoming_stats1.reduce(compile=False)
    incoming_stats2_reduced = incoming_stats2.reduce(compile=False)
    result_stats = merge_stats(
        base_stats=None,
        incoming_stats=[incoming_stats1_reduced, incoming_stats2_reduced],
    )

    # Verify first merge results
    check(
        result_stats.values, expected_first_round_values, atol=1e-2
    )  # Tolerance for EMA calculation
    check(result_stats.peek(), expected_first_round_peek, atol=1e-2)
    result_stats.reduce(compile=True)

    # Second round: Add more values to original stats
    incoming_stats1.push(40)
    incoming_stats1.push(50)
    incoming_stats1.push(60)

    incoming_stats2.push(400)
    incoming_stats2.push(500)
    incoming_stats2.push(600)

    # Second merge
    incoming_stats1_reduced = incoming_stats1.reduce(compile=False)
    incoming_stats2_reduced = incoming_stats2.reduce(compile=False)
    result_stats = merge_stats(
        base_stats=result_stats,
        incoming_stats=[incoming_stats1_reduced, incoming_stats2_reduced],
    )
    # Verify second merge results
    check(result_stats.values, expected_second_round_values, atol=1e-2)
    check(result_stats.peek(), expected_second_round_peek, atol=1e-2)
    result_stats.reduce(compile=True)

    # Third round: Add only one value to one stats object
    incoming_stats2.push(700)

    # Third merge
    incoming_stats1_reduced = incoming_stats1.reduce(compile=False)
    incoming_stats2_reduced = incoming_stats2.reduce(compile=False)
    result_stats = merge_stats(
        base_stats=result_stats,
        incoming_stats=[incoming_stats1_reduced, incoming_stats2_reduced],
    )
    # Verify third merge results
    check(result_stats.values, expected_third_round_values, atol=1e-2)
    check(result_stats.peek(), expected_third_round_peek, atol=1e-2)
    result_stats.reduce(compile=True)


def test_merge_in_parallel_empty_and_nan_values():
    """Test the merge_in_parallel method with empty and NaN value stats."""

    # Root stat and all other stats are empty/nan
    empty_stats = Stats(init_values=[])
    empty_stats2 = Stats(init_values=[])
    nan_stats = Stats(init_values=[np.nan])
    empty_stats.merge_in_parallel(empty_stats, empty_stats2, nan_stats)
    # Root stat should remain empty
    check(empty_stats.values, [])

    # Root stat has values but others are empty or NaN
    empty_stats = Stats(init_values=[])
    nan_stats = Stats(init_values=[np.nan])
    stats_with_values = Stats(init_values=[1.0, 2.0])
    original_values = stats_with_values.values.copy()
    stats_with_values.merge_in_parallel(empty_stats, nan_stats)
    # Values should remain unchanged since all other stats are filtered out
    check(stats_with_values.values, original_values)

    # Root stat is empty but one other stat has values
    empty_stats3 = Stats(init_values=[])
    stats_with_values2 = Stats(init_values=[3.0, 4.0])
    empty_stats3.merge_in_parallel(stats_with_values2)
    # empty_stats3 should now have stats_with_values2's values
    check(empty_stats3.values, stats_with_values2.values)

    # Root stat has NaN and other stat has values
    nan_stats3 = Stats(init_values=[np.nan])
    stats_with_values3 = Stats(init_values=[5.0, 6.0])
    nan_stats3.merge_in_parallel(stats_with_values3)
    # nan_stats3 should now have stats_with_values3's values
    check(nan_stats3.values, stats_with_values3.values)


def test_percentiles():
    """Test that percentiles work correctly.

    We don't test percentiles as part of aggregation tests because it is not compabible
    with `reduce_per_index_on_parallel_merge` only used for reduce=None.
    """
    # Test basic functionality with single stats
    # Use values 0-9 to make percentile calculations easy to verify
    stats = Stats(reduce=None, percentiles=True, window=10)
    for i in range(10):
        stats.push(i)

    # Values should be sorted when peeking
    check(stats.peek(compile=False), list(range(10)))

    # Test with window constraint - push one more value
    stats.push(10)

    # Window is 10, so the oldest value (0) should be dropped
    check(stats.peek(compile=False), list(range(1, 11)))

    # Test reduce
    check(stats.reduce(compile=False).values, list(range(1, 11)))

    # Check with explicit percentiles
    del stats
    stats = Stats(reduce=None, percentiles=[0, 50], window=10)
    for i in range(10)[::-1]:
        stats.push(i)

    check(stats.peek(compile=False), list(range(10)))
    check(stats.peek(compile=True), {0: 0, 50: 4.5})

    # Test merge_in_parallel with easy-to-calculate values
    stats1 = Stats(reduce=None, percentiles=True, window=20)
    # Push values 0, 2, 4, 6, 8 (even numbers 0-8)
    for i in range(0, 10, 2):
        stats1.push(i)
    check(stats1.reduce(compile=False).values, [0, 2, 4, 6, 8])

    stats2 = Stats(reduce=None, percentiles=True, window=20)
    # Push values 1, 3, 5, 7, 9 (odd numbers 1-9)
    for i in range(1, 10, 2):
        stats2.push(i)
    check(stats2.reduce(compile=False).values, [1, 3, 5, 7, 9])

    merged_stats = Stats(reduce=None, percentiles=True, window=20)
    merged_stats.merge_in_parallel(stats1, stats2)
    # Should merge and sort values from both stats
    # Merged values should be sorted: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    expected_merged = list(range(10))
    check(merged_stats.values, expected_merged)
    check(merged_stats.peek(compile=False), expected_merged)

    # Test compiled percentiles with numpy as reference
    expected_percentiles = np.percentile(expected_merged, [0, 50, 75, 90, 95, 99, 100])
    compiled_percentiles = merged_stats.peek(compile=True)

    # Check that our percentiles match numpy's calculations
    check(compiled_percentiles[0], expected_percentiles[0])  # 0th percentile
    check(compiled_percentiles[50], expected_percentiles[1])  # 50th percentile
    check(compiled_percentiles[75], expected_percentiles[2])  # 75th percentile
    check(compiled_percentiles[90], expected_percentiles[3])  # 90th percentile
    check(compiled_percentiles[95], expected_percentiles[4])  # 95th percentile
    check(compiled_percentiles[99], expected_percentiles[5])  # 99th percentile
    check(compiled_percentiles[100], expected_percentiles[6])  # 100th percentile

    # Test validation - window required
    with pytest.raises(ValueError, match="A window must be specified"):
        Stats(reduce=None, percentiles=True, window=None)

    # Test validation - percentiles must be a list
    with pytest.raises(ValueError, match="must be a list or bool"):
        Stats(reduce=None, percentiles=0.5, window=5)

    # Test validation - percentiles must contain numbers
    with pytest.raises(ValueError, match="must contain only ints or floats"):
        Stats(reduce=None, window=5, percentiles=["invalid"])

    # Test validation - percentiles must be between 0 and 100
    with pytest.raises(ValueError, match="must contain only values between 0 and 100"):
        Stats(reduce=None, window=5, percentiles=[-1, 50, 101])

    # Test validation - percentiles must be None for other reduce methods
    with pytest.raises(
        ValueError, match="`reduce` must be `None` when `percentiles` is not `False`"
    ):
        Stats(reduce="mean", window=5, percentiles=[50])

    with pytest.raises(
        ValueError,
        match=re.escape(
            "`reduce_per_index_on_aggregate` (True) must be `False` "
            "when `percentiles` is not `False`!"
        ),
    ):
        Stats(
            reduce=None, reduce_per_index_on_aggregate=True, percentiles=True, window=5
        )


def test_set_state_complete_replacement():
    """Test that set_state() completely replaces the logger's state.

    This test verifies the fix for the issue where set_state() would only update
    keys present in the new state but leave old keys intact, causing stale data
    to persist after checkpoint restoration.
    """
    # Test case 1: Basic replacement with fewer keys
    logger1 = MetricsLogger()
    logger1.log_value("solo", 0)
    logger1.log_value("duo", 0)

    logger2 = MetricsLogger()
    logger2.log_value("duo", 1)

    # Before fix: {'solo': 0, 'duo': 1} - 'solo' would persist
    # After fix: {'duo': 1} - only new state keys remain
    logger1.set_state(logger2.get_state())
    result = logger1.peek()
    expected = {"duo": 1}

    check(result, expected)

    # Test case 2: Complete replacement with different keys
    logger3 = MetricsLogger()
    logger3.log_value("old_key1", 10)
    logger3.log_value("old_key2", 20)
    logger3.log_value("shared_key", 30)

    logger4 = MetricsLogger()
    logger4.log_value("shared_key", 100)
    logger4.log_value("new_key", 200)

    logger3.set_state(logger4.get_state())
    result = logger3.peek()
    expected = {"shared_key": 100, "new_key": 200}

    check(result, expected)

    # Test case 3: Setting to empty state
    logger5 = MetricsLogger()
    logger5.log_value("key1", 1)
    logger5.log_value("key2", 2)

    empty_logger = MetricsLogger()
    logger5.set_state(empty_logger.get_state())
    result = logger5.peek()

    check(result, {})

    # Test case 4: Nested keys
    logger6 = MetricsLogger()
    logger6.log_value(("nested", "old_key"), 1)
    logger6.log_value(("nested", "shared_key"), 2)
    logger6.log_value("top_level", 3)

    logger7 = MetricsLogger()
    logger7.log_value(("nested", "shared_key"), 20)
    logger7.log_value(("nested", "new_key"), 30)

    logger6.set_state(logger7.get_state())
    result = logger6.peek()
    expected = {"nested": {"shared_key": 20, "new_key": 30}}

    check(result, expected)

    # Test case 5: Multiple set_state calls (simulating multiple restore_from_path calls)
    logger8 = MetricsLogger()
    logger8.log_value("initial", 0)

    # First set_state
    temp1 = MetricsLogger()
    temp1.log_value("first", 1)
    temp1.log_value("shared", 100)
    logger8.set_state(temp1.get_state())

    # Second set_state - should completely replace first state
    temp2 = MetricsLogger()
    temp2.log_value("second", 2)
    temp2.log_value("shared", 20)
    logger8.set_state(temp2.get_state())

    result = logger8.peek()
    expected = {"second": 2, "shared": 20}

    check(result, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
