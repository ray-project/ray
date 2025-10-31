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
    SeriesStats,
)
from ray.rllib.utils.framework import try_import_torch

from ray.rllib.utils.test_utils import check

torch, _ = try_import_torch()


def get_device(use_gpu):
    """Helper to get device based on GPU availability and test parameter."""
    if use_gpu:
        if not torch.cuda.is_available():
            pytest.skip("GPU not available")
        return torch.device("cuda")
    return torch.device("cpu")


@pytest.mark.parametrize(
    "stats_class,init_kwargs_list,setup_values,expected_reduced",
    [
        (ItemStats, [{}], [5], 5),
        (MeanStats, [{"window": 4}, {}], [2, 4, 6], 4.0),
        (MaxStats, [{"window": 4}, {}], [1, 5, 3], 5),
        (MinStats, [{"window": 4}, {}], [1, 5, 3], 1),
        (SumStats, [{"window": 4}, {}], [1, 5, 3], 9),
        (LifetimeSumStats, [{}], [10, 20], 30),
        (EmaStats, [{"ema_coeff": 0.01}], [10, 20], 10.1),
    ],
)
@pytest.mark.parametrize("use_gpu", [False, True])
def test_peek_and_reduce(
    stats_class, init_kwargs_list, setup_values, expected_reduced, use_gpu
):
    for init_kwargs in init_kwargs_list:
        stats = stats_class(**init_kwargs)
        for value in setup_values:
            stats.push(value)

        check(stats.peek(), expected_reduced)
        result = stats.reduce(compile=True)
        check(result, expected_reduced)

        if stats_class != LifetimeSumStats:
            # After clear, peek should return default value
            if stats_class == ItemStats:
                expected_cleared = None

            else:
                expected_cleared = np.nan
            check(stats.peek(), expected_cleared)

        # Test with PyTorch tensors of different dtypes (for numeric stats only)
        if torch is not None and stats_class is not ItemStats:
            device = get_device(use_gpu)
            dtypes_to_test = [
                torch.float32,
                torch.float64,
                torch.int32,
                torch.int64,
                torch.float16,
            ]

            for dtype in dtypes_to_test:
                if dtype == torch.float16 and stats_class is EmaStats:
                    # float16 values are less precise and errors add up quickly when calculating EMA
                    decimals = 1
                else:
                    decimals = 5
                tensor_stats = stats_class(**init_kwargs)
                for val in setup_values:
                    tensor_val = torch.tensor(val, dtype=dtype, device=device)
                    tensor_stats.push(tensor_val)

                # Verify tensors stay on device before reduce
                if isinstance(tensor_stats, SeriesStats) or isinstance(
                    tensor_stats, PercentilesStats
                ):
                    for value in tensor_stats.values:
                        if torch and isinstance(value, torch.Tensor):
                            assert value.device.type == device.type
                elif (
                    isinstance(tensor_stats, EmaStats)
                    and torch
                    and isinstance(tensor_stats._value, torch.Tensor)
                ):
                    assert tensor_stats._value.device.type == device.type
                elif (
                    isinstance(tensor_stats, LifetimeSumStats)
                    and torch
                    and isinstance(tensor_stats._lifetime_sum, torch.Tensor)
                ):
                    assert tensor_stats._lifetime_sum.device.type == device.type

                result = tensor_stats.reduce(compile=True)
                assert isinstance(result, (int, float))
                check(result, expected_reduced, decimals=decimals)

                tensor_stats_with_nan = stats_class(**init_kwargs)

                if stats_class is not LifetimeSumStats:
                    # Test with some NaN values mixed in
                    for val in setup_values:
                        tensor_val = torch.tensor(val, dtype=dtype, device=device)
                        tensor_stats_with_nan.push(tensor_val)

                    nan_tensor_val = torch.tensor(float("nan"), device=device)
                    tensor_stats_with_nan.push(nan_tensor_val)

                    result_with_nan = tensor_stats_with_nan.reduce(compile=True)
                    # Result should still be valid (stats should handle NaN)
                    assert isinstance(result_with_nan, (int, float))
                    check(result_with_nan, expected_reduced, decimals=decimals)


@pytest.mark.parametrize("use_gpu", [False, True])
def test_peek_and_reduce_percentiles_stats(use_gpu):
    # Test with regular Python values
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    stats = PercentilesStats(percentiles=[0, 50, 100], window=10)
    for value in values:
        stats.push(value)

    check(stats.peek(compile=False), values)
    check(stats.peek(compile=True), {0: 1, 50: 5.5, 100: 10})
    result = stats.reduce(compile=True)
    check(result, {0: 1, 50: 5.5, 100: 10})

    # Test with PyTorch tensors on the specified device
    if torch is not None:
        device = get_device(use_gpu)
        dtypes_to_test = [
            torch.float32,
            torch.float64,
            torch.int32,
            torch.int64,
            torch.float16,
        ]

        for dtype in dtypes_to_test:
            tensor_stats = PercentilesStats(percentiles=[0, 50, 100], window=10)

            for val in values:
                tensor_val = torch.tensor(val, dtype=dtype, device=device)
                tensor_stats.push(tensor_val)

            # Verify tensors stay on device before reduce
            for value in tensor_stats.values:
                if torch and isinstance(value, torch.Tensor):
                    assert value.device.type == device.type

            result = tensor_stats.reduce(compile=True)
            # Check the percentile values with tolerance
            check(result[0], 1, decimals=1)
            check(result[50], 5.5, decimals=1)
            check(result[100], 10, decimals=1)


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
    root_stats = stats_class(**init_kwargs, is_root=True)

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
    root_stats = ItemStats(is_root=True)

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
        (LifetimeSumStats, {"is_root": True}),
        (EmaStats, {"ema_coeff": 0.1}),
        (PercentilesStats, {"percentiles": [50], "window": 10}),
        (ItemSeriesStats, {"window": 5}),
    ],
)
def test_clone(stats_class, init_kwargs):
    original = stats_class(**init_kwargs)
    # Skip pushing for root stats (they can't be pushed to)
    if not original.is_root:
        original.push(123)

    # Create similar stats
    similar = stats_class.clone(original)

    # Check class-specific attributes
    # Note: PercentilesStats._get_init_args() doesn't preserve window (implementation issue)
    if hasattr(original, "_window") and original._window:
        check(similar._window, original._window)
    if hasattr(original, "_ema_coeff"):
        check(similar._ema_coeff, original._ema_coeff)
    if hasattr(original, "_percentiles"):
        check(similar._percentiles, original._percentiles)
    if hasattr(original, "is_root"):
        check(similar.is_root, original.is_root)

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
    "stats_class,init_kwargs,child1_values,child2_values,expected_result",
    [
        (
            EmaStats,
            {},
            [1, 2],  # 1.01
            [3, 4],  # 3.01
            2.01,  # mean of [1.01, 3.01]
        ),
        (
            MeanStats,
            {"window": None},
            [1, 2],
            [3, 4],
            2.5,  # mean of [1, 2, 3, 4]
        ),
        (
            SumStats,
            {"window": 5},
            [1, 2, 3],
            [4, 5, 6],
            21,  # sum of [1, 2, 3, 4, 5, 6]
        ),
        (
            MinStats,
            {"window": 5},
            [1, 2],
            [3, 4],
            1,  # min of [1, 2, 3, 4]
        ),
        (
            MaxStats,
            {"window": 5},
            [1, 2],
            [3, 4],
            4,  # max of [1, 2, 3, 4]
        ),
        (
            LifetimeSumStats,
            {},
            [10, 20],
            [30, 40],
            100,  # 10 + 20 + 30 + 40
        ),
        (
            ItemSeriesStats,
            {"window": 10},
            ["a", "b"],
            ["c", "d"],
            ["a", "b", "c", "d"],  # ["a", "b", "c", "d", "e", "f"]
        ),
        (
            PercentilesStats,
            {"window": 3, "percentiles": [50]},
            [1, 2],
            [3, 4],
            {50: 2.5},  # 50th percentile (median) of [1, 2, 3, 4]
        ),
    ],
)
def test_stats_merge(
    stats_class,
    init_kwargs,
    child1_values,
    child2_values,
    expected_result,
):
    """Test Stats.merge() for various stats types.

    Root stats cannot be pushed to, so they only include values from merged child stats.
    """
    # Create root stats
    root_stats = stats_class(**init_kwargs, is_root=True)

    # Create first child stats
    child1 = stats_class(**init_kwargs)
    for value in child1_values:
        child1.push(value)

    # Create second child stats
    child2 = stats_class(**init_kwargs)
    for value in child2_values:
        child2.push(value)

    # Merge
    root_stats.merge([child1, child2])

    # Check result
    check(root_stats.peek(), expected_result)


@pytest.mark.parametrize(
    "stats_class,kwargs,expected_first,expected_first_compile_false,expected_second_normal,expected_second_latest,expected_second_compile_false",
    [
        (
            MeanStats,
            {},
            2.5,
            [2.5],
            10.0,
            20.0,
            [20.0],
        ),
        (
            EmaStats,
            {"ema_coeff": 0.1},
            2.1,  # mean of EMA values [1.1, 3.1] from first merge
            [2.1],
            11.3,  # mean of all EMA values [1.1, 3.1, 11.0, 30.0] (approximate)
            20.5,  # mean of [11.0, 30.0] (second merge)
            [20.5],
        ),
        (
            ItemSeriesStats,
            {"window": 10},
            [1.0, 2.0, 3.0, 4.0],
            [
                1.0,
                2.0,
                3.0,
                4.0,
            ],  # compile=False same as compile=True for ItemSeriesStats
            [1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0],
            [10.0, 20.0, 30.0],
            [
                10.0,
                20.0,
                30.0,
            ],  # compile=False same as compile=True for ItemSeriesStats
        ),
        (
            PercentilesStats,
            {"window": 10},
            {
                0: 1.0,
                50: 2.5,
                75: 3.25,
                90: 3.7,
                95: 3.85,
                99: 3.97,
                100: 4.0,
            },  # percentiles of [1, 2, 3, 4]
            [1.0, 2.0, 3.0, 4.0],  # compile=False returns sorted list of values
            {
                0: 1.0,
                50: 4.0,
                75: 15.0,
                90: 24.0,
                95: 27.0,
                99: 29.4,
                100: 30.0,
            },  # percentiles of [1, 2, 3, 4, 10, 20, 30]
            {
                0: 10.0,
                50: 20.0,
                75: 25.0,
                90: 28.0,
                95: 29.0,
                99: 29.8,
                100: 30.0,
            },  # percentiles of [10, 20, 30]
            [10.0, 20.0, 30.0],  # compile=False returns sorted list of values
        ),
        (
            LifetimeSumStats,
            {},
            10.0,
            [10.0],
            70.0,
            60.0,
            [60.0],
        ),
    ],
)
def test_latest_merged_only_stats_types(
    stats_class,
    kwargs,
    expected_first,
    expected_first_compile_false,
    expected_second_normal,
    expected_second_latest,
    expected_second_compile_false,
):
    """Test latest_merged_only parameter for various Stats types."""
    # Each batch has values for two child stats
    first_batch_values = [[1.0, 2.0], [3.0, 4.0]]
    second_batch_values = [[10.0, 20.0], [30.0]]

    root_stats = stats_class(**kwargs, is_root=True)

    first_batch_stats = []
    for values in first_batch_values:
        child_stats = stats_class(**kwargs)
        for value in values:
            child_stats.push(value)
        first_batch_stats.append(child_stats)

    root_stats.merge(first_batch_stats)

    # Normal peek should include all merged values
    first_normal_result = root_stats.peek(compile=True, latest_merged_only=False)
    check(first_normal_result, expected_first)
    # Latest merged only should only consider the latest merge (same as normal after first merge)
    first_latest_result = root_stats.peek(compile=True, latest_merged_only=True)
    check(first_latest_result, expected_first)

    # Test compile=False behavior after first merge
    first_latest_result_compile_false = root_stats.peek(
        compile=False, latest_merged_only=True
    )
    check(first_latest_result_compile_false, expected_first_compile_false)

    # Create and merge second batch
    second_batch_stats = []
    for values in second_batch_values:
        child_stats = stats_class(**kwargs)
        for value in values:
            child_stats.push(value)
        second_batch_stats.append(child_stats)

    root_stats.merge(second_batch_stats)

    # Normal peek should include all values
    second_normal_result = root_stats.peek(compile=True, latest_merged_only=False)
    check(second_normal_result, expected_second_normal)

    # Latest merged only should only consider the latest merge
    second_latest_result = root_stats.peek(compile=True, latest_merged_only=True)
    check(second_latest_result, expected_second_latest)

    # Test compile=False behavior after second merge
    second_latest_result_compile_false = root_stats.peek(
        compile=False, latest_merged_only=True
    )
    check(second_latest_result_compile_false, expected_second_compile_false)


def test_latest_merged_only_no_merge_yet():
    """Test latest_merged_only when no merge has occurred yet."""
    root_stats = MeanStats(window=10, is_root=True)

    # Before any merge, latest_merged_only should return NaN
    result = root_stats.peek(compile=True, latest_merged_only=True)
    check(np.isnan(result), True)

    # Normal peek should also return NaN for empty stats
    result = root_stats.peek(compile=True, latest_merged_only=False)
    check(np.isnan(result), True)


def test_latest_merged_only_non_root_stats():
    """Test that latest_merged_only raises error on non-root stats."""
    stats = MeanStats(window=10)
    stats.push(1.0)

    # Should raise error when using latest_merged_only on non-root stats
    with pytest.raises(
        ValueError, match="latest_merged_only can only be used on root stats"
    ):
        stats.peek(compile=True, latest_merged_only=True)


def test_push_on_root_stats():
    """Test that push raises error on root stats."""
    for cls in [
        MeanStats,
        EmaStats,
        ItemSeriesStats,
        PercentilesStats,
        LifetimeSumStats,
        ItemStats,
        SumStats,
        MaxStats,
        MinStats,
    ]:
        root_stats = cls(is_leaf=False)

    # Should raise error when trying to push to root stats
    with pytest.raises(
        ValueError, match="Cannot push values to non-leaf stats objects"
    ):
        root_stats.push(1.0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
