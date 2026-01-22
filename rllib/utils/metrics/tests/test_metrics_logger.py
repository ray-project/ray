import time

import numpy as np
import pytest

from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.metrics.stats import (
    EmaStats,
    LifetimeSumStats,
    MeanStats,
    SumStats,
)
from ray.rllib.utils.test_utils import check


@pytest.fixture
def root_logger():
    return MetricsLogger(root=True)


@pytest.fixture
def leaf1():
    return MetricsLogger(root=False)


@pytest.fixture
def leaf2():
    return MetricsLogger(root=False)


@pytest.fixture
def intermediate():
    return MetricsLogger(root=False)


@pytest.mark.parametrize(
    "reduce_method,values,expected",
    [
        ("mean", [0.1, 0.2], 0.15),
        ("min", [0.3, 0.1, 0.2], 0.1),
        ("sum", [10, 20], 30),
        ("lifetime_sum", [10, 20], 30),
        ("ema", [1.0, 2.0], 1.01),
        ("item", [0.1, 0.2], 0.2),
        ("item_series", [0.1, 0.2], [0.1, 0.2]),
    ],
)
def test_basic_peek_and_reduce(root_logger, reduce_method, values, expected):
    """Test different reduction methods (mean, min, sum) with parameterization."""
    key = f"{reduce_method}_metric"

    for val in values:
        root_logger.log_value(key, val, reduce=reduce_method)

    # Check the result
    check(root_logger.peek(key), expected)

    # Test that reduce() returns the same result
    results = root_logger.reduce()
    check(results[key], expected)


@pytest.mark.parametrize(
    "reduce_method,leaf1_values,leaf2_values,intermediate_values,"
    "leaf1_expected,leaf2_expected,intermediate_expected_after_aggregate,"
    "intermediate_expected_after_log,root_expected_leafs,root_expected_intermediate",
    [
        # MeanStats
        (
            "mean",  # reduction method name
            [1.0, 2.0],  # values logged to leaf1 logger
            [3.0, 4.0],  # values logged to leaf2 logger
            [5.0, 6.0],  # values logged at intermediate logger
            1.5,  # result from leaf1 after logging (mean of [1, 2])
            3.5,  # result from leaf2 after logging (mean of [3, 4])
            2.5,  # result at intermediate after aggregating from leafs (mean of [1.5, 3.5])
            5.5,  # result at intermediate after logging values (mean of [5.0, 6.0])
            2.5,  # result at root from aggregated leafs (mean of [1.5, 3.5])
            5.5,  # result at root from intermediate logged values (mean of [5.0, 6.0])
        ),
        # EmaStats with default coefficient (0.01)
        (
            "ema",  # reduction method name
            [1.0, 2.0],  # values logged to leaf1 logger
            [3.0, 4.0],  # values logged to leaf2 logger
            [5.0, 6.0],  # values logged at intermediate logger
            1.01,  # result from leaf1 after logging (EMA of [1, 2] with coeff 0.01)
            3.01,  # result from leaf2 after logging (EMA of [3, 4] with coeff 0.01)
            2.01,  # result at intermediate after aggregating from leafs (mean of [1.01, 3.01])
            5.01,  # result at intermediate after logging values (EMA of [5.0, 6.0] with coeff 0.01)
            2.01,  # result at root from aggregated leafs (mean of [1.01, 3.01])
            5.01,  # result at root from intermediate logged values (EMA of [5.0, 6.0] with coeff 0.01)
        ),
        # SumStats
        (
            "sum",  # reduction method name
            [10, 20],  # values logged to leaf1 logger
            [30, 40],  # values logged to leaf2 logger
            [50, 60],  # values logged at intermediate logger
            30,  # result from leaf1 after logging (sum of [10, 20])
            70,  # result from leaf2 after logging (sum of [30, 40])
            100,  # result at intermediate after aggregating from leafs (sum of [30, 70])
            110,  # result at intermediate after logging values (sum of [50, 60])
            100,  # result at root from aggregated leafs (sum of [30, 70])
            110,  # result at root from intermediate logged values (sum of [50, 60])
        ),
        # LifetimeSumStats
        (
            "lifetime_sum",  # reduction method name
            [10, 20],  # values logged to leaf1 logger
            [30, 40],  # values logged to leaf2 logger
            [50, 60],  # values logged at intermediate logger
            [
                30
            ],  # result from leaf1 after logging (lifetime sum of [10, 20], returns list)
            [
                70
            ],  # result from leaf2 after logging (lifetime sum of [30, 40], returns list)
            [
                100
            ],  # result at intermediate after aggregating from leafs (sum of [30, 70], returns list)
            [
                110
            ],  # result at intermediate after logging values (sum of [50, 60], returns list)
            100,  # result at root from aggregated leafs (root logger converts list to scalar)
            110,  # result at root from intermediate logged values (root logger converts list to scalar)
        ),
        # MinStats
        (
            "min",  # reduction method name
            [5.0, 3.0],  # values logged to leaf1 logger
            [4.0, 2.0],  # values logged to leaf2 logger
            [1.0, 0.5],  # values logged at intermediate logger
            3.0,  # result from leaf1 after logging (min of [5.0, 3.0])
            2.0,  # result from leaf2 after logging (min of [4.0, 2.0])
            2.0,  # result at intermediate after aggregating from leafs (min of [3.0, 2.0])
            0.5,  # result at intermediate after logging values (min of [1.0, 0.5])
            2.0,  # result at root from aggregated leafs (min of [3.0, 2.0])
            0.5,  # result at root from intermediate logged values (min of [1.0, 0.5])
        ),
        # MaxStats
        (
            "max",  # reduction method name
            [5.0, 7.0],  # values logged to leaf1 logger
            [4.0, 6.0],  # values logged to leaf2 logger
            [8.0, 9.0],  # values logged at intermediate logger
            7.0,  # result from leaf1 after logging (max of [5.0, 7.0])
            6.0,  # result from leaf2 after logging (max of [4.0, 6.0])
            7.0,  # result at intermediate after aggregating from leafs (max of [7.0, 6.0])
            9.0,  # result at intermediate after logging values (max of [8.0, 9.0])
            7.0,  # result at root from aggregated leafs (max of [7.0, 6.0])
            9.0,  # result at root from intermediate logged values (max of [8.0, 9.0])
        ),
        # PercentilesStats
        (
            "percentiles",  # reduction method name
            [10.0, 20.0],  # values logged to leaf1 logger
            [30.0, 40.0],  # values logged to leaf2 logger
            [50.0, 60.0],  # values logged at intermediate logger
            {
                0.5: 10.05
            },  # result from leaf1 after logging (percentile 0.5 of [10.0, 20.0])
            {
                0.5: 30.05
            },  # result from leaf2 after logging (percentile 0.5 of [30.0, 40.0])
            {
                0.5: 10.15
            },  # result at intermediate after aggregating from leafs (percentile 0.5 of merged [10.0, 20.0, 30.0, 40.0])
            {
                0.5: 50.05
            },  # result at intermediate after logging values (percentile 0.5 of [50.0, 60.0])
            {
                0.5: 10.15
            },  # result at root from aggregated leafs (same as intermediate after aggregate)
            {
                0.5: 50.05
            },  # result at root from intermediate logged values (percentile 0.5 of [50.0, 60.0])
        ),
        # ItemSeriesStats
        (
            "item_series",  # reduction method name
            [1.0, 2.0],  # values logged to leaf1 logger
            [3.0, 4.0],  # values logged to leaf2 logger
            [5.0, 6.0],  # values logged at intermediate logger
            [
                1.0,
                2.0,
            ],  # result from leaf1 after logging (series of [1.0, 2.0])
            [
                3.0,
                4.0,
            ],  # result from leaf2 after logging (series of [3.0, 4.0])
            [
                1.0,
                2.0,
                3.0,
                4.0,
            ],  # result at intermediate after aggregating from leafs (concatenated series from leafs)
            [
                5.0,
                6.0,
            ],  # result at intermediate after logging values (series of [5.0, 6.0])
            [
                1.0,
                2.0,
                3.0,
                4.0,
            ],  # result at root from aggregated leafs (concatenated series from leafs)
            [
                5.0,
                6.0,
            ],  # result at root from intermediate logged values (series of [5.0, 6.0])
        ),
    ],
)
def test_multi_stage_aggregation(
    root_logger,
    leaf1,
    leaf2,
    intermediate,
    reduce_method,
    leaf1_values,
    leaf2_values,
    intermediate_values,
    leaf1_expected,
    leaf2_expected,
    intermediate_expected_after_aggregate,
    intermediate_expected_after_log,
    root_expected_leafs,
    root_expected_intermediate,
):
    """Test multi-stage aggregation for different Stats classes.

    This is a comprehensive test of how we envision MetricsLogger to be used in RLlib.
    It also creates a bunch of test coverage for Stats classes, which are tighly cloupled with MetricsLogger.

    Tests the aggregation flow:
    1. Two leaf loggers log values
    2. One intermediate logger aggregates from leaf loggers and logs values
    3. One root logger aggregates only
    """
    metric_name_leafs = reduce_method + "_metric_leaf"
    metric_name_intermediate = reduce_method + "_metric_intermediate"

    # Helper function to check values (handles PercentilesStats specially)
    def check_value(actual, expected):
        if reduce_method == "percentiles":
            # If actual is a PercentilesStats object (from reduce(compile=False)), call peek() to get dict
            if hasattr(actual, "peek"):
                actual = actual.peek()
            assert isinstance(actual, dict)
            assert 0.5 in actual
            if expected is not None:
                check(actual[0.5], expected[0.5], atol=0.01)
        elif expected is not None:
            check(actual, expected)

    # Leaf stage
    # Prepare kwargs for PercentileStats if needed
    if reduce_method == "percentiles":
        log_kwargs = {"window": 10, "percentiles": [0.5]}
    else:
        log_kwargs = {}

    for val in leaf1_values:
        leaf1.log_value(metric_name_leafs, val, reduce=reduce_method, **log_kwargs)
    for val in leaf2_values:
        leaf2.log_value(metric_name_leafs, val, reduce=reduce_method, **log_kwargs)

    check_value(leaf1.peek(metric_name_leafs), leaf1_expected)
    check_value(leaf2.peek(metric_name_leafs), leaf2_expected)

    leaf1_metrics = leaf1.reduce(compile=False)
    leaf2_metrics = leaf2.reduce(compile=False)

    # Intermediate stage
    # Note: For percentiles, intermediate loggers cannot log values directly
    # So we skip intermediate logging for percentiles and only test aggregation
    if reduce_method != "percentiles":
        for val in intermediate_values:
            intermediate.log_value(
                metric_name_intermediate, val, reduce=reduce_method, **log_kwargs
            )

    intermediate.aggregate([leaf1_metrics, leaf2_metrics])
    intermediate_metrics_after_aggregate = intermediate.reduce(compile=False)
    check_value(
        intermediate_metrics_after_aggregate[metric_name_leafs],
        intermediate_expected_after_aggregate,
    )
    if reduce_method != "percentiles":
        check_value(
            intermediate_metrics_after_aggregate[metric_name_intermediate],
            intermediate_expected_after_log,
        )

    # Aggregate at root level
    root_logger.aggregate([intermediate_metrics_after_aggregate])
    root_value_leafs = root_logger.peek(metric_name_leafs)
    check_value(root_value_leafs, root_expected_leafs)
    if reduce_method != "percentiles":
        root_value_intermediate = root_logger.peek(metric_name_intermediate)
        check_value(root_value_intermediate, root_expected_intermediate)


def test_windowed_reduction(root_logger, leaf1, leaf2):
    """Test window-based reduction with various window sizes."""

    # Test window with 'mean' reduction method
    leaf1.log_value("window_loss", 0.1, reduce="mean", window=2)
    leaf1.log_value("window_loss", 0.2)
    leaf1.log_value("window_loss", 0.3)
    leaf2.log_value("window_loss", 0.1, reduce="mean", window=2)
    leaf2.log_value("window_loss", 0.2)
    leaf2.log_value("window_loss", 0.3)

    leaf1_metrics = leaf1.reduce(compile=False)
    leaf2_metrics = leaf2.reduce(compile=False)
    root_logger.aggregate([leaf1_metrics, leaf2_metrics])
    check(root_logger.peek("window_loss"), 0.25)  # mean of [0.2, 0.3]

    # Test window with 'min' reduction method
    leaf1.log_value("window_min", 0.3, reduce="min", window=2)
    leaf1.log_value("window_min", 0.1)
    leaf1.log_value("window_min", 0.2)
    leaf2.log_value("window_min", 0.3, reduce="min", window=2)
    leaf2.log_value("window_min", 0.1)
    leaf2.log_value("window_min", 0.2)

    leaf1_metrics = leaf1.reduce(compile=False)
    leaf2_metrics = leaf2.reduce(compile=False)
    root_logger.aggregate([leaf1_metrics, leaf2_metrics])
    check(root_logger.peek("window_min"), 0.1)  # min of [0.1, 0.2]

    # Test window with 'sum' reduction method
    leaf1.log_value("window_sum", 10, reduce="sum", window=2)
    leaf1.log_value("window_sum", 20)
    leaf1.log_value("window_sum", 30)
    leaf2.log_value("window_sum", 10, reduce="sum", window=2)
    leaf2.log_value("window_sum", 20)
    leaf2.log_value("window_sum", 30)

    leaf1_metrics = leaf1.reduce(compile=False)
    leaf2_metrics = leaf2.reduce(compile=False)
    root_logger.aggregate([leaf1_metrics, leaf2_metrics])
    check(root_logger.peek("window_sum"), 100)  # sum of [20, 30]


def test_nested_keys(root_logger):
    """Test logging with nested key structures."""
    # Test nested key logging
    root_logger.log_value(("nested", "key"), 1.0)
    root_logger.log_value(("nested", "key"), 2.0)

    # Test peek with nested key
    check(root_logger.peek(("nested", "key")), 1.01)

    # Test reduce with nested key
    results = root_logger.reduce()
    check(results["nested"]["key"], 1.01)


def test_time_logging(root_logger):

    # Test time logging with window
    with root_logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.01)
    with root_logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.02)

    check(root_logger.peek("mean_time"), 0.015, atol=0.05)


def test_state_management(root_logger):
    """Test state management (get_state and set_state)."""
    # Log some values
    root_logger.log_value("state_test", 0.1)
    root_logger.log_value("state_test", 0.2)

    # Get state
    state = root_logger.get_state()

    # Create new logger and set state
    new_logger = MetricsLogger()
    new_logger.set_state(state)

    # Check that state was properly transferred
    check(new_logger.peek("state_test"), 0.101)


def test_throughput_tracking(root_logger, leaf1, leaf2):
    """Test throughput tracking functionality."""
    # Override the initialization time to make the test more accurate.
    root_logger._time_when_initialized = time.perf_counter()
    start_time = time.perf_counter()

    leaf1.log_value("value", 1, reduce="sum", with_throughput=True)
    leaf1.log_value("value", 2)
    leaf2.log_value("value", 3, reduce="sum", with_throughput=True)
    leaf2.log_value("value", 4)

    metrics = [leaf1.reduce(compile=False), leaf2.reduce(compile=False)]
    time.sleep(0.1)

    end_time = time.perf_counter()
    throughput = 10 / (end_time - start_time)

    root_logger.aggregate(metrics)
    check(root_logger.peek("value"), 10)
    check(root_logger.stats["value"].throughputs, throughput, rtol=0.1)

    # Test again but now don't initialize time since we are not starting a new experiment.
    leaf1.log_value("value", 5)
    leaf1.log_value("value", 6)
    leaf2.log_value("value", 7)
    leaf2.log_value("value", 8)

    metrics = [leaf1.reduce(compile=False), leaf2.reduce(compile=False)]
    time.sleep(0.1)

    end_time = time.perf_counter()
    throughput = 36 / (end_time - start_time)

    root_logger.aggregate(metrics)
    check(root_logger.peek("value"), 36)
    check(root_logger.peek("value", throughput=True), throughput, rtol=0.1)


def test_reset_and_delete(root_logger):
    """Test reset and delete functionality."""
    # Log some values
    root_logger.log_value("test1", 0.1)
    root_logger.log_value("test2", 0.2)

    # Test delete
    root_logger.delete("test1")
    with pytest.raises(KeyError):
        root_logger.peek("test1")

    # Test reset
    root_logger.reset()
    check(root_logger.reduce(), {})


def test_compile(root_logger):
    """Test the compile method that combines values and throughputs."""
    # Override the initialization time to make the test more accurate.
    root_logger._time_when_initialized = time.perf_counter()
    start_time = time.perf_counter()

    # Log some values with throughput tracking
    root_logger.log_value("count", 1, reduce="sum", with_throughput=True)
    root_logger.log_value("count", 2)

    # Log some nested values with throughput tracking
    root_logger.log_value(
        ["nested", "count"], 1, reduce="lifetime_sum", with_throughput=True
    )
    root_logger.log_value(["nested", "count"], 2)

    # Log some values without throughput tracking
    root_logger.log_value("simple", 1)
    root_logger.log_value("simple", 2)

    time.sleep(0.1)

    end_time = time.perf_counter()
    throughput = 3 / (end_time - start_time)

    # Get compiled results
    compiled = root_logger.compile()

    # Check that values and throughputs are correctly combined
    check(compiled["count"], 3)  # sum of [1, 2]
    check(compiled["count_throughput"], throughput, rtol=0.1)  # initial throughput
    check(compiled["nested"]["count"], 3)  # sum of [1, 2]
    check(
        compiled["nested"]["count_throughput"]["throughput_since_last_reduce"],
        throughput,
        rtol=0.1,
    )  # initial throughput
    check(
        compiled["nested"]["count_throughput"]["throughput_since_last_restore"],
        throughput,
        rtol=0.1,
    )  # initial throughput

    check(compiled["simple"], 1.01)
    assert (
        "simple_throughput" not in compiled
    )  # no throughput for non-throughput metric


def test_peek_with_default(root_logger):
    """Test peek method with default argument."""
    # Test with non-existent key
    check(root_logger.peek("non_existent", default=0.0), 0.0)

    # Test with existing key
    root_logger.log_value("existing", 1.0)
    ret = root_logger.peek("existing", default=0.0)
    check(ret, 1.0)  # Should return actual value, not default


def test_edge_cases(root_logger):
    """Test edge cases and error handling."""
    # Test invalid reduction method
    with pytest.raises(ValueError):
        root_logger.log_value("invalid_reduce", 0.1, reduce="invalid")

    # Test window and ema_coeff together
    with pytest.raises(ValueError):
        root_logger.log_value("invalid_window_ema", 0.1, window=2, ema_coeff=0.1)

    # Test clearing on reduce
    root_logger.log_value("clear_test", 0.1)
    root_logger.log_value("clear_test", 0.2)
    results = root_logger.reduce()
    check(results["clear_test"], 0.101)
    check(root_logger.peek("clear_test"), np.nan)  # Should be cleared


def test_legacy_stats_conversion():
    """Test converting legacy Stats objects to MetricsLogger state dict."""
    from ray.rllib.utils.metrics.legacy_stats import Stats

    # Create a nested structure of legacy Stats objects with various configurations
    legacy_stats = {}

    # 1. Top-level stats with different reduction methods
    # Mean with window
    legacy_stats["mean_metric"] = Stats(
        init_values=[1.0, 2.0, 3.0],
        reduce="mean",
        window=10,
    )

    # Mean with EMA coefficient
    legacy_stats["ema_metric"] = Stats(
        init_values=[5.0, 6.0],
        reduce="mean",
        ema_coeff=0.1,
    )

    # Min with window
    legacy_stats["min_metric"] = Stats(
        init_values=[10.0, 5.0, 15.0],
        reduce="min",
        window=5,
    )

    # Max with window
    legacy_stats["max_metric"] = Stats(
        init_values=[10.0, 25.0, 15.0],
        reduce="max",
        window=5,
    )

    # Sum with window
    legacy_stats["sum_metric"] = Stats(
        init_values=[1.0, 2.0, 3.0],
        reduce="sum",
        window=10,
        clear_on_reduce=True,
    )

    # Lifetime sum (sum with clear_on_reduce=False)
    legacy_stats["lifetime_sum_metric"] = Stats(
        init_values=[10.0, 20.0, 30.0],
        reduce="sum",
        window=None,
        clear_on_reduce=False,
    )

    # 2. Nested stats (one level deep)
    legacy_stats["nested"] = {
        "loss": Stats(
            init_values=[0.5, 0.4, 0.3],
            reduce="mean",
            window=100,
        ),
        "reward": Stats(
            init_values=[10.0, 15.0, 20.0],
            reduce="mean",
            window=50,
        ),
    }

    # Create a MetricsLogger state dict from legacy stats
    def create_state_from_legacy(legacy_stats_dict, prefix=""):
        """Recursively convert legacy stats to MetricsLogger state format."""
        state = {}

        def traverse(d, path_parts):
            for key, value in d.items():
                current_path = path_parts + [key]
                if isinstance(value, Stats):
                    # Convert Stats to state dict
                    flat_key = "--".join(current_path)
                    state[flat_key] = value.get_state()
                elif isinstance(value, dict):
                    # Recurse into nested dict
                    traverse(value, current_path)

        traverse(legacy_stats_dict, [])
        return {"stats": state}

    # Create state dict from legacy stats
    legacy_state_dict = create_state_from_legacy(legacy_stats)

    # Create a new MetricsLogger and load the legacy state
    logger = MetricsLogger(root=False)
    logger.set_state(legacy_state_dict)

    # Verify that values are correctly loaded
    # Check top-level stats
    check(logger.peek("mean_metric"), 2.0)  # mean of [1, 2, 3]
    check(logger.peek("min_metric"), 5.0)  # min of [10, 5, 15]
    check(logger.peek("max_metric"), 25.0)  # max of [10, 25, 15]
    check(logger.peek("sum_metric"), 6.0)  # sum of [1, 2, 3]
    check(
        logger.peek("lifetime_sum_metric"), 0.0
    )  # logger is not a root logger, so lifetime sum is 0

    # Check nested stats
    check(logger.peek(("nested", "loss")), 0.4)  # mean of [0.5, 0.4, 0.3]
    check(logger.peek(("nested", "reward")), 15.0)  # mean of [10, 15, 20]

    # Verify that we can continue logging to the restored logger
    logger.log_value("mean_metric", 4.0, reduce="mean", window=10)
    logger.log_value(("nested", "loss"), 0.2, reduce="mean", window=100)

    # Check that new values are properly integrated
    results = logger.reduce(compile=True)
    assert "mean_metric" in results
    assert "nested" in results
    assert "loss" in results["nested"]


def test_log_dict():
    """Test logging dictionaries of values.

    MetricsLogger.log_dict is a thin wrapper around MetricsLogger.log_value.
    We therefore don't test extensively here.

    Note: log_dict can only be used with non-root loggers. Root loggers can only aggregate.
    """
    # Create a non-root logger for logging values
    logger = MetricsLogger(root=False)

    # Test simple flat dictionary
    flat_dict = {
        "metric1": 1.0,
        "metric2": 2.0,
    }
    logger.log_dict(flat_dict, reduce="mean")

    check(logger.peek("metric1"), 1.0)
    check(logger.peek("metric2"), 2.0)

    # Test logging more values to the same keys
    flat_dict2 = {
        "metric1": 2.0,
        "metric2": 3.0,
    }
    logger.log_dict(flat_dict2, reduce="mean")

    check(logger.peek("metric1"), 1.5)
    check(logger.peek("metric2"), 2.5)


def test_log_dict_root_logger(root_logger):
    """Test that root loggers can use log_dict and create leaf stats."""
    flat_dict = {
        "metric1": 1.0,
        "metric2": 2.0,
    }

    # Root loggers should be able to use log_dict
    root_logger.log_dict(flat_dict, reduce="mean")

    check(root_logger.peek("metric1"), 1.0)
    check(root_logger.peek("metric2"), 2.0)

    # Should be able to push to these leaf stats
    root_logger.log_value("metric1", 2.0)
    check(root_logger.peek("metric1"), 1.5)

    root_logger.log_value("metric3", 3.0)
    check(root_logger.peek("metric3"), 3.0)


def test_compatibility_logic(root_logger):
    """Test compatibility logic that supersedes the 'legacy usage of MetricsLogger' comment."""
    # Test behavior 1: No reduce method + window -> should use mean reduction
    root_logger.log_value("metric_with_window", 1, window=2)
    root_logger.log_value("metric_with_window", 2)
    root_logger.log_value("metric_with_window", 3)
    check(root_logger.peek("metric_with_window"), 2.5)
    assert isinstance(root_logger.stats["metric_with_window"], MeanStats)

    # Test behavior 2: No reduce method (and no window) -> should default to "ema"
    root_logger.log_value("metric_no_reduce", 1.0)
    root_logger.log_value("metric_no_reduce", 2.0)
    check(root_logger.peek("metric_no_reduce"), 1.01)
    assert isinstance(root_logger.stats["metric_no_reduce"], EmaStats)

    # Test behavior 3: reduce=sum + clear_on_reduce=False -> should use lifetime_sum
    root_logger.log_value("metric_lifetime", 10, reduce="sum", clear_on_reduce=False)
    root_logger.log_value("metric_lifetime", 20)
    check(root_logger.peek("metric_lifetime"), 30)
    assert isinstance(root_logger.stats["metric_lifetime"], LifetimeSumStats)

    # Test behavior 4: reduce=sum + clear_on_reduce=True -> should use SumStats (not lifetime_sum)
    root_logger.log_value("metric_sum_clear", 10, reduce="sum", clear_on_reduce=True)
    root_logger.log_value("metric_sum_clear", 20)
    check(root_logger.peek("metric_sum_clear"), 30)
    assert isinstance(root_logger.stats["metric_sum_clear"], SumStats)

    # Test behavior 5: reduce=sum + clear_on_reduce=None -> should use SumStats
    root_logger.log_value("metric_sum_default", 10, reduce="sum")
    root_logger.log_value("metric_sum_default", 20)
    check(root_logger.peek("metric_sum_default"), 30)
    assert isinstance(root_logger.stats["metric_sum_default"], SumStats)

    # Test behavior 6: clear_on_reduce=True with other reduce methods -> should warn but still work
    root_logger.log_value(
        "metric_mean_clear", 1.0, reduce="mean", clear_on_reduce=True, window=5
    )
    root_logger.log_value("metric_mean_clear", 2.0)
    check(root_logger.peek("metric_mean_clear"), 1.5)
    assert isinstance(root_logger.stats["metric_mean_clear"], MeanStats)

    # Test behavior 7: Compatibility logic works with log_dict
    logger = MetricsLogger(root=False)
    logger.log_dict({"metric_dict": 1.0}, window=3)
    logger.log_dict({"metric_dict": 2.0})
    logger.log_dict({"metric_dict": 3.0})
    check(logger.peek("metric_dict"), 2.0)  # mean of [1, 2, 3]
    assert isinstance(logger.stats["metric_dict"], MeanStats)

    # Test behavior 9: Default EMA coefficient (0.01) is used when not specified
    root_logger.log_value("metric_ema_default", 1.0)
    assert root_logger.stats["metric_ema_default"]._ema_coeff == 0.01

    # Test behavior 10: Custom EMA coefficient is preserved
    root_logger.log_value("metric_ema_custom", 1.0, reduce="ema", ema_coeff=0.1)
    assert root_logger.stats["metric_ema_custom"]._ema_coeff == 0.1

    # Test behavior 11: reduce=None with window -> should use mean (not ema)
    root_logger.log_value("metric_none_window", 1.0, reduce=None, window=2)
    root_logger.log_value("metric_none_window", 2.0)
    check(root_logger.peek("metric_none_window"), 1.5)
    assert isinstance(root_logger.stats["metric_none_window"], MeanStats)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
