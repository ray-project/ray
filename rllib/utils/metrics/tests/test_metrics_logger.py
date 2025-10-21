from typing import List

import time

import numpy as np
import pytest

import ray
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.metrics.stats import (
    MeanStats,
    EmaStats,
    MinStats,
    MaxStats,
    SumStats,
    LifetimeSumStats,
    PercentilesStats,
    ItemSeriesStats,
)


@pytest.fixture
def root_logger():
    return MetricsLogger(root=True)


@pytest.fixture
def actors() -> List:
    """Create 2 parallel Ray Actors to log values.

    The Actors simulate parallel components that log values to their own MetricsLogger instance.
    During experiment runtime, these Actors would be EnvRunners, Learners, or any other components that log values to their own MetricsLogger instance.
    """

    @ray.remote
    class Actor:
        def __init__(self):
            self.metrics = MetricsLogger(root=False)

        def log_value(self, name, value, **kwargs):
            self.metrics.log_value(name, value, **kwargs)

        def get_metrics(self):
            return self.metrics.reduce(compile=False)

    return [Actor.remote() for _ in range(2)]


def test_log_value(root_logger):
    """Test basic value logging and reduction."""
    # Test simple value logging
    root_logger.log_value("loss", 0.1)
    root_logger.log_value("loss", 0.2)

    # Test peek
    check(root_logger.peek("loss"), 0.101)

    # Test reduce
    results = root_logger.reduce()
    check(results["loss"], 0.101)


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
def test_basic_reduction_methods(root_logger, reduce_method, values, expected):
    """Test different reduction methods (mean, min, sum) with parameterization."""
    key = f"{reduce_method}_metric"

    for val in values:
        root_logger.log_value(key, val, reduce=reduce_method)

    # Check the result
    check(root_logger.peek(key), expected)

    # Test that reduce() returns the same result
    results = root_logger.reduce()
    check(results[key], expected)


def test_ema(root_logger, actors):
    """Comprehensive test of EMA behavior for mean reduction."""
    # Test default EMA coefficient (0.01)
    actors[0].log_value.remote("default_ema", 1.0, reduce="ema")
    actors[0].log_value.remote("default_ema", 2.0)
    actors[1].log_value.remote("default_ema", 3.0, reduce="ema")
    actors[1].log_value.remote("default_ema", 4.0)

    ema_coeff = 0.02
    actors[0].log_value.remote("custom_ema", 1.0, reduce="ema", ema_coeff=ema_coeff)
    actors[0].log_value.remote("custom_ema", 2.0)
    actors[1].log_value.remote("custom_ema", 3.0, reduce="ema", ema_coeff=ema_coeff)
    actors[1].log_value.remote("custom_ema", 4.0)

    actor0_metrics = ray.get(actors[0].get_metrics.remote())
    actor1_metrics = ray.get(actors[1].get_metrics.remote())

    check(actor0_metrics["default_ema"], 1.01)
    check(actor1_metrics["default_ema"], 3.01)
    check(actor0_metrics["custom_ema"], 1.02)
    check(actor1_metrics["custom_ema"], 3.02)

    root_logger.aggregate([actor0_metrics, actor1_metrics])

    # Values at root logger should now be the mean of the ema logged by the actors.
    check(root_logger.peek("default_ema"), 2.01)
    check(root_logger.peek("custom_ema"), 2.02)

    # Log a series of values to `custom_ema` key and check if EMA approaches the expected value
    values = [5.0] * 1000  # Actual mean is 5.0
    expected = 2.02  # Initial value

    for val in values:
        root_logger.log_value("custom_ema", val)
        # EMA formula: new_ema = (1 - ema_coeff) * old_ema + ema_coeff * new_value
        expected = (1.0 - ema_coeff) * expected + ema_coeff * val
        check(root_logger.peek("custom_ema"), expected)

    check(expected, 5.0, atol=0.05)


def test_windowed_reduction(root_logger, actors):
    """Test window-based reduction with various window sizes."""

    # Test window with 'mean' reduction method
    actors[0].log_value.remote("window_loss", 0.1, reduce="mean", window=2)
    actors[0].log_value.remote("window_loss", 0.2)
    actors[0].log_value.remote("window_loss", 0.3)
    actors[1].log_value.remote("window_loss", 0.1, reduce="mean", window=2)
    actors[1].log_value.remote("window_loss", 0.2)
    actors[1].log_value.remote("window_loss", 0.3)

    actor0_metrics = ray.get(actors[0].get_metrics.remote())
    actor1_metrics = ray.get(actors[1].get_metrics.remote())
    root_logger.aggregate([actor0_metrics, actor1_metrics])
    check(root_logger.peek("window_loss"), 0.25)  # mean of [0.2, 0.3]

    # Test window with 'min' reduction method
    actors[0].log_value.remote("window_min", 0.3, reduce="min", window=2)
    actors[0].log_value.remote("window_min", 0.1)
    actors[0].log_value.remote("window_min", 0.2)
    actors[1].log_value.remote("window_min", 0.3, reduce="min", window=2)
    actors[1].log_value.remote("window_min", 0.1)
    actors[1].log_value.remote("window_min", 0.2)

    actor0_metrics = ray.get(actors[0].get_metrics.remote())
    actor1_metrics = ray.get(actors[1].get_metrics.remote())
    root_logger.aggregate([actor0_metrics, actor1_metrics])
    check(root_logger.peek("window_min"), 0.1)  # min of [0.1, 0.2]

    # Test window with 'sum' reduction method
    actors[0].log_value.remote("window_sum", 10, reduce="sum", window=2)
    actors[0].log_value.remote("window_sum", 20)
    actors[0].log_value.remote("window_sum", 30)
    actors[1].log_value.remote("window_sum", 10, reduce="sum", window=2)
    actors[1].log_value.remote("window_sum", 20)
    actors[1].log_value.remote("window_sum", 30)

    actor0_metrics = ray.get(actors[0].get_metrics.remote())
    actor1_metrics = ray.get(actors[1].get_metrics.remote())
    root_logger.aggregate([actor0_metrics, actor1_metrics])
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

    # Test deeply nested keys
    root_logger.log_value(["deeply", "nested", "key"], 0.1)
    root_logger.log_value(["deeply", "nested", "key"], 0.2)
    check(root_logger.peek(["deeply", "nested", "key"]), 0.101)

    # Test different reduction methods with nested keys
    root_logger.log_value(["nested", "sum"], 10, reduce="lifetime_sum")
    root_logger.log_value(["nested", "sum"], 20)
    check(root_logger.peek(["nested", "sum"]), 30)

    root_logger.log_value(["nested", "min"], 0.3, reduce="min")
    root_logger.log_value(["nested", "min"], 0.1)
    check(root_logger.peek(["nested", "min"]), 0.1)


def test_time_logging(root_logger):
    """Test time logging functionality."""
    # Test time logging with EMA
    with root_logger.log_time("ema_time", reduce="ema", ema_coeff=0.1):
        time.sleep(0.01)
    with root_logger.log_time("ema_time", reduce="ema", ema_coeff=0.1):
        time.sleep(0.02)
    check(root_logger.peek("ema_time"), 0.0102, atol=0.05)

    # Test time logging with window
    with root_logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.01)
    with root_logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.02)

    check(root_logger.peek("mean_time"), 0.015, atol=0.05)

    # Test time logging with different reduction methods
    with root_logger.log_time("sum_time", reduce="sum"):
        time.sleep(0.01)
    with root_logger.log_time("sum_time"):
        time.sleep(0.01)
    check(root_logger.peek("sum_time"), 0.02, atol=0.05)

    # Test time logging with lifetime sum
    with root_logger.log_time("lifetime_sum_time", reduce="lifetime_sum"):
        time.sleep(0.01)
    with root_logger.log_time("lifetime_sum_time", reduce="lifetime_sum"):
        time.sleep(0.01)
    check(root_logger.peek("lifetime_sum_time"), 0.02, atol=0.05)

    # Test time logging with min
    with root_logger.log_time("min_time", reduce="min"):
        time.sleep(0.02)
    with root_logger.log_time("min_time", reduce="min"):
        time.sleep(0.01)
    check(root_logger.peek("min_time"), 0.01, atol=0.05)

    # Test time logging with max
    with root_logger.log_time("max_time", reduce="max"):
        time.sleep(0.01)
    with root_logger.log_time("max_time", reduce="max"):
        time.sleep(0.02)
    check(root_logger.peek("max_time"), 0.02, atol=0.05)

    # Test time logging with percentiles
    with root_logger.log_time(
        "percentiles_time", reduce="percentiles", window=2, percentiles=[0.5]
    ):
        time.sleep(0.01)
    with root_logger.log_time(
        "percentiles_time", reduce="percentiles", window=2, percentiles=[0.5]
    ):
        time.sleep(0.02)
    check(root_logger.peek("percentiles_time"), {0.5: 0.015}, atol=0.05)


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


def test_aggregate(root_logger):
    """Test merging multiple stats dictionaries."""
    # Create two loggers with different values
    logger1 = MetricsLogger(root=True)
    logger1.log_value("loss", 0.1, reduce="mean", window=2)
    logger1.log_value("loss", 0.2)

    logger2 = MetricsLogger()
    logger2.log_value("loss", 0.3, reduce="mean", window=2)
    logger2.log_value("loss", 0.4)

    root_logger.log_value("loss", 0.5, reduce="mean", window=2)

    # Reduce both loggers
    results1 = logger1.reduce()
    results2 = logger2.reduce()

    # Merge results into main logger
    root_logger.aggregate([results1, results2])

    # Check merged results
    # This should ignore the 0.5 value in `logger`
    check(root_logger.peek("loss"), 0.25)


def test_throughput_tracking(root_logger, actors):
    """Test throughput tracking functionality."""
    # Override the initialization time to make the test more accurate.
    root_logger._time_when_initialized = time.perf_counter()
    start_time = time.perf_counter()

    actors[0].log_value.remote("value", 1, reduce="sum", with_throughput=True)
    actors[0].log_value.remote("value", 2)
    actors[1].log_value.remote("value", 3, reduce="sum", with_throughput=True)
    actors[1].log_value.remote("value", 4)

    metrics = [ray.get(actor.get_metrics.remote()) for actor in actors]
    time.sleep(0.1)

    end_time = time.perf_counter()
    throughput = 10 / (end_time - start_time)

    root_logger.aggregate(metrics)
    check(root_logger.peek("value"), 10)
    check(root_logger.stats["value"].throughputs, throughput, rtol=0.1)

    # Test again but now don't initialize time since we are not starting a new experiment.
    actors[0].log_value.remote("value", 5)
    actors[0].log_value.remote("value", 6)
    actors[1].log_value.remote("value", 7)
    actors[1].log_value.remote("value", 8)

    metrics = [ray.get(actor.get_metrics.remote()) for actor in actors]
    time.sleep(0.1)

    end_time = time.perf_counter()
    throughput = 26 / (end_time - start_time)

    root_logger.aggregate(metrics)
    check(root_logger.peek("value"), 26)
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


def test_lifetime_stats(root_logger):
    """Test lifetime stats behavior."""
    child1 = MetricsLogger()
    child2 = MetricsLogger()

    child1.log_value("lifetime_metric", 10, reduce="lifetime_sum")
    child2.log_value("lifetime_metric", 20, reduce="lifetime_sum")

    results1 = child1.reduce()
    results2 = child2.reduce()
    root_logger.aggregate([results1, results2])
    check(root_logger.peek("lifetime_metric"), 30)

    child1.log_value("lifetime_metric", 5, reduce="lifetime_sum")
    child2.log_value("lifetime_metric", 15, reduce="lifetime_sum")

    results1 = child1.reduce()
    results2 = child2.reduce()
    check(results1["lifetime_metric"], [5])
    check(results2["lifetime_metric"], [15])

    root_logger.aggregate([results1, results2])
    check(root_logger.peek("lifetime_metric"), 50)


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
    )

    # Lifetime sum (sum with clear_on_reduce=False)
    legacy_stats["lifetime_sum_metric"] = Stats(
        init_values=[10.0, 20.0, 30.0],
        reduce="sum",
        window=None,
        clear_on_reduce=False,
    )

    # Lifetime sum with throughput tracking
    legacy_stats["lifetime_sum_with_throughput"] = Stats(
        init_values=[100.0, 200.0],
        reduce="sum",
        window=None,
        clear_on_reduce=False,
        throughput=50.0,  # Initial throughput value
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
            reduce="sum",
            window=50,
        ),
    }

    # 3. Deeply nested stats (two levels deep)
    legacy_stats["deeply"] = {
        "nested": {
            "metric": Stats(
                init_values=[1.0, 2.0],
                reduce="mean",
                ema_coeff=0.05,
            ),
            "count": Stats(
                init_values=[5.0, 10.0, 15.0],
                reduce="sum",
                window=None,
                clear_on_reduce=False,
            ),
        }
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
    logger = MetricsLogger(root=True)
    logger.set_state(legacy_state_dict)

    # Verify that values are correctly loaded
    # Check top-level stats
    check(logger.peek("mean_metric"), 2.0)  # mean of [1, 2, 3]
    check(logger.peek("min_metric"), 5.0)  # min of [10, 5, 15]
    check(logger.peek("max_metric"), 25.0)  # max of [10, 25, 15]
    check(logger.peek("sum_metric"), 6.0)  # sum of [1, 2, 3]
    check(logger.peek("lifetime_sum_metric"), 60.0)  # sum of [10, 20, 30]
    check(logger.peek("lifetime_sum_with_throughput"), 300.0)  # sum of [100, 200]

    # Check nested stats
    check(logger.peek(("nested", "loss")), 0.4)  # mean of [0.5, 0.4, 0.3]
    check(logger.peek(("nested", "reward")), 45.0)  # sum of [10, 15, 20]

    # Check deeply nested stats
    check(logger.peek(["deeply", "nested", "count"]), 30.0)  # sum of [5, 10, 15]

    # Verify that we can continue logging to the restored logger
    logger.log_value("mean_metric", 4.0, reduce="mean", window=10)
    logger.log_value(("nested", "loss"), 0.2, reduce="mean", window=100)

    # Check that new values are properly integrated
    results = logger.reduce(compile=True)
    assert "mean_metric" in results
    assert "nested" in results
    assert "loss" in results["nested"]


def test_log_dict(root_logger):
    """Test logging dictionaries of values.

    MetricsLogger.log_dict is a thin wrapper around MetricsLogger.log_value.
    We therefore don't test extensively here.
    """
    # Test simple flat dictionary
    flat_dict = {
        "metric1": 1.0,
        "metric2": 2.0,
    }
    root_logger.log_dict(flat_dict, reduce="mean")

    check(root_logger.peek("metric1"), 1.0)
    check(root_logger.peek("metric2"), 2.0)

    # Test logging more values to the same keys
    flat_dict2 = {
        "metric1": 2.0,
        "metric2": 3.0,
    }
    root_logger.log_dict(flat_dict2, reduce="mean")

    check(root_logger.peek("metric1"), 1.5)
    check(root_logger.peek("metric2"), 2.5)


@pytest.mark.parametrize(
    "stats_cls,reduce_method,log_kwargs,initial_values,external_values,expected_after_merge",
    [
        # MeanStats with window
        (
            MeanStats,
            "mean",
            {"window": 5},
            [1.0, 2.0, 3.0],
            [4.0, 5.0],
            3.0,  # mean of [1, 2, 3, 4, 5]
        ),
        # MinStats with window
        (
            MinStats,
            "min",
            {"window": 5},
            [10.0, 5.0, 8.0],
            [3.0, 7.0],
            3.0,  # min of [10, 5, 8, 3, 7]
        ),
        # MaxStats with window
        (
            MaxStats,
            "max",
            {"window": 5},
            [10.0, 5.0, 8.0],
            [15.0, 7.0],
            15.0,  # max of [10, 5, 8, 15, 7]
        ),
        # SumStats with window
        (
            SumStats,
            "sum",
            {"window": 5},
            [10.0, 20.0, 30.0],
            [40.0, 50.0],
            150.0,  # sum of [10, 20, 30, 40, 50]
        ),
        # LifetimeSumStats
        (
            LifetimeSumStats,
            "lifetime_sum",
            {},
            [100.0, 200.0],
            [150.0, 250.0],
            700.0,  # 300 + 400
        ),
        # PercentilesStats with window
        (
            PercentilesStats,
            "percentiles",
            {"window": 10, "percentiles": [50]},
            [1.0, 2.0, 3.0],
            [4.0, 5.0],
            {50: 3.0},  # median of [1, 2, 3, 4, 5]
        ),
        # ItemSeriesStats with window
        (
            ItemSeriesStats,
            "item_series",
            {"window": 5},
            ["a", "b", "c", "d"],
            ["e", "f"],
            [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
            ],  # incoming first, then existing, values should not (yet) be reduced
        ),
    ],
)
def test_log_value_with_stats_objects(
    root_logger,
    stats_cls,
    reduce_method,
    log_kwargs,
    initial_values,
    external_values,
    expected_after_merge,
):
    """Test logging Stats objects directly with MetricsLogger.log_value().

    This test verifies that when Stats objects are logged via log_value(value=<stats_object>),
    the internal stats objects are correctly extended/merged with the logged stats values.

    Note: Not all Stats types support merging with replace=False (which is what log_value uses).
    EmaStats and ItemStats require replace=True during merge, so they are not included in this
    parameterized test. SeriesStats-based classes (MeanStats, MinStats, MaxStats, SumStats) and
    PercentilesStats, ItemSeriesStats, and LifetimeSumStats support replace=False merging.
    """
    metric_name = f"{reduce_method}_metric"

    # Log initial values to the logger
    for i, value in enumerate(initial_values):
        if i == 0:
            root_logger.log_value(
                metric_name, value, reduce=reduce_method, **log_kwargs
            )
        else:
            root_logger.log_value(metric_name, value)

    # Create an external Stats object and push values
    external_stats = stats_cls(is_root_stats=True, **log_kwargs)
    for value in external_values:
        external_stats.push(value)

    # Log the external stats object - should merge the values
    root_logger.log_value(metric_name, value=external_stats)

    # Check that the merge worked correctly
    check(root_logger.peek(metric_name), expected_after_merge)


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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
