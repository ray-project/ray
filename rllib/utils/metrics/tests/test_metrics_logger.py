import time
import pytest
import numpy as np

import ray
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import check


@pytest.fixture
def logger():
    return MetricsLogger(root=True)


def test_log_value(logger):
    """Test basic value logging and reduction."""
    # Test simple value logging
    logger.log_value("loss", 0.1)
    logger.log_value("loss", 0.2)

    # Test peek
    check(logger.peek("loss"), 0.101)

    # Test reduce
    results = logger.reduce()
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
def test_basic_reduction_methods(logger, reduce_method, values, expected):
    """Test different reduction methods (mean, min, sum) with parameterization."""
    key = f"{reduce_method}_metric"

    for val in values:
        logger.log_value(key, val, reduce=reduce_method)

    # Check the result
    check(logger.peek(key), expected)

    # Test that reduce() returns the same result
    results = logger.reduce()
    check(results[key], expected)


def test_ema(logger):
    """Comprehensive test of EMA behavior for mean reduction."""
    # Test default EMA coefficient (0.01)
    logger.log_value("default_ema", 1.0, reduce="ema")
    logger.log_value("default_ema", 2.0)
    # Expected: 0.99 * 1.0 + 0.01 * 2.0 = 1.01
    check(logger.peek("default_ema"), 1.01)

    ema_coeff = 0.2
    logger.log_value("custom_ema", 1.0, reduce="ema", ema_coeff=ema_coeff)

    # Log a series of values and check if EMA approaches the expected value
    values = [5.0] * 100  # Actual mean is 5.0
    expected = 1.0  # Initial value

    for val in values:
        logger.log_value("custom_ema", val)
        # EMA formula: new_ema = (1 - ema_coeff) * old_ema + ema_coeff * new_value
        expected = (1.0 - ema_coeff) * expected + ema_coeff * val
        check(logger.peek("custom_ema"), expected)

    # After several updates, EMA should be approaching the actual mean
    assert abs(expected - 5.0) < 1e-9, f"EMA {expected} should be approaching 5.0"

    # Test EMA with larger coefficient (faster adaptation)
    logger.log_value("fast_ema", 0.1, reduce="ema", ema_coeff=0.5)
    logger.log_value("fast_ema", 0.2)
    logger.log_value("fast_ema", 0.3)
    # Expected: first update = 0.5*0.1 + 0.5*0.2 = 0.15
    #           second update = 0.5*0.15 + 0.5*0.3 = 0.225
    check(logger.peek("fast_ema"), 0.225)


def test_windowed_reduction(logger):
    """Test window-based reduction with various window sizes."""
    # Test with window=2
    logger.log_value("window_loss", 0.1, reduce="mean", window=2, clear_on_reduce=True)
    logger.log_value("window_loss", 0.2)
    logger.log_value("window_loss", 0.3)
    check(logger.peek("window_loss"), 0.25)  # mean of [0.2, 0.3]

    # Test with window=3
    logger.log_value("window3_loss", 0.1, reduce="mean", window=3, clear_on_reduce=True)
    logger.log_value("window3_loss", 0.2)
    logger.log_value("window3_loss", 0.3)
    logger.log_value("window3_loss", 0.4)
    check(logger.peek("window3_loss"), 0.3)  # mean of [0.2, 0.3, 0.4]

    # Test window with different reduction methods
    logger.log_value("window_min", 0.3, reduce="min", window=2, clear_on_reduce=True)
    logger.log_value("window_min", 0.1)
    logger.log_value("window_min", 0.2)
    check(logger.peek("window_min"), 0.1)  # min of [0.1, 0.2]

    logger.log_value("window_sum", 10, reduce="sum", window=2, clear_on_reduce=True)
    logger.log_value("window_sum", 20)
    logger.log_value("window_sum", 30)
    check(logger.peek("window_sum"), 50)  # sum of [20, 30]


def test_nested_keys(logger):
    """Test logging with nested key structures."""
    # Test nested key logging
    logger.log_value(("nested", "key"), 1.0)
    logger.log_value(("nested", "key"), 2.0)

    # Test peek with nested key
    check(logger.peek(("nested", "key")), 1.01)

    # Test reduce with nested key
    results = logger.reduce()
    check(results["nested"]["key"], 1.01)

    # Test deeply nested keys
    logger.log_value(["deeply", "nested", "key"], 0.1)
    logger.log_value(["deeply", "nested", "key"], 0.2)
    check(logger.peek(["deeply", "nested", "key"]), 0.101)

    # Test different reduction methods with nested keys
    logger.log_value(["nested", "sum"], 10, reduce="lifetime_sum")
    logger.log_value(["nested", "sum"], 20)
    check(logger.peek(["nested", "sum"]), 30)

    logger.log_value(["nested", "min"], 0.3, reduce="min")
    logger.log_value(["nested", "min"], 0.1)
    check(logger.peek(["nested", "min"]), 0.1)


def test_time_logging(logger):
    """Test time logging functionality."""
    # Test time logging with EMA
    with logger.log_time("ema_time", reduce="ema", ema_coeff=0.1):
        time.sleep(0.1)
    with logger.log_time("ema_time", reduce="ema", ema_coeff=0.1):
        time.sleep(0.2)
    check(logger.peek("ema_time"), 0.101, atol=0.05)

    # Test time logging with window
    with logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.2)
    with logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.3)
    with logger.log_time("mean_time", reduce="mean", window=2):
        time.sleep(0.4)
    check(logger.peek("mean_time"), 0.35, atol=0.05)

    # Test time logging with different reduction methods
    with logger.log_time("sum_time", reduce="sum"):
        time.sleep(0.1)
    with logger.log_time("sum_time"):
        time.sleep(0.1)
    check(logger.peek("sum_time"), 0.2, atol=0.05)

    # Test time logging with lifetime sum
    with logger.log_time("lifetime_sum_time", reduce="lifetime_sum"):
        time.sleep(0.1)
    with logger.log_time("lifetime_sum_time", reduce="lifetime_sum"):
        time.sleep(0.1)
    check(logger.peek("lifetime_sum_time"), 0.2, atol=0.05)

    # Test time logging with min
    with logger.log_time("min_time", reduce="min"):
        time.sleep(0.1)
    check(logger.peek("min_time"), 0.1, atol=0.05)

    # Test time logging with max
    with logger.log_time("max_time", reduce="max"):
        time.sleep(0.1)
    check(logger.peek("max_time"), 0.1, atol=0.05)

    # Test time logging with percentiles
    with logger.log_time(
        "percentiles_time", reduce="percentiles", window=2, percentiles=[0.5]
    ):
        time.sleep(0.1)
    with logger.log_time(
        "percentiles_time", reduce="percentiles", window=2, percentiles=[0.5]
    ):
        time.sleep(0.2)
    check(logger.peek("percentiles_time"), {0.5: 0.15}, atol=0.05)


def test_state_management(logger):
    """Test state management (get_state and set_state)."""
    # Log some values
    logger.log_value("state_test", 0.1)
    logger.log_value("state_test", 0.2)

    # Get state
    state = logger.get_state()

    # Create new logger and set state
    new_logger = MetricsLogger()
    new_logger.set_state(state)

    # Check that state was properly transferred
    check(new_logger.peek("state_test"), 0.101)


def test_aggregate(logger):
    """Test merging multiple stats dictionaries."""
    # Create two loggers with different values
    logger1 = MetricsLogger(root=True)
    logger1.log_value("loss", 0.1, reduce="mean", window=2)
    logger1.log_value("loss", 0.2)

    logger2 = MetricsLogger()
    logger2.log_value("loss", 0.3, reduce="mean", window=2)
    logger2.log_value("loss", 0.4)

    logger.log_value("loss", 0.5, reduce="mean", window=2)

    # Reduce both loggers
    results1 = logger1.reduce()
    results2 = logger2.reduce()

    # Merge results into main logger
    logger.aggregate([results1, results2])

    # Check merged results
    # This should ignore the 0.5 value in `logger`
    check(logger.peek("loss"), 0.25)


def test_throughput_tracking(logger):
    """Test throughput tracking functionality."""
    # Create 2 parallel Ray Actors that each log a few values
    @ray.remote
    class Actor:
        def __init__(self):
            self.metrics = MetricsLogger()

        def log_value(self, value):
            self.metrics.log_value("value", value, reduce="sum", with_throughput=True)

        def get_metrics(self):
            return self.metrics.reduce()

    actors = [Actor.remote() for _ in range(2)]

    # Override the initialization time to make the test more accurate.
    logger._time_when_initialized = time.perf_counter()
    start_time = time.perf_counter()

    actors[0].log_value.remote(1)
    actors[0].log_value.remote(2)
    actors[1].log_value.remote(3)
    actors[1].log_value.remote(4)

    metrics = [ray.get(actor.get_metrics.remote()) for actor in actors]
    time.sleep(1)

    end_time = time.perf_counter()
    throughput = 10 / (end_time - start_time)

    logger.aggregate(metrics)
    check(logger.peek("value"), 10)
    check(logger.stats["value"].throughputs, throughput, rtol=0.1)

    # Test again but now don't initialize time since we are not starting a new experiment.
    actors[0].log_value.remote(5)
    actors[0].log_value.remote(6)
    actors[1].log_value.remote(7)
    actors[1].log_value.remote(8)

    metrics = [ray.get(actor.get_metrics.remote()) for actor in actors]
    time.sleep(1)

    end_time = time.perf_counter()
    throughput = 26 / (end_time - start_time)

    logger.aggregate(metrics)
    check(logger.peek("value"), 26)
    check(logger.stats["value"].throughputs, throughput, rtol=0.1)


def test_reset_and_delete(logger):
    """Test reset and delete functionality."""
    # Log some values
    logger.log_value("test1", 0.1)
    logger.log_value("test2", 0.2)

    # Test delete
    logger.delete("test1")
    with pytest.raises(KeyError):
        logger.peek("test1")

    # Test reset
    logger.reset()
    check(logger.reduce(), {})


def test_compile(logger):
    """Test the compile method that combines values and throughputs."""
    # Log some values with throughput tracking
    logger.log_value("count", 1, reduce="sum", with_throughput=True)
    time.sleep(0.1)
    logger.log_value("count", 2, reduce="sum", with_throughput=True)

    # Log some nested values with throughput tracking
    logger.log_value(["nested", "count"], 3, reduce="sum", with_throughput=True)
    time.sleep(0.1)
    logger.log_value(["nested", "count"], 4, reduce="sum", with_throughput=True)

    # Log some values without throughput tracking
    logger.log_value("simple", 5.0)
    logger.log_value("simple", 6.0)

    # Get compiled results
    compiled = logger.compile()

    breakpoint()

    # Check that values and throughputs are correctly combined
    check(compiled["count"], 3)  # sum of [1, 2]
    check(compiled["count_throughput"], 20, rtol=0.1)  # initial throughput
    check(compiled["nested"]["count"], 7)  # sum of [3, 4]
    check(compiled["nested"]["count_throughput"], 40, rtol=0.1)  # initial throughput

    check(compiled["simple"], 5.01)
    assert (
        "simple_throughput" not in compiled
    )  # no throughput for non-throughput metric


def test_peek_with_default(logger):
    """Test peek method with default argument."""
    # Test with non-existent key
    check(logger.peek("non_existent", default=0.0), 0.0)

    # Test with existing key
    logger.log_value("existing", 1.0)
    ret = logger.peek("existing", default=0.0)
    check(ret, 1.0)  # Should return actual value, not default


def test_edge_cases(logger):
    """Test edge cases and error handling."""
    # Test invalid reduction method
    with pytest.raises(ValueError):
        logger.log_value("invalid_reduce", 0.1, reduce="invalid")

    # Test window and ema_coeff together
    with pytest.raises(ValueError):
        logger.log_value("invalid_window_ema", 0.1, window=2, ema_coeff=0.1)

    # Test clear_on_reduce
    logger.log_value("clear_test", 0.1, clear_on_reduce=True)
    logger.log_value("clear_test", 0.2, clear_on_reduce=True)
    results = logger.reduce()
    check(results["clear_test"], 0.101)
    check(logger.peek("clear_test"), np.nan)  # Should be cleared


def test_lifetime_stats():
    """Test lifetime stats behavior with clear_on_reduce=False."""
    # Create a root logger
    root_logger = MetricsLogger(root=True)

    # Create non-root loggers
    child1 = MetricsLogger()
    child2 = MetricsLogger()

    # Log lifetime stats to both children
    child1.log_value("lifetime_metric", 10, reduce="sum", clear_on_reduce=False)
    child2.log_value("lifetime_metric", 20, reduce="sum", clear_on_reduce=False)

    # Reduce both children and merge into root
    results1 = child1.reduce()
    results2 = child2.reduce()
    root_logger.aggregate([results1, results2])
    check(root_logger.peek("lifetime_metric"), 30)

    # Log more values to child loggers
    child1.log_value("lifetime_metric", 5, reduce="sum", clear_on_reduce=False)
    child2.log_value("lifetime_metric", 15, reduce="sum", clear_on_reduce=False)

    # Reduce children again - non-root loggers should not retain previous values and return lists to merge downstream
    results1 = child1.reduce()
    results2 = child2.reduce()
    check(results1["lifetime_metric"], [15])  # 15 (10+5)
    check(results2["lifetime_metric"], [35])  # 35 (20+15)

    # Merge new results into root - root should accumulate
    root_logger.aggregate([results1, results2])
    check(root_logger.peek("lifetime_metric"), 50)  # 30 + 5 + 15


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
