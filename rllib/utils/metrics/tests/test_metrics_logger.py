import time
import pytest
import numpy as np
import torch

from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import check


@pytest.fixture
def logger():
    return MetricsLogger(root=True)


def test_basic_value_logging(logger):
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
        ("mean", [0.1, 0.2], 0.101),
        ("min", [0.3, 0.1, 0.2], 0.1),
        ("sum", [10, 20], 30),
    ],
)
def test_reduction_methods(logger, reduce_method, values, expected):
    """Test different reduction methods (mean, min, sum) with parameterization."""
    key = f"{reduce_method}_metric"

    # Log the first value with the reduction method specified
    logger.log_value(key, values[0], reduce=reduce_method)

    # Log remaining values
    for val in values[1:]:
        logger.log_value(key, val)

    # Check the result
    check(logger.peek(key), expected)

    # Test that reduce() returns the same result
    results = logger.reduce()
    check(results[key], expected)


def test_ema_behavior(logger):
    """Comprehensive test of EMA behavior for mean reduction."""
    # Test default EMA coefficient (0.01)
    logger.log_value("default_ema", 1.0, reduce="mean")
    logger.log_value("default_ema", 2.0)
    # Expected: 0.99 * 1.0 + 0.01 * 2.0 = 1.01
    check(logger.peek("default_ema"), 1.01)

    # Test with custom EMA coefficient (0.2)
    ema_coeff = 0.2
    logger.log_value("custom_ema", 1.0, ema_coeff=ema_coeff)

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
    logger.log_value("fast_ema", 0.1, ema_coeff=0.5)
    logger.log_value("fast_ema", 0.2)
    logger.log_value("fast_ema", 0.3)
    # Expected: first update = 0.5*0.1 + 0.5*0.2 = 0.15
    #           second update = 0.5*0.15 + 0.5*0.3 = 0.225
    check(logger.peek("fast_ema"), 0.225)


def test_window_based_reduction(logger):
    """Test window-based reduction with various window sizes."""
    # Test with window=2
    logger.log_value("window_loss", 0.1, window=2)
    logger.log_value("window_loss", 0.2, window=2)
    logger.log_value("window_loss", 0.3, window=2)
    check(logger.peek("window_loss"), 0.25)  # mean of [0.2, 0.3]

    # Test with window=3
    logger.log_value("window3_loss", 0.1, window=3)
    logger.log_value("window3_loss", 0.2, window=3)
    logger.log_value("window3_loss", 0.3, window=3)
    logger.log_value("window3_loss", 0.4, window=3)
    check(logger.peek("window3_loss"), 0.3)  # mean of [0.2, 0.3, 0.4]

    # Test window with different reduction methods
    logger.log_value("window_min", 0.3, window=2, reduce="min")
    logger.log_value("window_min", 0.1, window=2)
    logger.log_value("window_min", 0.2, window=2)
    check(logger.peek("window_min"), 0.1)  # min of [0.1, 0.2]

    logger.log_value("window_sum", 10, window=2, reduce="sum")
    logger.log_value("window_sum", 20, window=2)
    logger.log_value("window_sum", 30, window=2)
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
    logger.log_value(["nested", "sum"], 10, reduce="sum")
    logger.log_value(["nested", "sum"], 20)
    check(logger.peek(["nested", "sum"]), 30)

    logger.log_value(["nested", "min"], 0.3, reduce="min")
    logger.log_value(["nested", "min"], 0.1)
    check(logger.peek(["nested", "min"]), 0.1)


def test_tensor_mode(logger):
    """Test tensor mode functionality."""
    # Test with PyTorch tensors
    logger.activate_tensor_mode()
    logger.log_value("torch_loss", torch.tensor(0.1))
    logger.log_value("torch_loss", torch.tensor(0.2))
    logger.deactivate_tensor_mode()
    value = logger.peek("torch_loss")
    check(value, 0.101)
    check(isinstance(value, torch.Tensor), True)


def test_time_logging(logger):
    """Test time logging functionality."""
    # Test time logging with EMA
    with logger.log_time("block_time", ema_coeff=0.1):
        time.sleep(0.1)

    # Test time logging with window
    with logger.log_time("window_time", window=2):
        time.sleep(0.2)

    # Check that times are approximately correct
    check(logger.peek("block_time"), 0.1, atol=0.05)
    check(logger.peek("window_time"), 0.2, atol=0.05)

    # Test time logging with different reduction methods
    with logger.log_time("sum_time", reduce="sum"):
        time.sleep(0.1)
    with logger.log_time("sum_time"):
        time.sleep(0.1)
    check(logger.peek("sum_time"), 0.2, atol=0.05)


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


def test_merge_and_log_n_dicts(logger):
    """Test merging multiple stats dictionaries."""
    # Create two loggers with different values
    logger1 = MetricsLogger()
    logger1.log_value("loss", 0.1, window=2)
    logger1.log_value("loss", 0.2, window=2)

    logger2 = MetricsLogger()
    logger2.log_value("loss", 0.3, window=2)
    logger2.log_value("loss", 0.4, window=2)

    logger.log_value("loss", 0.5, window=2)
    logger.log_value("loss", 0.6, window=2)

    # Reduce both loggers
    results1 = logger1.reduce()
    results2 = logger2.reduce()

    # Merge results into main logger
    logger.merge_and_log_n_dicts([results1, results2])

    # Check merged results
    # This may seem counterintuitive, because mean(0.1, 0.2, 0.3, 0.4, 0.5, 0.6) = 0.35.
    # However, we are aggregating in logger, so values from logger1 and logger2 are merged in parallel and are given priority over already existing values in logger.
    # Therefore, mean(0.2, 0.4) = 0.3
    check(logger.peek("loss"), 0.3, rtol=0.01)


def test_throughput_tracking(logger):
    """Test throughput tracking functionality."""
    # Test basic throughput tracking
    start_time = time.perf_counter()
    logger.log_value("count", 1, reduce="sum", with_throughput=True)
    num_iters = 100
    for _ in range(num_iters):
        time.sleep(0.1 / num_iters)  # Simulate some time passing
        logger.log_value("count", 2, reduce="sum", with_throughput=True)
    end_time = time.perf_counter()

    # Get value and throughput
    check(logger.peek("count"), num_iters * 2 + 1)
    approx_throughput = (num_iters * 2 + 1) / (end_time - start_time)
    check(
        logger.peek("count", throughput=True), approx_throughput, rtol=0.1
    )  # 10% tolerance in throughput

    # Test _get_throughputs() method without key (returns all throughputs)
    throughputs = logger.peek(throughput=True)
    check(throughputs["count_throughput"], approx_throughput, rtol=0.2)

    # Test with nested keys
    nested_start_time = time.perf_counter()
    logger.log_value(("nested", "count"), 1, reduce="sum", with_throughput=True)
    for _ in range(num_iters):
        time.sleep(0.1 / num_iters)  # Simulate some time passing
        logger.log_value(("nested", "count"), 3, reduce="sum", with_throughput=True)
    nested_end_time = time.perf_counter()

    # Check nested value
    check(logger.peek(("nested", "count")), num_iters * 3 + 1)

    # Check nested throughput with specific key
    nested_approx_throughput = (num_iters * 3 + 1) / (
        nested_end_time - nested_start_time
    )
    check(
        logger.peek(("nested", "count"), throughput=True),
        nested_approx_throughput,
        rtol=0.2,
    )

    # Check getting throughput for a parent key
    nested_throughputs = logger.peek("nested", throughput=True)
    check(nested_throughputs, {"count_throughput": nested_approx_throughput}, rtol=0.2)

    # Verify all throughputs are present in the full throughput dict
    all_throughputs = logger.peek(throughput=True)
    check("count_throughput" in all_throughputs, True)
    check("nested" in all_throughputs, True)
    check("count_throughput" in all_throughputs["nested"], True)


def test_has_throughput_property(logger):
    """Test the has_throughput property functionality."""
    # Create a Stats object with throughput tracking
    logger.log_value("with_throughput", 10, reduce="sum", with_throughput=True)
    check(logger.peek("with_throughput"), 10)
    check(
        logger.peek("with_throughput", throughput=True), np.nan
    )  # Initial throughput should be np.nan

    # Create a Stats object without throughput tracking
    logger.log_value("without_throughput", 10, reduce="sum")
    check(logger.peek("without_throughput"), 10)

    # Test that throughputs() only includes Stats with has_throughput=True
    throughputs = logger.peek(throughput=True)
    check("with_throughput_throughput" in throughputs, True)
    check("without_throughput_throughput" in throughputs, False)


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


def test_lifetime_stats_behavior():
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
    root_logger.merge_and_log_n_dicts([results1, results2])
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
    root_logger.merge_and_log_n_dicts([results1, results2])
    check(root_logger.peek("lifetime_metric"), 50)  # 30 + 5 + 15


def test_hierarchical_metrics_system():
    """Test a complete hierarchical metrics system with diverse feature combinations.
    The test mimics how we use MetricsLogger in EnvRunners, AggregatorActors and the driver (Algorithm object).

    This test creates a tree structure of MetricsLoggers:

        Root        (Driver)
        ┌─┴─┐
      A      B      (AggregatorActor)
    ┌─┴─┐  ┌─┴─┐
    A1 A2  B1 B2    (EnvRunner)

    Each logger logs different types of metrics, and we test the aggregation
    of all these metrics through multiple reduction steps.
    """
    # Create the logger hierarchy
    root = MetricsLogger(root=True)  # Root logger

    # Level 1 loggers
    node_a = MetricsLogger()
    node_b = MetricsLogger()

    # Level 2 loggers (leaves)
    leaf_a1 = MetricsLogger()
    leaf_a2 = MetricsLogger()
    leaf_b1 = MetricsLogger()
    leaf_b2 = MetricsLogger()

    # ----- Round 1: Initial metrics -----

    # Log different types of metrics in leaf nodes

    # 1. Simple mean metrics with default EMA (no window)
    leaf_a1.log_value("mean_metric", 1.0)
    leaf_a2.log_value("mean_metric", 2.0)
    leaf_b1.log_value("mean_metric", 3.0)
    leaf_b2.log_value("mean_metric", 4.0)

    # 2. Window-based mean metrics
    leaf_a1.log_value("window_mean", 10, reduce="mean", window=2)
    leaf_a1.log_value("window_mean", 20, window=2)
    leaf_a2.log_value("window_mean", 30, reduce="mean", window=2)
    leaf_a2.log_value("window_mean", 40, window=2)
    leaf_b1.log_value("window_mean", 50, reduce="mean", window=2)
    leaf_b1.log_value("window_mean", 60, window=2)
    leaf_b2.log_value("window_mean", 70, reduce="mean", window=2)
    leaf_b2.log_value("window_mean", 80, window=2)

    # 3. Min metrics
    leaf_a1.log_value("min_value", 15, reduce="min")
    leaf_a1.log_value("min_value", 10)
    leaf_a2.log_value("min_value", 25, reduce="min")
    leaf_a2.log_value("min_value", 20)
    leaf_b1.log_value("min_value", 35, reduce="min")
    leaf_b1.log_value("min_value", 30)
    leaf_b2.log_value("min_value", 45, reduce="min")
    leaf_b2.log_value("min_value", 40)

    # 4. Max metrics
    leaf_a1.log_value("max_value", 50, reduce="max")
    leaf_a1.log_value("max_value", 100)
    leaf_a2.log_value("max_value", 150, reduce="max")
    leaf_a2.log_value("max_value", 200)
    leaf_b1.log_value("max_value", 250, reduce="max")
    leaf_b1.log_value("max_value", 300)
    leaf_b2.log_value("max_value", 350, reduce="max")
    leaf_b2.log_value("max_value", 400)

    # 5. Lifetime sum metrics (clear_on_reduce=False)
    leaf_a1.log_value("lifetime_sum", 5, reduce="sum", clear_on_reduce=False)
    leaf_a2.log_value("lifetime_sum", 10, reduce="sum", clear_on_reduce=False)
    leaf_b1.log_value("lifetime_sum", 15, reduce="sum", clear_on_reduce=False)
    leaf_b2.log_value("lifetime_sum", 20, reduce="sum", clear_on_reduce=False)

    # 6. Regular sum metrics (with automatic clear_on_reduce=True for non-root)
    leaf_a1.log_value("regular_sum", 1, reduce="sum")
    leaf_a2.log_value("regular_sum", 2, reduce="sum")
    leaf_b1.log_value("regular_sum", 3, reduce="sum")
    leaf_b2.log_value("regular_sum", 4, reduce="sum")

    # 7. Nested metrics of different types
    leaf_a1.log_value(["nested", "mean"], 1.0, reduce="mean")
    leaf_a2.log_value(["nested", "mean"], 2.0, reduce="mean")
    leaf_b1.log_value(["nested", "mean"], 3.0, reduce="mean")
    leaf_b2.log_value(["nested", "mean"], 4.0, reduce="mean")

    leaf_a1.log_value(["nested", "sum"], 10, reduce="sum")
    leaf_a2.log_value(["nested", "sum"], 20, reduce="sum")
    leaf_b1.log_value(["nested", "sum"], 30, reduce="sum")
    leaf_b2.log_value(["nested", "sum"], 40, reduce="sum")

    leaf_a1.log_value(["nested", "lifetime"], 100, reduce="sum", clear_on_reduce=False)
    leaf_a2.log_value(["nested", "lifetime"], 200, reduce="sum", clear_on_reduce=False)
    leaf_b1.log_value(["nested", "lifetime"], 300, reduce="sum", clear_on_reduce=False)
    leaf_b2.log_value(["nested", "lifetime"], 400, reduce="sum", clear_on_reduce=False)

    # 8. Multiple levels of nesting
    leaf_a1.log_value(["deeply", "nested", "metric"], 1, reduce="sum")
    leaf_a2.log_value(["deeply", "nested", "metric"], 2, reduce="sum")
    leaf_b1.log_value(["deeply", "nested", "metric"], 3, reduce="sum")
    leaf_b2.log_value(["deeply", "nested", "metric"], 4, reduce="sum")

    # Reduce level 2 (leaves) and merge into level 1
    results_a1 = leaf_a1.reduce()
    results_a2 = leaf_a2.reduce()
    results_b1 = leaf_b1.reduce()
    results_b2 = leaf_b2.reduce()

    # Verify leaf results
    check(results_a1["mean_metric"], 1.0)
    check(results_a1["window_mean"], 15.0)  # Mean of [10, 20]
    check(results_a1["min_value"], 10)
    check(results_a1["max_value"], 100)
    check(results_a1["lifetime_sum"], 5)
    check(results_a1["regular_sum"], 1)
    check(results_a1["nested"]["mean"], 1.0)
    check(results_a1["nested"]["sum"], 10)
    check(results_a1["nested"]["lifetime"], 100)
    check(results_a1["deeply"]["nested"]["metric"], 1)

    # Merge level 2 results into level 1 nodes
    node_a.merge_and_log_n_dicts([results_a1, results_a2])
    node_b.merge_and_log_n_dicts([results_b1, results_b2])

    # Verify level 1 aggregation
    check(node_a.peek("mean_metric"), 1.5)  # Mean of [1.0, 2.0]
    check(
        node_a.peek("window_mean"), 30.0
    )  # Mean of windowed stats merged in parallel. In this case: [mean(values) * num_values] = [mean([20.0, 40.0]) * 2] = [30.0, 30.0]
    check(node_a.peek("min_value"), 10)  # Min of [10, 20]
    check(node_a.peek("max_value"), 200)  # Max of [100, 200]
    check(node_a.peek("lifetime_sum"), 15)  # Sum of [5, 10]
    check(node_a.peek("regular_sum"), 3)  # Sum of [1, 2]
    check(node_a.peek(["nested", "mean"]), 1.5)  # Mean of [1.0, 2.0]
    check(node_a.peek(["nested", "sum"]), 30)  # Sum of [10, 20]
    check(node_a.peek(["nested", "lifetime"]), 300)  # Sum of [100, 200]
    check(node_a.peek(["deeply", "nested", "metric"]), 3)  # Sum of [1, 2]

    check(node_b.peek("mean_metric"), 3.5)  # Mean of [3.0, 4.0]
    check(
        node_b.peek("window_mean"), 70.0
    )  # Mean of windowed stats merged in parallel. In this case: [mean(values) * len(values)] = [mean([60.0, 80.0]) * 2] = [70.0, 70.0]
    check(node_b.peek("min_value"), 30)  # Min of [30, 40]
    check(node_b.peek("max_value"), 400)  # Max of [300, 400]
    check(node_b.peek("lifetime_sum"), 35)  # Sum of [15, 20]
    check(node_b.peek("regular_sum"), 7)  # Sum of [3, 4]

    # Reduce level 1 and merge into root
    results_a = node_a.reduce()
    results_b = node_b.reduce()

    # Verify intermediate node results after reduction
    # The lifetime sum values should be cleared in non-root loggers
    check(results_a["lifetime_sum"], 15)  # Only current value, not accumulated
    check(results_b["lifetime_sum"], 35)  # Only current value, not accumulated

    # Merge level 1 results into root
    root.merge_and_log_n_dicts([results_a, results_b])

    # Verify root aggregation from first round
    check(root.peek("mean_metric"), 2.5)  # Mean of [1.5, 3.5]
    check(
        root.peek("window_mean"), 50.0
    )  # Mean of windowed stats merged in parallel. In this case: [mean(values) * len(values)] = [mean([30.0, 70.0]) * 2] = [50.0, 50.0]
    check(root.peek("min_value"), 10)  # Min of [10, 30]
    check(root.peek("max_value"), 400)  # Max of [200, 400]
    check(root.peek("lifetime_sum"), 50)  # Sum of [15, 35]
    check(root.peek("regular_sum"), 10)  # Sum of [3, 7]
    check(root.peek(["nested", "mean"]), 2.5)  # Mean of [1.5, 3.5]
    check(root.peek(["nested", "sum"]), 100)  # Sum of [30, 70]
    check(root.peek(["nested", "lifetime"]), 1000)  # Sum of [300, 700]
    check(root.peek(["deeply", "nested", "metric"]), 10)  # Sum of [3, 7]

    # ----- Round 2: Add more metrics to show lifetime accumulation -----

    # Log more data in leaf nodes
    leaf_a1.log_value("lifetime_sum", 1, reduce="sum", clear_on_reduce=False)
    leaf_a2.log_value("lifetime_sum", 2, reduce="sum", clear_on_reduce=False)
    leaf_b1.log_value("lifetime_sum", 3, reduce="sum", clear_on_reduce=False)
    leaf_b2.log_value("lifetime_sum", 4, reduce="sum", clear_on_reduce=False)

    leaf_a1.log_value(["nested", "lifetime"], 10, reduce="sum", clear_on_reduce=False)
    leaf_a2.log_value(["nested", "lifetime"], 20, reduce="sum", clear_on_reduce=False)
    leaf_b1.log_value(["nested", "lifetime"], 30, reduce="sum", clear_on_reduce=False)
    leaf_b2.log_value(["nested", "lifetime"], 40, reduce="sum", clear_on_reduce=False)

    # Reduce level 2 (leaves) and merge into level 1
    results_a1 = leaf_a1.reduce()
    results_a2 = leaf_a2.reduce()
    results_b1 = leaf_b1.reduce()
    results_b2 = leaf_b2.reduce()

    # Verify leaf results - these should only contain the new values, not accumulated ones
    check(results_a1["lifetime_sum"], [6])  # 6 (5+1)
    check(results_a1["nested"]["lifetime"], [110])  # 110 (100+10)

    # Merge level 2 results into level 1 nodes
    node_a.merge_and_log_n_dicts([results_a1, results_a2])
    node_b.merge_and_log_n_dicts([results_b1, results_b2])

    # Verify level 1 aggregation
    check(node_a.peek("lifetime_sum"), [18])  # 18 (15+3)
    check(node_a.peek(["nested", "lifetime"]), [330])  # 330 (300+30)

    # Reduce level 1 and merge into root
    results_a = node_a.reduce()
    results_b = node_b.reduce()

    # Merge level 1 results into root
    root.merge_and_log_n_dicts([results_a, results_b])

    # Verify root aggregation - root should accumulate lifetime stats
    check(root.peek("lifetime_sum"), 60)  # 50 from round 1 + 3 + 7 from round 2
    check(
        root.peek(["nested", "lifetime"]), 1100
    )  # 1000 from round 1 + 30 + 70 from round 2

    # ----- Round 3: Test additional metrics -----

    # Add some EMA-based metrics
    leaf_a1.log_value("ema_metric", 1.0, reduce="mean", ema_coeff=0.1)
    leaf_a1.log_value("ema_metric", 2.0)  # EMA should be 0.9*1.0 + 0.1*2.0 = 1.1
    leaf_a2.log_value("ema_metric", 3.0, reduce="mean", ema_coeff=0.1)
    leaf_a2.log_value("ema_metric", 4.0)  # EMA should be 0.9*3.0 + 0.1*4.0 = 3.1

    # Add window metrics with different sizes
    leaf_b1.log_value("window3_metric", 10, reduce="mean", window=3)
    leaf_b1.log_value("window3_metric", 20, window=3)
    leaf_b1.log_value("window3_metric", 30, window=3)
    leaf_b1.log_value("window3_metric", 40, window=3)  # Mean of [20, 30, 40] = 30

    leaf_b2.log_value("window3_metric", 50, reduce="mean", window=3)
    leaf_b2.log_value("window3_metric", 60, window=3)
    leaf_b2.log_value("window3_metric", 70, window=3)
    leaf_b2.log_value("window3_metric", 80, window=3)  # Mean of [60, 70, 80] = 70

    # Reduce level 2 and merge to level 1
    results_a1 = leaf_a1.reduce()
    results_a2 = leaf_a2.reduce()
    results_b1 = leaf_b1.reduce()
    results_b2 = leaf_b2.reduce()

    check(results_a1["ema_metric"], 1.1)
    check(results_a2["ema_metric"], 3.1)
    check(results_b1["window3_metric"], 30)
    check(results_b2["window3_metric"], 70)

    # Merge into level 1
    node_a.merge_and_log_n_dicts([results_a1, results_a2])
    node_b.merge_and_log_n_dicts([results_b1, results_b2])

    # Verify level 1
    check(node_a.peek("ema_metric"), 2.1)  # Mean of [1.1, 3.1]
    # Mean of windowed stats merged in parallel.
    # In this case: [mean(values_1) * len(values_1)] + [mean(values_2) * len(values_2)] = [mean([80, 40]) * 2] + [mean([70, 30]) * 2] = [60, 60, 50, 50]
    # Since window is 3, we take the first 3 values from the list, which are [60, 60, 50].
    # Then we take the mean of these 3 values, which is 56.666666666666664.
    check(node_b.peek("window3_metric"), (60 + 60 + 50) / 3)  # 56.666666666666664

    # Reduce level 1 and merge to root
    results_a = node_a.reduce()
    results_b = node_b.reduce()

    # Merge to root
    root.merge_and_log_n_dicts([results_a, results_b])

    # Only root knows about the new metrics
    check("ema_metric" in root.stats, True)
    check("window3_metric" in root.stats, True)

    # Root should have correct values
    check(root.peek("ema_metric"), 2.1)  # Only from node_a
    check(root.peek("window3_metric"), (60 + 60 + 50) / 3)  # Only from node_b

    # Lifetime metrics should still be accumulating only at root
    check(root.peek("lifetime_sum"), 60)  # Still the same as after round 2
    check(root.peek(["nested", "lifetime"]), 1100)  # Still the same as after round 2


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
