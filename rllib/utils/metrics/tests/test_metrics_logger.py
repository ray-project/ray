import time
import pytest
import numpy as np
import torch

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
        ("mean", [0.1, 0.2], 0.101),
        ("min", [0.3, 0.1, 0.2], 0.1),
        ("sum", [10, 20], 30),
    ],
)
def test_basic_reduction_methods(logger, reduce_method, values, expected):
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


def test_ema(logger):
    """Comprehensive test of EMA behavior for mean reduction."""
    # Test default EMA coefficient (0.01)
    logger.log_value("default_ema", 1.0, reduce="mean")
    logger.log_value("default_ema", 2.0)
    # Expected: 0.99 * 1.0 + 0.01 * 2.0 = 1.01
    check(logger.peek("default_ema"), 1.01)

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


def test_windowed_reduction(logger):
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


def test_aggregate(logger):
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
    logger.aggregate([results1, results2])

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
        logger.peek("count", throughput=True), approx_throughput, rtol=0.15
    )  # 15% tolerance in throughput

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


def test_throughput_aggregation():
    """Test aggregation of throughput metrics from different (remote) sources."""

    @ray.remote
    class EnvRunner:
        def __init__(self):
            self.metrics = MetricsLogger()

        def increase(self, count=1):
            self.metrics.log_value(
                "counter",
                count,
                reduce="sum",
                clear_on_reduce=False,  # lifetime counter
                with_throughput=True,
            )

        def get_metrics(self):
            return self.metrics.reduce()

    env_runners = [EnvRunner.remote() for _ in range(3)]

    # Main logger.
    main_metrics = MetricsLogger()

    env_runners[0].increase.remote(count=0)
    env_runners[1].increase.remote(count=0)
    _ = [ray.get(act.get_metrics.remote()) for act in env_runners]

    # Add 1 count for actor0 and 5 counts for actor1 to the lifetime counters
    # in each of the 5 iterations.
    # 5 iterations -> expect final count of 5 * 6 = 30
    for _ in range(5):
        time.sleep(0.1)
        env_runners[0].increase.remote(count=1)
        env_runners[1].increase.remote(count=5)

    # Pull metrics from both actors.
    results = [ray.get(act.get_metrics.remote()) for act in env_runners]
    main_metrics.aggregate(results)
    # The first aggregate (before the key even exists in `main_metrics`, throughput
    # should be NaN.
    check(main_metrics.peek("counter"), 30)
    # After first aggregation, throughput should be NaN, b/c the Stats did not exist
    # within the `MetricsLogger`.
    assert np.isnan(main_metrics.stats["counter"].throughput)

    # Add 1 count for actor0 and 2 counts for actor1 to the lifetime counters
    # in each of the 5 iterations.
    # 5 iterations each 1 sec -> expect throughput of 3/0.2sec = 5/sec.
    for _ in range(5):
        time.sleep(0.2)
        env_runners[0].increase.remote(count=1)
        env_runners[1].increase.remote(count=2)
    results = [ray.get(act.get_metrics.remote()) for act in env_runners]
    main_metrics.aggregate(results)

    check(main_metrics.peek("counter"), 30 + 15)
    tp = main_metrics.stats["counter"].throughput
    check(tp, 15, atol=2)

    time.sleep(1.0)
    env_runners[2].increase.remote(count=50)
    results = ray.get(env_runners[2].get_metrics.remote())
    main_metrics.aggregate([results])

    check(main_metrics.peek("counter"), 30 + 15 + 50)
    tp = main_metrics.stats["counter"].throughput
    # Expect throughput - due to the EMA - to be only slightly higher than
    # the original value of 15.
    check(tp, 16, atol=2)


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


def test_hierarchical_metrics_system():
    """Test a hierarchical system of MetricsLoggers.

    This test creates a tree structure of MetricsLoggers:

        Root        (Root/Algorithm object)
        ┌─┴─┐
      A1    A2      (AggregatorActor)
    ┌─┴─┐  ┌─┴─┐
    E1 E2  E3  E4   (EnvRunner)

    We test the aggregation of all these metrics through multiple reduction steps.
    """
    # Test parameters
    # Change the metric_name to test different metrics if ever needed
    metric_name = "window_mean"
    metric_config = {"reduce": "mean", "window": 2}
    leaf_values_round1 = [[10, 20], [30, 40], [50, 60], [70, 80]]
    leaf_values_round2 = [[25, 35], [45, 55], [65, 75], [85, 95]]
    expected_leaf_results_round1 = [15.0, 35.0, 55.0, 75.0]
    expected_root_peek_round1 = 50.0
    expected_compiled_result = 65.0

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

    leaves = [leaf_a1, leaf_a2, leaf_b1, leaf_b2]

    # Round 1: Log values to leaf nodes
    for leaf, values in zip(leaves, leaf_values_round1):
        for value in values:
            leaf.log_value(metric_name, value, **metric_config)

    # Reduce level 2 (leaves) and merge into level 1
    results_a1 = leaf_a1.reduce()
    results_a2 = leaf_a2.reduce()
    results_b1 = leaf_b1.reduce()
    results_b2 = leaf_b2.reduce()

    leaf_results = [results_a1, results_a2, results_b1, results_b2]

    # Verify leaf results
    for i, result in enumerate(leaf_results):
        check(result[metric_name], expected_leaf_results_round1[i])

    # Merge level 2 results into level 1 nodes
    node_a.aggregate([results_a1, results_a2])
    node_b.aggregate([results_b1, results_b2])

    # Reduce level 1 and merge into root
    results_a = node_a.reduce()
    results_b = node_b.reduce()

    # Merge level 1 results into root
    root.aggregate([results_a, results_b])

    # Verify root aggregation from first round
    if isinstance(metric_name, list):
        check(root.peek(metric_name), expected_root_peek_round1)
    else:
        check(root.peek(metric_name), expected_root_peek_round1)

    # Round 2: Log more values to leaf nodes
    for leaf, values in zip(leaves, leaf_values_round2):
        for value in values:
            leaf.log_value(metric_name, value, **metric_config)

    # Reduce level 2 (leaves) and merge into level 1
    results_a1 = leaf_a1.reduce()
    results_a2 = leaf_a2.reduce()
    results_b1 = leaf_b1.reduce()
    results_b2 = leaf_b2.reduce()

    node_a.aggregate([results_a1, results_a2])
    node_b.aggregate([results_b1, results_b2])
    results_a = node_a.reduce()
    results_b = node_b.reduce()
    root.aggregate([results_a, results_b])

    # Verify all metrics using compile() to get a complete snapshot
    compiled_results = root.compile()
    check(compiled_results[metric_name], expected_compiled_result)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
