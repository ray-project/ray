import time
import pytest
import numpy as np
import torch

from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import check


@pytest.fixture
def logger():
    return MetricsLogger()


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


def test_ema_coefficient(logger):
    """Test EMA coefficient for mean reduction."""
    # Test with ema_coeff=0.1
    ema_coeff = 0.2
    logger.log_value("loss", 1.0, ema_coeff=ema_coeff)

    # Log a series of values and check if EMA approaches the actual mean
    values = [5.0] * 100  # Actual mean is 5.0
    # Initial value
    expected = 1.0

    for val in values:
        logger.log_value("loss", val)
        # EMA formula: new_ema = (1 - ema_coeff) * old_ema + ema_coeff * new_value
        expected = (1.0 - ema_coeff) * expected + ema_coeff * val
        check(logger.peek("loss"), expected)

    # After several updates, EMA should be approaching the actual mean
    assert abs(expected - 5.0) < 1e-9, f"EMA {expected} should be approaching 5.0"


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


def test_different_reduction_methods(logger):
    """Test different reduction methods (mean, min, sum)."""
    # Test mean reduction
    logger.log_value("mean_loss", 0.1, reduce="mean")
    logger.log_value("mean_loss", 0.2)
    check(logger.peek("mean_loss"), 0.101)

    # Test min reduction
    logger.log_value("min_loss", 0.3, reduce="min")
    logger.log_value("min_loss", 0.1)
    logger.log_value("min_loss", 0.2)
    check(logger.peek("min_loss"), 0.1)

    # Test sum reduction
    logger.log_value("total_steps", 10, reduce="sum")
    logger.log_value("total_steps", 20)
    check(logger.peek("total_steps"), 30)


def test_window_based_reduction(logger):
    """Test window-based reduction."""
    # Test with window=2
    logger.log_value("window_loss", 0.1, window=2)
    logger.log_value("window_loss", 0.2)
    logger.log_value("window_loss", 0.3)
    check(logger.peek("window_loss"), 0.25)  # mean of [0.2, 0.3]

    # Test with window=3
    logger.log_value("window3_loss", 0.1, window=3)
    logger.log_value("window3_loss", 0.2)
    logger.log_value("window3_loss", 0.3)
    logger.log_value("window3_loss", 0.4)
    check(logger.peek("window3_loss"), 0.3)  # mean of [0.2, 0.3, 0.4]


def test_ema_reduction(logger):
    """Test EMA-based reduction."""
    # Test with ema_coeff=0.1
    logger.log_value("ema_loss", 0.1, ema_coeff=0.1)
    logger.log_value("ema_loss", 0.2)
    logger.log_value("ema_loss", 0.3)

    # EMA calculation:
    # First value: 0.1
    # Second value: 0.9 * 0.1 + 0.1 * 0.2 = 0.11
    # Third value: 0.9 * 0.11 + 0.1 * 0.3 = 0.129
    check(logger.peek("ema_loss"), 0.129)


def test_tensor_mode(logger):
    """Test tensor mode functionality."""
    # Test with PyTorch tensors

    logger.activate_tensor_mode()
    logger.log_value("torch_loss", torch.tensor(0.1))
    logger.log_value("torch_loss", torch.tensor(0.2))
    tensor_metrics = logger.deactivate_tensor_mode()
    logger.tensors_to_numpy(tensor_metrics)
    check(logger.peek("torch_loss"), 0.101)


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


def test_edge_cases(logger):
    """Test edge cases and error handling."""
    # Test non-existent key
    with pytest.raises(KeyError):
        logger.peek("non_existent")

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


def test_merge_and_log_n_dicts(logger):
    """Test merging multiple stats dictionaries."""
    # Create two loggers with different values
    logger1 = MetricsLogger()
    logger1.log_value("loss", 0.1, window=2)
    logger1.log_value("loss", 0.2)

    logger2 = MetricsLogger()
    logger2.log_value("loss", 0.3, window=2)
    logger2.log_value("loss", 0.4)

    # Reduce both loggers
    results1 = logger1.reduce()
    results2 = logger2.reduce()

    # Merge results into main logger
    logger.merge_and_log_n_dicts([results1, results2])

    # Check merged results
    check(logger.peek("loss"), 0.25)  # mean of [0.2, 0.4]


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
        logger.throughputs("count"), approx_throughput, rtol=0.05
    )  # 1% tolerance in throughput

    # Test throughputs() method without key (returns all throughputs)
    throughputs = logger.throughputs()
    check(throughputs["count_throughput"], approx_throughput, rtol=0.05)

    # Test throughput with custom EMA coefficient
    start_time = time.perf_counter()
    logger.log_value(
        "custom_ema", 1, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
    )
    num_iters = 100
    for i in range(num_iters):
        time.sleep(0.1 / num_iters)  # Simulate some time passing
        logger.log_value(
            "custom_ema",
            2,
            reduce="sum",
            with_throughput=True,
            throughput_ema_coeff=0.1,
        )
    end_time = time.perf_counter()

    # With higher EMA coefficient, throughput should adapt more quickly but tolerance should be higher
    approx_throughput = (num_iters * 2 + 1) / (end_time - start_time)
    check(logger.throughputs("custom_ema"), approx_throughput, rtol=0.5)

    # Test error cases
    with pytest.raises(ValueError):
        # Can't enable throughput for non-sum reduction
        logger.log_value("invalid", 1, reduce="mean", with_throughput=True)

    with pytest.raises(ValueError):
        # Can't enable throughput with window
        logger.log_value("invalid", 1, reduce="sum", window=10, with_throughput=True)


def test_has_throughput_property(logger):
    """Test the has_throughput property functionality."""
    # Create a Stats object with throughput tracking
    logger.log_value("with_throughput", 10, reduce="sum", with_throughput=True)
    check(logger.peek("with_throughput"), 10)
    check(logger.throughputs("with_throughput"), 0)  # Initial throughput should be 0

    # Create a Stats object without throughput tracking
    logger.log_value("without_throughput", 10, reduce="sum")
    check(logger.peek("without_throughput"), 10)

    # Test that throughputs() only includes Stats with has_throughput=True
    throughputs = logger.throughputs()
    check("with_throughput_throughput" in throughputs, True)
    check("without_throughput_throughput" in throughputs, False)

    # Test throughput value access
    with pytest.raises(ValueError):
        logger.throughputs(
            "without_throughput"
        )  # Should raise error for non-throughput stats


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


def test_log_dict_with_throughput(logger):
    """Test log_dict with throughput tracking and custom EMA coefficient."""
    # Test basic throughput tracking with log_dict
    logger.log_dict(
        {"count": 1}, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
    )

    start_time = time.perf_counter()
    num_iters = 100
    for _ in range(num_iters):
        time.sleep(0.1 / num_iters)  # Simulate some time passing
        logger.log_dict(
            {"count": 2}, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
        )
    end_time = time.perf_counter()

    # Get value and throughput
    check(logger.peek("count"), num_iters * 2 + 1)
    approx_throughput = (num_iters * 2 + 1) / (end_time - start_time)
    check(
        logger.throughputs("count"), approx_throughput, rtol=0.5
    )  # Higher tolerance due to EMA

    # Test nested dict with throughput
    logger.log_dict(
        {"nested": {"count": 1}},
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )

    start_time = time.perf_counter()
    for _ in range(num_iters):
        time.sleep(0.01)  # Simulate some time passing
        logger.log_dict(
            {"nested": {"count": 2}},
            reduce="sum",
            with_throughput=True,
            throughput_ema_coeff=0.1,
        )
    end_time = time.perf_counter()

    # Check nested throughput
    approx_throughput = (num_iters * 2 + 1) / (end_time - start_time)
    check(logger.throughputs(["nested", "count"]), approx_throughput, rtol=0.5)

    # Test error cases
    with pytest.raises(ValueError):
        # Can't enable throughput for non-sum reduction
        logger.log_dict({"invalid": 1}, reduce="mean", with_throughput=True)

    with pytest.raises(ValueError):
        # Can't enable throughput with window
        logger.log_dict({"invalid": 1}, reduce="sum", window=10, with_throughput=True)


def test_log_time_with_throughput(logger):
    """Test log_time with throughput tracking and custom EMA coefficient."""
    num_iters = 100
    sleep_time = 0.1 / num_iters
    slept_time = 0
    for _ in range(num_iters):
        with logger.log_time(
            "time", reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
        ):
            time.sleep(sleep_time)
        slept_time += sleep_time

    # Get value and throughput
    approx_throughput = 0.1 / (slept_time)
    check(
        logger.throughputs("time"), approx_throughput, rtol=0.5
    )  # Higher tolerance due to EMA

    # Test error cases
    with pytest.raises(ValueError):
        # Can't enable throughput for non-sum reduction
        with logger.log_time("invalid", reduce="mean", with_throughput=True):
            time.sleep(0.1)

    with pytest.raises(ValueError):
        # Can't enable throughput with window
        with logger.log_time("invalid", reduce="sum", window=10, with_throughput=True):
            time.sleep(0.1)


def test_merge_and_log_n_dicts_with_throughput(logger):
    """Test merge_and_log_n_dicts with throughput tracking and custom EMA coefficient."""
    # Create two loggers with different values
    logger1 = MetricsLogger()
    logger1.log_value(
        "count", 1, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
    )
    logger1.log_value(
        "count", 2, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
    )

    logger2 = MetricsLogger()
    logger2.log_value(
        "count", 3, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
    )
    logger2.log_value(
        "count", 4, reduce="sum", with_throughput=True, throughput_ema_coeff=0.1
    )

    # Reduce both loggers
    results1 = logger1.reduce()
    results2 = logger2.reduce()

    # Merge results into main logger
    logger.merge_and_log_n_dicts(
        [results1, results2],
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )

    # Check merged results
    check(logger.peek("count"), 10)  # sum of all values
    check(logger.throughputs("count"), 0.0)  # Initial throughput should be 0

    # Test nested dict merging
    logger1 = MetricsLogger()
    logger1.log_value(
        ["nested", "count"],
        1,
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )
    logger1.log_value(
        ["nested", "count"],
        2,
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )

    logger2 = MetricsLogger()
    logger2.log_value(
        ["nested", "count"],
        3,
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )
    logger2.log_value(
        ["nested", "count"],
        4,
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )

    # Reduce both loggers
    results1 = logger1.reduce()
    results2 = logger2.reduce()

    # Merge results into main logger
    logger.merge_and_log_n_dicts(
        [results1, results2],
        reduce="sum",
        with_throughput=True,
        throughput_ema_coeff=0.1,
    )

    # Check merged results
    check(logger.peek(["nested", "count"]), 10)  # sum of all values
    check(
        logger.throughputs(["nested", "count"]), 0.0
    )  # Initial throughput should be 0

    # Test error cases
    with pytest.raises(ValueError):
        # Can't enable throughput for non-sum reduction
        logger.merge_and_log_n_dicts(
            [{"invalid": 1}], reduce="mean", with_throughput=True
        )

    with pytest.raises(ValueError):
        # Can't enable throughput with window
        logger.merge_and_log_n_dicts(
            [{"invalid": 1}], reduce="sum", window=10, with_throughput=True
        )
