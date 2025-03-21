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
    logger.log_value("loss", 0.3)
    
    # Test peek
    check(logger["loss"].peek(), 0.2)  # mean of [0.1, 0.2, 0.3]
    
    # Test reduce
    results = logger.reduce()
    check(results["loss"], 0.2)


def test_nested_keys(logger):
    """Test logging with nested key structures."""
    # Test nested key logging
    logger.log_value(("nested", "key"), 1.0)
    logger.log_value(("nested", "key"), 2.0)
    
    # Test peek with nested key
    check(logger["nested"]["key"].peek(), 1.5)
    
    # Test reduce with nested key
    results = logger.reduce()
    check(results["nested"]["key"], 1.5)


def test_different_reduction_methods(logger):
    """Test different reduction methods (mean, min, sum)."""
    # Test mean reduction
    logger.log_value("mean_loss", 0.1, reduce="mean")
    logger.log_value("mean_loss", 0.2)
    check(logger["mean_loss"].peek(), 0.15)
    
    # Test min reduction
    logger.log_value("min_loss", 0.3, reduce="min")
    logger.log_value("min_loss", 0.1)
    logger.log_value("min_loss", 0.2)
    check(logger["min_loss"].peek(), 0.1)
    
    # Test sum reduction
    logger.log_value("total_steps", 10, reduce="sum")
    logger.log_value("total_steps", 20)
    check(logger["total_steps"].peek(), 30)


def test_window_based_reduction(logger):
    """Test window-based reduction."""
    # Test with window=2
    logger.log_value("window_loss", 0.1, window=2)
    logger.log_value("window_loss", 0.2)
    logger.log_value("window_loss", 0.3)
    check(logger["window_loss"].peek(), 0.25)  # mean of [0.2, 0.3]
    
    # Test with window=3
    logger.log_value("window3_loss", 0.1, window=3)
    logger.log_value("window3_loss", 0.2)
    logger.log_value("window3_loss", 0.3)
    logger.log_value("window3_loss", 0.4)
    check(logger["window3_loss"].peek(), 0.3)  # mean of [0.2, 0.3, 0.4]


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
    check(logger["ema_loss"].peek(), 0.129)


def test_tensor_mode(logger):
    """Test tensor mode functionality."""
    # Test with PyTorch tensors
    if torch:
        logger.activate_tensor_mode()
        logger.log_value("torch_loss", torch.tensor(0.1))
        logger.log_value("torch_loss", torch.tensor(0.2))
        tensor_metrics = logger.deactivate_tensor_mode()
        logger.tensors_to_numpy(tensor_metrics)
        check(logger["torch_loss"].peek(), 0.15)


def test_time_logging(logger):
    """Test time logging functionality."""
    # Test time logging with EMA
    with logger.log_time("block_time", ema_coeff=0.1):
        time.sleep(0.1)
    
    # Test time logging with window
    with logger.log_time("window_time", window=2):
        time.sleep(0.2)
    
    # Check that times are approximately correct
    check(logger["block_time"].peek(), 0.1, atol=0.05)
    check(logger["window_time"].peek(), 0.2, atol=0.05)


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
    check(new_logger["state_test"].peek(), 0.15)


def test_edge_cases(logger):
    """Test edge cases and error handling."""
    # Test non-existent key
    with pytest.raises(KeyError):
        logger["non_existent"].peek()
    
    # Test invalid reduction method
    with pytest.raises(ValueError):
        logger.log_value("invalid_reduce", 0.1, reduce="invalid")
    
    # Test window and ema_coeff together
    with pytest.raises(ValueError):
        logger.log_value("invalid_window_ema", 0.1, window=2, ema_coeff=0.1)
    
    # Test clear_on_reduce
    logger.log_value("clear_test", 0.1, clear_on_reduce=True)
    logger.log_value("clear_test", 0.2)
    results = logger.reduce()
    check(results["clear_test"], 0.15)
    check(logger["clear_test"].peek(), 0.0)  # Should be cleared


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
    check(logger["loss"].peek(), 0.25)  # mean of [0.2, 0.4]


def test_throughput_tracking(logger):
    """Test throughput tracking functionality."""
    # Test basic throughput tracking
    logger.log_value("count", 10, reduce="sum", with_throughput=True)
    time.sleep(0.1)  # Simulate some time passing
    logger.log_value("count", 20, reduce="sum", with_throughput=True)
    
    # Get value and throughput
    stats = logger["count"]
    check(stats.peek(), 30)
    check(stats.throughput, 100)  # 30/0.3 ≈ 100

    # Test throughputs() method
    throughputs = logger.throughputs()
    check(throughputs["count_throughput"], 100)

    # Test that throughput tracking is only enabled for sum reduction
    with pytest.raises(ValueError):
        logger.log_value("invalid_throughput", 1.0, reduce="mean", with_throughput=True)

    # Test nested throughput tracking
    logger.log_value(("learner", "samples"), 50, reduce="sum", with_throughput=True)
    time.sleep(0.1)
    logger.log_value(("learner", "samples"), 100, reduce="sum", with_throughput=True)
    
    # Check nested throughputs
    throughputs = logger.throughputs()
    check(throughputs["learner"]["samples_throughput"], 500)  # 150/0.3 ≈ 500


def test_has_throughput_property(logger):
    """Test the has_throughput property functionality."""
    # Create a Stats object with throughput tracking
    logger.log_value("with_throughput", 10, reduce="sum", with_throughput=True)
    stats = logger["with_throughput"]
    check(stats.has_throughput, True)

    # Create a Stats object without throughput tracking
    logger.log_value("without_throughput", 10, reduce="sum")
    stats = logger["without_throughput"]
    check(stats.has_throughput, False)

    # Test that throughputs() only includes Stats with has_throughput=True
    throughputs = logger.throughputs()
    check("with_throughput_throughput" in throughputs, True)
    check("without_throughput_throughput" in throughputs, False)

    # Test throughput value access
    check(stats.throughput, 0)  # Should be 0 for Stats without throughput tracking
    check(logger["with_throughput"].throughput, 0)  # Initial throughput should be 0


def test_reset_and_delete(logger):
    """Test reset and delete functionality."""
    # Log some values
    logger.log_value("test1", 0.1)
    logger.log_value("test2", 0.2)
    
    # Test delete
    logger.delete("test1")
    with pytest.raises(KeyError):
        logger["test1"].peek()
    
    # Test reset
    logger.reset()
    check(logger.reduce(), {})
