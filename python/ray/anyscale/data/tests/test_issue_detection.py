import unittest
from unittest.mock import patch
import numpy as np
from ray.anyscale.data.issue_detection.detectors.hanging_detector import (
    DEFAULT_OP_TASK_STATS_MIN_COUNT,
    DEFAULT_OP_TASK_STATS_STD_FACTOR,
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
)
from ray.data.context import DataContext
from ray.data._internal.execution.streaming_executor import StreamingExecutor
import logging
import io
import time
import ray
import threading


class TestHangingExecutionIssueDetector(unittest.TestCase):
    def test_hanging_detector_configuration(self):
        """Test hanging detector configuration and initialization."""
        # Test default configuration from DataContext
        ctx = DataContext.get_current()
        default_config = ctx.issue_detectors_config.hanging_detector_config
        assert default_config.op_task_stats_min_count == DEFAULT_OP_TASK_STATS_MIN_COUNT
        assert (
            default_config.op_task_stats_std_factor == DEFAULT_OP_TASK_STATS_STD_FACTOR
        )

        # Test custom configuration
        min_count = 5
        std_factor = 3.0
        custom_config = HangingExecutionIssueDetectorConfig(
            op_task_stats_min_count=min_count,
            op_task_stats_std_factor=std_factor,
        )
        ctx.issue_detectors_config.hanging_detector_config = custom_config

        executor = StreamingExecutor(ctx)
        detector = HangingExecutionIssueDetector(executor, ctx)
        assert detector._op_task_stats_min_count == min_count
        assert detector._op_task_stats_std_factor_threshold == std_factor

    @patch(
        "ray.data._internal.execution.interfaces.op_runtime_metrics.TaskDurationStats"
    )
    def test_basic_hanging_detection(self, mock_stats_cls):
        # Set up logging capture
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logger = logging.getLogger("ray.anyscale.data.issue_detection")
        logger.addHandler(handler)

        # Set up mock stats to return values that will trigger adaptive threshold
        mocked_mean = 0.5
        mocked_stddev = 0.05
        mock_stats = mock_stats_cls.return_value
        mock_stats.count.return_value = 20  # Enough samples
        mock_stats.mean.return_value = mocked_mean
        mock_stats.stddev.return_value = mocked_stddev

        # Set a short issue detection interval for testing
        ctx = DataContext.get_current()
        ctx.issue_detectors_config.detection_time_interval_s = 0.00

        # test no hanging doesn't log hanging warning
        def f1(x):
            return x

        _ = ray.data.range(1).map(f1).materialize()

        log_output = log_capture.getvalue()
        assert "hanging" not in log_output

        # # test hanging does log hanging warning
        def f2(x):
            time.sleep(1.1)
            return x

        _ = ray.data.range(1).map(f2).materialize()

        log_output = log_capture.getvalue()
        assert "hanging" in log_output

    def test_realistic_hanging_detection(self):
        """Test hanging detection in a realistic scenario with multiple tasks."""
        # Set up logging capture
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logger = logging.getLogger("ray.anyscale.data.issue_detection")
        logger.addHandler(handler)

        # Configure detector for quick detection
        ctx = DataContext.get_current()
        ctx.issue_detectors_config.detection_time_interval_s = 0.1
        ctx.issue_detectors_config.hanging_detector_config = (
            HangingExecutionIssueDetectorConfig(
                op_task_stats_min_count=5,
                op_task_stats_std_factor=3.0,
            )
        )

        def processing_fn(batch):
            if 0 in batch["id"]:  # Make one specific task hang
                while True:
                    time.sleep(1)
            else:
                # Normal tasks take ~0.1s with some random variation
                time.sleep(0.1 + np.random.normal(0, 0.01))
            return batch

        # Use an event to signal thread termination
        stop_event = threading.Event()

        def materialize_in_thread():
            try:
                ds = ray.data.range(10_000).map_batches(processing_fn, batch_size=100)
                for _ in ds.iter_batches():
                    if stop_event.is_set():
                        break
            except Exception:
                pass  # Ignore exceptions from the hanging task

        thread = threading.Thread(target=materialize_in_thread)
        thread.daemon = True
        thread.start()

        # Wait for hanging warning
        start_time = time.perf_counter()
        max_wait = 5
        warning_found = False

        while time.perf_counter() - start_time < max_wait:
            log_output = log_capture.getvalue()
            if "hanging" in log_output:
                warning_found = True
                break
            time.sleep(0.1)

        # Signal thread to stop and wait for it
        stop_event.set()
        thread.join(timeout=2)  # Wait up to 2 seconds for thread to finish

        # Clean up
        logger.removeHandler(handler)

        assert warning_found, "No hanging warning detected within timeout"
