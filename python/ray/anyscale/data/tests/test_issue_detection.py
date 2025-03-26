import io
import logging
import threading
import time
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import ray
from ray.anyscale.data.issue_detection.detectors.hanging_detector import (
    DEFAULT_OP_TASK_STATS_MIN_COUNT,
    DEFAULT_OP_TASK_STATS_STD_FACTOR,
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
)
from ray.anyscale.data.issue_detection.detectors.high_memory_detector import (
    HighMemoryIssueDetector,
)
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    MapOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.context import DataContext
from ray.tests.conftest import *  # noqa


class TestHangingExecutionIssueDetector:
    def test_hanging_detector_configuration(self, restore_data_context):
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
    def test_basic_hanging_detection(
        self, mock_stats_cls, ray_start_2_cpus, restore_data_context
    ):
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
        detector_cfg = ctx.issue_detectors_config.hanging_detector_config
        detector_cfg.detection_time_interval_s = 0.00

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

    @pytest.mark.parametrize(
        "ray_start_10_cpus",
        # NOTE: By default, test fixtures initialize Ray with 150 MiB of object store
        # memory. This means that Ray Data can only launch 1 task before getting
        # backpressured, and that interferes with the test. So, we increase the object
        # store memory to 2 GiB. (I'm choosing 2 GiB because it's the largest value you
        # can use on Mac, which most developers use.)
        [{"object_store_memory": 2 * 1024**3}],
        indirect=True,
    )
    def test_realistic_hanging_detection(
        self,
        ray_start_10_cpus,
        caplog,
        propagate_logs,
        restore_data_context,
    ):
        """Test hanging detection in a realistic scenario with multiple tasks."""
        # Set up logging capture
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logger = logging.getLogger("ray.anyscale.data.issue_detection")
        logger.addHandler(handler)

        # Configure detector for quick detection
        ctx = DataContext.get_current()
        ctx.issue_detectors_config.hanging_detector_config = (
            HangingExecutionIssueDetectorConfig(
                op_task_stats_min_count=5,
                op_task_stats_std_factor=3.0,
                detection_time_interval_s=0.1,
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


@pytest.mark.parametrize(
    "configured_memory, actual_memory, should_return_issue",
    [
        # User has appropriately configured memory, so no issue.
        (4 * 1024**3, 4 * 1024**3, False),
        # User hasn't configured memory correctly and memory use is high, so issue.
        (None, 4 * 1024**3, True),
        (1, 4 * 1024**3, True),
        # User hasn't configured memory correctly but memory use is low, so no issue.
        (None, 4 * 1024**3 - 1, False),
    ],
)
def test_high_memory_detection(
    configured_memory, actual_memory, should_return_issue, restore_data_context
):
    ctx = DataContext.get_current()

    input_data_buffer = InputDataBuffer(ctx, input_data=[])
    map_operator = MapOperator.create(
        map_transformer=MagicMock(),
        input_op=input_data_buffer,
        data_context=ctx,
        ray_remote_args={"memory": configured_memory},
    )
    map_operator._metrics = MagicMock(average_max_uss_per_task=actual_memory)
    topology = {input_data_buffer: MagicMock(), map_operator: MagicMock()}
    executor = MagicMock(_topology=topology)

    detector = HighMemoryIssueDetector(executor, ctx)
    issues = detector.detect()

    assert should_return_issue == bool(issues)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
