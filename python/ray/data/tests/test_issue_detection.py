import io
import logging
import re
import time
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    MapOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.issue_detection.detectors.hanging_detector import (
    DEFAULT_OP_TASK_STATS_MIN_COUNT,
    DEFAULT_OP_TASK_STATS_STD_FACTOR,
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
)
from ray.data._internal.issue_detection.detectors.high_memory_detector import (
    HighMemoryIssueDetector,
)
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
        logger = logging.getLogger("ray.data._internal.issue_detection")
        logger.addHandler(handler)

        # Set up mock stats to return values that will trigger adaptive threshold
        mocked_mean = 2.0  # Increase from 0.5 to 2.0 seconds
        mocked_stddev = 0.2  # Increase from 0.05 to 0.2 seconds
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
        warn_msg = (
            r"A task of operator .+ with task index .+ has been running for [\d\.]+s"
        )
        assert re.search(warn_msg, log_output) is None, log_output

        # # test hanging does log hanging warning
        def f2(x):
            time.sleep(5.0)  # Increase from 1.1 to 5.0 seconds to exceed new threshold
            return x

        _ = ray.data.range(1).map(f2).materialize()

        log_output = log_capture.getvalue()
        assert re.search(warn_msg, log_output) is not None, log_output

    def test_hanging_detector_detects_issues(
        self, caplog, propagate_logs, restore_data_context
    ):
        """Test hanging detector adaptive thresholds with real Ray Data pipelines and extreme configurations."""

        ctx = DataContext.get_current()
        # Configure hanging detector with extreme std_factor values
        ctx.issue_detectors_config.hanging_detector_config = (
            HangingExecutionIssueDetectorConfig(
                op_task_stats_min_count=1,
                op_task_stats_std_factor=1,
                detection_time_interval_s=0,
            )
        )

        # Create a pipeline with many small blocks to ensure concurrent tasks
        def sleep_task(x):
            if x["id"] == 2:
                # Issue detection is based on the mean + stdev. One of the tasks must take
                # awhile, so doing it just for one of the rows.
                time.sleep(1)
            return x

        with caplog.at_level(logging.WARNING):
            ray.data.range(3, override_num_blocks=3).map(
                sleep_task, concurrency=1
            ).materialize()

        # Check if hanging detection occurred
        hanging_detected = (
            "has been running for" in caplog.text
            and "longer than the average task duration" in caplog.text
        )

        assert hanging_detected, caplog.text


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
