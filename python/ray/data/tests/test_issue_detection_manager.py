import json
import os
import sys
import time
from unittest.mock import MagicMock

import pytest

import ray
from ray._private import ray_constants
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    MapOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.issue_detection.detectors.hanging_detector import (
    HangingExecutionIssueDetectorConfig,
)
from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueType,
)
from ray.data._internal.issue_detection.issue_detector_manager import (
    IssueDetectorManager,
)
from ray.data._internal.operator_event_exporter import (
    format_export_issue_event_name,
)
from ray.data.context import DataContext


def _get_exported_data():
    exported_file = os.path.join(
        ray._private.worker._global_node.get_session_dir_path(),
        "logs",
        "export_events",
        "event_EXPORT_DATASET_OPERATOR_EVENT.log",
    )
    assert os.path.isfile(exported_file)

    with open(exported_file, "r") as f:
        data = f.readlines()

    return [json.loads(line) for line in data]


def test_report_issues():
    ray.init()
    ray_constants.RAY_ENABLE_EXPORT_API_WRITE_CONFIG = "EXPORT_DATASET_OPERATOR_EVENT"
    ctx = DataContext.get_current()
    input_operator = InputDataBuffer(ctx, input_data=[])
    map_operator = MapOperator.create(
        map_transformer=MagicMock(),
        input_op=input_operator,
        data_context=ctx,
        ray_remote_args={},
    )
    topology = {input_operator: MagicMock(), map_operator: MagicMock()}
    executor = StreamingExecutor(ctx)
    executor._topology = topology
    detector = IssueDetectorManager(executor)

    detector._report_issues(
        [
            Issue(
                dataset_name="dataset",
                operator_id=input_operator.id,
                issue_type=IssueType.HANGING,
                message="Hanging detected",
            ),
            Issue(
                dataset_name="dataset",
                operator_id=map_operator.id,
                issue_type=IssueType.HIGH_MEMORY,
                message="High memory usage detected",
            ),
        ]
    )
    assert input_operator.metrics.issue_detector_hanging == 1
    assert input_operator.metrics.issue_detector_high_memory == 0
    assert map_operator.metrics.issue_detector_hanging == 0
    assert map_operator.metrics.issue_detector_high_memory == 1

    data = _get_exported_data()
    assert len(data) == 2
    assert data[0]["event_data"]["dataset_id"] == "dataset"
    assert data[0]["event_data"]["operator_id"] == f"{input_operator.name}_0"
    assert data[0]["event_data"]["operator_name"] == input_operator.name
    assert data[0]["event_data"]["event_type"] == format_export_issue_event_name(
        IssueType.HANGING
    )
    assert data[0]["event_data"]["message"] == "Hanging detected"
    assert data[1]["event_data"]["dataset_id"] == "dataset"
    assert data[1]["event_data"]["operator_id"] == f"{map_operator.name}_1"
    assert data[1]["event_data"]["operator_name"] == map_operator.name
    assert data[1]["event_data"]["event_type"] == format_export_issue_event_name(
        IssueType.HIGH_MEMORY
    )
    assert data[1]["event_data"]["message"] == "High memory usage detected"


@pytest.mark.parametrize(
    "should_trigger, test_description",
    [
        (False, "high threshold (1000.0) - should NOT detect issues"),
        (True, "low threshold (0.0) - should detect issues for slow tasks"),
    ],
)
def test_hanging_detector_detects_issues(should_trigger, test_description):
    """Test hanging detector adaptive thresholds with real Ray Data pipelines and extreme configurations."""
    import io
    import logging

    ctx = DataContext.get_current()
    # Configure hanging detector with extreme std_factor values
    ctx.issue_detectors_config.hanging_detector_config = (
        HangingExecutionIssueDetectorConfig(
            op_task_stats_min_count=1,
            op_task_stats_std_factor=1,
            detection_time_interval_s=0,
        )
    )

    # Set up logging capture to detect hanging warnings
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.WARNING)

    # Get the issue detector manager logger
    logger = logging.getLogger(
        "ray.data._internal.issue_detection.issue_detector_manager"
    )
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    original_propagate = logger.propagate
    logger.propagate = False

    try:
        # Create a pipeline with many small blocks to ensure concurrent tasks
        def sleep_task(x):
            if x["id"] == 9 and should_trigger:
                # Issue detection is based on the mean + stdev. One of the tasks must take
                # awhile, so doing it just for one of the rows.
                time.sleep(1)
            return x

        ray.data.range(10, override_num_blocks=10).map(sleep_task).materialize()

        # Check if hanging detection occurred
        log_output = log_capture.getvalue()
        hanging_detected = (
            "has been running for" in log_output
            and "longer than the average task duration" in log_output
        )

        if should_trigger:
            assert hanging_detected, test_description
        else:
            assert not hanging_detected, test_description

    finally:
        # Clean up logging
        logger.removeHandler(handler)
        logger.propagate = original_propagate


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
