import json
import os
import sys
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
