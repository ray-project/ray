import sys
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    MapOperator,
)
from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueType,
)
from ray.data._internal.issue_detection.issue_detector_manager import (
    IssueDetectorManager,
)
from ray.data.context import DataContext


def test_report_issues():
    ctx = DataContext.get_current()
    input_operator = InputDataBuffer(ctx, input_data=[])
    map_operator = MapOperator.create(
        map_transformer=MagicMock(),
        input_op=input_operator,
        data_context=ctx,
        ray_remote_args={},
    )
    topology = {input_operator: MagicMock(), map_operator: MagicMock()}
    executor = MagicMock(_topology=topology)
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
