import logging
import os
import sys

import pytest

from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.tests.util import MockEventLogger
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
)
from ray.core.generated.instance_manager_pb2 import LaunchRequest, TerminationRequest

# coding: utf-8


OUTDATED = TerminationRequest.Cause.OUTDATED
IDLE = TerminationRequest.Cause.IDLE
MAX_NUM_NODE_PER_TYPE = TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE
MAX_NUM_NODES = TerminationRequest.Cause.MAX_NUM_NODES


def launch_request(instance_type: str, count: int) -> LaunchRequest:
    return LaunchRequest(
        instance_type=instance_type,
        count=count,
    )


def termination_request(
    instance_type: str, cause: TerminationRequest.Cause
) -> TerminationRequest:
    return TerminationRequest(
        instance_id="",
        instance_type=instance_type,
        cause=cause,
    )


logger = logging.getLogger(__name__)


def test_log_scheduling_updates():
    mock_logger = MockEventLogger(logger)
    event_logger = AutoscalerEventLogger(mock_logger)

    launch_requests = [
        launch_request("m4.large", 2),
        launch_request("m4.xlarge", 2),
    ]
    terminate_requests = [
        termination_request("m4.large", IDLE),
        termination_request("m4.xlarge", OUTDATED),
    ]
    infeasible_requests = [
        ResourceRequestUtil.make({"CPU": 4, "GPU": 1}),
    ] * 100 + [ResourceRequestUtil.make({"CPU": 4})]

    gang_resource_requests = [
        [
            ResourceRequestUtil.make({"CPU": 4, "GPU": 1}),
            ResourceRequestUtil.make({"CPU": 4, "GPU": 1}),
        ]
    ]
    cluster_resource_constraints = [
        ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
    ] * 100

    event_logger.log_cluster_scheduling_update(
        launch_requests=launch_requests,
        terminate_requests=terminate_requests,
        infeasible_requests=infeasible_requests,
        infeasible_gang_requests=[
            GangResourceRequest(requests=reqs) for reqs in gang_resource_requests
        ],
        infeasible_cluster_resource_constraints=[
            ClusterResourceConstraint(
                resource_requests=ResourceRequestUtil.group_by_count(
                    cluster_resource_constraints
                )
            )
        ],
        cluster_resources={"CPU": 5, "GPU": 5, "TPU": 2},
    )

    assert mock_logger.get_logs("info") == [
        "Adding 2 node(s) of type m4.large.",
        "Adding 2 node(s) of type m4.xlarge.",
        "Removing 1 nodes of type m4.large (idle).",
        "Removing 1 nodes of type m4.xlarge (outdated).",
        "Resized to 5 CPUs, 5 GPUs, 2 TPUs.",
    ]
    expect_lines = [
        "No available node types can fulfill resource requests",  # noqa
        "No available node types can fulfill placement group requests",  # noqa
        "No available node types can fulfill cluster constraint",  # noqa
    ]
    for expect_line, actual_line in zip(expect_lines, mock_logger.get_logs("error")):
        assert expect_line in actual_line

    assert mock_logger.get_logs("error") == []
    assert mock_logger.get_logs("debug") == [
        "Current cluster resources: {'CPU': 5, 'GPU': 5, 'TPU': 2}."
    ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
