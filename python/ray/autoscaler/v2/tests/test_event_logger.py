import logging
import os
import sys

import pytest

from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
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
        infeasible_constraints=[
            ClusterResourceConstraint(
                min_bundles=ResourceRequestUtil.group_by_count(
                    cluster_resource_constraints
                )
            )
        ],
        cluster_shape={"type-1": 1, "type-2": 2},
        node_type_configs={
            "type-1": NodeTypeConfig(
                name="type-1",
                max_worker_nodes=10,
                min_worker_nodes=1,
                resources={"CPU": 1, "GPU": 1},
            ),
            "type-2": NodeTypeConfig(
                name="type-2",
                max_worker_nodes=10,
                min_worker_nodes=1,
                resources={"CPU": 2, "GPU": 2, "TPU": 1},
            ),
        },
    )

    assert mock_logger.get_logs("info") == [
        "Adding 2 m4.large, 2 m4.xlarge.",
        "Terminating 1 nodes because they are idle, 1 nodes because they are outdated.",  # noqa
        "Resized to 5 CPUs, 5 GPUs, 2 TPUs.",
    ]

    assert mock_logger.get_logs("warning") == [
        "No available node types could fulfill: {'CPU': 4.0, 'GPU': 1.0}*100, {'CPU': 4.0}*1",  # noqa
        "No available node types could fulfill placement group requests (detail=): {'CPU': 4.0, 'GPU': 1.0}*2",  # noqa
        "No available node types could fulfill cluster constraint: {'CPU': 1.0, 'GPU': 1.0}*100",  # noqa
    ]

    assert mock_logger.get_logs("error") == []
    assert mock_logger.get_logs("debug") == [
        "Current cluster shape: {'type-1': 1, 'type-2': 2}."
    ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
