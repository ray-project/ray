import os
import sys

# coding: utf-8
from typing import Dict, List, Optional

import pytest

from ray.autoscaler.v2.scheduler import (
    NodeTypeConfig,
    ResourceDemandScheduler,
    SchedulingRequest,
)
from ray.autoscaler.v2.schema import AutoscalerInstance, NodeType
from ray.autoscaler.v2.tests.util import make_autoscaler_instance
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    NodeState,
    ResourceRequestByCount,
)
from ray.core.generated.instance_manager_pb2 import Instance

ResourceMap = Dict[str, float]


def sched_request(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    max_num_worker_nodes: Optional[int] = None,
    resource_requests: Optional[List[ResourceRequestByCount]] = None,
    gang_resource_requests: Optional[List[GangResourceRequest]] = None,
    cluster_resource_constraints: Optional[List[ClusterResourceConstraint]] = None,
    instances: Optional[List[AutoscalerInstance]] = None,
) -> SchedulingRequest:

    if resource_requests is None:
        resource_requests = []
    if gang_resource_requests is None:
        gang_resource_requests = []
    if cluster_resource_constraints is None:
        cluster_resource_constraints = []
    if instances is None:
        instances = []

    return SchedulingRequest(
        resource_requests=resource_requests,
        gang_resource_requests=gang_resource_requests,
        cluster_resource_constraints=cluster_resource_constraints,
        instances=instances,
        node_type_configs=node_type_configs,
        max_num_worker_nodes=max_num_worker_nodes,
    )


def test_min_worker_nodes():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=1,
            max_worker_nodes=10,
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "type_3": NodeTypeConfig(
            name="type_3",
            resources={"CPU": 1},
            min_worker_nodes=2,
            max_worker_nodes=10,
        ),
    }
    # With empty cluster
    request = sched_request(
        node_type_configs=node_type_configs,
    )

    reply = scheduler.schedule(request)

    expected_to_launch = {"type_1": 1, "type_3": 2}
    reply = scheduler.schedule(request)
    actual_to_launch = {req.instance_type: req.count for req in reply.to_launch}
    assert sorted(actual_to_launch) == sorted(expected_to_launch)

    # With existing ray nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            make_autoscaler_instance(ray_node=NodeState(ray_node_type_name="type_1")),
            make_autoscaler_instance(ray_node=NodeState(ray_node_type_name="type_1")),
        ],
    )

    expected_to_launch = {"type_3": 2}
    reply = scheduler.schedule(request)
    actual_to_launch = {req.instance_type: req.count for req in reply.to_launch}

    assert sorted(actual_to_launch) == sorted(expected_to_launch)

    # With existing instances pending.
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(instance_type="type_1", status=Instance.REQUESTED)
            ),
            make_autoscaler_instance(
                im_instance=Instance(instance_type="type_1", status=Instance.ALLOCATED)
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_no_longer_exists", status=Instance.REQUESTED
                )
            ),
        ],
    )
    expected_to_launch = {"type_3": 2}
    reply = scheduler.schedule(request)
    actual_to_launch = {req.instance_type: req.count for req in reply.to_launch}
    assert sorted(actual_to_launch) == sorted(expected_to_launch)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
