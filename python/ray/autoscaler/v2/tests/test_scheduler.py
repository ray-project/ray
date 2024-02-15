import os
import sys

# coding: utf-8
from typing import Dict, List, Optional, Tuple

import pytest

from ray.autoscaler.v2.scheduler import (
    NodeTypeConfig,
    ResourceDemandScheduler,
    SchedulingNode,
    SchedulingNodeStatus,
    SchedulingReply,
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
from ray.core.generated.instance_manager_pb2 import Instance, TerminationRequest

ResourceMap = Dict[str, float]


def sched_request(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    max_num_nodes: Optional[int] = None,
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
        current_instances=instances,
        node_type_configs=node_type_configs,
        max_num_nodes=max_num_nodes,
    )


def _launch_and_terminate(
    reply: SchedulingReply,
) -> Tuple[Dict[NodeType, int], List[str]]:
    actual_to_launch = {req.instance_type: req.count for req in reply.to_launch}
    actual_to_terminate = [
        (req.instance_id, req.ray_node_id, req.cause) for req in reply.to_terminate
    ]

    return actual_to_launch, actual_to_terminate


class TestSchedulingNode:
    @staticmethod
    def test_is_schedulable():
        instance = make_autoscaler_instance(im_instance=None)
        assert SchedulingNode.is_schedulable(instance) is False

        all_im_status = set(Instance.InstanceStatus.values())
        positive_statuses = {
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
        }
        negative_statues = {
            Instance.UNKNOWN,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
            Instance.ALLOCATION_FAILED,
            Instance.RAY_INSTALL_FAILED,
            Instance.TERMINATION_FAILED,
        }
        for status in all_im_status:
            instance = make_autoscaler_instance(
                im_instance=Instance(instance_type="type_1", status=status)
            )

            if status in positive_statuses:
                assert SchedulingNode.is_schedulable(instance) is True
            elif status in negative_statues:
                assert SchedulingNode.is_schedulable(instance) is False
            else:
                assert False, f"Unknown status {status}"

    @staticmethod
    def test_new_node():
        # Assert none IM instance.
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 1},
                min_worker_nodes=0,
                max_worker_nodes=10,
                labels={"foo": "foo"},
            ),
        }
        instance = make_autoscaler_instance(im_instance=None)
        assert SchedulingNode.new(instance, node_type_configs) is None

        # A running ray node
        instance = make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 0},
                total_resources={"CPU": 1},
                node_id=b"r1",
                dynamic_labels={"foo": "bar"},
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="1",
                node_id="r1",
            ),
        )
        node = SchedulingNode.new(instance, node_type_configs)
        assert node is not None
        assert node.node_type == "type_1"
        assert node.status == SchedulingNodeStatus.SCHEDULABLE
        assert node.ray_node_id == "r1"
        assert node.im_instance_id == "1"
        assert node.available_resources == {"CPU": 0}
        assert node.total_resources == {"CPU": 1}
        assert node.labels == {"foo": "bar"}

        # A outdated node.
        instance = make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_no_longer_exists",
                status=Instance.REQUESTED,
                instance_id="1",
            ),
        )
        node = SchedulingNode.new(instance, node_type_configs)
        assert node is not None
        assert node.node_type == "type_no_longer_exists"
        assert node.status == SchedulingNodeStatus.TO_TERMINATE
        assert node.termination_request is not None
        assert node.termination_request.cause == TerminationRequest.Cause.OUTDATED

        # A pending ray node
        instance = make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.REQUESTED,
                instance_id="1",
            )
        )
        node = SchedulingNode.new(instance, node_type_configs)
        assert node is not None
        assert node.node_type == "type_1"
        assert node.status == SchedulingNodeStatus.SCHEDULABLE
        assert node.available_resources == {"CPU": 1}
        assert node.total_resources == {"CPU": 1}
        assert node.labels == {"foo": "foo"}


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
    actual_to_launch, _ = _launch_and_terminate(reply)
    assert sorted(actual_to_launch) == sorted(expected_to_launch)

    # With existing ray nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_1", status=Instance.RAY_RUNNING
                ),
                ray_node=NodeState(ray_node_type_name="type_1"),
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_1", status=Instance.RAY_RUNNING
                ),
                ray_node=NodeState(ray_node_type_name="type_1"),
            ),
        ],
    )

    expected_to_launch = {"type_3": 2}
    reply = scheduler.schedule(request)
    actual_to_launch, _ = _launch_and_terminate(reply)
    assert actual_to_launch == expected_to_launch

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
                    instance_type="type_no_longer_exists",
                    status=Instance.REQUESTED,
                    instance_id="0",
                )
            ),
        ],
    )
    expected_to_launch = {"type_3": 2}
    reply = scheduler.schedule(request)
    actual_to_launch, _ = _launch_and_terminate(reply)
    assert actual_to_launch == expected_to_launch


def test_max_workers_per_type():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=2,
            max_worker_nodes=2,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
    )

    reply = scheduler.schedule(request)

    expected_to_terminate = []
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert sorted(actual_to_terminate) == sorted(expected_to_terminate)

    instances = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_1", status=Instance.ALLOCATED, instance_id="0"
            ),
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 1},
                total_resources={"CPU": 1},
                node_id=b"r1",
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="1",
                node_id="r1",
            ),
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 0.5},
                total_resources={"CPU": 1},
                node_id=b"r2",
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="2",
                node_id="r2",
            ),
        ),
    ]

    # 3 running instances with max of 2 allowed for type 1.
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
    )

    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert actual_to_terminate == [
        ("0", "", TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE)
    ]

    # 3 running instances with max of 1 allowed for type 1.
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
    )

    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert sorted(actual_to_terminate) == sorted(
        [
            ("0", "", TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE),
            # Lower resource util.
            (
                "1",
                "r1",
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
            ),
        ]
    )


def test_max_num_nodes():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=2,
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=2,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        max_num_nodes=1,
    )

    reply = scheduler.schedule(request)

    expected_to_terminate = []
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert sorted(actual_to_terminate) == sorted(expected_to_terminate)

    instances = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_1", status=Instance.ALLOCATED, instance_id="0"
            ),
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 1},
                total_resources={"CPU": 1},
                node_id=b"r1",
                idle_duration_ms=10,
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="1",
                node_id="r1",
            ),
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_2",
                available_resources={"CPU": 0.5},
                total_resources={"CPU": 1},
                node_id=b"r2",
            ),
            im_instance=Instance(
                instance_type="type_2",
                status=Instance.RAY_RUNNING,
                instance_id="2",
                node_id="r2",
            ),
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_2",
                available_resources={"CPU": 0.0},
                total_resources={"CPU": 1},
                node_id=b"r3",
            ),
            im_instance=Instance(
                instance_type="type_2",
                status=Instance.RAY_RUNNING,
                instance_id="3",
                node_id="r3",
            ),
        ),
    ]

    # 4 running with 4 max => no termination
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
        max_num_nodes=4,
    )

    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert actual_to_terminate == []

    # 4 running with 3 max => terminate 1
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
        max_num_nodes=3,
    )

    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    # Terminate one non-ray running first.
    assert actual_to_terminate == [("0", "", TerminationRequest.Cause.MAX_NUM_NODES)]

    # 4 running with 2 max => terminate 2
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
        max_num_nodes=2,
    )
    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    # Terminate one non-ray running first.
    assert sorted(actual_to_terminate) == sorted(
        [
            ("0", "", TerminationRequest.Cause.MAX_NUM_NODES),  # non-ray running
            ("1", "r1", TerminationRequest.Cause.MAX_NUM_NODES),  # idle
        ]
    )

    # 4 running with 1 max => terminate 3
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
        max_num_nodes=1,
    )
    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert sorted(actual_to_terminate) == sorted(
        [
            ("0", "", TerminationRequest.Cause.MAX_NUM_NODES),  # non-ray running
            ("1", "r1", TerminationRequest.Cause.MAX_NUM_NODES),  # idle
            ("2", "r2", TerminationRequest.Cause.MAX_NUM_NODES),  # less resource util
        ]
    )

    # Combine max_num_nodes with max_num_nodes_per_type
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=2,
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=0,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
        max_num_nodes=1,
    )
    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert sorted(actual_to_terminate) == sorted(
        [
            ("0", "", TerminationRequest.Cause.MAX_NUM_NODES),  # non-ray running
            ("2", "r2", TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE),  # type-2
            ("3", "r3", TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE),  # type-2
        ]
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
