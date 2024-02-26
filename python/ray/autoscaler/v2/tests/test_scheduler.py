import os
import sys

# coding: utf-8
from typing import Dict, List, Optional, Tuple

import pytest

import ray
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
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    NodeState,
    ResourceRequest,
)
from ray.core.generated.instance_manager_pb2 import Instance, TerminationRequest

ResourceMap = Dict[str, float]


def sched_request(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    max_num_nodes: Optional[int] = None,
    resource_requests: Optional[List[ResourceRequest]] = None,
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
        resource_requests=ResourceRequestUtil.group_by_count(resource_requests),
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


def test_single_resources():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Request 1 CPU should start a node.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
    )
    reply = scheduler.schedule(request)
    to_lauch, _ = _launch_and_terminate(reply)
    assert sorted(to_lauch) == sorted({"type_1": 1})

    # Request multiple CPUs should start multiple nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 3,
    )
    reply = scheduler.schedule(request)
    to_lauch, _ = _launch_and_terminate(reply)
    assert sorted(to_lauch) == sorted({"type_1": 3})

    # Request resources with already existing nodes should not launch new nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
        instances=[
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
        ],
    )
    reply = scheduler.schedule(request)
    to_lauch, _ = _launch_and_terminate(reply)
    assert sorted(to_lauch) == sorted({})

    # Request resources with already existing nodes not sufficient should launch
    # new nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
        instances=[
            make_autoscaler_instance(
                ray_node=NodeState(
                    ray_node_type_name="type_1",
                    available_resources={"CPU": 0.9},
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
        ],
    )
    reply = scheduler.schedule(request)
    to_lauch, _ = _launch_and_terminate(reply)
    assert sorted(to_lauch) == sorted({"type_1": 1})

    # Request resources with already pending nodes should NOT launch new nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_1", status=Instance.REQUESTED, instance_id="0"
                ),
            ),
        ],
    )
    reply = scheduler.schedule(request)
    to_lauch, _ = _launch_and_terminate(reply)
    assert sorted(to_lauch) == sorted({})


def test_implicit_resources():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }
    implicit_resource = ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a"

    # implicit resources should scale up clusters.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({implicit_resource: 1})],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_1": 1})

    # implicit resources should be satisfied by existing node.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({implicit_resource: 1}),
            ResourceRequestUtil.make({"CPU": 1}),
        ],
        instances=[
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
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {}


def test_max_worker_num_enforce_with_resource_requests():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }
    max_num_nodes = 2

    # Request 10 CPUs should start at most 2 nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        max_num_nodes=max_num_nodes,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 3,
        instances=[
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
        ],
    )
    reply = scheduler.schedule(request)
    to_lauch, _ = _launch_and_terminate(reply)
    assert sorted(to_lauch) == sorted({"type_1": 1})


def test_multi_requests_fittable():
    """
    Test multiple requests can be fit into a single node.
    """
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 3},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_1": 1, "type_2": 1})
    assert reply.infeasible_resource_requests == []

    # Change the ordering of requests should not affect the result.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_1": 1, "type_2": 1})
    assert reply.infeasible_resource_requests == []

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 2}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 0.5, "GPU": 0.5}),
            ResourceRequestUtil.make({"CPU": 0.5, "GPU": 0.5}),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_1": 1, "type_2": 1})
    assert reply.infeasible_resource_requests == []

    # However, if we already have fragmentation. We should not be able
    # to fit more requests.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
        ],
        instances=[
            make_autoscaler_instance(
                ray_node=NodeState(
                    ray_node_type_name="type_1",
                    available_resources={"CPU": 0, "GPU": 1},
                    total_resources={"CPU": 1, "GPU": 1},
                    node_id=b"r1",
                ),
                im_instance=Instance(
                    instance_type="type_1",
                    status=Instance.RAY_RUNNING,
                    instance_id="1",
                    node_id="r1",
                ),
            ),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_2": 1})
    assert len(reply.infeasible_resource_requests) == 1


def test_multi_node_types_score():
    """
    Test that when multiple node types are possible, choose the best scoring ones:
    1. The number of resources utilized.
    2. The amount of utilization.
    """
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_large": NodeTypeConfig(
            name="type_large",
            resources={"CPU": 10},  # Large machines
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_small": NodeTypeConfig(
            name="type_small",
            resources={"CPU": 5},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_gpu": NodeTypeConfig(
            name="type_gpu",
            resources={"CPU": 2, "GPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    # Request 1 CPU should just start the small machine and not the GPU machine
    # since it has more types of resources.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_small": 1})

    # type_small should be preferred over type_large.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 2})],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_small": 1})


def test_multi_node_types_score_with_gpu(monkeypatch):
    """
    Test that when multiple node types are possible, choose the best scoring ones:
    - The GPU scoring.
    """
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_gpu": NodeTypeConfig(
            name="type_gpu",
            resources={"CPU": 1, "GPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_multi": NodeTypeConfig(
            name="type_multi",
            resources={"CPU": 2, "XXX": 2},  # Some random resource.
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_multi": 1})

    with monkeypatch.context() as m:
        m.setattr(ray.autoscaler.v2.scheduler, "AUTOSCALER_CONSERVE_GPU_NODES", 0)
        # type_multi should now be preferred over type_gpu.
        reply = scheduler.schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        assert sorted(to_launch) == sorted({"type_gpu": 1})


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
