import os
import sys
import time

# coding: utf-8
from typing import Dict, List, Optional, Tuple

import pytest

import ray
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.scheduler import (
    NodeTypeConfig,
    ResourceDemandScheduler,
    ResourceRequestSource,
    SchedulingNode,
    SchedulingNodeStatus,
    SchedulingReply,
    SchedulingRequest,
    logger,
)
from ray.autoscaler.v2.schema import AutoscalerInstance, NodeType
from ray.autoscaler.v2.tests.util import MockEventLogger, make_autoscaler_instance
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    NodeState,
    NodeStatus,
    ResourceRequest,
)
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    NodeKind,
    TerminationRequest,
)

ResourceMap = Dict[str, float]

logger.setLevel("DEBUG")

event_logger = AutoscalerEventLogger(MockEventLogger(logger))


def sched_request(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    max_num_nodes: Optional[int] = None,
    resource_requests: Optional[List[ResourceRequest]] = None,
    gang_resource_requests: Optional[List[List[ResourceRequest]]] = None,
    cluster_resource_constraints: Optional[List[ResourceRequest]] = None,
    instances: Optional[List[AutoscalerInstance]] = None,
    idle_timeout_s: Optional[float] = None,
    disable_launch_config_check: Optional[bool] = False,
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
        gang_resource_requests=[
            GangResourceRequest(requests=reqs) for reqs in gang_resource_requests
        ],
        cluster_resource_constraints=(
            [
                ClusterResourceConstraint(
                    min_bundles=ResourceRequestUtil.group_by_count(
                        cluster_resource_constraints
                    )
                )
            ]
            if cluster_resource_constraints
            else []
        ),
        current_instances=instances,
        node_type_configs=node_type_configs,
        max_num_nodes=max_num_nodes,
        idle_timeout_s=idle_timeout_s,
        disable_launch_config_check=disable_launch_config_check,
    )


def _launch_and_terminate(
    reply: SchedulingReply,
) -> Tuple[Dict[NodeType, int], List[str]]:
    actual_to_launch = {req.instance_type: req.count for req in reply.to_launch}
    actual_to_terminate = [
        (req.instance_id, req.ray_node_id, req.cause) for req in reply.to_terminate
    ]

    return actual_to_launch, actual_to_terminate


def schedule(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    current_nodes_available_count: Dict,
    resource_requests: List[Dict],
    anti_affinity: bool = False,
    max_nodes: Optional[int] = None,
) -> SchedulingReply:

    ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
    instances: List[AutoscalerInstance] = []
    for node_type, count in current_nodes_available_count.items():
        for i in range(count):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type=node_type,
                        status=Instance.RAY_RUNNING,
                        instance_id=f"{node_type}-{i}",
                        node_id=f"r{i}{node_type}",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}{node_type}".encode("utf-8"),
                        ray_node_type_name=node_type,
                        available_resources=node_type_configs[node_type].resources,
                        total_resources=node_type_configs[node_type].resources,
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-{node_type}-{i}",
                )
            )

    if anti_affinity:
        gang_requests = [
            [
                ResourceRequestUtil.make(r, [(ANTI_AFFINITY, "af", "af")])
                for r in resource_requests
            ]
        ]
        request = sched_request(
            node_type_configs=node_type_configs,
            gang_resource_requests=gang_requests,
            max_num_nodes=max_nodes,
            instances=instances,
        )
    else:
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make(r) for r in resource_requests],
            instances=instances,
            max_num_nodes=max_nodes,
        )
    return ResourceDemandScheduler(event_logger).schedule(request)


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
    @pytest.mark.parametrize(
        "disable_launch_config_check", [True, False], ids=["disabled", "enabled"]
    )
    def test_new_node(disable_launch_config_check):
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
        assert (
            SchedulingNode.new(instance, node_type_configs, disable_launch_config_check)
            is None
        )

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
        node = SchedulingNode.new(
            instance, node_type_configs, disable_launch_config_check
        )
        assert node is not None
        assert node.node_type == "type_1"
        assert node.status == SchedulingNodeStatus.SCHEDULABLE
        assert node.ray_node_id == "r1"
        assert node.im_instance_id == "1"
        assert node.available_resources_for_sched == {
            ResourceRequestSource.PENDING_DEMAND: {"CPU": 0},
            ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT: {"CPU": 1},
        }
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
        node = SchedulingNode.new(
            instance, node_type_configs, disable_launch_config_check
        )
        if not disable_launch_config_check:
            assert node is not None
            assert node.node_type == "type_no_longer_exists"
            assert node.status == SchedulingNodeStatus.TO_TERMINATE
            assert node.termination_request is not None
            assert node.termination_request.cause == TerminationRequest.Cause.OUTDATED
        else:
            assert node is None

        # A pending ray node
        instance = make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.REQUESTED,
                instance_id="1",
            )
        )
        node = SchedulingNode.new(
            instance, node_type_configs, disable_launch_config_check
        )
        assert node is not None
        assert node.node_type == "type_1"
        assert node.status == SchedulingNodeStatus.SCHEDULABLE
        assert node.available_resources_for_sched == {
            ResourceRequestSource.PENDING_DEMAND: {"CPU": 1},
            ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT: {"CPU": 1},
        }
        assert node.total_resources == {"CPU": 1}
        assert node.labels == {"foo": "foo"}

    @staticmethod
    def test_new_head_node():
        # An allocated head node.
        node_type_configs = {
            "head": NodeTypeConfig(
                name="head",
                resources={"CPU": 1},
                min_worker_nodes=0,
                max_worker_nodes=1,
            ),
        }
        instance = make_autoscaler_instance(
            im_instance=Instance(
                instance_type="head",
                status=Instance.ALLOCATED,
                instance_id="1",
                node_kind=NodeKind.HEAD,
            )
        )
        node = SchedulingNode.new(
            instance, node_type_configs, disable_launch_config_check=False
        )
        assert node is not None
        # It's important to check if the node is a head node
        assert node.node_kind == NodeKind.HEAD
        assert node.status == SchedulingNodeStatus.SCHEDULABLE

        # An running head node.
        instance = make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="head",
                available_resources={"CPU": 0},
                total_resources={"CPU": 1},
                node_id=b"r1",
            ),
            im_instance=Instance(
                instance_type="head",
                status=Instance.RAY_RUNNING,
                instance_id="1",
                node_id="r1",
                node_kind=NodeKind.HEAD,
            ),
        )
        node = SchedulingNode.new(
            instance, node_type_configs, disable_launch_config_check=False
        )
        assert node is not None
        assert node.node_kind == NodeKind.HEAD
        assert node.status == SchedulingNodeStatus.SCHEDULABLE


def test_min_worker_nodes():
    scheduler = ResourceDemandScheduler(event_logger)
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


def test_max_workers_head_node_type():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "head_type": NodeTypeConfig(
            name="head_type",
            resources={},
            min_worker_nodes=0,
            max_worker_nodes=2,
        )
    }
    instances = [
        # A head node
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="head_type",
                status=Instance.ALLOCATED,
                instance_id="0",
                node_kind=NodeKind.HEAD,
            ),
        ),
        # A worker node
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="head_type",
                status=Instance.ALLOCATED,
                instance_id="1",
                node_kind=NodeKind.WORKER,
            ),
        ),
        # A worker node
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="head_type",
                status=Instance.ALLOCATED,
                instance_id="2",
                node_kind=NodeKind.WORKER,
            ),
        ),
    ]

    request = sched_request(node_type_configs=node_type_configs, instances=instances)
    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert len(actual_to_terminate) == 1
    assert actual_to_terminate[0][0] in ["1", "2"]
    assert actual_to_terminate[0][2] == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE


def test_max_workers_per_type():
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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
    scheduler = ResourceDemandScheduler(event_logger)
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


def test_resource_constrains():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=1,
            max_worker_nodes=5,
        ),
        "type_gpu": NodeTypeConfig(
            name="type_gpu",
            resources={"CPU": 1, "GPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    # Resource constraints should not launch extra with min_nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        cluster_resource_constraints=[
            ResourceRequestUtil.make({"CPU": 1}),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_cpu": 1})

    # Constraints should launch extra nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        cluster_resource_constraints=[
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"GPU": 1}),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_cpu": 1, "type_gpu": 1})

    # Resource constraints should not launch extra with max_nodes
    # fails to atomically ensure constraints.
    request = sched_request(
        node_type_configs=node_type_configs,
        cluster_resource_constraints=[
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"GPU": 2}),
            ResourceRequestUtil.make({"GPU": 2}),
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted({"type_cpu": 1})
    assert len(reply.infeasible_cluster_resource_constraints) == 1


@pytest.mark.parametrize(
    "disable_launch_config_check", [True, False], ids=["disabled", "enabled"]
)
def test_outdated_nodes(disable_launch_config_check):
    """
    Test that nodes with outdated node configs are terminated.
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=2,
            max_worker_nodes=5,
            launch_config_hash="hash1",
        ),
        "head_node": NodeTypeConfig(
            name="head_node",
            resources={"CPU": 0},
            launch_config_hash="hash2",
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        disable_launch_config_check=disable_launch_config_check,
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_cpu",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash2",
                    instance_id="i-1",
                    node_id="r-1",
                ),
                ray_node=NodeState(
                    ray_node_type_name="type_cpu",
                    available_resources={"CPU": 1},
                    total_resources={"CPU": 1},
                    node_id=b"r-1",
                ),
                cloud_instance_id="c-1",
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_cpu",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",  # matched
                    instance_id="i-2",
                    node_id="r-2",
                ),
                ray_node=NodeState(
                    ray_node_type_name="type_cpu",
                    available_resources={"CPU": 1},
                    total_resources={"CPU": 1},
                    node_id=b"r-2",
                ),
                cloud_instance_id="c-2",
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="head_node",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",  # mismatched -> but don't terminate
                    instance_id="i-3",
                    node_kind=NodeKind.HEAD,
                    node_id="r-3",
                ),
                ray_node=NodeState(
                    ray_node_type_name="head_node",
                    available_resources={"CPU": 0},
                    total_resources={"CPU": 0},
                    node_id=b"r-3",
                ),
                cloud_instance_id="c-3",
            ),
        ],
    )

    reply = scheduler.schedule(request)
    to_launch, to_terminate = _launch_and_terminate(reply)
    if not disable_launch_config_check:
        assert to_terminate == [("i-1", "r-1", TerminationRequest.Cause.OUTDATED)]
        assert to_launch == {"type_cpu": 1}  # Launch 1 to replace the outdated node.
    else:
        assert to_terminate == []
        assert to_launch == {}


@pytest.mark.parametrize("idle_timeout_s", [1, 2, 10])
@pytest.mark.parametrize("has_resource_constraints", [True, False])
def test_idle_termination(idle_timeout_s, has_resource_constraints):
    """
    Test that idle nodes are terminated.
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=5,
            launch_config_hash="hash1",
        ),
        "head_node": NodeTypeConfig(
            name="head_node",
            resources={"CPU": 0},
            launch_config_hash="hash2",
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    idle_time_s = 5
    constraints = (
        []
        if not has_resource_constraints
        else [ResourceRequestUtil.make({"CPU": 1})] * 2
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_cpu",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",
                    instance_id="i-1",
                    node_id="r-1",
                ),
                ray_node=NodeState(
                    node_id=b"r-1",
                    ray_node_type_name="type_cpu",
                    available_resources={"CPU": 0},
                    total_resources={"CPU": 1},
                    idle_duration_ms=0,  # Non idle
                    status=NodeStatus.RUNNING,
                ),
                cloud_instance_id="c-1",
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_id="i-2",
                    instance_type="type_cpu",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",
                    node_id="r-2",
                ),
                ray_node=NodeState(
                    ray_node_type_name="type_cpu",
                    node_id=b"r-2",
                    available_resources={"CPU": 1},
                    total_resources={"CPU": 1},
                    idle_duration_ms=idle_time_s * 1000,
                    status=NodeStatus.IDLE,
                ),
                cloud_instance_id="c-2",
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_id="i-3",
                    instance_type="head_node",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash2",
                    node_kind=NodeKind.HEAD,
                    node_id="r-3",
                ),
                ray_node=NodeState(
                    ray_node_type_name="head_node",
                    node_id=b"r-3",
                    available_resources={"CPU": 0},
                    total_resources={"CPU": 0},
                    idle_duration_ms=999 * 1000,  # idle
                    status=NodeStatus.IDLE,
                ),
                cloud_instance_id="c-3",
            ),
        ],
        idle_timeout_s=idle_timeout_s,
        cluster_resource_constraints=constraints,
    )

    reply = scheduler.schedule(request)
    _, to_terminate = _launch_and_terminate(reply)
    if idle_timeout_s <= idle_time_s and not has_resource_constraints:
        assert len(to_terminate) == 1
        assert to_terminate == [("i-2", "r-2", TerminationRequest.Cause.IDLE)]
    else:
        assert len(to_terminate) == 0


def test_gang_scheduling():
    """
    Test that gang scheduling works.
    """
    scheduler = ResourceDemandScheduler(event_logger)
    AFFINITY = ResourceRequestUtil.PlacementConstraintType.AFFINITY
    ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY

    node_type_configs = {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=5,
            launch_config_hash="hash1",
        )
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=[
            [
                ResourceRequestUtil.make({"CPU": 1}, [(AFFINITY, "pg", "")]),
                ResourceRequestUtil.make({"CPU": 1}, [(AFFINITY, "pg", "")]),
            ]
        ],
    )

    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    # Should be grouped on the same node.
    assert sorted(to_launch) == sorted({"type_cpu": 1})

    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=[
            [
                ResourceRequestUtil.make({"CPU": 1}, [(ANTI_AFFINITY, "pg", "")]),
                ResourceRequestUtil.make({"CPU": 1}, [(ANTI_AFFINITY, "pg", "")]),
            ]
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    # Should be placed on different nodes.
    assert sorted(to_launch) == sorted({"type_cpu": 2})

    # Atomic gang scheduling
    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=[
            [
                # Couldn't fit on a node.
                ResourceRequestUtil.make({"CPU": 3}, [(AFFINITY, "pg", "")]),
                ResourceRequestUtil.make({"CPU": 3}, [(AFFINITY, "pg", "")]),
            ]
        ],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {}
    assert len(reply.infeasible_gang_resource_requests) == 1


def test_gang_scheduling_with_others():
    """
    Test that a mix of the various demands:
    - resource requests from tasks/actors
    - gang requests from placement groups
    - cluster resource constraints
    - min/max worker counts
    - existing nodes.
    """
    scheduler = ResourceDemandScheduler(event_logger)
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 4},
            min_worker_nodes=2,
            max_worker_nodes=4,
            launch_config_hash="hash1",
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 1, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
            launch_config_hash="hash2",
        ),
    }

    # Placement constraints
    AFFINITY = ResourceRequestUtil.PlacementConstraintType.AFFINITY
    ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
    gang_requests = [
        [
            ResourceRequestUtil.make({"CPU": 2}, [(ANTI_AFFINITY, "ak", "av")]),
            ResourceRequestUtil.make({"CPU": 2}, [(ANTI_AFFINITY, "ak", "av")]),
            ResourceRequestUtil.make({"CPU": 2}, [(ANTI_AFFINITY, "ak", "av")]),
            ResourceRequestUtil.make({"CPU": 2}, [(ANTI_AFFINITY, "ak", "av")]),
        ],
        [
            ResourceRequestUtil.make({"CPU": 3}, [(AFFINITY, "c", "c1")]),
            ResourceRequestUtil.make({"CPU": 3}, [(AFFINITY, "c", "c1")]),
        ],
        [
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
        ],
    ]

    # Resource requests
    resource_requests = [
        ResourceRequestUtil.make({"CPU": 2}),
        ResourceRequestUtil.make({"GPU": 1, "CPU": 1}),
        ResourceRequestUtil.make({"GPU": 1}),
    ]

    # Cluster constraints
    cluster_constraints = [ResourceRequestUtil.make({"CPU": 1})] * 10

    instances = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                launch_config_hash="hash1",
                instance_id="i-1",
            ),
            ray_node=NodeState(
                node_id=b"r-1",
                ray_node_type_name="type_1",
                available_resources={"CPU": 2},
                total_resources={"CPU": 4},
                idle_duration_ms=0,
                status=NodeStatus.RUNNING,
            ),
            cloud_instance_id="c-1",
        ),
        make_autoscaler_instance(
            im_instance=Instance(
                instance_type="type_2",
                status=Instance.RAY_RUNNING,
                launch_config_hash="hash2",
                instance_id="i-2",
            ),
            ray_node=NodeState(
                node_id=b"r-2",
                ray_node_type_name="type_2",
                available_resources={"CPU": 1, "GPU": 1},
                total_resources={"CPU": 1, "GPU": 1},
                idle_duration_ms=0,
                status=NodeStatus.RUNNING,
            ),
            cloud_instance_id="c-2",
        ),
    ]

    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=gang_requests,
        resource_requests=resource_requests,
        cluster_resource_constraints=cluster_constraints,
        instances=instances,
        idle_timeout_s=999,
    )
    # Calculate the expected number of nodes to launch:
    # - 1 type_1, 1 type_2 to start with => CPU: 2/5, GPU: 1/1
    # - added 1 type_1 for minimal request -> +1 type_1
    # ==> 2 type_1, 1 type_2 (CPU: 6/9, GPU: 1/1)
    # - enforce cluster constraint (10 CPU) -> +1 type_1, CPU: 10/13, GPU: 1/1
    # ==> 3 type_1, 1 type_2 (CPU: 10/13, GPU: 1/1)
    # - sched gang requests:
    #   - anti affinity (8CPU) => +1 type_1, CPU: 6/17, GPU: 1/1
    #   - no constraint (3CPU) => CPU: 3/17, GPU: 1/1
    #   - affinity (not feasible)
    # ==> 4 type_1, 1 type_2 (CPU: 3/17, GPU: 1/1)
    # - sched resource requests:
    #   - 2CPU => CPU: 1/17, GPU: 1/1
    #   - 1GPU, 1CPU => CPU: 0/17, GPU: 0/1
    #   - 1GPU => adding a new type_2
    # ==> 4 type_1, 2 type_2 (CPU: 0/17, GPU: 0/2)
    # Therefore:
    # - added nodes: 3 type_1, 1 type_2
    # - infeasible: 1 gang request, 1 resource request
    expected_to_launch = {"type_1": 3, "type_2": 1}
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert sorted(to_launch) == sorted(expected_to_launch)
    assert len(reply.infeasible_gang_resource_requests) == 1
    assert len(reply.infeasible_resource_requests) == 0


def test_bin_pack():
    def bin_pack_residual(
        node_resources: Dict[NodeType, Dict],
        resource_requests: List[Dict],
        anti_affinity: bool = False,
    ) -> List[Dict]:
        node_type_configs = {}
        for type_name, node_resource_dict in node_resources.items():
            node_type_configs[type_name] = NodeTypeConfig(
                name=type_name,
                resources=node_resource_dict,
                min_worker_nodes=0,
                max_worker_nodes=1,  # we only care about bin packing. Just allow 1
            )

        reply = schedule(node_type_configs, {}, resource_requests, anti_affinity)
        if anti_affinity:
            infeasible = []
            for r in reply.infeasible_gang_resource_requests:
                infeasible.append(ResourceRequestUtil.to_resource_maps(r.requests))
            return infeasible
        else:
            return ResourceRequestUtil.to_resource_maps(
                reply.infeasible_resource_requests
            )

    assert bin_pack_residual({"type_1": {"CPU": 0}}, [{"GPU": 2}, {"GPU": 2}]) == [
        {"GPU": 2},
        {"GPU": 2},
    ]
    assert bin_pack_residual({"type_1": {"GPU": 2}}, [{"GPU": 2}, {"GPU": 2}]) == [
        {"GPU": 2}
    ]
    assert bin_pack_residual({"type_1": {"GPU": 4}}, [{"GPU": 2}, {"GPU": 2}]) == []

    assert (
        bin_pack_residual(
            {"type_1": {"GPU": 2}, "type_2": {"GPU": 2, "CPU": 2}},
            [{"GPU": 2}, {"GPU": 2}],
        )
        == []
    )
    assert bin_pack_residual(
        {"type_1": {"GPU": 2}, "type_2": {"CPU": 2}},
        [{"GPU": 2}, {"GPU": 2}],
    ) == [{"GPU": 2}]

    assert bin_pack_residual(
        {"type_1": {"GPU": 2}, "type_2": {"CPU": 2}},
        [{"GPU": 2}, {"GPU": 2}],
    ) == [{"GPU": 2}]

    assert bin_pack_residual(
        {"type_1": {"GPU": 3}},
        [{"GPU": 1}, {"GPU": 1}],
        anti_affinity=True,
    ) == [[{"GPU": 1.0}, {"GPU": 1.0}]]

    assert (
        bin_pack_residual(
            {"type_1": {"GPU": 3}},
            [{"GPU": 1}, {"GPU": 1}],
            anti_affinity=False,
        )
        == []
    )

    implicit_resource = ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a"
    assert (
        bin_pack_residual(
            {"type_1": {"CPU": 1}},
            [{implicit_resource: 0.5}, {implicit_resource: 0.5}],
        )
        == []
    )
    assert bin_pack_residual(
        {"type_1": {"CPU": 1}},
        [{implicit_resource: 1}, {implicit_resource: 0.5}],
    ) == [{implicit_resource: 0.5}]


@pytest.mark.parametrize(
    "source",
    [
        ResourceRequestSource.PENDING_DEMAND,
        ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT,
    ],
    ids=["demand", "cluster_resource_constraint"],
)
def test_node_schedule_score(source):
    def try_schedule(node_resources: Dict, requests: List[Dict]) -> Tuple:
        node_type_config = NodeTypeConfig(
            name="type_1",
            resources=node_resources,
            min_worker_nodes=0,
            max_worker_nodes=1,
        )
        node = SchedulingNode.from_node_config(
            node_config=node_type_config,
            status=SchedulingNodeStatus.SCHEDULABLE,
            node_kind=NodeKind.WORKER,
        )
        requests = [ResourceRequestUtil.make(r) for r in requests]
        infeasible, score = node.try_schedule(requests, source)
        return ResourceRequestUtil.to_resource_maps(infeasible), score

    assert try_schedule({"CPU": 1}, [{"CPU": 1}]) == ([], (True, 1, 1.0, 1.0))

    assert try_schedule({"GPU": 4}, [{"GPU": 2}]) == ([], (True, 1, 0.5, 0.5))
    assert try_schedule({"GPU": 4}, [{"GPU": 1}, {"GPU": 1}]) == (
        [],
        (True, 1, 0.5, 0.5),
    )
    assert try_schedule({"GPU": 2}, [{"GPU": 2}]) == ([], (True, 1, 2, 2))
    assert try_schedule({"GPU": 2}, [{"GPU": 1}, {"GPU": 1}]) == ([], (True, 1, 2, 2))
    assert try_schedule({"GPU": 1}, [{"GPU": 1, "CPU": 1}, {"GPU": 1}]) == (
        [{"GPU": 1, "CPU": 1}],
        (True, 1, 1, 1),
    )
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"GPU": 1, "CPU": 1}, {"GPU": 1}]) == (
        [{"GPU": 1}],
        (True, 2, 1, 1),
    )
    assert try_schedule({"GPU": 2, "TPU": 1}, [{"GPU": 2}]) == ([], (True, 1, 0, 1))
    assert try_schedule({"CPU": 64}, [{"CPU": 64}]) == ([], (True, 1, 64, 64))
    assert try_schedule({"CPU": 64}, [{"CPU": 32}]) == ([], (True, 1, 8, 8))
    assert try_schedule({"CPU": 64}, [{"CPU": 16}, {"CPU": 16}]) == (
        [],
        (True, 1, 8, 8),
    )

    # GPU Scores
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"CPU": 1}]) == (
        [],
        (False, 1, 0.0, 0.5),
    )
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"CPU": 1, "GPU": 1}]) == (
        [],
        (True, 2, 1.0, 1.0),
    )
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"GPU": 1}]) == (
        [],
        (True, 1, 0.0, 0.5),
    )

    # Zero resources
    assert try_schedule({"CPU": 0, "custom": 1}, [{"custom": 1}]) == (
        [],
        (True, 1, 1, 1),
    )
    assert try_schedule({"CPU": 0, "custom": 1}, [{"CPU": 1}]) == (
        [{"CPU": 1}],
        (True, 0, 0.0, 0.0),
    )

    # Implicit resources
    implicit_resource = ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a"
    assert try_schedule({"CPU": 1}, [{implicit_resource: 1}]) == (
        [],
        (True, 0, 0.0, 0.0),
    )
    assert try_schedule({"CPU": 1}, [{implicit_resource: 1}] * 2) == (
        [{implicit_resource: 1}],
        (True, 0, 0.0, 0.0),
    )


def test_get_nodes_packing_heuristic():
    node_type_configs = {
        "m4.large": NodeTypeConfig(
            name="m4.large",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "m4.4xlarge": NodeTypeConfig(
            name="m4.4xlarge",
            resources={"CPU": 16},
            min_worker_nodes=0,
            max_worker_nodes=8,
        ),
        "m4.16xlarge": NodeTypeConfig(
            name="m4.16xlarge",
            resources={"CPU": 64},
            min_worker_nodes=0,
            max_worker_nodes=4,
        ),
        "p2.xlarge": NodeTypeConfig(
            name="p2.xlarge",
            resources={"CPU": 16, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "p2.8xlarge": NodeTypeConfig(
            name="p2.8xlarge",
            resources={"CPU": 32, "GPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=4,
        ),
    }

    def get_nodes_for(
        resource_requests,
        anti_affinity=False,
        max_nodes: Optional[int] = None,
        current_nodes: Optional[Dict] = None,
    ):
        reply = schedule(
            node_type_configs,
            current_nodes or {},
            resource_requests,
            anti_affinity=anti_affinity,
            max_nodes=max_nodes,
        )
        to_launch, _ = _launch_and_terminate(reply)
        return to_launch

    assert get_nodes_for([{"GPU": 8}]) == {"p2.8xlarge": 1}
    assert get_nodes_for([{"GPU": 1}] * 6) == {"p2.8xlarge": 1}
    assert get_nodes_for([{"GPU": 1}] * 4) == {"p2.xlarge": 4}
    assert get_nodes_for([{"CPU": 32, "GPU": 1}] * 3) == {"p2.8xlarge": 3}
    assert get_nodes_for([{"CPU": 64, "GPU": 1}] * 3) == {}
    assert get_nodes_for([{"CPU": 64}] * 3) == {"m4.16xlarge": 3}
    assert get_nodes_for([{"CPU": 64}, {"CPU": 1}]) == {
        "m4.16xlarge": 1,
        "m4.large": 1,
    }
    assert get_nodes_for([{"CPU": 64}, {"CPU": 9}, {"CPU": 9}]) == {
        "m4.16xlarge": 1,
        "m4.4xlarge": 2,
    }

    assert get_nodes_for([{"CPU": 16}] * 5) == {
        "m4.16xlarge": 1,
        "m4.4xlarge": 1,
    }
    assert get_nodes_for([{"CPU": 8}] * 10) == {
        "m4.16xlarge": 1,
        "m4.4xlarge": 1,
    }
    assert get_nodes_for([{"CPU": 1}] * 100) == {
        "m4.16xlarge": 1,
        "m4.4xlarge": 2,
        "m4.large": 2,
    }

    assert get_nodes_for([{"GPU": 1}] + ([{"CPU": 1}] * 64)) == {
        "m4.16xlarge": 1,
        "p2.xlarge": 1,
    }
    assert get_nodes_for(([{"GPU": 1}] * 8) + ([{"CPU": 1}] * 64)) == {
        "m4.4xlarge": 2,
        "p2.8xlarge": 1,
    }
    assert get_nodes_for([{"GPU": 1}] * 8, anti_affinity=False) == {"p2.8xlarge": 1}
    assert get_nodes_for([{"GPU": 1}] * 8, anti_affinity=True) == {"p2.xlarge": 8}

    # GPU/CPU scheduling
    node_type_configs = {
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 16},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 16, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    assert get_nodes_for([{"CPU": 16}]) == {"cpu": 1}
    assert get_nodes_for([{"CPU": 1}] * 30 + [{"GPU": 1, "CPU": 1}]) == {
        "cpu": 1,
        "gpu": 1,
    }
    assert get_nodes_for([{"GPU": 1, "CPU": 1}] + [{"CPU": 1}] * 30) == {
        "cpu": 1,
        "gpu": 1,
    }
    assert get_nodes_for([{"GPU": 1, "CPU": 1}] + [{"CPU": 1}] * 15) == {
        "gpu": 1,
    }

    # GPU should be avoided
    node_type_configs = {
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 100, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }
    assert get_nodes_for([{"CPU": 1}] * 100, max_nodes=10) == {"cpu": 10}
    # max_to_add eleven nodes allowed. First ten chosen to be "cpu",
    # last chosen to be "gpu" due max_workers constraint on "cpu".
    assert get_nodes_for([{"CPU": 1}] * 100, max_nodes=11) == {"cpu": 10, "gpu": 1}
    assert get_nodes_for([{"CPU": 1}] * 100 + [{"GPU": 1}], max_nodes=100) == {"gpu": 1}
    assert get_nodes_for([{"GPU": 1}] * 4 + [{"CPU": 1}] * 404, max_nodes=100) == {
        "gpu": 4,
        "cpu": 4,
    }

    # Max limit should be respected
    node_type_configs = {
        "m4.large": NodeTypeConfig(
            name="m4.large",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    assert get_nodes_for([{"CPU": 1}] * 10, max_nodes=2) == {"m4.large": 2}
    assert (
        get_nodes_for([{"CPU": 1}] * 10, max_nodes=10, current_nodes={"m4.large": 10})
        == {}
    )
    assert get_nodes_for([{"CPU": 1}] * 10, max_nodes=10) == {"m4.large": 5}
    assert get_nodes_for([{"CPU": 1}] * 40) == {"m4.large": 10}

    # Min workers should be respected
    node_type_configs = {
        "m2.large": NodeTypeConfig(
            name="m2.large",
            resources={"CPU": 1},
            min_worker_nodes=5,
            max_worker_nodes=10,
        ),
        "m4.large": NodeTypeConfig(
            name="m4.large",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"GPU": 2},
            min_worker_nodes=2,
            max_worker_nodes=2,
        ),
        "gpubla": NodeTypeConfig(
            name="gpubla", resources={"GPU": 1}, min_worker_nodes=0, max_worker_nodes=0
        ),
    }

    assert get_nodes_for([{"CPU": 2}] * 5) == {"m2.large": 5, "m4.large": 5, "gpu": 2}
    assert get_nodes_for(
        [{"CPU": 2}] * 5, current_nodes={"m2.large": 1, "m4.large": 1}
    ) == {"m2.large": 4, "m4.large": 4, "gpu": 2}
    assert get_nodes_for([{"GPU": 1}] * 5) == {"m2.large": 5, "gpu": 2}


def test_min_workers_and_others():
    node_type_configs = {
        "p2.8xlarge": NodeTypeConfig(
            name="p2.8xlarge",
            resources={"CPU": 32, "GPU": 8},
            min_worker_nodes=2,
            max_worker_nodes=4,
        ),
        "p2.xlarge": NodeTypeConfig(
            name="p2.xlarge",
            resources={"CPU": 16, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    def get_nodes_for(resource_requests, current_nodes=None, max_nodes=None):
        reply = schedule(
            node_type_configs,
            current_nodes or {},
            resource_requests,
            max_nodes=max_nodes,
        )
        to_launch, _ = _launch_and_terminate(reply)
        infeasible = ResourceRequestUtil.to_resource_maps(
            reply.infeasible_resource_requests
        )
        return to_launch, infeasible

    assert get_nodes_for([{"GPU": 8}]) == ({"p2.8xlarge": 2}, [])
    assert get_nodes_for([{"GPU": 8}] * 2) == ({"p2.8xlarge": 2}, [])
    assert get_nodes_for([{"GPU": 8}] * 4) == ({"p2.8xlarge": 4}, [])
    assert get_nodes_for([{"GPU": 8}] * 8) == ({"p2.8xlarge": 4}, [{"GPU": 8}] * 4)

    assert get_nodes_for(
        [{"GPU": 8}] * 3 + [{"GPU": 1}], current_nodes={"p2.8xlarge": 1}
    ) == ({"p2.xlarge": 1, "p2.8xlarge": 2}, [])
    assert get_nodes_for(
        [{"GPU": 8}] * 3 + [{"GPU": 1}], current_nodes={"p2.8xlarge": 2}
    ) == ({"p2.xlarge": 1, "p2.8xlarge": 1}, [])
    assert get_nodes_for(
        [{"GPU": 8}] * 3 + [{"GPU": 1}], current_nodes={"p2.8xlarge": 3}
    ) == ({"p2.xlarge": 1}, [])
    assert get_nodes_for(
        [{"GPU": 8}] * 5 + [{"GPU": 1}], current_nodes={"p2.8xlarge": 3}
    ) == ({"p2.xlarge": 1, "p2.8xlarge": 1}, [{"GPU": 8}])

    node_type_configs = {
        "p2.xlarge": NodeTypeConfig(
            name="p2.xlarge",
            resources={"CPU": 16, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }
    assert get_nodes_for([{"GPU": 1}] * 4, max_nodes=1) == (
        {"p2.xlarge": 1},
        [{"GPU": 1}] * 3,
    )
    assert get_nodes_for([{"GPU": 1}] * 4, max_nodes=2) == (
        {"p2.xlarge": 2},
        [{"GPU": 1}] * 2,
    )
    assert get_nodes_for([{"GPU": 1}] * 4, max_nodes=4) == ({"p2.xlarge": 4}, [])


def test_gang_scheduling_complex():
    node_type_configs = {
        "m4.large": NodeTypeConfig(
            name="m4.large",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "p2.8xlarge": NodeTypeConfig(
            name="p2.8xlarge",
            resources={"CPU": 32, "GPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=4,
        ),
    }

    ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
    AFFINITY = ResourceRequestUtil.PlacementConstraintType.AFFINITY

    def get_nodes_for(gang_resource_requests) -> Tuple[Dict, List[List[Dict]]]:
        scheduler = ResourceDemandScheduler(event_logger)
        gang_requests = []
        for resource_requests, placement_constraint in gang_resource_requests:
            key = f"PG_{str(time.time())}"
            gang_requests.append(
                [
                    ResourceRequestUtil.make(
                        r,
                        [(placement_constraint, key, key)]
                        if placement_constraint
                        else [],
                    )
                    for r in resource_requests
                ]
            )

        request = sched_request(
            node_type_configs=node_type_configs,
            gang_resource_requests=gang_requests,
        )
        reply = scheduler.schedule(request)

        to_launch, _ = _launch_and_terminate(reply)
        infeasible = []
        for r in reply.infeasible_gang_resource_requests:
            infeasible.append(ResourceRequestUtil.to_resource_maps(r.requests))

        return to_launch, infeasible

    # Test various constraints.
    get_nodes_for([([{"CPU": 16}, {"CPU": 16}], ANTI_AFFINITY)]) == (
        {"p2.8xlarge": 2},
        [],
    )
    get_nodes_for([([{"CPU": 16}, {"CPU": 16}], None)]) == (
        {"p2.8xlarge": 1},
        [],
    )
    get_nodes_for([([{"CPU": 2}, {"CPU": 2}], AFFINITY)]) == (
        {"p2.8xlarge": 1},
        [],
    )
    get_nodes_for([([{"CPU": 2}, {"CPU": 2}], None)]) == (
        {"m4.large": 2},
        [],
    )
    get_nodes_for([([{"CPU": 32}, {"CPU": 32}], AFFINITY)]) == (
        {},
        [[{"CPU": 32}, {"CPU": 32}]],
    )

    # Test many anti-affinity
    get_nodes_for(
        [
            ([{"CPU": 4}, {"CPU": 4}], ANTI_AFFINITY),
            ([{"CPU": 4}, {"CPU": 4}], ANTI_AFFINITY),
            ([{"CPU": 4}, {"CPU": 4}], ANTI_AFFINITY),
            ([{"CPU": 4}, {"CPU": 4}], ANTI_AFFINITY),
        ]
    ) == ({"p2.8xlarge": 2}, [])

    # Test multiple affinity
    get_nodes_for(
        [
            ([{"CPU": 16}, {"CPU": 16}], AFFINITY),
            ([{"GPU": 4}, {"GPU": 4}], AFFINITY),
        ]
    ) == ({"p2.8xlarge": 1}, [])


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
