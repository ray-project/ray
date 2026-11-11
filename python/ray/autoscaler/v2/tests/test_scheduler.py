import copy
import os
import sys
import time

# coding: utf-8
from typing import Dict, List, Optional, Sequence, Tuple, Union
from unittest.mock import patch

import pytest

import ray
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import WorkerReplicaConfig
from ray.autoscaler.v2.scheduler import (
    NodeTypeConfig,
    ResourceDemandScheduler,
    ResourceRequestSource,
    SchedulingNode,
    SchedulingNodeStatus,
    SchedulingReply,
    SchedulingRequest,
    UnschedulableRequestCache,
    logger,
)
from ray.autoscaler.v2.schema import (
    AutoscalerInstance,
    IPPRGroupSpec,
    IPPRSpecs,
    IPPRStatus,
    NodeType,
)
from ray.autoscaler.v2.tests.util import MockEventLogger, make_autoscaler_instance
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    BundleSelector,
    ClusterResourceConstraint,
    GangResourceRequest,
    LocalityConstraint,
    LocalityRequirement,
    NodeState,
    NodeStatus,
    ResourceRequest,
    ResourceRequestByCount,
)
from ray.core.generated.common_pb2 import (
    LabelSelector,
    LabelSelectorConstraint,
    LabelSelectorOperator,
    PlacementStrategy,
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
    pending_idle_timeout_s: Optional[float] = None,
    disable_launch_config_check: Optional[bool] = False,
    ippr_specs: Optional[IPPRSpecs] = None,
    ippr_statuses: Optional[Dict[str, IPPRStatus]] = None,
    cloud_resource_availabilities: Optional[Dict[NodeType, float]] = None,
) -> SchedulingRequest:

    if resource_requests is None:
        resource_requests = []
    if gang_resource_requests is None:
        gang_resource_requests = []
    if cluster_resource_constraints is None:
        cluster_resource_constraints = []
    if instances is None:
        instances = []
    if ippr_statuses is None:
        ippr_statuses = {}
    if cloud_resource_availabilities is None:
        cloud_resource_availabilities = {}

    return SchedulingRequest(
        resource_requests=ResourceRequestUtil.group_by_count(resource_requests),
        gang_resource_requests=[
            GangResourceRequest(requests=reqs) for reqs in gang_resource_requests
        ],
        cluster_resource_constraints=(
            [
                ClusterResourceConstraint(
                    resource_requests=ResourceRequestUtil.group_by_count(
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
        pending_idle_timeout_s=pending_idle_timeout_s,
        disable_launch_config_check=disable_launch_config_check,
        ippr_specs=ippr_specs,
        ippr_statuses=ippr_statuses,
        cloud_resource_availabilities=cloud_resource_availabilities,
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
    cloud_resource_availabilities: Optional[Dict[NodeType, float]] = None,
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
            cloud_resource_availabilities=cloud_resource_availabilities,
        )
    else:
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make(r) for r in resource_requests],
            instances=instances,
            max_num_nodes=max_nodes,
            cloud_resource_availabilities=cloud_resource_availabilities,
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
            Instance.ALLOCATION_TIMEOUT,
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
    def test_new_running_instance_without_ray_node_returns_none():
        # Regression test: a RAY_RUNNING instance whose ray_node is missing in
        # GCS used to crash SchedulingNode.new with an AssertionError. The
        # defensive guard returns None instead.
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 1},
                min_worker_nodes=0,
                max_worker_nodes=10,
            ),
        }
        instance = make_autoscaler_instance(
            ray_node=None,
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="i-stale",
                node_id="r-gone",
            ),
        )
        node = SchedulingNode.new(
            instance,
            node_type_configs,
            disable_launch_config_check=False,
        )
        assert node is None

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


def test_terminate_max_allocated_workers_per_type():
    scheduler = ResourceDemandScheduler(event_logger)
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=2,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
    )

    reply = scheduler.schedule(request)

    # No instances created, no-op.
    expected_to_terminate = []
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert sorted(actual_to_terminate) == sorted(expected_to_terminate)

    instances = [
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_0",
                available_resources={"CPU": 1},
                total_resources={"CPU": 1},
                node_id=b"r0",
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.ALLOCATED,
                instance_id="0",
                node_id="r0",
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
                status=Instance.ALLOCATED,
                instance_id="1",
                node_id="r1",
            ),
        ),
    ]

    # 2 nodes in allocated state with max of 2 allowed for type 1.
    # Scheduler should leave all of the allocated instances.
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=instances,
    )

    reply = scheduler.schedule(request)
    _, actual_to_terminate = _launch_and_terminate(reply)
    assert actual_to_terminate == []

    # Max nodes is now 0 for type 1, scheduler should terminate
    # both allocated instances to conform with max num nodes per type.
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=0,
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
            (
                "0",
                "",
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
            ),  # allocated instance
            ("1", "", TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE),
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
            min_worker_nodes=3,
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
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_cpu",
                    status=Instance.ALLOCATED,
                    launch_config_hash="",  # pending -> not compared until running
                    instance_id="i-4",
                ),
                ray_node=None,
                cloud_instance_id="c-4",
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
@pytest.mark.parametrize("has_resource_requests", [True, False])
@pytest.mark.parametrize("has_gang_resource_requests", [True, False])
def test_idle_termination(
    idle_timeout_s,
    has_resource_constraints,
    has_resource_requests,
    has_gang_resource_requests,
):
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
    constraints = []
    if has_resource_constraints:
        constraints = [ResourceRequestUtil.make({"CPU": 1})] * 2

    resource_requests = []
    if has_resource_requests:
        resource_requests = [ResourceRequestUtil.make({"CPU": 1})] * 2

    ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
    gang_resource_requests = []
    if has_gang_resource_requests:
        gang_resource_requests = [
            [  # This is a strict spread placement group that requires 2 nodes.
                ResourceRequestUtil.make({"CPU": 1}, [(ANTI_AFFINITY, "pg", "")]),
                ResourceRequestUtil.make({"CPU": 1}, [(ANTI_AFFINITY, "pg", "")]),
            ]
        ]

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
        resource_requests=resource_requests,
        gang_resource_requests=gang_resource_requests,
    )

    reply = scheduler.schedule(request)
    _, to_terminate = _launch_and_terminate(reply)
    if (
        idle_timeout_s <= idle_time_s
        and not has_resource_constraints
        and not has_resource_requests
        and not has_gang_resource_requests
    ):
        assert len(to_terminate) == 1
        assert to_terminate == [("i-2", "r-2", TerminationRequest.Cause.IDLE)]
    else:
        assert len(to_terminate) == 0


@pytest.mark.parametrize("min_workers", [0, 1])
def test_idle_termination_with_min_worker(min_workers):
    """
    Test that idle nodes are terminated.
    """
    idle_timeout_s = 1

    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=min_workers,
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
    constraints = []

    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_id="i-1",
                    instance_type="type_cpu",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",
                    node_id="r-1",
                ),
                ray_node=NodeState(
                    ray_node_type_name="type_cpu",
                    node_id=b"r-1",
                    available_resources={"CPU": 1},
                    total_resources={"CPU": 1},
                    idle_duration_ms=idle_time_s * 1000,
                    status=NodeStatus.IDLE,
                ),
                cloud_instance_id="c-1",
            ),
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_id="i-2",
                    instance_type="head_node",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash2",
                    node_kind=NodeKind.HEAD,
                    node_id="r-2",
                ),
                ray_node=NodeState(
                    ray_node_type_name="head_node",
                    node_id=b"r-2",
                    available_resources={"CPU": 0},
                    total_resources={"CPU": 0},
                    idle_duration_ms=999 * 1000,  # idle
                    status=NodeStatus.IDLE,
                ),
                cloud_instance_id="c-2",
            ),
        ],
        idle_timeout_s=idle_timeout_s,
        cluster_resource_constraints=constraints,
    )

    reply = scheduler.schedule(request)
    _, to_terminate = _launch_and_terminate(reply)
    assert idle_timeout_s <= idle_time_s
    if min_workers == 0:
        assert len(to_terminate) == 1
        assert to_terminate == [("i-1", "r-1", TerminationRequest.Cause.IDLE)]
    else:
        assert min_workers > 0
        assert len(to_terminate) == 0


@pytest.mark.parametrize("node_type_idle_timeout_s", [1, 2, 10])
def test_idle_termination_with_node_type_idle_timeout(node_type_idle_timeout_s):
    """
    Test that idle nodes are terminated when idle_timeout_s is set for node type.
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_cpu_with_idle_timeout": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=5,
            idle_timeout_s=node_type_idle_timeout_s,
            launch_config_hash="hash1",
        ),
    }

    idle_time_s = 5
    constraints = []

    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_cpu_with_idle_timeout",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",
                    instance_id="i-1",
                    node_id="r-1",
                ),
                ray_node=NodeState(
                    node_id=b"r-1",
                    ray_node_type_name="type_cpu_with_idle_timeout",
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
                    instance_type="type_cpu_with_idle_timeout",
                    status=Instance.RAY_RUNNING,
                    launch_config_hash="hash1",
                    node_id="r-2",
                ),
                ray_node=NodeState(
                    ray_node_type_name="type_cpu_with_idle_timeout",
                    node_id=b"r-2",
                    available_resources={"CPU": 1},
                    total_resources={"CPU": 1},
                    idle_duration_ms=idle_time_s * 1000,
                    status=NodeStatus.IDLE,
                ),
                cloud_instance_id="c-2",
            ),
        ],
        # Set autoscaler idle_timeout_s to a value greater than
        # node_type_idle_timeout_s and idle_time_s.
        idle_timeout_s=idle_time_s * 1000,
        cluster_resource_constraints=constraints,
    )

    reply = scheduler.schedule(request)
    _, to_terminate = _launch_and_terminate(reply)
    if node_type_idle_timeout_s <= idle_time_s:
        assert len(to_terminate) == 1
        assert to_terminate == [("i-2", "r-2", TerminationRequest.Cause.IDLE)]
    else:
        assert len(to_terminate) == 0


def _pending_instance(
    instance_id: str, status: Instance.InstanceStatus, instance_type: str = "type_cpu"
) -> AutoscalerInstance:
    return make_autoscaler_instance(
        im_instance=Instance(
            instance_id=instance_id,
            instance_type=instance_type,
            status=status,
            launch_config_hash="hash1",
        ),
        cloud_instance_id=(f"c-{instance_id}" if status != Instance.QUEUED else None),
    )


def _running_instance(
    instance_id: str, idle_duration_ms: int, instance_type: str = "type_cpu"
) -> AutoscalerInstance:
    return make_autoscaler_instance(
        im_instance=Instance(
            instance_id=instance_id,
            instance_type=instance_type,
            status=Instance.RAY_RUNNING,
            launch_config_hash="hash1",
            node_id=f"r-{instance_id}",
        ),
        ray_node=NodeState(
            node_id=f"r-{instance_id}".encode(),
            ray_node_type_name=instance_type,
            available_resources={"CPU": 1},
            total_resources={"CPU": 1},
            idle_duration_ms=idle_duration_ms,
            status=NodeStatus.IDLE,
        ),
        cloud_instance_id=f"c-{instance_id}",
    )


@pytest.mark.parametrize(
    "status",
    [
        Instance.QUEUED,
        Instance.REQUESTED,
        Instance.ALLOCATED,
        Instance.RAY_INSTALLING,
    ],
    ids=Instance.InstanceStatus.Name,
)
def test_idle_termination_pending_nodes(status):
    """
    Test that pending nodes not needed by any resource demands are terminated
    after being continuously unneeded for the idle timeout, except for the
    REQUESTED ones whose launch requests can't be cancelled.
    """
    idle_timeout_s = 10
    scheduler = ResourceDemandScheduler(event_logger)
    node_type_configs = {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=5,
            launch_config_hash="hash1",
        ),
    }

    def _schedule(now_s: float, resource_requests: List[ResourceRequest]):
        request = sched_request(
            node_type_configs=node_type_configs,
            instances=[_pending_instance("i-1", status)],
            idle_timeout_s=idle_timeout_s,
            pending_idle_timeout_s=idle_timeout_s,
            # KubeRay disables the launch config check.
            disable_launch_config_check=True,
            resource_requests=resource_requests,
        )
        with patch("time.monotonic", return_value=now_s):
            return _launch_and_terminate(scheduler.schedule(request))

    # Not needed, but not for long enough.
    assert _schedule(0, []) == ({}, [])
    assert _schedule(idle_timeout_s, []) == ({}, [])
    # Needed again by a pending demand, which resets the idle duration.
    assert _schedule(idle_timeout_s + 1, [ResourceRequestUtil.make({"CPU": 1})]) == (
        {},
        [],
    )
    assert _schedule(idle_timeout_s + 2, []) == ({}, [])
    assert _schedule(2 * idle_timeout_s + 2, []) == ({}, [])

    # Not needed since t=idle_timeout_s + 2, so it has now been continuously
    # unneeded for idle_timeout_s + 1 secs, which exceeds the idle timeout.
    _, to_terminate = _schedule(2 * idle_timeout_s + 3, [])
    if status == Instance.REQUESTED:
        assert to_terminate == []
    else:
        assert to_terminate == [("i-1", "", TerminationRequest.Cause.IDLE)]


def test_idle_termination_pending_nodes_needed_by_constraints():
    """
    Test that pending nodes needed by cluster resource constraints are not
    terminated.
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
    }
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            _pending_instance("i-1", Instance.ALLOCATED),
            _pending_instance("i-2", Instance.ALLOCATED),
        ],
        idle_timeout_s=1,
        pending_idle_timeout_s=1,
        disable_launch_config_check=True,
        cluster_resource_constraints=[ResourceRequestUtil.make({"CPU": 1})],
    )
    for now_s in [0, 10]:
        with patch("time.monotonic", return_value=now_s):
            _, to_terminate = _launch_and_terminate(scheduler.schedule(request))
    # Only one of the pending nodes is needed by the constraints.
    assert len(to_terminate) == 1
    assert to_terminate[0][2] == TerminationRequest.Cause.IDLE


def test_idle_termination_pending_nodes_with_min_worker():
    """
    Test that the min_worker_nodes are honored when terminating the unneeded
    pending nodes, and the pending nodes are terminated before the running ones.
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
    }
    request = sched_request(
        node_type_configs=node_type_configs,
        instances=[
            _running_instance("i-1", idle_duration_ms=100 * 1000),
            _pending_instance("i-2", Instance.ALLOCATED),
            _pending_instance("i-3", Instance.QUEUED),
        ],
        idle_timeout_s=1,
        pending_idle_timeout_s=1,
        disable_launch_config_check=True,
    )
    for now_s in [0, 10]:
        with patch("time.monotonic", return_value=now_s):
            _, to_terminate = _launch_and_terminate(scheduler.schedule(request))
    assert len(to_terminate) == 1
    # All 3 nodes are idle for longer than the timeouts, but min_worker_nodes=2
    # only allows terminating 1 of them. Idle termination visits the pending
    # nodes (i-2, i-3) before the running one (i-1), even though i-1 has been
    # idle for longer, so the one terminated is pending and i-1 is kept. i-2 and
    # i-3 have been unneeded for the same duration, so either may be picked.
    assert to_terminate[0][0] in ("i-2", "i-3")


def _pending_idle_test_configs() -> Dict[NodeType, NodeTypeConfig]:
    return {
        "type_cpu": NodeTypeConfig(
            name="type_cpu",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=5,
            launch_config_hash="hash1",
        ),
    }


def _schedule_pending_at(
    scheduler: ResourceDemandScheduler,
    now_s: float,
    instance_ids: List[str],
    num_cpu_requests: int,
    idle_timeout_s: float,
) -> List[str]:
    request = sched_request(
        node_type_configs=_pending_idle_test_configs(),
        instances=[_pending_instance(i, Instance.ALLOCATED) for i in instance_ids],
        idle_timeout_s=idle_timeout_s,
        pending_idle_timeout_s=idle_timeout_s,
        disable_launch_config_check=True,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * num_cpu_requests,
    )
    with patch("time.monotonic", return_value=now_s):
        _, to_terminate = _launch_and_terminate(scheduler.schedule(request))
    return [instance_id for instance_id, _, _ in to_terminate]


def test_idle_termination_pending_nodes_tracked_by_node_type():
    """
    Test that the idle durations of the pending nodes are tracked per node type,
    so they don't reset when the requests land on a different pending node of
    the same type in each round.
    """
    idle_timeout_s = 10
    scheduler = ResourceDemandScheduler(event_logger)

    # One of the two pending nodes is needed by the request. The request lands on
    # a different pending node each round, as the instance order changes.
    assert _schedule_pending_at(scheduler, 0, ["i-1", "i-2"], 1, idle_timeout_s) == []
    assert _schedule_pending_at(scheduler, 6, ["i-2", "i-1"], 1, idle_timeout_s) == []
    assert _schedule_pending_at(scheduler, 11, ["i-1", "i-2"], 1, idle_timeout_s) in (
        ["i-1"],
        ["i-2"],
    )


def test_idle_termination_pending_nodes_count_changes():
    """
    Test that the oldest idle durations are kept when the number of unneeded
    pending nodes drops, and that the durations of the terminated nodes are not
    inherited by the remaining ones.
    """
    idle_timeout_s = 10
    scheduler = ResourceDemandScheduler(event_logger)

    # 2 unneeded pending nodes since t=0.
    assert _schedule_pending_at(scheduler, 0, ["i-1", "i-2"], 0, idle_timeout_s) == []
    # Only 1 unneeded pending node since t=5, which keeps the duration since t=0.
    assert _schedule_pending_at(scheduler, 5, ["i-1", "i-2"], 1, idle_timeout_s) == []
    # 2 unneeded pending nodes again, the 2nd one since t=8.
    assert _schedule_pending_at(scheduler, 8, ["i-1", "i-2"], 0, idle_timeout_s) == []
    # The 1st one has been unneeded since t=0 and is terminated.
    terminated = _schedule_pending_at(scheduler, 11, ["i-1", "i-2"], 0, idle_timeout_s)
    assert len(terminated) == 1
    remaining = "i-2" if terminated == ["i-1"] else "i-1"
    # The remaining one has only been unneeded since t=8.
    assert _schedule_pending_at(scheduler, 12, [remaining], 0, idle_timeout_s) == []
    assert _schedule_pending_at(scheduler, 19, [remaining], 0, idle_timeout_s) == [
        remaining
    ]


def test_idle_termination_pending_nodes_after_max_workers_termination():
    """
    Test that the pending nodes terminated for max_worker_nodes don't take away
    the idle durations of the remaining unneeded pending nodes, since they are
    terminated before the unneeded pending nodes are counted.
    """
    idle_timeout_s = 10
    scheduler = ResourceDemandScheduler(event_logger)

    def _schedule(now_s: float, instance_ids: List[str], max_workers: int):
        configs = _pending_idle_test_configs()
        configs["type_cpu"].max_worker_nodes = max_workers
        request = sched_request(
            node_type_configs=configs,
            instances=[_pending_instance(i, Instance.ALLOCATED) for i in instance_ids],
            idle_timeout_s=idle_timeout_s,
            pending_idle_timeout_s=idle_timeout_s,
            disable_launch_config_check=True,
        )
        with patch("time.monotonic", return_value=now_s):
            _, to_terminate = _launch_and_terminate(scheduler.schedule(request))
        return to_terminate

    # All 3 pending nodes are unneeded since t=0.
    instance_ids = ["i-1", "i-2", "i-3"]
    assert _schedule(0, instance_ids, max_workers=3) == []

    # Lowering max_worker_nodes terminates 1 of them.
    to_terminate = _schedule(5, instance_ids, max_workers=2)
    assert len(to_terminate) == 1
    assert to_terminate[0][2] == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE
    instance_ids.remove(to_terminate[0][0])

    # Both remaining pending nodes keep their durations since t=0.
    to_terminate = _schedule(11, instance_ids, max_workers=2)
    assert sorted(to_terminate) == sorted(
        (i, "", TerminationRequest.Cause.IDLE) for i in instance_ids
    )


@pytest.mark.parametrize("pending_idle_timeout_s", [5, 100, None])
def test_idle_termination_pending_nodes_timeout(pending_idle_timeout_s):
    """
    Test that the pending nodes use their own idle timeout, independent of the
    regular idle timeout, and are never terminated when it's disabled.
    """
    scheduler = ResourceDemandScheduler(event_logger)
    request = sched_request(
        node_type_configs=_pending_idle_test_configs(),
        instances=[
            _running_instance("i-1", idle_duration_ms=50 * 1000),
            _pending_instance("i-2", Instance.ALLOCATED),
        ],
        idle_timeout_s=1,
        pending_idle_timeout_s=pending_idle_timeout_s,
        disable_launch_config_check=True,
    )
    with patch("time.monotonic", return_value=0):
        scheduler.schedule(request)
    with patch("time.monotonic", return_value=10):
        _, to_terminate = _launch_and_terminate(scheduler.schedule(request))

    # The running node always follows the regular idle timeout.
    expected = [("i-1", "r-i-1", TerminationRequest.Cause.IDLE)]
    if pending_idle_timeout_s == 5:
        expected.append(("i-2", "", TerminationRequest.Cause.IDLE))
    assert sorted(to_terminate) == sorted(expected)


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

    assert try_schedule({"CPU": 1}, [{"CPU": 1}]) == ([], (0, True, 1, 1.0, 1.0))

    assert try_schedule({"GPU": 4}, [{"GPU": 2}]) == ([], (0, True, 1, 0.5, 0.5))
    assert try_schedule({"GPU": 4}, [{"GPU": 1}, {"GPU": 1}]) == (
        [],
        (0, True, 1, 0.5, 0.5),
    )
    assert try_schedule({"GPU": 2}, [{"GPU": 2}]) == ([], (0, True, 1, 2, 2))
    assert try_schedule({"GPU": 2}, [{"GPU": 1}, {"GPU": 1}]) == (
        [],
        (0, True, 1, 2, 2),
    )
    assert try_schedule({"GPU": 1}, [{"GPU": 1, "CPU": 1}, {"GPU": 1}]) == (
        [{"GPU": 1, "CPU": 1}],
        (0, True, 1, 1, 1),
    )
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"GPU": 1, "CPU": 1}, {"GPU": 1}]) == (
        [{"GPU": 1}],
        (0, True, 2, 1, 1),
    )
    assert try_schedule({"GPU": 2, "TPU": 1}, [{"GPU": 2}]) == ([], (0, True, 1, 0, 1))
    assert try_schedule({"CPU": 64}, [{"CPU": 64}]) == ([], (0, True, 1, 64, 64))
    assert try_schedule({"CPU": 64}, [{"CPU": 32}]) == ([], (0, True, 1, 8, 8))
    assert try_schedule({"CPU": 64}, [{"CPU": 16}, {"CPU": 16}]) == (
        [],
        (0, True, 1, 8, 8),
    )

    # GPU Scores
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"CPU": 1}]) == (
        [],
        (0, False, 1, 0.0, 0.5),
    )
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"CPU": 1, "GPU": 1}]) == (
        [],
        (0, True, 2, 1.0, 1.0),
    )
    assert try_schedule({"GPU": 1, "CPU": 1}, [{"GPU": 1}]) == (
        [],
        (0, True, 1, 0.0, 0.5),
    )

    # Zero resources
    assert try_schedule({"CPU": 0, "custom": 1}, [{"custom": 1}]) == (
        [],
        (0, True, 1, 1, 1),
    )
    assert try_schedule({"CPU": 0, "custom": 1}, [{"CPU": 1}]) == (
        [{"CPU": 1}],
        (0, True, 0, 0.0, 0.0),
    )

    # Implicit resources
    implicit_resource = ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a"
    assert try_schedule({"CPU": 1}, [{implicit_resource: 1}]) == (
        [],
        (0, True, 0, 0.0, 0.0),
    )
    assert try_schedule({"CPU": 1}, [{implicit_resource: 1}] * 2) == (
        [{implicit_resource: 1}],
        (0, True, 0, 0.0, 0.0),
    )


@pytest.mark.parametrize(
    "source",
    [
        ResourceRequestSource.PENDING_DEMAND,
        ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT,
    ],
    ids=["demand", "cluster_resource_constraint"],
)
def test_node_schedule_label_selector_score(source):
    def try_schedule_ls(
        node_resources: Dict,
        node_labels: Dict[str, str],
        selectors,
    ) -> Tuple:
        cfg = NodeTypeConfig(
            name="type_1",
            resources=node_resources,
            min_worker_nodes=0,
            max_worker_nodes=1,
            labels=node_labels,
        )
        node = SchedulingNode.from_node_config(
            node_config=cfg,
            status=SchedulingNodeStatus.SCHEDULABLE,
            node_kind=NodeKind.WORKER,
        )
        req = ResourceRequestUtil.make({"CPU": 1}, label_selectors=selectors)
        infeasible, score = node.try_schedule([req], source)
        return ResourceRequestUtil.to_resource_maps(infeasible), score

    labels = {"ray.io/accelerator-type": "A100"}

    # 1) A matching label selector should be schedulable on node type_1
    label_selector_1 = [
        [
            (
                "ray.io/accelerator-type",
                LabelSelectorOperator.LABEL_OPERATOR_IN,
                ["TPU-v6e"],
            )
        ],
        [
            (
                "ray.io/accelerator-type",
                LabelSelectorOperator.LABEL_OPERATOR_IN,
                ["B200"],
            )
        ],
        [
            (
                "ray.io/accelerator-type",
                LabelSelectorOperator.LABEL_OPERATOR_IN,
                ["A100"],
            )
        ],
    ]
    assert try_schedule_ls({"CPU": 1}, labels, label_selector_1) == (
        [],
        (1 / 3, True, 1, 1.0, 1.0),
    )

    # Matching the second selector (index 1 of 3) yields normalized score 2/3.
    assert try_schedule_ls(
        {"CPU": 1}, {"ray.io/accelerator-type": "B200"}, label_selector_1
    ) == (
        [],
        (2 / 3, True, 1, 1.0, 1.0),
    )

    # Matching the first selector (index 0) always yields normalized score 1.0,
    # regardless of how many fallback selectors follow.
    assert try_schedule_ls(
        {"CPU": 1}, {"ray.io/accelerator-type": "TPU-v6e"}, label_selector_1
    ) == (
        [],
        (1.0, True, 1, 1.0, 1.0),
    )
    single_selector = [
        [("ray.io/accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])]
    ]
    assert try_schedule_ls({"CPU": 1}, labels, single_selector) == (
        [],
        (1.0, True, 1, 1.0, 1.0),
    )

    # 2) A non‑matching label selector should be infeasible
    label_selector_2 = [
        [("ray.io/accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["B200"])]
    ]
    assert try_schedule_ls({"CPU": 1}, labels, label_selector_2) == (
        [{"CPU": 1.0}],
        (0, True, 0, 0.0, 0.0),
    )

    # 3) Requests scheduled in an earlier pass must not contaminate the
    # matches_labels score of requests scheduled in a subsequent pass.
    cfg = NodeTypeConfig(
        name="type_1",
        resources={"CPU": 2},
        min_worker_nodes=0,
        max_worker_nodes=1,
        labels=labels,
    )
    node = SchedulingNode.from_node_config(
        node_config=cfg,
        status=SchedulingNodeStatus.SCHEDULABLE,
        node_kind=NodeKind.WORKER,
    )
    req_primary = ResourceRequestUtil.make({"CPU": 1}, label_selectors=single_selector)
    infeasible, score_1 = node.try_schedule([req_primary], source)
    assert infeasible == []
    assert score_1[0] == 1.0

    req_fallback = ResourceRequestUtil.make(
        {"CPU": 1}, label_selectors=label_selector_1
    )
    infeasible, score_2 = node.try_schedule([req_fallback], source)
    assert infeasible == []
    assert score_2[0] == 1 / 3


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


def test_schedule_node_with_matching_labels():
    """
    Test that a node with matching labels is considered schedulable and used to satisfy a request
    with a label_selector.
    """
    scheduler = ResourceDemandScheduler(event_logger)
    node_type_configs = {
        "labelled_node": NodeTypeConfig(
            name="labelled_node",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
            labels={"accelerator": "A100"},
        ),
    }

    # The existing instance has matching dynamic label.
    instance = make_autoscaler_instance(
        im_instance=Instance(
            instance_type="labelled_node",
            status=Instance.RAY_RUNNING,
            instance_id="1",
            node_id=b"r-1",
        ),
        ray_node=NodeState(
            node_id=b"r-1",
            ray_node_type_name="labelled_node",
            available_resources={"CPU": 1},
            total_resources={"CPU": 1},
            labels={"accelerator": "A100"},
            status=NodeStatus.RUNNING,
        ),
        cloud_instance_id="c-1",
    )

    # No new nodes should be launched if the existing node satisfies the request.
    resource_request = ResourceRequestUtil.make(
        {"CPU": 1},
        label_selectors=[
            [("accelerator", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])]
        ],
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[resource_request],
        instances=[instance],
    )
    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {}


def test_scale_up_node_to_satisfy_labels():
    """
    Test that a resource request with a label selector scales up a new node with
    labels to satisfy the constraint.
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "tpu_node": NodeTypeConfig(
            name="tpu_node",
            resources={"CPU": 1},
            labels={"accelerator": "TPU"},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "gpu_node": NodeTypeConfig(
            name="gpu_node",
            resources={"CPU": 1},
            labels={"accelerator": "A100"},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Request: want a node with label "accelerator: A100"
    resource_request = ResourceRequestUtil.make(
        {"CPU": 1},
        label_selectors=[
            [("accelerator", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])]
        ],
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[resource_request],
    )

    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)

    assert to_launch == {"gpu_node": 1}


def test_label_selector_fallback_priority():
    """
    Test that a resource request with multiple label selectors scales up
    the expected node given its fallback priority (i.e. earlier selectors are
    satisfied first).
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "tpu_node": NodeTypeConfig(
            name="tpu_node",
            resources={"CPU": 1},
            labels={"accelerator-type": "TPU"},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "gpu_node": NodeTypeConfig(
            name="gpu_node",
            resources={"CPU": 1},
            labels={"accelerator-type": "A100"},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # 1). TPU node is scaled up to satisfy first label selector.
    req1 = ResourceRequestUtil.make(
        {"CPU": 1},
        label_selectors=[
            [("accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["TPU"])],
            [("accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])],
        ],
    )
    reply1 = scheduler.schedule(
        sched_request(node_type_configs=node_type_configs, resource_requests=[req1])
    )
    to_launch1, _ = _launch_and_terminate(reply1)
    assert to_launch1 == {"tpu_node": 1}

    # 1). Label selector falls back to second priority and scales up A100 node.
    req2 = ResourceRequestUtil.make(
        {"CPU": 1},
        label_selectors=[
            # infeasible
            [("accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["B200"])],
            [("accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])],
        ],
    )
    reply2 = scheduler.schedule(
        sched_request(node_type_configs=node_type_configs, resource_requests=[req2])
    )
    to_launch2, _ = _launch_and_terminate(reply2)
    assert to_launch2 == {"gpu_node": 1}


def test_pg_with_bundle_infeasible_label_selectors():
    """
    Test that placement group scheduling honors bundle_label_selectors.
    """
    scheduler = ResourceDemandScheduler(event_logger)
    AFFINITY = ResourceRequestUtil.PlacementConstraintType.AFFINITY

    node_type_configs = {
        "gpu_node": NodeTypeConfig(
            name="gpu_node",
            resources={"CPU": 4, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=5,
            labels={"accelerator": "A100"},
        ),
        "tpu_node": NodeTypeConfig(
            name="tpu_node",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=5,
            labels={"accelerator": "TPU"},
        ),
    }

    # Create ResourceRequests for a placement group where each bundle has different label selectors
    gpu_request = ResourceRequestUtil.make(
        {"CPU": 2, "GPU": 1},
        constraints=[(AFFINITY, "pg-1", "")],
        label_selectors=[
            [("accelerator", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])]
        ],
    )
    tpu_request = ResourceRequestUtil.make(
        {"CPU": 2},
        constraints=[(AFFINITY, "pg-1", "")],
        label_selectors=[
            [("accelerator", LabelSelectorOperator.LABEL_OPERATOR_IN, ["TPU"])]
        ],
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=[[gpu_request, tpu_request]],
    )

    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)

    assert sorted(to_launch) == sorted({"gpu_node": 1, "tpu_node": 1})

    # Both bundles require A100, but no node has enough resources -> infeasible
    infeasbile_gpu_request = ResourceRequestUtil.make(
        {"CPU": 3, "GPU": 1},
        constraints=[(AFFINITY, "pg-2", "")],
        label_selectors=[
            [("accelerator", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])]
        ],
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=[[infeasbile_gpu_request, infeasbile_gpu_request]],
    )

    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)

    assert to_launch == {}
    assert len(reply.infeasible_gang_resource_requests) == 1


def test_get_nodes_with_resource_availabilities():
    node_type_configs = {
        "type_gpu1": NodeTypeConfig(
            name="type_gpu1",
            resources={"CPU": 8, "GPU": 1, "gpu1": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "type_gpu2": NodeTypeConfig(
            name="type_gpu2",
            resources={"CPU": 8, "GPU": 1, "gpu2": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "type_gpu3": NodeTypeConfig(
            name="type_gpu3",
            resources={"CPU": 8, "GPU": 1, "gpu3": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "type_gpu4": NodeTypeConfig(
            name="type_gpu4",
            resources={"CPU": 1, "GPU": 1, "gpu4": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    def get_nodes_for(
        resource_requests,
        anti_affinity=False,
        max_nodes: Optional[int] = None,
        current_nodes: Optional[Dict] = None,
        cloud_resource_availabilities=None,
    ):
        reply = schedule(
            node_type_configs,
            current_nodes or {},
            resource_requests,
            anti_affinity=anti_affinity,
            max_nodes=max_nodes,
            cloud_resource_availabilities=cloud_resource_availabilities,
        )
        to_launch, _ = _launch_and_terminate(reply)
        infeasible = ResourceRequestUtil.to_resource_maps(
            reply.infeasible_resource_requests
        )
        return to_launch, infeasible

    # Pick the node type with the highest availability score when utilization scores are equal.
    assert get_nodes_for(
        [{"CPU": 8, "GPU": 1}],
        cloud_resource_availabilities={
            "type_gpu1": 0.1,
            "type_gpu2": 1,
            "type_gpu3": 0.2,
        },
    ) == ({"type_gpu2": 1}, [])

    # The availability score is set to 1 by default.
    assert get_nodes_for(
        [{"CPU": 8, "GPU": 1}],
        cloud_resource_availabilities={"type_gpu2": 0.1, "type_gpu3": 0.2},
    ) == ({"type_gpu1": 1}, [])

    assert get_nodes_for(
        [{"CPU": 8, "GPU": 1}] * 2,
        cloud_resource_availabilities={
            "type_gpu1": 0.1,
            "type_gpu2": 0.1,
            "type_gpu3": 1,
        },
    ) == ({"type_gpu3": 2}, [])

    # The utilization score is the first factor to be considered.
    assert get_nodes_for([{"CPU": 1, "GPU": 1}], cloud_resource_availabilities={}) == (
        {"type_gpu4": 1},
        [],
    )

    # The utilization score is the first factor to be considered.
    assert get_nodes_for(
        [{"CPU": 1, "GPU": 1}],
        cloud_resource_availabilities={
            "type_gpu1": 0.1,
            "type_gpu2": 0.1,
            "type_gpu3": 1,
            "type_gpu4": 0.1,
        },
    ) == ({"type_gpu4": 1}, [])


def test_infeasible_shape_caching():
    """
    Test that identical requests failing to schedule on a node are cached,
    drastically reducing calls to _try_schedule_one to prevent O(N^2 * M) hangs.
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=1,  # Cluster can fit max one node
        ),
    }

    # Start with 1 existing node that has 2 CPUs available.
    instances = [
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 2},
                total_resources={"CPU": 2},
                node_id=b"r1",
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="1",
                node_id="r1",
            ),
            cloud_instance_id="c-1",
        ),
    ]

    # Submit 1,000 identical tasks that all request 2 CPUs.
    # Every request after the initial one should be cached and fail early.
    resource_requests = [ResourceRequestUtil.make({"CPU": 2}) for _ in range(1000)]

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=resource_requests,
        instances=instances,
        max_num_nodes=1,
    )

    # Validate _try_schedule_one is only called twice by the scheduler.
    orig_try_schedule_one = SchedulingNode._try_schedule_one
    with patch.object(
        SchedulingNode,
        "_try_schedule_one",
        autospec=True,
        side_effect=orig_try_schedule_one,
    ) as mock_try_schedule:
        reply = scheduler.schedule(request)

        # 1 task should be scheduled on the existing node. The other 999 fail.
        assert len(reply.infeasible_resource_requests) == 999

        #   Call 1: Fits the first 2-CPU request (Node is now full).
        #   Call 2: Evaluates the second 2-CPU request, fails, and adds to infeasible_shapes.
        #   Calls 3-1000: Bypassed entirely by the cache.
        assert mock_try_schedule.call_count == 2


def test_infeasible_shape_caching_with_label_mutation():
    """
    Test that dynamically adding labels clears the unavailable_shapes cache
    so interleaved valid requests aren't skipped.
    """
    scheduler = ResourceDemandScheduler(event_logger)
    ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    instances = [
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 4},
                total_resources={"CPU": 4},
                node_id=b"r1",
                labels={"required-label": "true"},
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="1",
                node_id="r1",
            ),
            cloud_instance_id="c-1",
        ),
    ]

    # Req A: needs "pg: 1" which is missing on node, fails initially.
    req_a = ResourceRequestUtil.make(
        {"CPU": 1},
        constraints=[(ANTI_AFFINITY, "dummy-anti", "1")],
        label_selectors=[[("pg", LabelSelectorOperator.LABEL_OPERATOR_IN, ["1"])]],
    )

    # Req B: Needs "required-label: true", adds "pg=1" to the node upon scheduling.
    req_b = ResourceRequestUtil.make(
        {"CPU": 1},
        constraints=[(ANTI_AFFINITY, "pg", "1")],
        label_selectors=[
            [("required-label", LabelSelectorOperator.LABEL_OPERATOR_IN, ["true"])]
        ],
    )

    # Manually group the requests to force an interleaved [A, B, A] ordering.
    req_a_grouped = ResourceRequestUtil.group_by_count([req_a])[0]
    req_b_grouped = ResourceRequestUtil.group_by_count([req_b])[0]
    resource_requests = [req_a_grouped, req_b_grouped, req_a_grouped]

    request = SchedulingRequest(
        disable_launch_config_check=False,
        node_type_configs=node_type_configs,
        resource_requests=resource_requests,
        current_instances=instances,
        max_num_nodes=1,
    )

    reply = scheduler.schedule(request)

    # Expected Sequence of Events:
    # 1. Req A1 evaluates -> fails (no pg=1 on node). Caches shape A.
    # 2. Req B evaluates -> succeeds. Mutates node state to add pg=1. Clears cache.
    # 3. Req A2 evaluates -> Cache miss. Succeeds because node now has pg=1.

    # Only the first Request A should have been marked infeasible.
    assert len(reply.infeasible_resource_requests) == 1

    # The scheduled node should have launched 0 new nodes (everything fit on the existing node)
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {}


def test_identical_node_state_caching():
    """
    Test that the scheduler avoids redundant deepcopies and simulations
    for nodes with identical states.
    """
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=100,
        ),
    }

    # Create 100 identical pending nodes
    instances = []
    for i in range(100):
        instances.append(
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type="type_1",
                    status=Instance.REQUESTED,
                    instance_id=f"pending-{i}",
                )
            )
        )

    # Submit a single request that requires 1 CPU
    resource_requests = [ResourceRequestUtil.make({"CPU": 1})]

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=resource_requests,
        instances=instances,
        max_num_nodes=100,
    )

    # Track how many times try_schedule is actually called on a node
    orig_try_schedule = SchedulingNode.try_schedule
    with patch.object(
        SchedulingNode,
        "try_schedule",
        autospec=True,
        side_effect=orig_try_schedule,
    ) as mock_try_schedule:

        reply = scheduler.schedule(request)

        # The scheduler should evaluate exactly one of the 100 identical pending nodes
        assert mock_try_schedule.call_count == 1

        # It should successfully schedule the task without needing to launch any new nodes,
        # because it used the first pending node.
        to_launch, _ = _launch_and_terminate(reply)
        assert to_launch == {}
        assert len(reply.infeasible_resource_requests) == 0


def test_ippr_resize_to_maximum_capacity():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Existing running node
    instance = make_autoscaler_instance(
        ray_node=NodeState(
            ray_node_type_name="type_1",
            available_resources={"CPU": 1},
            total_resources={"CPU": 1},
            node_id=b"r1",
        ),
        im_instance=Instance(
            instance_type="type_1",
            status=Instance.RAY_RUNNING,
            instance_id="i-1",
            node_id="r1",
        ),
        cloud_instance_id="pod-1",
    )

    # IPPR limits/specs and provider suggestion to upsize CPU to 2
    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=8 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )
    ippr_status = IPPRStatus(
        cloud_instance_id="pod-1",
        spec=ippr_specs.groups["type_1"],
        current_cpu=1,
        current_memory=1 * 1024 * 1024 * 1024,
        desired_cpu=1,
        desired_memory=1 * 1024 * 1024 * 1024,
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 2})],
        instances=[instance],
        ippr_specs=ippr_specs,
        ippr_statuses={"pod-1": ippr_status},
    )

    reply = scheduler.schedule(request)
    # Scheduler should issue one IPPR action with desired set to suggested values
    assert len(reply.to_ippr) == 1
    assert reply.to_ippr[0].cloud_instance_id == "pod-1"
    assert reply.to_ippr[0].desired_cpu == 4.0
    assert reply.to_ippr[0].desired_memory == 8 * 1024 * 1024 * 1024
    assert reply.to_launch == []


def test_ippr_resize_scale_out_if_one_ippr_is_new():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Existing running node
    instance = make_autoscaler_instance(
        ray_node=NodeState(
            ray_node_type_name="type_1",
            available_resources={"CPU": 1},
            total_resources={"CPU": 1},
            node_id=b"r1",
        ),
        im_instance=Instance(
            instance_type="type_1",
            status=Instance.RAY_RUNNING,
            instance_id="i-1",
            node_id="r1",
        ),
        cloud_instance_id="pod-1",
    )

    # IPPR limits/specs and provider suggestion to upsize CPU to 2
    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=8 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )
    ippr_status = IPPRStatus(
        cloud_instance_id="pod-1",
        spec=ippr_specs.groups["type_1"],
        current_cpu=1,
        current_memory=1 * 1024 * 1024 * 1024,
        desired_cpu=1,
        desired_memory=1 * 1024 * 1024 * 1024,
        k8s_resize_status="new",  # error or timeout will be rollback with a new IPPR action
        raylet_id="r1",
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 2})],
        instances=[instance],
        ippr_specs=ippr_specs,
        ippr_statuses={"pod-1": ippr_status},
    )

    reply = scheduler.schedule(request)
    # Scheduler should issue a new IPPR for the rollback.
    assert len(reply.to_ippr) == 1
    assert reply.to_ippr[0].cloud_instance_id == "pod-1"
    assert reply.to_ippr[0].desired_cpu == 1
    assert reply.to_ippr[0].desired_memory == 1 * 1024 * 1024 * 1024
    # Scheduler should scale out a new node
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {"type_1": 1}


def test_ippr_resize_scale_out_if_one_ippr_is_inprogress():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Existing running node
    instance = make_autoscaler_instance(
        ray_node=NodeState(
            ray_node_type_name="type_1",
            available_resources={"CPU": 1},
            total_resources={"CPU": 1},
            node_id=b"r1",
        ),
        im_instance=Instance(
            instance_type="type_1",
            status=Instance.RAY_RUNNING,
            instance_id="i-1",
            node_id="r1",
        ),
        cloud_instance_id="pod-1",
    )

    # IPPR limits/specs and provider suggestion to upsize CPU to 2
    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=8 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )
    ippr_status = IPPRStatus(
        cloud_instance_id="pod-1",
        spec=ippr_specs.groups["type_1"],
        current_cpu=1,
        current_memory=1 * 1024 * 1024 * 1024,
        desired_cpu=2,
        desired_memory=2 * 1024 * 1024 * 1024,
        k8s_resize_status="inprogress",
        raylet_id="r1",
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 4})],
        instances=[instance],
        ippr_specs=ippr_specs,
        ippr_statuses={"pod-1": ippr_status},
    )

    reply = scheduler.schedule(request)
    # Scheduler should not issue new IPPR action because the IPPR is in progress
    assert len(reply.to_ippr) == 0
    # Scheduler should scale out a new node
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {"type_1": 1}


def test_ippr_in_progress_exposes_desired_capacity_avoids_launch():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Existing running node with an in-progress resize to CPU=4
    instance = make_autoscaler_instance(
        ray_node=NodeState(
            ray_node_type_name="type_1",
            available_resources={"CPU": 1},
            total_resources={"CPU": 1},
            node_id=b"r1",
        ),
        im_instance=Instance(
            instance_type="type_1",
            status=Instance.RAY_RUNNING,
            instance_id="i-1",
            node_id="r1",
        ),
        cloud_instance_id="pod-1",
    )

    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=8 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )
    ippr_status = IPPRStatus(
        cloud_instance_id="pod-1",
        spec=ippr_specs.groups["type_1"],
        current_cpu=1,
        current_memory=1 * 1024 * 1024 * 1024,
        desired_cpu=4,
        desired_memory=8 * 1024 * 1024 * 1024,
        resizing_at=int(time.time()),
        k8s_resize_status="inprogress",
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 2})],
        instances=[instance],
        ippr_specs=ippr_specs,
        ippr_statuses={"pod-1": ippr_status},
    )

    reply = scheduler.schedule(request)
    # The scheduler should fit the 2-CPU request on the existing node (using desired capacity)
    assert reply.to_launch == []
    assert reply.to_ippr == []  # already in progress, no new IPPR action


def test_ippr_does_not_resize_pending_node_without_ray_node_id():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Existing pending node has no ray_node_id yet.
    instance = make_autoscaler_instance(
        im_instance=Instance(
            instance_type="type_1",
            status=Instance.ALLOCATED,
            instance_id="i-1",
        ),
        cloud_instance_id="pod-1",
    )

    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=8 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )
    ippr_status = IPPRStatus(
        cloud_instance_id="pod-1",
        spec=ippr_specs.groups["type_1"],
        current_cpu=1,
        current_memory=1 * 1024 * 1024 * 1024,
        desired_cpu=1,
        desired_memory=1 * 1024 * 1024 * 1024,
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 2})],
        instances=[instance],
        ippr_specs=ippr_specs,
        ippr_statuses={"pod-1": ippr_status},
    )

    reply = scheduler.schedule(request)
    # Pending nodes without a ray_node_id should not be selected for IPPR.
    assert reply.to_ippr == []
    # Scheduler should also not launch a new node since the pending node could fulfill the request after IPPR.
    to_launch, _ = _launch_and_terminate(reply)
    assert to_launch == {}


def test_ippr_capacity_of_unselected_candidates_not_modified():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    instances = [
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
                instance_id="i-1",
                node_id="r1",
            ),
            cloud_instance_id="pod-1",
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 1},
                total_resources={"CPU": 1},
                node_id=b"r2",
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="i-2",
                node_id="r2",
            ),
            cloud_instance_id="pod-2",
        ),
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 1},
                total_resources={"CPU": 1},
                node_id=b"r3",
                idle_duration_ms=10_000,
            ),
            im_instance=Instance(
                instance_type="type_1",
                status=Instance.RAY_RUNNING,
                instance_id="i-3",
                node_id="r3",
            ),
            cloud_instance_id="pod-3",
        ),
    ]

    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=8 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )
    ippr_statuses = {
        "pod-1": IPPRStatus(
            cloud_instance_id="pod-1",
            spec=ippr_specs.groups["type_1"],
            current_cpu=1,
            current_memory=1 * 1024 * 1024 * 1024,
            desired_cpu=1,
            desired_memory=1 * 1024 * 1024 * 1024,
        ),
        "pod-2": IPPRStatus(
            cloud_instance_id="pod-2",
            spec=ippr_specs.groups["type_1"],
            current_cpu=1,
            current_memory=1 * 1024 * 1024 * 1024,
            desired_cpu=1,
            desired_memory=1 * 1024 * 1024 * 1024,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        gang_resource_requests=[[ResourceRequestUtil.make({"CPU": 2})]],
        instances=instances,
        idle_timeout_s=0,
        ippr_specs=ippr_specs,
        ippr_statuses=ippr_statuses,
    )

    reply = scheduler.schedule(request)

    assert reply.to_launch == []
    # Only one IPPR candidate is selected for this gang request.
    assert len(reply.to_ippr) == 1
    assert {status.cloud_instance_id for status in reply.to_ippr} == {"pod-1"}
    assert {status.desired_cpu for status in reply.to_ippr} == {4.0}
    _, to_terminate = _launch_and_terminate(reply)
    assert [instance_id for instance_id, _, _ in to_terminate] == ["i-3"]
    # if pod-2 is accidentally selected for IPPR (it should not be),
    # the cluster resources should be bigger than 5.0
    assert reply.cluster_resources["CPU"] == 5.0


def test_ippr_max_limits_affect_new_node_capacity():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # No existing instances; IPPR max allows new nodes to expose larger capacity
    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=4 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 4,
        ippr_specs=ippr_specs,
    )

    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)
    # With IPPR max=4, all four 1-CPU bundles should fit on a single launched node
    assert to_launch == {"type_1": 1}
    assert reply.to_ippr == []


def test_ippr_max_limits_affect_new_node_capacity_2():
    scheduler = ResourceDemandScheduler(event_logger)

    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    ippr_specs = IPPRSpecs(
        groups={
            "type_1": IPPRGroupSpec(
                min_cpu=1,
                max_cpu=4,
                min_memory=1 * 1024 * 1024 * 1024,
                max_memory=4 * 1024 * 1024 * 1024,
                resize_timeout=60,
            )
        }
    )

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 6,
        ippr_specs=ippr_specs,
    )

    reply = scheduler.schedule(request)
    to_launch, _ = _launch_and_terminate(reply)

    # Each launched node should be evaluated with IPPR max=4 CPU capacity:
    # six 1-CPU bundles should require exactly two new nodes, not three.
    assert to_launch == {"type_1": 2}
    assert reply.to_ippr == []


class TestSchedulerPerformanceOptimizations:
    """Tests for large-cluster performance optimizations."""

    def test_precompute_serializes_once_per_unique_request(self, monkeypatch):
        """The same request object repeated count times should be serialized once."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 10, "memory": 100},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 10,
        )
        # Patch after sched_request: group_by_count inside it serializes each
        # request legitimately, which would skew the count below.
        calls = 0
        serialized_ids = set()
        orig = ResourceRequest.SerializeToString

        def counting_serialize(self, deterministic=None):
            nonlocal calls
            calls += 1
            serialized_ids.add(id(self))
            return orig(self, deterministic=deterministic)

        monkeypatch.setattr(ResourceRequest, "SerializeToString", counting_serialize)
        ResourceDemandScheduler(event_logger).schedule(request)
        assert calls == 1 and len(serialized_ids) == 1

    def test_quick_reject_skips_exhausted_nodes(self):
        """Nodes with no available resources should be skipped without deepcopy."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 10, "memory": 100},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # Create instances where all resources are allocated (available = 0).
        instances = []
        for i in range(50):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={},  # All resources used up
                        total_resources={"CPU": 10, "memory": 100},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )

        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 2})] * 10,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # Should launch new nodes since existing are exhausted.
        assert to_launch == {"type_1": 2}

    def test_quick_reject_partial_resources(self):
        """Nodes with some resources but below minimum demand are skipped."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 10, "memory": 100},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # Node has 1 CPU available but all requests need 4 CPU.
        instances = []
        for i in range(10):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={"CPU": 1, "memory": 10},
                        total_resources={"CPU": 10, "memory": 100},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )

        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 4})] * 5,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # All existing nodes have < 4 CPU available, must launch new.
        # 5 requests × 4 CPU each, new nodes have 10 CPU → fits 2 per node → need 3.
        assert to_launch == {"type_1": 3}

    def test_quick_reject_does_not_skip_feasible_nodes(self):
        """Nodes with sufficient resources should still be scheduled on."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 10, "memory": 100},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # Nodes have plenty of resources.
        instances = []
        for i in range(5):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={"CPU": 10, "memory": 100},
                        total_resources={"CPU": 10, "memory": 100},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )

        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 2})] * 10,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # Existing nodes can handle all requests (5 nodes × 10 CPU ÷ 2 CPU = 25 slots).
        assert to_launch == {}

    def test_quick_reject_cpu_exhausted_but_memory_available(self):
        """Nodes with CPU exhausted but memory available should be skipped."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 1, "memory": 1000},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # CPU is fully used but memory is abundant.
        instances = []
        for i in range(50):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={"CPU": 0, "memory": 800},
                        total_resources={"CPU": 1, "memory": 1000},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )

        # All requests need both CPU and memory.
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 0.2, "memory": 30})]
            * 10,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # 50 nodes have CPU=0, no request (needing CPU=0.2) can fit.
        # 10 requests * 0.2 CPU, each new node has 1 CPU -> need 2 nodes.
        assert to_launch == {"type_1": 2}

    def test_quick_reject_mixed_shapes_or_across_shapes(self):
        """Nodes kept if they can fit at least one request shape (OR across shapes).

        Scenario: two types of requests with different resource shapes.
        - Shape A: {CPU: 2} (CPU-only)
        - Shape B: {GPU: 1} (GPU-only)
        Nodes have CPU available but no GPU. They should NOT be rejected because
        shape A can still fit (OR across shapes keeps the node).
        """
        node_type_configs = {
            "type_cpu": NodeTypeConfig(
                name="type_cpu",
                resources={"CPU": 8},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
            "type_gpu": NodeTypeConfig(
                name="type_gpu",
                resources={"CPU": 4, "GPU": 1},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # CPU nodes with available CPU but no GPU.
        instances = []
        for i in range(3):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_cpu",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_cpu-{i}",
                        node_id=f"r{i}type_cpu",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_cpu".encode("utf-8"),
                        ray_node_type_name="type_cpu",
                        available_resources={"CPU": 4},
                        total_resources={"CPU": 8},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_cpu-{i}",
                )
            )

        # Mix of CPU-only and GPU-only requests.
        resource_requests = [ResourceRequestUtil.make({"CPU": 2})] * 4 + [
            ResourceRequestUtil.make({"GPU": 1})
        ] * 2
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=resource_requests,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # CPU requests: 4 × 2 CPU = 8 CPU needed. 3 nodes × 4 CPU avail = 12 CPU.
        # Existing CPU nodes can handle all CPU requests (no new CPU nodes needed).
        # GPU requests: 2 × 1 GPU. No existing node has GPU → need new GPU nodes.
        # type_gpu has 1 GPU each → need 2 GPU nodes.
        assert to_launch == {"type_gpu": 2}

    def test_quick_reject_mixed_shapes_all_exhausted(self):
        """Nodes rejected when they cannot fit ANY request shape.

        Scenario: two types of requests with different resource shapes.
        - Shape A: {CPU: 2, memory: 100}
        - Shape B: {GPU: 1}
        Nodes have CPU=0 (exhausted) and no GPU. Neither shape can fit, so the
        node should be rejected by the pre-filter.
        """
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 4, "memory": 500},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
            "type_gpu": NodeTypeConfig(
                name="type_gpu",
                resources={"CPU": 1, "GPU": 1, "memory": 500},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # Nodes with CPU exhausted, plenty of memory, no GPU.
        instances = []
        for i in range(5):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={"CPU": 0, "memory": 400},
                        total_resources={"CPU": 4, "memory": 500},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )

        # Shape A needs CPU+memory; Shape B needs GPU.
        # Existing nodes have CPU=0 (fails shape A) and no GPU (fails shape B).
        resource_requests = [
            ResourceRequestUtil.make({"CPU": 2, "memory": 100})
        ] * 3 + [ResourceRequestUtil.make({"GPU": 1})] * 2
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=resource_requests,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # All existing nodes are rejected (can't fit shape A or B).
        # Shape A: 3 × {CPU:2, memory:100}. type_1 has 4 CPU, 500 mem → fits 2/node → need 2.
        # Shape B: 2 × {GPU:1}. type_gpu has 1 GPU each (CPU=1 can't fit shape A) → need 2.
        assert to_launch == {"type_1": 2, "type_gpu": 2}

    def test_quick_reject_keeps_nodes_for_zero_resource_requests(self):
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 1, "memory": 1000},
                min_worker_nodes=0,
                # Cluster is already at its configured maximum.
                max_worker_nodes=5,
            ),
        }
        instances = []
        for i in range(5):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={"CPU": 0, "memory": 800},
                        total_resources={"CPU": 1, "memory": 1000},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )
        # 3 CPU requests that genuinely cannot be placed (every node has CPU=0
        # and the cluster is at max_worker_nodes), plus 2 zero-resource
        # requests that fit on any of the 5 existing nodes.
        resource_requests = [
            ResourceRequestUtil.make({"CPU": 0.2, "memory": 30})
        ] * 3 + [ResourceRequestUtil.make({})] * 2
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=resource_requests,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # At max_worker_nodes, so nothing can be launched either way.
        assert to_launch == {}
        # Only the 3 CPU requests are infeasible. The 2 zero-resource requests
        # must be placed on the existing nodes, not reported as unschedulable.
        assert len(reply.infeasible_resource_requests) == 3

    def test_quick_reject_keeps_nodes_for_implicit_resource_requests(self):
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 1, "memory": 1000},
                min_worker_nodes=0,
                # Cluster is already at its configured maximum.
                max_worker_nodes=5,
            ),
        }
        instances = []
        for i in range(5):
            instances.append(
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="type_1",
                        status=Instance.RAY_RUNNING,
                        instance_id=f"type_1-{i}",
                        node_id=f"r{i}type_1",
                    ),
                    ray_node=NodeState(
                        node_id=f"r{i}type_1".encode("utf-8"),
                        ray_node_type_name="type_1",
                        available_resources={"CPU": 0, "memory": 800},
                        total_resources={"CPU": 1, "memory": 1000},
                        idle_duration_ms=0,
                        status=NodeStatus.RUNNING,
                    ),
                    cloud_instance_id=f"c-type_1-{i}",
                )
            )
        # 3 CPU requests that cannot be placed (every node has CPU=0 and the
        # cluster is at max_worker_nodes), plus one implicit-resource request
        # that every node satisfies implicitly (nodes do not list implicit
        # keys in available_resources).
        implicit_resource = ray._raylet.IMPLICIT_RESOURCE_PREFIX + "a"
        resource_requests = [
            ResourceRequestUtil.make({"CPU": 0.2, "memory": 30})
        ] * 3 + [ResourceRequestUtil.make({implicit_resource: 1})]
        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=resource_requests,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # At max_worker_nodes, so nothing can be launched either way.
        assert to_launch == {}
        # Only the 3 CPU requests are infeasible. The implicit-resource request
        # must be placed on an existing node, not reported as unschedulable.
        assert len(reply.infeasible_resource_requests) == 3


_IN = LabelSelectorOperator.LABEL_OPERATOR_IN
_NOT_IN = LabelSelectorOperator.LABEL_OPERATOR_NOT_IN
_ANTI = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
_AFF = ResourceRequestUtil.PlacementConstraintType.AFFINITY
_SLICE = "ray.io/tpu-slice-name"
_WID = "ray.io/tpu-worker-id"
_POD = "ray.io/tpu-pod-type"
_ACC = "ray.io/accelerator-type"
_TOPO = "ray.io/tpu-topology"
_GDOM = "ray.io/gpu-domain"
_mk_req = ResourceRequestUtil.make


def _locality_gang(
    reqs: List[ResourceRequest],
    label: str = _SLICE,
    pinned: Optional[str] = None,
    not_in: Optional[List[str]] = None,
) -> GangResourceRequest:
    cs = []
    if pinned is not None:
        cs.append(
            LabelSelectorConstraint(
                label_key=label, operator=_IN, label_values=[pinned]
            )
        )
    if not_in:
        cs.append(
            LabelSelectorConstraint(
                label_key=label, operator=_NOT_IN, label_values=not_in
            )
        )
    loc = LocalityRequirement(
        locality_constraint=LocalityConstraint(
            label_name=label, placement_strategy=PlacementStrategy.STRICT_PACK
        ),
        label_selector=LabelSelector(label_constraints=cs),
    )
    return GangResourceRequest(
        requests=reqs,
        bundle_selectors=[
            BundleSelector(resource_requests=reqs, locality_requirement=loc)
        ],
    )


def _make_locality_gang_request(
    bundles: List[Dict[str, float]],
    bundle_labels: Union[Dict[str, str], List[Dict[str, str]]],
    locality_label: str,
    pinned_domain: Optional[str] = None,
) -> GangResourceRequest:
    per_b = (
        bundle_labels
        if isinstance(bundle_labels, list)
        else [bundle_labels] * len(bundles)
    )
    reqs = [
        _mk_req(
            b,
            label_selectors=[[(k, _IN, [v]) for k, v in lbl.items()]] if lbl else None,
        )
        for b, lbl in zip(bundles, per_b)
    ]
    return _locality_gang(reqs, locality_label, pinned=pinned_domain)


def _run_node(
    nt: str,
    iid: str,
    total: Dict[str, float],
    labels: Dict[str, str],
    used: Optional[Dict[str, float]] = None,
    idle_ms: int = 0,
) -> AutoscalerInstance:
    used = used or {}
    return make_autoscaler_instance(
        im_instance=Instance(
            instance_type=nt,
            status=Instance.RAY_RUNNING,
            instance_id=iid,
            node_id=iid.encode(),
        ),
        ray_node=NodeState(
            node_id=iid.encode(),
            ray_node_type_name=nt,
            total_resources=total,
            available_resources={k: v - used.get(k, 0) for k, v in total.items()},
            labels=labels,
            status=NodeStatus.IDLE if idle_ms and not used else NodeStatus.RUNNING,
            idle_duration_ms=idle_ms,
        ),
        cloud_instance_id=f"cloud-{iid}",
    )


def _pending_node(nt: str, iid: str, status=Instance.REQUESTED) -> AutoscalerInstance:
    return make_autoscaler_instance(
        im_instance=Instance(instance_type=nt, status=status, instance_id=iid)
    )


def _resource_request(
    res: Dict[str, float], count: int = 1, sel=None
) -> ResourceRequestByCount:
    return ResourceRequestByCount(
        request=_mk_req(res, label_selectors=sel), count=count
    )


def _sched(
    cfgs: Dict[NodeType, NodeTypeConfig],
    insts: Optional[List[AutoscalerInstance]] = None,
    *,
    gangs: Optional[List[GangResourceRequest]] = None,
    reqs: Optional[List[ResourceRequestByCount]] = None,
    crc: Optional[List[ClusterResourceConstraint]] = None,
    max_nodes: Optional[int] = None,
    idle_s: Optional[float] = None,
    pending_idle_s: Optional[float] = None,
    unneeded: Optional[Dict[NodeType, List[float]]] = None,
    avail: Optional[Dict[NodeType, float]] = None,
    ippr_specs: Optional[IPPRSpecs] = None,
    ippr_statuses: Optional[Dict[str, IPPRStatus]] = None,
    sched: Optional[ResourceDemandScheduler] = None,
) -> SchedulingReply:
    s = sched or ResourceDemandScheduler(event_logger)
    if unneeded is not None:
        s._pending_unneeded_since_s = {k: list(v) for k, v in unneeded.items()}
    return s.schedule(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=cfgs,
            current_instances=insts or [],
            gang_resource_requests=gangs or [],
            resource_requests=reqs or [],
            cluster_resource_constraints=crc or [],
            max_num_nodes=max_nodes,
            idle_timeout_s=idle_s,
            pending_idle_timeout_s=pending_idle_s,
            cloud_resource_availabilities=avail or {},
            ippr_specs=ippr_specs,
            ippr_statuses=ippr_statuses or {},
        )
    )


def _schedule_gangs(
    cfgs, gangs, insts=None, avail=None
) -> Tuple[Dict[NodeType, int], int]:
    rep = _sched(cfgs, insts, gangs=gangs, avail=avail)
    return _launch_and_terminate(rep)[0], len(rep.infeasible_gang_resource_requests)


def _mk_ctx(cfgs, insts, gangs=None):
    req = SchedulingRequest(
        disable_launch_config_check=True,
        node_type_configs=cfgs,
        current_instances=insts,
        gang_resource_requests=gangs or [],
    )
    return ResourceDemandScheduler.ScheduleContext.from_schedule_request(req)


TPU_LABELS = {_ACC: "TPU-V6E", _TOPO: "4x4", _POD: "v6e-16"}
_TOPO_CHIPS = {"4x4": 16, "2x4": 8, "2x2": 4, "2x2x2": 8}
_HEAD_RES = {"CPU": 4, "TPU": 4, "TPU-v6e-16-head": 1}


def _tpu_config(
    name: str = "tpu_4x4",
    min_workers: int = 0,
    max_workers: int = 16,
    resources: Optional[Dict[str, float]] = None,
    labels: Optional[Dict[str, str]] = None,
    workers_per_replica: Optional[int] = -1,
    priority: int = 0,
) -> NodeTypeConfig:
    res = resources or {"CPU": 4, "TPU": 4}
    lbls = TPU_LABELS if labels is None else labels
    if workers_per_replica == -1:
        topo, tpu_n = lbls.get(_TOPO), int(res.get("TPU", 0))
        workers_per_replica = (
            max(1, _TOPO_CHIPS[topo] // tpu_n)
            if topo in _TOPO_CHIPS and tpu_n > 0
            else None
        )
    heads = tuple(
        sorted(k for k in res if k.startswith("TPU-") and k.endswith("-head"))
    )
    rc = (
        WorkerReplicaConfig(workers_per_replica, _SLICE, _WID, heads)
        if workers_per_replica is not None
        else None
    )
    return NodeTypeConfig(
        name=name,
        resources=res,
        labels=lbls,
        min_worker_nodes=min_workers,
        max_worker_nodes=max_workers,
        priority=priority,
        replica_config=rc,
    )


TPU_CONFIGS = {"tpu_4x4": _tpu_config()}
TPU_SINGLE = _tpu_config(
    "tpu_single",
    max_workers=4,
    resources={"CPU": 4, "TPU": 8},
    labels={_ACC: "TPU-V6E", _TOPO: "2x4", _POD: "v6e-8"},
)
_TPU_V6E4 = _tpu_config(
    "tpu_v6e4", max_workers=4, labels={_ACC: "TPU-V6E", _TOPO: "2x2", _POD: "v6e-4"}
)
_CPU_AND_TPU = {
    **TPU_CONFIGS,
    "cpu": _tpu_config("cpu", max_workers=8, resources={"CPU": 8}, labels={}),
}
_TWO_HOST_CFGS = {
    "tpu_4x4": _tpu_config("tpu_4x4", max_workers=8, priority=10),
    "tpu_2x4": _tpu_config("tpu_2x4", max_workers=4, labels=TPU_SINGLE.labels),
}


def _tpu_gang(
    wids: Sequence[Optional[int]] = (0, 1, 2, 3),
    bundle: Optional[Dict[str, float]] = None,
    pod_type: Optional[str] = "v6e-16",
    pinned: Optional[str] = None,
    not_in: Optional[List[str]] = None,
) -> GangResourceRequest:
    reqs = []
    for w in wids:
        lbl = {**({_POD: pod_type} if pod_type else {})}
        if w is not None:
            lbl[_WID] = str(w)
        sel = [[(k, _IN, [v]) for k, v in lbl.items()]] if lbl else None
        reqs.append(_mk_req(bundle or {"CPU": 1, "TPU": 4}, label_selectors=sel))
    return _locality_gang(reqs, pinned=pinned, not_in=not_in)


def _tpu_host(
    iid: str,
    s: str,
    w: int = 0,
    used: Optional[Dict[str, float]] = None,
    idle_ms: int = 0,
    head: bool = False,
    **extra: float,
) -> AutoscalerInstance:
    res = {"CPU": 4, "TPU": 4, **({"TPU-v6e-16-head": 1} if head else {}), **extra}
    lbls = {**TPU_LABELS, _SLICE: s, _WID: str(w)}
    return _run_node("tpu_4x4", iid, res, lbls, used, idle_ms)


def _pending_tpu(
    iid: str, status=Instance.ALLOCATED, nt: str = "tpu_4x4"
) -> AutoscalerInstance:
    return _pending_node(nt, iid, status)


def _booting_slice(idx: int) -> List[AutoscalerInstance]:
    u = {"CPU": 1, "object_store_memory": 100}
    return [
        _tpu_host(
            f"s{idx}-0",
            f"slice-{idx}",
            0,
            u,
            object_store_memory=1000,
            **{f"slice-{idx}": 1},
        ),
        _tpu_host(f"s{idx}-1", f"slice-{idx}", 1, u, object_store_memory=1000),
        _pending_node("tpu_4x4", f"s{idx}-p2"),
        _pending_node("tpu_4x4", f"s{idx}-p3"),
    ]


_DEG_B0 = [_tpu_host("s0-0", "slice-0", 0, {"TPU": 4}), _tpu_host("s0-1", "slice-0", 1)]
_DEG_B1 = [_tpu_host("s1-0", "slice-1", 0, {"TPU": 4}), _tpu_host("s1-1", "slice-1", 1)]
_DEG_B_IDLE = _DEG_B0 + [
    _tpu_host("s1-0", "slice-1", 0, {"CPU": 1}),
    _tpu_host("s1-1", "slice-1", 1, {"CPU": 1}),
    _pending_node("tpu_4x4", "p0"),
    _pending_node("tpu_4x4", "p1"),
]
_IDLE_S0 = [_tpu_host(f"s0-{w}", "slice-0", w) for w in range(4)]
_P4 = [_pending_tpu(f"p{w}") for w in range(4)]
_SH0 = _run_node(
    "tpu_single",
    "sh-0",
    {"CPU": 4, "TPU": 8},
    {**TPU_SINGLE.labels, _SLICE: "single-0", _WID: "0"},
)
_V40 = _run_node(
    "tpu_v6e4",
    "v0",
    {"CPU": 4, "TPU": 4},
    {**_TPU_V6E4.labels, _SLICE: "v4-0", _WID: "0"},
)
_PG_CPU = GangResourceRequest(requests=[_mk_req({"CPU": 2}, [(_AFF, "pg", "")])] * 2)


@pytest.mark.parametrize(
    "cfgs,gangs,insts,exp",
    [
        (TPU_CONFIGS, [_tpu_gang()], None, ({"tpu_4x4": 4}, 0)),
        (TPU_CONFIGS, [_tpu_gang()] * 2, None, ({"tpu_4x4": 8}, 0)),
        (TPU_CONFIGS, [_tpu_gang()], _booting_slice(0), ({}, 0)),
        (TPU_CONFIGS, [_tpu_gang()] * 2, _booting_slice(0), ({"tpu_4x4": 4}, 0)),
        (
            TPU_CONFIGS,
            [_tpu_gang()] * 2,
            _booting_slice(0) + _booting_slice(1),
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang(pinned="slice-0")],
            _booting_slice(0) + _P4[:2],
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang()] * 2,
            [
                _tpu_host(f"s0-{w}", "slice-0", w, {"TPU": 4} if w == 0 else None)
                for w in range(4)
            ]
            + _P4,
            ({"tpu_4x4": 4}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang([i // 4 for i in range(16)], {"TPU": 1})] * 2,
            [
                _tpu_host(f"s0-{w}", "slice-0", w, {"TPU": 1} if w == 0 else None)
                for w in range(4)
            ]
            + _P4,
            ({"tpu_4x4": 4}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang([i // 2 for i in range(8)], {"TPU": 1})],
            None,
            ({"tpu_4x4": 4}, 0),
        ),
        (TPU_CONFIGS, [_tpu_gang((0, 1))], None, ({"tpu_4x4": 4}, 0)),
        (
            TPU_CONFIGS,
            [_tpu_gang((0, 1)), _tpu_gang((2, 3))],
            None,
            ({"tpu_4x4": 4}, 0),
        ),
        (
            {"tpu_4x4": _tpu_config(min_workers=4)},
            [_tpu_gang()],
            None,
            ({"tpu_4x4": 4}, 0),
        ),
        (
            {"tpu_4x4": _tpu_config(max_workers=4)},
            [_tpu_gang()] * 2,
            None,
            ({"tpu_4x4": 4}, 1),
        ),
        (TPU_CONFIGS, [_tpu_gang()], _DEG_B0 + _P4, ({}, 0)),
        (TPU_CONFIGS, [_tpu_gang()], _DEG_B_IDLE, ({}, 0)),
        (TPU_CONFIGS, [_tpu_gang(range(5)), _tpu_gang()], None, ({"tpu_4x4": 4}, 1)),
        (
            TPU_CONFIGS,
            [_tpu_gang((0, 0))],
            [_tpu_host("s0-0", "slice-0", 0), _pending_tpu("p1")],
            ({}, 1),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang()],
            [
                _tpu_host(f"s{s}-{w}", f"slice-{s}", w, {"TPU": 4} if w == 0 else None)
                for s in (0, 1)
                for w in range(4)
            ],
            ({"tpu_4x4": 4}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang((0,), pinned="slice-0")],
            [_tpu_host("s0-0", "slice-0", 0, {"TPU": 4})],
            ({}, 1),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang((2, 3), pinned="slice-0")],
            _DEG_B0 + _P4[:2],
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang(), _tpu_gang(pinned="slice-0")],
            _IDLE_S0,
            ({"tpu_4x4": 4}, 0),
        ),
        (
            {"tpu_4x4": _tpu_config(min_workers=4), "tpu_single": TPU_SINGLE},
            [_tpu_gang((None,), {"CPU": 1, "TPU": 8}, "v6e-8")],
            None,
            ({"tpu_4x4": 4, "tpu_single": 1}, 0),
        ),
        (
            {**TPU_CONFIGS, "tpu_single": TPU_SINGLE},
            [
                _tpu_gang((None,), {"CPU": 1, "TPU": 8}, "v6e-8", "single-0"),
                _tpu_gang(),
            ],
            [_SH0, *_DEG_B_IDLE],
            ({}, 0),
        ),
        (
            {"tpu_single": TPU_SINGLE},
            [_tpu_gang((None,) * 3, pod_type=None)],
            None,
            ({}, 1),
        ),
        (
            _CPU_AND_TPU,
            [_tpu_gang((None,) * 4, {"CPU": 1}, None)],
            [_run_node("cpu", "c0", {"CPU": 8}, {})],
            ({"tpu_4x4": 4}, 0),
        ),
        (_CPU_AND_TPU, [_PG_CPU, _tpu_gang()], _IDLE_S0, ({"cpu": 1}, 0)),
        (
            {**TPU_CONFIGS, "tpu_v6e4": _TPU_V6E4},
            [_tpu_gang((None,), {"CPU": 4, "TPU": 4}, "v6e-4", "v4-0"), _tpu_gang()],
            [_V40, *_DEG_B_IDLE],
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang([i // 2 for i in range(8)], {"CPU": 2, "TPU": 2}), _tpu_gang()],
            _DEG_B_IDLE,
            ({"tpu_4x4": 4}, 0),
        ),
        (
            _TWO_HOST_CFGS,
            [
                _tpu_gang((None,) * 4, {"TPU": 1}, None),
                _tpu_gang((None,) * 3, pod_type=None),
                _tpu_gang((None,) * 2, pod_type=None),
            ],
            None,
            ({"tpu_2x4": 4, "tpu_4x4": 4}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang((0, 1, 2, 3), {"CPU": 1, "TPU": 1})],
            _booting_slice(0),
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang((0, 0, 0, 0), {"CPU": 1, "TPU": 1})],
            _booting_slice(0),
            ({"tpu_4x4": 4}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang((2, 3), pinned="slice-0"), _tpu_gang((2, 3), pinned="slice-1")],
            _DEG_B0 + _DEG_B1 + _P4,
            ({}, 0),
        ),
        (TPU_CONFIGS, [_tpu_gang(not_in=["slice-0"])], _IDLE_S0, ({"tpu_4x4": 4}, 0)),
        (
            TPU_CONFIGS,
            [_tpu_gang(not_in=["slice-0"]), _tpu_gang((2, 3), pinned="slice-0")],
            _booting_slice(0) + _booting_slice(1),
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang()] * 2,
            [_tpu_host("s1-0", "slice-1", 0, idle_ms=5000)]
            + [_tpu_host(f"s2-{w}", "slice-2", w, idle_ms=5000) for w in range(3)]
            + _P4,
            ({}, 0),
        ),
    ],
    ids=[
        "scale_zero_1",
        "scale_zero_2",
        "staggered_1",
        "staggered_2g_1s",
        "staggered_2s",
        "pinned_extra_pend",
        "no_attach_complete",
        "per_chip_1_busy",
        "subhost_8b_4w",
        "subslice_2h",
        "complementary_subslices",
        "reuse_min_workers",
        "max_workers_caps",
        "prefer_pending_over_deg_busy",
        "prefer_boot_idle_over_deg_busy",
        "oob_wid",
        "dup_wid_infeasible",
        "fragmented_slices",
        "pinned_deg_busy_infeasible",
        "pinned_deg_pairs_boot",
        "pinned_outranks_unpinned",
        "min_workers_other_tpu",
        "pinned_other_type_no_regroup",
        "single_host_3b_infeasible",
        "no_cpu_for_tpu_slice",
        "slice_outranks_node_pack",
        "no_leak_across_types",
        "no_leak_across_gangs",
        "two_host_vs_four_host_pack",
        "subhost_1_per_wid",
        "subhost_4_on_w0_launches",
        "two_pinned_deg_slices",
        "not_in_launches_new",
        "not_in_with_pinned",
        "probe_1b_staggered_2_gangs_idle_gt_0",
    ],
)
def test_gang_locality_requirement_tpu_slices(cfgs, gangs, insts, exp):
    """STRICT_PACK on ray.io/tpu-slice-name for multi-host TPU slices."""
    assert _schedule_gangs(cfgs, gangs, insts) == exp


@pytest.mark.parametrize(
    "case",
    [
        "demands_spread_avail_crc_ippr",
        "idle_and_max_cap_subslice_protection",
        "repro2_idle_e_f_f2_g_h_b",
        "repro2_i_j_and_p2_2_exact_timestamp_release",
        "repro6_and_probe1_staggered_busy_shield",
        "pinned_rescheduling_head_and_queued_crc",
        "impossible_and_contradictory_pins",
    ],
)
def test_multihost_tpu_whole_replica_launch_and_termination(case):
    """KubeRay multi-host TPU worker groups launch and terminate whole slices."""
    head_cfgs = {"tpu_4x4": _tpu_config(resources=_HEAD_RES)}
    alloc_8 = [_pending_tpu(f"p{i}") for i in range(8)]

    if case == "demands_spread_avail_crc_ippr":
        capped = _sched(
            _CPU_AND_TPU,
            reqs=[_resource_request({"TPU": 4}, 5), _resource_request({"CPU": 8}, 2)],
            max_nodes=6,
        )
        assert _launch_and_terminate(capped) == ({"tpu_4x4": 4, "cpu": 2}, [])
        assert len(capped.infeasible_resource_requests) == 1
        sp = GangResourceRequest(
            requests=[_mk_req({"TPU": 4}, [(_ANTI, "sp", "1")])] * 4
        )
        assert _schedule_gangs(TPU_CONFIGS, [sp]) == ({"tpu_4x4": 4}, 0)
        assert _schedule_gangs(
            {"spot": _tpu_config("spot"), "od": _tpu_config("od")},
            [_tpu_gang()],
            avail={"spot": 0.0, "od": 1.0},
        ) == ({"od": 4}, 0)
        shared = _sched(
            TPU_CONFIGS,
            gangs=[_tpu_gang()],
            crc=[
                ClusterResourceConstraint(
                    resource_requests=[_resource_request({"TPU": 4})]
                )
            ],
        )
        assert _launch_and_terminate(shared) == ({"tpu_4x4": 4}, [])
        ippr = IPPRGroupSpec(
            min_cpu=4, max_cpu=8, min_memory=1000, max_memory=2000, resize_timeout=60
        )
        assert _launch_and_terminate(
            _sched(
                TPU_CONFIGS,
                gangs=[_tpu_gang(bundle={"CPU": 8, "TPU": 4})],
                ippr_specs=IPPRSpecs(groups={"tpu_4x4": ippr}),
            )
        ) == ({"tpu_4x4": 4}, [])

    elif case == "idle_and_max_cap_subslice_protection":
        s0_idle = [_tpu_host(f"s0-{w}", "slice-0", w, idle_ms=90_000) for w in range(4)]
        s1_sub = [
            _tpu_host(
                f"s1-{w}",
                "slice-1",
                w,
                {"TPU": 4} if w < 2 else None,
                0 if w < 2 else 90_000,
            )
            for w in range(4)
        ]
        assert {
            (t.instance_id, t.cause)
            for t in _sched(TPU_CONFIGS, s0_idle + s1_sub, idle_s=60.0).to_terminate
        } == {(f"s0-{w}", TerminationRequest.Cause.IDLE) for w in range(4)}
        assert {
            (t.instance_id, t.cause)
            for t in _sched(
                {"tpu_4x4": _tpu_config(max_workers=4)}, s0_idle + s1_sub
            ).to_terminate
        } == {
            (f"s0-{w}", TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE)
            for w in range(4)
        }
        g_cap = _sched(
            _CPU_AND_TPU,
            s0_idle
            + [_run_node("cpu", f"c{i}", {"CPU": 8}, {}, {"CPU": 8}) for i in range(4)],
            max_nodes=6,
        )
        assert {(t.instance_id, t.cause) for t in g_cap.to_terminate} == {
            (f"s0-{w}", TerminationRequest.Cause.MAX_NUM_NODES) for w in range(4)
        }
        assert {
            t.instance_id
            for t in _sched(
                {"tpu_4x4": _tpu_config(max_workers=4)}, s0_idle[:3] + _P4
            ).to_terminate
        } == {f"p{i}" for i in range(4)}
        m_cap = _sched(
            head_cfgs,
            [_pending_tpu(f"ri{i}", Instance.RAY_INSTALLING) for i in range(4)]
            + [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(4)],
            max_nodes=4,
        )
        assert {t.instance_id for t in m_cap.to_terminate} == {
            f"q{i}" for i in range(4)
        } and m_cap.cluster_resources["TPU-v6e-16-head"] == 1.0
        assert {
            t.instance_id
            for t in _sched(
                head_cfgs,
                [_pending_tpu(f"rq{i}", Instance.REQUESTED) for i in range(4)]
                + [_pending_tpu(f"al{i}", Instance.ALLOCATED) for i in range(4)],
                max_nodes=4,
            ).to_terminate
        } == {f"al{i}" for i in range(4)}
        assert {
            t.instance_id
            for t in _sched(
                {
                    "cpu": NodeTypeConfig(
                        name="cpu",
                        resources={"CPU": 2},
                        min_worker_nodes=0,
                        max_worker_nodes=1,
                    )
                },
                [
                    _pending_node("cpu", "rq", Instance.REQUESTED),
                    _pending_node("cpu", "al", Instance.ALLOCATED),
                ],
            ).to_terminate
        } == {"al"}

    elif case == "repro2_idle_e_f_f2_g_h_b":
        for s0_n, s1_n in ((4, 3), (3, 3), (3, 0)):
            insts = [
                _tpu_host(f"s0-{w}", "slice-0", w, idle_ms=90_000, head=(w == 0))
                for w in range(s0_n)
            ] + [
                _tpu_host(f"s1-{w}", "slice-1", w, {"TPU": 4}, head=(w == 0))
                for w in range(s1_n)
            ]
            assert {
                t.instance_id
                for t in _sched(head_cfgs, insts, idle_s=30.0).to_terminate
            } == {f"s0-{w}" for w in range(s0_n)}
        min4 = {"tpu_4x4": _tpu_config(min_workers=4)}
        two_s = [
            _tpu_host(f"s{s}-{w}", f"slice-{s}", w, idle_ms=60_000 + s)
            for s in (0, 1)
            for w in range(4)
        ]
        unlab_8 = [
            _run_node(
                "tpu_4x4",
                f"u{i}",
                {"CPU": 4, "TPU": 4},
                dict(TPU_LABELS),
                idle_ms=90_000,
            )
            for i in range(8)
        ]
        assert len(_sched(min4, two_s, idle_s=30.0).to_terminate) == 4
        assert len(_sched(min4, unlab_8, idle_s=30.0).to_terminate) == 4
        for min_w, exp_n in ((4, 4), (0, 8)):
            assert (
                len(
                    _sched(
                        {"tpu_4x4": _tpu_config(min_workers=min_w)},
                        alloc_8,
                        pending_idle_s=10.0,
                        unneeded={"tpu_4x4": [0.0] * 8},
                    ).to_terminate
                )
                == exp_n
            )

    elif case == "repro2_i_j_and_p2_2_exact_timestamp_release":
        q4 = [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(4)]
        rep_i = _sched(
            {"tpu_4x4": _tpu_config(min_workers=4)},
            [_tpu_host(f"s0-{w}", "slice-0", w, idle_ms=90_000) for w in range(4)] + q4,
            idle_s=10.0,
            pending_idle_s=10.0,
            unneeded={"tpu_4x4": [0.0] * 4},
        )
        assert {t.instance_id for t in rep_i.to_terminate} == {
            f"q{i}" for i in range(4)
        }

        cpu_min1 = {
            "cpu": NodeTypeConfig(
                name="cpu", resources={"CPU": 4}, min_worker_nodes=1, max_worker_nodes=4
            )
        }
        rep_cpu = _sched(
            cpu_min1,
            [
                _run_node("cpu", "c0", {"CPU": 4}, {}, idle_ms=90_000),
                _pending_tpu("cq0", Instance.QUEUED, "cpu"),
            ],
            idle_s=10.0,
            pending_idle_s=10.0,
            unneeded={"cpu": [0.0]},
        )
        assert [t.instance_id for t in rep_cpu.to_terminate] == ["cq0"]

        sj = ResourceDemandScheduler(event_logger)
        rep_j = _sched(
            TPU_CONFIGS,
            [_tpu_host(f"s0-{w}", "slice-0", w, idle_ms=90_000) for w in range(3)]
            + _P4,
            idle_s=10.0,
            pending_idle_s=10.0,
            unneeded={"tpu_4x4": [0.0] * 4},
            sched=sj,
        )
        assert {t.instance_id for t in rep_j.to_terminate} == {
            *(f"s0-{w}" for w in range(3)),
            *(f"p{i}" for i in range(4)),
        }
        assert sj._pending_unneeded_since_s["tpu_4x4"] == []

        # P2-2 RED/GREEN discriminator: two QUEUED replicas where the 2nd has older timestamps;
        # terminating the older replica removes `old_ts` (not `fresh_ts` via `.pop(0)`) across deepcopy!
        now = time.monotonic()
        old_ts, fresh_ts = now - 500.0, now - 2.0
        sp2 = ResourceDemandScheduler(event_logger)
        rep_p2 = _sched(
            TPU_CONFIGS,
            [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(8)],
            pending_idle_s=10.0,
            unneeded={"tpu_4x4": [fresh_ts] * 4 + [old_ts] * 4},
            sched=sp2,
        )
        assert len(rep_p2.to_terminate) == 4
        assert sp2._pending_unneeded_since_s["tpu_4x4"] == [fresh_ts] * 4

    elif case == "repro6_and_probe1_staggered_busy_shield":
        for max_w in (16, 4):
            rep6 = _sched(
                {"tpu_4x4": _tpu_config(max_workers=max_w, resources=_HEAD_RES)},
                [
                    _tpu_host("s0-0", "slice-0", 0, {"TPU": 4}, head=True),
                    _tpu_host("s0-1", "slice-0", 1, {"TPU": 4}),
                    *_P4[:2],
                ],
                idle_s=30.0,
                pending_idle_s=10.0,
                unneeded={"tpu_4x4": [0.0] * 2},
            )
            assert rep6.to_terminate == []
        probe1 = _sched(
            TPU_CONFIGS,
            [_tpu_host("s1-0", "slice-1", 0, {"TPU": 4})]
            + [_tpu_host(f"s2-{w}", "slice-2", w, idle_ms=5_000) for w in range(3)]
            + _P4,
            idle_s=30.0,
            pending_idle_s=10.0,
            unneeded={"tpu_4x4": [0.0] * 4},
        )
        assert _launch_and_terminate(probe1) == ({}, [])
        ambig = _sched(
            TPU_CONFIGS,
            alloc_8[:6]
            + [
                _tpu_host("s2-0", "slice-2", 0, idle_ms=75_000),
                _tpu_host("s2-1", "slice-2", 1, idle_ms=10_000),
            ],
            gangs=[_tpu_gang((0,))],
            idle_s=60.0,
            pending_idle_s=30.0,
            unneeded={"tpu_4x4": [0.0] * 6},
        )
        assert _launch_and_terminate(ambig) == ({}, [])

    elif case == "pinned_rescheduling_head_and_queued_crc":
        sibs = [_tpu_host(f"r0-{w}", "rep-0", w, idle_ms=90_000) for w in (1, 2, 3)]
        rw0 = _tpu_gang((0,), pinned="rep-0")
        pre = _sched(TPU_CONFIGS, sibs, gangs=[rw0], idle_s=60.0)
        assert (
            pre.to_terminate == [] and len(pre.infeasible_gang_resource_requests) == 1
        )
        post = _sched(
            TPU_CONFIGS,
            [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(4)]
            + sibs
            + [_pending_tpu("p0")],
            gangs=[rw0, _tpu_gang((0, 1))],
            idle_s=60.0,
            pending_idle_s=30.0,
            unneeded={"tpu_4x4": [0.0, 0.0]},
        )
        assert (
            _launch_and_terminate(post) == ({}, [])
            and not post.infeasible_gang_resource_requests
        )
        for min_w in (0, 4):
            hr = _sched(
                {"tpu_4x4": _tpu_config(min_workers=min_w, resources=_HEAD_RES)},
                reqs=[_resource_request({"TPU-v6e-16-head": 1}, 4)],
            )
            assert _launch_and_terminate(hr) == ({"tpu_4x4": 16}, [])
        ch = _mk_req({"CPU": 1, "TPU": 1, "TPU-v6e-16-head": 1})
        co = _sched(
            {"tpu_4x4": _tpu_config(max_workers=4, resources=_HEAD_RES)},
            gangs=[
                GangResourceRequest(
                    requests=[_mk_req({"CPU": 1, "TPU": 1}, [(_ANTI, "s1", "1")])] * 2
                ),
                _locality_gang([ch]),
            ],
        )
        assert not co.infeasible_gang_resource_requests and _launch_and_terminate(
            co
        ) == ({"tpu_4x4": 4}, [])
        q16 = [_pending_tpu(f"q-{i}", Instance.QUEUED) for i in range(16)]
        w0_sel = [[(_WID, _IN, ["0"])]]
        for reqs, crc, exp_t in (
            ([_resource_request({"TPU-v6e-16-head": 1}, 3)], [], 4),
            (
                [],
                [
                    ClusterResourceConstraint(
                        resource_requests=[_resource_request({"TPU-v6e-16-head": 1}, 3)]
                    )
                ],
                4,
            ),
            ([_resource_request({"TPU": 4}, 3, w0_sel)], [], 4),
            ([_resource_request({"TPU": 1}, 4, w0_sel)], [], 12),
        ):
            assert (
                len(
                    _sched(
                        head_cfgs,
                        q16,
                        reqs=reqs,
                        crc=crc,
                        pending_idle_s=10.0,
                        unneeded={"tpu_4x4": [0.0] * 16},
                    ).to_terminate
                )
                == exp_t
            )

    elif case == "impossible_and_contradictory_pins":
        s_full = [_tpu_host(f"sf-{w}", "s-full", w, idle_ms=90_000) for w in range(4)]
        for imp in (
            _locality_gang([_mk_req({"TPU": 999})], pinned="s-full"),
            GangResourceRequest(
                requests=[
                    _mk_req({"TPU": 999}, label_selectors=[[(_SLICE, _IN, ["s-full"])]])
                ]
            ),
        ):
            rep = _sched(TPU_CONFIGS, s_full, gangs=[imp], idle_s=60.0)
            assert len(rep.infeasible_gang_resource_requests) == 1
            assert {t.instance_id for t in rep.to_terminate} == {
                f"sf-{w}" for w in range(4)
            }
        excl = _locality_gang(
            [_mk_req({"TPU": 4}, label_selectors=[[(_SLICE, _IN, ["s-x"])]])],
            not_in=["s-x"],
        )
        assert (
            ResourceDemandScheduler._pinned_locality_domains([excl], TPU_CONFIGS) == {}
        )


@pytest.mark.parametrize(
    "case",
    [
        "slot_norm_and_wid_selectors",
        "staggered_subslice_and_cross_gang_tiers",
        "s1_s2_s3_progression_and_head_renorm",
        "s4_s5_fallback_capacity_and_provider_boundary",
    ],
)
def test_multihost_tpu_slot_normalization_and_invariants(case):
    """Multi-host TPU slot normalization, pool scoring, gang materialization, and
    provider boundaries preserve exact worker-id and head bijections."""
    if case == "slot_norm_and_wid_selectors":
        min8 = {
            "tpu_4x4": _tpu_config(min_workers=8, max_workers=24, resources=_HEAD_RES)
        }
        for cnt, exp_l, exp_h, cfgs in (
            (0, {"tpu_4x4": 4}, 2.0, min8),
            (5, {"tpu_4x4": 16}, 5.0, min8),
            (
                4,
                {"tpu_4x4": 12},
                4.0,
                {"tpu_4x4": _tpu_config(max_workers=24, resources=_HEAD_RES)},
            ),
        ):
            rep = _sched(
                cfgs,
                _P4,
                reqs=[_resource_request({"TPU-v6e-16-head": 1}, cnt)] if cnt else [],
            )
            assert (
                _launch_and_terminate(rep) == (exp_l, [])
                and rep.cluster_resources["TPU-v6e-16-head"] == exp_h
            )
        rb = _mk_req(
            {"TPU-v6e-16-head": 1},
            label_selectors=[[(_WID, _IN, ["0"]), (_POD, _IN, ["v6e-16"])]],
        )
        rr = _sched(
            {"tpu_4x4": _tpu_config(max_workers=8, resources=_HEAD_RES)},
            gangs=[GangResourceRequest(requests=[rb])],
        )
        assert (
            _launch_and_terminate(rr) == ({"tpu_4x4": 4}, [])
            and rr.cluster_resources["TPU-v6e-16-head"] == 1.0
        )
        sn = _sched(
            {"tpu_single": TPU_SINGLE},
            reqs=[_resource_request({"TPU": 4}, 2, [[(_WID, _NOT_IN, ["0"])]])],
        )
        assert (
            _launch_and_terminate(sn) == ({}, [])
            and len(sn.infeasible_resource_requests) == 2
        )
        for op, val, cnt, exp_l, exp_h in (
            (_IN, "2", 2, {"tpu_4x4": 8}, 2.0),
            (_NOT_IN, "0", 3, {"tpu_4x4": 4}, 1.0),
        ):
            rep = _sched(
                {"tpu_4x4": _tpu_config(max_workers=8, resources=_HEAD_RES)},
                reqs=[_resource_request({"TPU": 4}, cnt, [[(_WID, op, [val])]])],
            )
            assert (
                _launch_and_terminate(rep) == (exp_l, [])
                and rep.cluster_resources["TPU-v6e-16-head"] == exp_h
            )

    elif case == "staggered_subslice_and_cross_gang_tiers":
        stag = [
            _tpu_host(
                f"s{s}-{w}",
                f"slice-{s}",
                w,
                {"TPU": 4} if (s, w) not in ((0, 2), (2, 3)) else None,
                head=(w == 0),
            )
            for s, ws in ((0, (0, 1, 2)), (1, (1, 2)), (2, (1, 2, 3)))
            for w in ws
        ] + _P4
        assert _schedule_gangs(
            {"tpu_4x4": _tpu_config(max_workers=12)},
            [_tpu_gang((None, None))] * 3,
            stag,
        ) == ({}, 0)
        assert _schedule_gangs(TPU_CONFIGS, [_tpu_gang((None, None))] * 3, stag) == (
            {"tpu_4x4": 4},
            0,
        )
        w01 = [
            _tpu_host(f"s{s}-{w}", f"slice-{s}", w, {"TPU": 4}, head=(w == 0))
            for s in (0, 1)
            for w in (0, 1)
        ] + _P4
        assert _schedule_gangs(TPU_CONFIGS, [_tpu_gang((0, 1))], w01) == ({}, 0)

        sp_ni = GangResourceRequest(
            requests=[
                _mk_req(
                    {"TPU": 1},
                    [(_ANTI, "g1", "1")],
                    [
                        [(_WID, _NOT_IN, ["0"])],
                        [("zone", _IN, ["none"])],
                        [(_ACC, _IN, ["TPU-V6E"])],
                    ],
                )
            ]
            * 5
        )
        lc = _locality_gang(
            [
                _mk_req(
                    {"TPU": 2},
                    label_selectors=[
                        [(_WID, _IN, ["0"]), ("zone", _IN, ["ca"])],
                        [(_WID, _IN, ["0"]), ("zone", _IN, ["tx"])],
                    ],
                )
            ]
        )
        tx_ca = {
            "ca": _tpu_config(
                "ca", max_workers=8, labels={**TPU_LABELS, "zone": "ca"}, priority=10
            ),
            "tx": _tpu_config("tx", labels={**TPU_LABELS, "zone": "tx"}),
        }
        ctx = _mk_ctx(
            tx_ca,
            [_pending_node(nt, f"{nt}-{i}") for nt in ("tx", "ca") for i in range(4)],
            [sp_ni, lc],
        )
        assert not ResourceDemandScheduler._sched_gang_resource_requests(
            ctx, [sp_ni, lc]
        )
        assert [
            n.node_type
            for n in ctx.get_nodes()
            if any(
                r.resources_bundle.get("TPU") == 2
                for r in n.sched_requests[ResourceRequestSource.PENDING_DEMAND]
            )
        ] == ["ca"]

    elif case == "s1_s2_s3_progression_and_head_renorm":
        s1 = _sched(
            TPU_CONFIGS,
            [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(8)],
            reqs=[_resource_request({"TPU": 4}, 1, [[(_WID, _IN, ["0"])]])],
            pending_idle_s=1.0,
            unneeded={"tpu_4x4": [0.0] * 8},
        )
        assert len(s1.to_terminate) == 4 and not s1.infeasible_resource_requests
        s2 = _sched(
            {
                "tpu_4x4": _tpu_config(
                    max_workers=4, resources={**_HEAD_RES, "memory": 100}
                )
            },
            reqs=[
                _resource_request({"CPU": 4, "memory": 100}),
                _resource_request({"CPU": 1, "TPU-v6e-16-head": 1}),
            ],
        )
        assert not s2.infeasible_resource_requests and _launch_and_terminate(s2) == (
            {"tpu_4x4": 4},
            [],
        )

        s3_insts = (
            [_tpu_host(f"si-{w}", "s-idle", w) for w in (1, 2, 3)]
            + [
                _tpu_host(f"sb-{w}", "s-busy", w, {"TPU": 4}, head=w == 0)
                for w in (0, 1, 3)
            ]
            + [_pending_tpu(f"a{i}") for i in range(5)]
            + [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(4)]
        )
        ctx = _mk_ctx({"tpu_4x4": _tpu_config(resources=_HEAD_RES)}, s3_insts)
        nodes, inf = ResourceDemandScheduler._try_schedule_gang(
            ctx, _tpu_gang(not_in=["s-idle"])
        )
        assert (
            not inf
            and [
                n.im_instance_status
                for n in nodes
                if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            ]
            == [Instance.ALLOCATED] * 4
        )

        sa_g = _make_locality_gang_request(
            [{"CPU": 4, "TPU": 4}] + [{"CPU": 2, "TPU": 4}] * 3,
            {_POD: "v6e-16"},
            _SLICE,
            "s-A",
        )
        rep = _sched(
            {"tpu_4x4": _tpu_config(resources=_HEAD_RES)},
            [
                _tpu_host(
                    f"sa-{w}", "s-A", w, {"CPU": 2} if w == 0 else None, head=(w == 0)
                )
                for w in (0, 1, 2)
            ]
            + [_tpu_host(f"sb-{w}", "s-B", w) for w in (1, 2, 3)]
            + [
                _pending_tpu("pi", Instance.RAY_INSTALLING),
                _pending_tpu("pr", Instance.REQUESTED),
            ],
            gangs=[GangResourceRequest(requests=[_mk_req({"TPU": 4})]), sa_g],
            reqs=[_resource_request({"TPU-v6e-16-head": 1}, 2)],
        )
        assert (
            not rep.infeasible_gang_resource_requests
            and not rep.infeasible_resource_requests
            and rep.cluster_resources["TPU-v6e-16-head"] == 2.0
        )

        sub_g = GangResourceRequest(
            requests=[
                _mk_req(
                    {"TPU": 4},
                    label_selectors=[[(_SLICE, _IN, ["s-b"]), (_WID, _IN, ["3"])]],
                )
            ]
        )
        for busy in (True, False):
            sr = _sched(
                TPU_CONFIGS,
                [
                    _tpu_host(
                        f"sp-{w}",
                        "s-b",
                        w,
                        {"TPU": 4} if busy else None,
                        0 if busy else 90_000,
                    )
                    for w in range(3)
                ]
                + [_pending_tpu("boot")],
                gangs=[sub_g],
                idle_s=60.0,
            )
            assert (
                _launch_and_terminate(sr) == ({}, [])
                and not sr.infeasible_gang_resource_requests
            )

    elif case == "s4_s5_fallback_capacity_and_provider_boundary":
        h_cfgs = {"tpu_4x4": _tpu_config(resources=_HEAD_RES)}
        q8 = [_pending_tpu(f"q{i}", Instance.QUEUED) for i in range(8)]
        fb = _sched(
            h_cfgs,
            q8,
            reqs=[
                _resource_request(
                    {"TPU": 4},
                    2,
                    [[(_WID, _IN, ["0"])], [(_WID, _IN, ["0", "1", "2", "3"])]],
                )
            ],
            pending_idle_s=1.0,
            unneeded={"tpu_4x4": [0.0] * 8},
        )
        assert fb.to_terminate == [] and fb.cluster_resources["TPU-v6e-16-head"] == 2.0
        r4b = _sched(
            h_cfgs,
            [
                _tpu_host(
                    f"s1-{w}", "s-1", w, {"TPU": 4} if w == 0 else None, head=(w == 0)
                )
                for w in (0, 1, 2)
            ]
            + [_tpu_host("s2-0", "s-2", 0, {"TPU": 4}, head=True)]
            + q8,
            pending_idle_s=1.0,
            unneeded={"tpu_4x4": [0.0] * 8},
        )
        assert len(r4b.to_terminate) == 8 and r4b.cluster_resources == {
            "CPU": 16.0,
            "TPU": 16.0,
            "TPU-v6e-16-head": 2.0,
        }

        c2x4 = {
            "tpu_2x4": _tpu_config(
                "tpu_2x4", min_workers=6, max_workers=6, labels=TPU_SINGLE.labels
            )
        }
        r5a = _sched(
            c2x4,
            [
                _run_node(
                    "tpu_2x4",
                    f"{s}-{w}",
                    {"CPU": 4, "TPU": 4},
                    {**TPU_SINGLE.labels, _SLICE: s, _WID: str(w)},
                )
                for s, ws in (("s0", (1,)), ("s1", (1,)), ("s2", (0, 1)))
                for w in ws
            ],
            reqs=[_resource_request({"TPU": 4}, 4, [[(_WID, _NOT_IN, ["0"])]])],
        )
        assert (
            _launch_and_terminate(r5a) == ({}, [])
            and len(r5a.infeasible_resource_requests) == 1
        )

        nl_w0 = GangResourceRequest(
            requests=[
                _mk_req({"CPU": 4, "TPU": 4}, label_selectors=[[(_WID, _IN, ["0"])]])
            ]
        )
        r5b = _sched(
            {"tpu_4x4": _tpu_config(max_workers=8, resources=_HEAD_RES)},
            [
                _tpu_host("s1-0", "s-1", 0, {"TPU": 4}, head=True),
                _tpu_host("s1-1", "s-1", 1),
                _tpu_host("s2-3", "s-2", 3),
                _pending_tpu("ri0", Instance.RAY_INSTALLING),
                _pending_tpu("ri1", Instance.RAY_INSTALLING),
                _pending_tpu("a0"),
                _pending_tpu("rq0", Instance.REQUESTED),
                _pending_tpu("rq1", Instance.REQUESTED),
            ],
            gangs=[nl_w0, _tpu_gang((0,))],
        )
        assert (
            _launch_and_terminate(r5b) == ({}, [])
            and len(r5b.infeasible_gang_resource_requests) == 1
            and r5b.cluster_resources["TPU-v6e-16-head"] == 2.0
        )

        gcp = _sched(
            {
                "gcp": _tpu_config(
                    "gcp",
                    max_workers=1,
                    resources={"CPU": 8, "TPU": 4, "TPU-v4-16-head": 1},
                    labels={_TOPO: "2x2x2", _POD: "v4-16"},
                    workers_per_replica=None,
                )
            },
            reqs=[_resource_request({"TPU-v4-16-head": 1})],
        )
        assert not gcp.infeasible_resource_requests and _launch_and_terminate(gcp) == (
            {"gcp": 1},
            [],
        )


GPU_LABELS = {_ACC: "GB200"}


def _gpu_config(
    name: str, rack: Optional[str] = None, min_w: int = 0, max_w: int = 8, prio: int = 0
) -> NodeTypeConfig:
    lbl = {**GPU_LABELS, **({_GDOM: rack} if rack else {})}
    return NodeTypeConfig(
        name=name,
        resources={"CPU": 8, "GPU": 4},
        labels=lbl,
        min_worker_nodes=min_w,
        max_worker_nodes=max_w,
        priority=prio,
    )


def _gpu_gang(n: int = 2, pinned: Optional[str] = None) -> GangResourceRequest:
    return _make_locality_gang_request(
        [{"CPU": 1, "GPU": 4}] * n, GPU_LABELS, _GDOM, pinned
    )


def _gpu_node(
    nt: str, iid: str, rack: Optional[str] = None, used=None
) -> AutoscalerInstance:
    return _run_node(
        nt,
        iid,
        {"CPU": 8, "GPU": 4},
        {**GPU_LABELS, **({_GDOM: rack} if rack else {})},
        used,
    )


@pytest.mark.parametrize(
    "case",
    [
        "dyn_and_single_nt",
        "static_mixed_and_shared",
        "hetero_and_ippr",
        "fallback_tiers_and_pin",
    ],
)
def test_gang_locality_requirement_gpu_domains(case):
    """STRICT_PACK on ray.io/gpu-domain with dynamic and static per-rack domains."""
    dyn = {"gb200": _gpu_config("gb200")}
    racks = {"ra": _gpu_config("ra", "rack-a"), "rb": _gpu_config("rb", "rack-b")}
    half_g = _make_locality_gang_request([{"GPU": 2}] * 3, GPU_LABELS, _GDOM)

    if case == "dyn_and_single_nt":
        assert _schedule_gangs(
            dyn, [_gpu_gang(), _gpu_gang(1)], [_gpu_node("gb200", "u0")]
        ) == (
            {"gb200": 3},
            0,
        )
        frag = [
            _gpu_node("gb200", f"r{r}-{n}", f"rk-{r}", {"GPU": 4 * n})
            for r in (0, 1)
            for n in (0, 1)
        ]
        assert _schedule_gangs(dyn, [_gpu_gang()], frag) == ({"gb200": 2}, 0)
        assert _schedule_gangs(dyn, [half_g] * 2) == ({"gb200": 4}, 0)
        sp_od = {
            "sp": _gpu_config("sp", max_w=1, prio=10),
            "od": _gpu_config("od", max_w=4, prio=1),
        }
        assert _schedule_gangs(sp_od, [_gpu_gang()]) == ({"od": 2}, 0)
        assert _schedule_gangs(
            sp_od,
            [_gpu_gang()],
            [_gpu_node("od", "o0", "rk-0"), _pending_node("od", "o1")],
        ) == ({}, 0)

    elif case == "static_mixed_and_shared":
        rb0 = _gpu_node("rb", "rb0", "rack-b")
        assert _schedule_gangs(racks, [_gpu_gang()], [rb0]) == ({"rb": 1}, 0)
        assert _schedule_gangs(
            racks,
            [_make_locality_gang_request([{"CPU": 1, "GPU": 4}], {}, _GDOM, "rack-a")],
            [rb0],
        ) == ({"ra": 1}, 0)
        boot = [
            _gpu_node("gb200", "n0", "rk-0", {"CPU": 1}),
            _pending_node("gb200", "n1"),
        ]
        assert _schedule_gangs(
            {"gb200": _gpu_config("gb200"), "rb": _gpu_config("rb", "rack-b", min_w=1)},
            [_gpu_gang(), _gpu_gang(1, "rack-b")],
            boot,
        ) == ({"rb": 1}, 0)
        assert _schedule_gangs(
            dyn, [_gpu_gang()] * 2, boot + [_pending_node("gb200", "n2")]
        ) == (
            {"gb200": 1},
            0,
        )
        for mx in (2, 4):
            assert _schedule_gangs(
                {"g": _gpu_config("g", min_w=2, max_w=mx)}, [_gpu_gang()]
            ) == ({"g": 2}, 0)
        sh = {"a": _gpu_config("a", "sh"), "b": _gpu_config("b", "sh")}
        sn = [
            _gpu_node("a", "a0", "sh"),
            _gpu_node("b", "b0", "sh"),
            _pending_node("a", "ap"),
            _pending_node("b", "bp"),
        ]
        assert _schedule_gangs(sh, [_gpu_gang(4, "sh")], sn) == ({}, 0)

    elif case == "hetero_and_ippr":
        het = {
            "g2": NodeTypeConfig(
                name="g2",
                resources={"CPU": 8, "GPU": 2},
                labels=GPU_LABELS,
                min_worker_nodes=0,
                max_worker_nodes=5,
                priority=10,
            ),
            "g4": NodeTypeConfig(
                name="g4",
                resources={"CPU": 8, "GPU": 4},
                labels=GPU_LABELS,
                min_worker_nodes=0,
                max_worker_nodes=2,
                priority=5,
            ),
        }
        hg = [
            _make_locality_gang_request([{"CPU": 2, "GPU": 2}] * n, GPU_LABELS, _GDOM)
            for n in (2, 3, 4)
        ]
        assert _schedule_gangs(het, hg) == ({"g2": 5, "g4": 2}, 0)
        assert _schedule_gangs(
            het,
            hg,
            [_pending_node("g2", f"a{i}") for i in range(5)]
            + [_pending_node("g4", f"b{i}") for i in range(2)],
        ) == ({}, 0)
        spec = IPPRGroupSpec(
            min_cpu=8, max_cpu=16, min_memory=1000, max_memory=2000, resize_timeout=60
        )
        ir = _sched(
            dyn,
            [
                make_autoscaler_instance(
                    im_instance=Instance(
                        instance_type="gb200",
                        status=Instance.ALLOCATED,
                        instance_id=f"p{i}",
                    ),
                    cloud_instance_id=f"pod-{i}",
                )
                for i in (0, 1)
            ],
            gangs=[half_g] * 2,
            ippr_specs=IPPRSpecs(groups={"gb200": spec}),
            ippr_statuses={
                f"pod-{i}": IPPRStatus(
                    cloud_instance_id=f"pod-{i}",
                    spec=spec,
                    current_cpu=8,
                    current_memory=1000,
                    desired_cpu=8,
                    desired_memory=1000,
                )
                for i in (0, 1)
            },
        )
        assert (
            _launch_and_terminate(ir)[0] == {"gb200": 2}
            and not ir.infeasible_gang_resource_requests
        )

    elif case == "fallback_tiers_and_pin":
        bad = _gpu_gang(pinned="rack-a")
        bad.bundle_selectors[
            0
        ].locality_requirement.label_selector.label_constraints.append(
            LabelSelectorConstraint(
                label_key=_GDOM, operator=_IN, label_values=["rack-b"]
            )
        )
        assert _schedule_gangs(racks, [bad], [_gpu_node("rb", "rb0", "rack-b")]) == (
            {},
            1,
        )
        cg = _locality_gang(
            [
                _mk_req(
                    {"GPU": 4},
                    label_selectors=[[("zone", _IN, ["ca"])], [("zone", _IN, ["tx"])]],
                )
            ]
            * 2,
            _GDOM,
        )
        zr = {
            "tx": NodeTypeConfig(
                name="tx",
                resources={"GPU": 4},
                labels={"zone": "tx", _GDOM: "ra"},
                min_worker_nodes=0,
                max_worker_nodes=4,
                priority=10,
            ),
            "ca": NodeTypeConfig(
                name="ca",
                resources={"GPU": 4},
                labels={"zone": "ca", _GDOM: "rz"},
                min_worker_nodes=0,
                max_worker_nodes=4,
            ),
        }
        tx_r = [
            _run_node("tx", f"t{i}", {"GPU": 4}, zr["tx"].labels, idle_ms=90_000)
            for i in (0, 1)
        ]
        assert _launch_and_terminate(
            _sched(
                zr,
                tx_r + [_run_node("ca", "c0", {"GPU": 4}, zr["ca"].labels)],
                gangs=[cg],
                idle_s=60.0,
            )
        ) == ({}, [])
        c2 = _sched(
            zr,
            tx_r
            + [_run_node("ca", f"c{i}", {"GPU": 4}, zr["ca"].labels) for i in (0, 1)],
            gangs=[cg],
            idle_s=60.0,
        )
        assert {t.instance_id for t in c2.to_terminate} == {"t0", "t1"}


@pytest.mark.parametrize(
    "case", ["divisibility_and_bounds", "npu_tier_vs_running", "repro1_gpu_lws"]
)
def test_generic_worker_replica_config_and_locality_invariants(case):
    """Generic WorkerReplicaConfig invariants: divisibility validation, disjoint slot
    bounds, running-vs-booting tier preference, and domainless GPU LWS groups."""
    if case == "divisibility_and_bounds":
        for min_w, max_w in ((2, 8), (0, 6)):
            with pytest.raises(ValueError, match="workers_per_replica"):
                NodeTypeConfig(
                    name="x",
                    resources={"CPU": 4},
                    min_worker_nodes=min_w,
                    max_worker_nodes=max_w,
                    replica_config=WorkerReplicaConfig(4),
                )
        assert (
            ResourceDemandScheduler._max_bundles_per_node(
                [
                    _mk_req({"CPU": 1}, label_selectors=[[("k", _IN, [s])]])
                    for s in "aabc"
                ]
            )
            == 2
        )
        assert (
            ResourceDemandScheduler._max_bundles_per_node(
                [
                    _mk_req(
                        {"CPU": 1},
                        label_selectors=[[("k", _IN, [s])], [("k", _IN, ["x"])]],
                    )
                    for s in "ab"
                ]
            )
            == 2
        )
        assert (
            ResourceDemandScheduler._max_bundles_per_node(
                [
                    _mk_req({"CPU": 1}, [(_ANTI, "s", "1")] if i < 3 else [])
                    for i in range(5)
                ]
            )
            == 3
        )

    elif case == "npu_tier_vs_running":
        inc = {
            "cpu": NodeTypeConfig(
                name="cpu",
                resources={"CPU": 8},
                labels={"r": "0"},
                min_worker_nodes=0,
                max_worker_nodes=4,
            ),
            "gd": _gpu_config("gd"),
        }
        assert _schedule_gangs(
            inc,
            [_gpu_gang()],
            [
                _run_node("cpu", "c0", {"CPU": 8}, {_GDOM: "r0"}),
                _pending_node("gd", "gp"),
            ],
        ) == ({"gd": 1}, 0)
        rcfg = NodeTypeConfig(
            name="r2",
            resources={"CPU": 4, "NPU": 2, "ldr": 1},
            labels={"zone": "tx"},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(2, "v/rack", "v/slot", ("ldr",)),
        )
        insts = [
            _run_node(
                "r2",
                "r-0",
                {"CPU": 4, "NPU": 2, "ldr": 1},
                {"zone": "fb", "v/rack": "rk0", "v/slot": "0"},
            ),
            _pending_node("r2", "p-1"),
        ]
        tr = _mk_req(
            {"CPU": 1, "NPU": 2},
            label_selectors=[[("zone", _IN, ["tx"])], [("zone", _IN, ["fb"])]],
        )
        for g, attr, exp in (
            (
                _make_locality_gang_request([{"CPU": 1, "NPU": 2}], {}, "v/rack"),
                "ray_node_id",
                "r-0",
            ),
            (_locality_gang([tr], "v/rack"), "im_instance_id", "p-1"),
        ):
            ctx = _mk_ctx({"r2": rcfg}, insts)
            placed, inf = ResourceDemandScheduler._try_schedule_gang(ctx, g)
            assert not inf and [
                getattr(n, attr)
                for n in placed
                if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            ] == [exp]

    elif case == "repro1_gpu_lws":

        def _lws(sz=4, mn=0, mx=12):
            return {
                "lws": NodeTypeConfig(
                    name="lws",
                    resources={"CPU": 8, "GPU": 8},
                    labels=GPU_LABELS,
                    min_worker_nodes=mn,
                    max_worker_nodes=mx,
                    replica_config=WorkerReplicaConfig(sz),
                )
            }

        def _lp(iid, busy=False):
            return _run_node(
                "lws",
                iid,
                {"CPU": 8, "GPU": 8},
                dict(GPU_LABELS),
                {"GPU": 8} if busy else None,
                0 if busy else 90_000,
            )

        assert _launch_and_terminate(_sched(_lws(), gangs=[_gpu_gang(2)])) == (
            {"lws": 4},
            [],
        )
        da = [_lp("g0", True), _lp("g1", True)] + [
            _pending_node("lws", f"a{i}") for i in range(4)
        ]
        assert _launch_and_terminate(_sched(_lws(mn=4), da[:2])) == ({}, [])
        assert _launch_and_terminate(_sched(_lws(mn=8), da)) == ({}, [])
        assert _launch_and_terminate(_sched(_lws(mn=12), da)) == ({"lws": 4}, [])

        b1_i3 = [_lp("A"), _lp("B"), _lp("C", True), _lp("D")]
        assert _sched(_lws(2, 0, 8), b1_i3, idle_s=10.0).to_terminate == []
        assert _sched(_lws(2, 2, 8), b1_i3, idle_s=10.0).to_terminate == []
        i4 = [_lp(c) for c in "ABCD"]
        assert len(_sched(_lws(2, 2, 8), i4, idle_s=10.0).to_terminate) == 2
        assert len(_sched(_lws(2, 0, 8), i4, idle_s=10.0).to_terminate) == 4
        c_none = {
            "lws": NodeTypeConfig(
                name="lws",
                resources={"CPU": 8, "GPU": 8},
                labels=GPU_LABELS,
                min_worker_nodes=0,
                max_worker_nodes=8,
            )
        }
        assert {
            t.instance_id for t in _sched(c_none, b1_i3, idle_s=10.0).to_terminate
        } == {"A", "B", "D"}
        rq = _sched(
            _lws(4, 4),
            [_lp(f"g{i}") for i in range(4)]
            + [_pending_tpu(f"q{i}", Instance.QUEUED, "lws") for i in range(4)],
            idle_s=60.0,
            pending_idle_s=10.0,
            unneeded={"lws": [0.0] * 4},
        )
        assert {t.instance_id for t in rq.to_terminate} == {f"q{i}" for i in range(4)}

        # Provider replica_id (cloud_instance_replica_id): domainless LWS scales down
        # idle replica rep-0 while rep-1 stays busy, and reshuffled booting slices
        # stay grouped as G1/G2 under max_workers + pending_idle_timeout_s.
        def _with_rep(inst, rep):
            inst.cloud_instance_id = (
                inst.cloud_instance_id or f"c-{inst.im_instance.instance_id}"
            )
            inst.cloud_instance_replica_id = rep
            return inst

        lws_labeled = [
            _with_rep(_lp("A"), "rep-0"),
            _with_rep(_lp("B"), "rep-0"),
            _with_rep(_lp("C", True), "rep-1"),
            _with_rep(_lp("D"), "rep-1"),
        ]
        assert {
            t.instance_id
            for t in _sched(_lws(2, 2, 8), lws_labeled, idle_s=10.0).to_terminate
        } == {"A", "B"}
        reshuf = [
            _with_rep(
                _pending_tpu(
                    f"p{i}",
                    Instance.RAY_INSTALLING if i in (1, 6) else Instance.ALLOCATED,
                ),
                "G1" if i < 4 else "G2",
            )
            for i in range(8)
        ]
        g1, g2 = {f"p{i}" for i in range(4)}, {f"p{i}" for i in range(4, 8)}
        assert {
            t.instance_id
            for t in _sched(
                {"tpu_4x4": _tpu_config(max_workers=4)}, reshuf
            ).to_terminate
        } in (g1, g2)
        now = time.monotonic()
        assert {
            t.instance_id
            for t in _sched(
                TPU_CONFIGS,
                reshuf,
                pending_idle_s=30.0,
                unneeded={"tpu_4x4": [now - 100.0] * 4 + [now] * 4},
            ).to_terminate
        } in (g1, g2)
        half_req = reshuf[:2] + [
            _pending_tpu("r2", Instance.REQUESTED),
            _pending_tpu("r3", Instance.REQUESTED),
        ]
        assert _launch_and_terminate(
            _sched(
                {"tpu_4x4": _tpu_config(max_workers=8)},
                half_req,
                gangs=[_tpu_gang((0, 1, 2, 3))],
            )
        ) == ({}, [])


@pytest.mark.skipif(sys.platform == "win32", reason="KubeRay config skipped on Windows")
def test_kuberay_cr_to_scheduler_and_reconciler_full_stack_seam():
    """Unmocked seam test: `get_basic_ray_cr` -> `_derive_autoscaling_config_from_ray_cr`
    -> `AutoscalingConfig` -> `ResourceDemandScheduler` -> `Reconciler`."""
    from collections import Counter, defaultdict

    from ray.autoscaler._private.kuberay.autoscaling_config import (
        _derive_autoscaling_config_from_ray_cr,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
    from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
    from ray.autoscaler.v2.instance_manager.node_provider import CloudInstance
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
    from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
    from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
        CloudResourceMonitor,
    )
    from ray.core.generated.autoscaler_pb2 import ClusterResourceState
    from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

    ray_cr = get_basic_ray_cr()
    ray_cr["metadata"]["namespace"] = "default"
    ray_cr["spec"]["headGroupSpec"]["rayStartParams"] = {"num-cpus": "0"}
    base = ray_cr["spec"]["workerGroupSpecs"][0]
    gke_acc = "cloud.google.com/gke-tpu-accelerator"
    gke_topo = "cloud.google.com/gke-tpu-topology"

    def _wg(name, hosts, max_r, lim, sel=None, ann=None):
        wg = copy.deepcopy(base)
        wg.update(groupName=name, replicas=0, minReplicas=0, maxReplicas=max_r)
        wg.update(numOfHosts=hosts, rayStartParams={})
        wg["template"].setdefault("metadata", {})["annotations"] = ann or {}
        wg["template"]["spec"]["nodeSelector"] = sel or {}
        wg["template"]["spec"]["containers"][0]["resources"] = {
            "limits": lim,
            "requests": lim,
        }
        return wg

    tpu_lim = {"cpu": "8", "memory": "16Gi", "google.com/tpu": "4"}
    v4_lim = {"cpu": "4", "memory": "8Gi", "google.com/tpu": "4"}
    gpu_lim = {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "8"}
    ds_ann = {"cloud.google.com/gke-tpu-slice-topology": "2x2x2"}
    ray_cr["spec"]["workerGroupSpecs"] = [
        _wg("v6e-16", 4, 3, tpu_lim, {gke_acc: "tpu-v6e-slice", gke_topo: "4x4"}),
        _wg("tpu7x-ds", 2, 2, tpu_lim, {gke_acc: "tpu7x"}, ds_ann),
        _wg("v6e-4", 1, 2, v4_lim, {gke_acc: "tpu-v6e-slice", gke_topo: "2x2"}),
        _wg("gpu-lws", 4, 2, gpu_lim),
    ]
    acfg = AutoscalingConfig(
        configs=_derive_autoscaling_config_from_ray_cr(ray_cr), skip_content_hash=True
    )
    sh_sel = [[(_TOPO, _IN, ["2x2"]), (_ACC, _IN, ["TPU-V6E"]), (_WID, _IN, ["0"])]]
    sh_gang = GangResourceRequest(
        requests=[_mk_req({"CPU": 1, "TPU": 4}, [(_AFF, "sh", "")], sh_sel)]
    )
    gangs = [
        _tpu_gang((0, 1, 2, 3), pod_type="v6e-16"),
        _tpu_gang((0, 1), pod_type="v7x-16"),
        sh_gang,
    ]
    tasks = [
        _resource_request({"TPU-v6e-16-head": 1}, 2),
        _resource_request({"GPU": 8}),
    ]

    storage = InstanceStorage(cluster_id="seam", storage=InMemoryStorage())
    im = InstanceManager(
        instance_storage=storage, instance_status_update_subscribers=[]
    )
    hnid = b"h".ljust(28, b"\0")
    head_im = Instance(
        instance_id="h0",
        instance_type="headgroup",
        status=Instance.RAY_RUNNING,
        node_kind=NodeKind.HEAD,
        node_id=hnid.hex(),
        cloud_instance_id="hpod",
        version=0,
    )
    assert storage.batch_upsert_instances([head_im], expected_storage_version=0)[0]
    hns = NodeState(
        node_id=hnid,
        instance_id="hpod",
        ray_node_type_name="headgroup",
        status=NodeStatus.RUNNING,
    )
    cloud = {"hpod": CloudInstance("hpod", "headgroup", NodeKind.HEAD, True)}
    p_idx = 0

    def _tick(ns_list, demand=True):
        crs = ClusterResourceState(
            node_states=ns_list,
            pending_gang_resource_requests=gangs if demand else [],
            pending_resource_requests=tasks if demand else [],
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=ResourceDemandScheduler(),
            cloud_provider=None,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=crs,
            non_terminated_cloud_instances=cloud,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=acfg,
        )
        return storage.get_instances()[0]

    def _cnt(insts, st):
        return dict(
            Counter(
                i.instance_type
                for i in insts.values()
                if i.node_kind != NodeKind.HEAD and i.status == st
            )
        )

    def _alloc(insts):
        nonlocal p_idx
        for i in insts.values():
            if i.status == Instance.REQUESTED:
                p_idx += 1
                cid = f"{i.instance_type}-p{p_idx}"
                cloud[cid] = CloudInstance(cid, i.instance_type, NodeKind.WORKER, True)

    i1 = _tick([hns])
    assert _cnt(i1, Instance.REQUESTED) == {"v6e-16": 8, "tpu7x-ds": 2}
    assert _cnt(i1, Instance.QUEUED) == {"gpu-lws": 4, "v6e-4": 1}
    _alloc(i1)
    i2 = _tick([hns])
    assert _cnt(i2, Instance.ALLOCATED) == {"v6e-16": 8, "tpu7x-ds": 2}
    assert _cnt(i2, Instance.REQUESTED) == {"gpu-lws": 4, "v6e-4": 1}
    _alloc(i2)
    i3 = _tick([hns])
    cids: Dict[str, List[str]] = defaultdict(list)
    for i in sorted(i3.values(), key=lambda x: x.cloud_instance_id):
        if i.node_kind != NodeKind.HEAD and i.status == Instance.ALLOCATED:
            cids[i.instance_type].append(i.cloud_instance_id)

    nt_cfgs = acfg.get_node_type_configs()
    ns_all, seq = [hns], 0

    def _reg(nt, cid, res, busy, s_name=None, w=0):
        nonlocal seq
        seq += 1
        lbls = {**nt_cfgs[nt].labels, _SLICE: s_name, _WID: str(w)} if s_name else {}
        st = NodeStatus.RUNNING if busy else NodeStatus.IDLE
        av = {**res, **({busy: 0.0} if busy else {})}
        ns_all.append(
            NodeState(
                node_id=f"w{seq}".encode().ljust(28, b"\0"),
                instance_id=cid,
                ray_node_type_name=nt,
                status=st,
                idle_duration_ms=0 if busy else 600_000,
                total_resources=res,
                available_resources=av,
                labels=lbls,
            )
        )

    m16, m8 = float(16 * 1024**3), float(8 * 1024**3)
    for s in range(2):
        for w in range(4):
            h = {"TPU-v6e-16-head": 1.0} if w == 0 else {}
            b = "TPU" if (s == 0 and w == 3) else None
            _reg(
                "v6e-16",
                cids["v6e-16"][s * 4 + w],
                {"CPU": 8.0, "TPU": 4.0, "memory": m16, **h},
                b,
                f"vs-{s}",
                w,
            )
    for w, cid in enumerate(cids["tpu7x-ds"]):
        h = {"TPU-v7x-16-head": 1.0} if w == 0 else {}
        _reg(
            "tpu7x-ds",
            cid,
            {"CPU": 8.0, "TPU": 4.0, "memory": m16, **h},
            "TPU",
            "ds-0",
            w,
        )
    for cid in cids["v6e-4"]:
        _reg(
            "v6e-4",
            cid,
            {"CPU": 4.0, "TPU": 4.0, "TPU-v6e-4-head": 1.0, "memory": m8},
            "TPU",
            "v4-0",
            0,
        )
    for cid in cids["gpu-lws"]:
        _reg("gpu-lws", cid, {"CPU": 8.0, "GPU": 8.0, "memory": m16}, None)

    i4 = _tick(ns_all, demand=False)
    stopped = {
        i.cloud_instance_id
        for i in i4.values()
        if i.status == Instance.RAY_STOP_REQUESTED
    }
    assert stopped == set(cids["v6e-16"][4:]) | set(cids["gpu-lws"])
    assert _cnt(i4, Instance.RAY_RUNNING) == {"v6e-16": 4, "tpu7x-ds": 2, "v6e-4": 1}


class TestPrecomputeSerializeKeys:
    """Tests for precomputed shape_keys optimization in try_schedule."""

    def test_precomputed_keys_produce_same_result_as_fallback(self):
        """try_schedule with precomputed shape_keys gives identical results to fallback."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 4},
                min_worker_nodes=0,
                max_worker_nodes=10,
            ),
        }

        node = SchedulingNode.from_node_config(
            node_config=node_type_configs["type_1"],
            status=SchedulingNodeStatus.SCHEDULABLE,
            node_kind=NodeKind.WORKER,
        )
        requests = [ResourceRequestUtil.make({"CPU": 1}) for _ in range(6)]

        # Run without shape_keys (fallback path).
        import copy

        node_copy1 = copy.deepcopy(node)
        remaining1, score1 = node_copy1.try_schedule(
            requests, ResourceRequestSource.PENDING_DEMAND
        )

        # Run with precomputed shape_keys.
        shape_keys = {id(r): r.SerializeToString(deterministic=True) for r in requests}
        node_copy2 = copy.deepcopy(node)
        remaining2, score2 = node_copy2.try_schedule(
            requests, ResourceRequestSource.PENDING_DEMAND, shape_keys
        )

        assert len(remaining1) == len(remaining2)
        assert score1 == score2
        # 4 CPU node fits 4 requests, 2 remain.
        assert len(remaining1) == 2

    def test_precomputed_keys_fallback_on_missing_id(self):
        """If a request id is not in shape_keys, fallback to SerializeToString."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 2},
                min_worker_nodes=0,
                max_worker_nodes=10,
            ),
        }

        node = SchedulingNode.from_node_config(
            node_config=node_type_configs["type_1"],
            status=SchedulingNodeStatus.SCHEDULABLE,
            node_kind=NodeKind.WORKER,
        )
        requests = [ResourceRequestUtil.make({"CPU": 1}) for _ in range(3)]

        # Intentionally pass an empty shape_keys dict — all ids will miss.
        shape_keys = {}
        import copy

        node_copy = copy.deepcopy(node)
        remaining, score = node_copy.try_schedule(
            requests, ResourceRequestSource.PENDING_DEMAND, shape_keys
        )

        # Should still work via fallback: 2 CPU fits 2 requests, 1 remains.
        assert len(remaining) == 1

    def test_unschedulable_request_cache_bytes_interface(self):
        """UnschedulableRequestCache works correctly with raw bytes keys."""
        cache = UnschedulableRequestCache()

        key_a = b"shape_a"
        key_b = b"shape_b"

        assert not cache.contains(key_a)
        assert not cache.contains(key_b)

        cache.add(key_a)
        assert cache.contains(key_a)
        assert not cache.contains(key_b)

        cache.add(key_b)
        assert cache.contains(key_a)
        assert cache.contains(key_b)

        cache.clear()
        assert not cache.contains(key_a)
        assert not cache.contains(key_b)

    def test_precomputed_keys_with_duplicate_objects(self):
        """Duplicate request objects (same id) work correctly with shape_keys."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 2},
                min_worker_nodes=0,
                max_worker_nodes=10,
            ),
        }

        node = SchedulingNode.from_node_config(
            node_config=node_type_configs["type_1"],
            status=SchedulingNodeStatus.SCHEDULABLE,
            node_kind=NodeKind.WORKER,
        )

        # Simulate ungroup_by_count: same object repeated multiple times.
        single_request = ResourceRequestUtil.make({"CPU": 1})
        requests = [single_request] * 5  # All have the same id()

        shape_keys = {
            id(single_request): single_request.SerializeToString(deterministic=True)
        }

        import copy

        node_copy = copy.deepcopy(node)
        remaining, score = node_copy.try_schedule(
            requests, ResourceRequestSource.PENDING_DEMAND, shape_keys
        )

        # 2 CPU node fits 2 requests, 3 remain.
        assert len(remaining) == 3

    def test_precomputed_keys_end_to_end_scheduling(self):
        """Full scheduling round with precomputed keys produces correct launch decisions."""
        node_type_configs = {
            "type_1": NodeTypeConfig(
                name="type_1",
                resources={"CPU": 4},
                min_worker_nodes=0,
                max_worker_nodes=100,
            ),
        }
        # One existing node with 2 CPU available.
        instances = [
            make_autoscaler_instance(
                ray_node=NodeState(
                    ray_node_type_name="type_1",
                    available_resources={"CPU": 2},
                    total_resources={"CPU": 4},
                    node_id=b"r1",
                ),
                im_instance=Instance(
                    instance_type="type_1",
                    status=Instance.RAY_RUNNING,
                    instance_id="1",
                    node_id="r1",
                ),
                cloud_instance_id="c-1",
            ),
        ]

        # 10 requests × 1 CPU each. Existing node fits 2, need 2 new nodes for 8.
        resource_requests = [ResourceRequestUtil.make({"CPU": 1})] * 10

        request = sched_request(
            node_type_configs=node_type_configs,
            resource_requests=resource_requests,
            instances=instances,
        )
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        to_launch, _ = _launch_and_terminate(reply)
        # Existing node handles 2, remaining 8 need 8/4 = 2 new nodes.
        assert to_launch == {"type_1": 2}


_A_THEN_B = [
    [("accel", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A"])],
    [("accel", LabelSelectorOperator.LABEL_OPERATOR_IN, ["B"])],
]
_B_THEN_A = _A_THEN_B[::-1]


def _accel_node_types(cpu: int, max_type_b: int = 10) -> Dict[NodeType, NodeTypeConfig]:
    return {
        f"type_{accel.lower()}": NodeTypeConfig(
            name=f"type_{accel.lower()}",
            resources={"CPU": cpu},
            min_worker_nodes=0,
            max_worker_nodes=max_type_b if accel == "B" else 10,
            labels={"accel": accel},
        )
        for accel in ("A", "B")
    }


@pytest.mark.parametrize(
    "node_cpu,max_type_b,max_num_nodes,request_cpus,expected_launch",
    [
        # One 2-CPU node fits both requests via fallback, but each request still
        # gets its primary node type.
        (2, 10, None, [1], {"type_a": 1, "type_b": 1}),
        # type_b unavailable: fall back and bin-pack onto the launched type_a node.
        (2, 0, None, [1], {"type_a": 1}),
        # max_num_nodes reached: same fallback onto the launched type_a node.
        (2, 10, 1, [1], {"type_a": 1}),
        # Interleaved sizes: smaller requests pack onto their primary launched node.
        (4, 10, None, [2, 1], {"type_a": 1, "type_b": 1}),
    ],
)
def test_label_selector_fallback_new_nodes(
    node_cpu, max_type_b, max_num_nodes, request_cpus, expected_launch
):
    requests = [
        ResourceRequestUtil.make({"CPU": cpu}, label_selectors=selectors)
        for cpu in request_cpus
        for selectors in (_A_THEN_B, _B_THEN_A)
    ]
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(
            node_type_configs=_accel_node_types(node_cpu, max_type_b),
            resource_requests=requests,
            max_num_nodes=max_num_nodes,
        )
    )
    assert _launch_and_terminate(reply)[0] == expected_launch
    assert not reply.infeasible_resource_requests


def test_label_selector_fallback_existing_nodes():
    """Each request lands on its running primary node, so neither node is idle."""
    instances = [
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name=f"type_{accel.lower()}",
                available_resources={"CPU": 2},
                total_resources={"CPU": 2},
                labels={"accel": accel},
                idle_duration_ms=10,
                node_id=f"r-{accel}".encode(),
            ),
            im_instance=Instance(
                instance_type=f"type_{accel.lower()}",
                status=Instance.RAY_RUNNING,
                instance_id=f"i-{accel}",
                node_id=f"r-{accel}",
            ),
            cloud_instance_id=f"c-{accel}",
        )
        for accel in ("A", "B")
    ]
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(
            node_type_configs=_accel_node_types(2),
            resource_requests=[
                ResourceRequestUtil.make({"CPU": 1}, label_selectors=s)
                for s in (_A_THEN_B, _B_THEN_A)
            ],
            instances=instances,
            idle_timeout_s=0,
        )
    )
    assert _launch_and_terminate(reply) == ({}, [])
    assert not reply.infeasible_resource_requests


def test_ippr_resized_node_hosts_deferred_requests():
    """A pod resized in place for a primary-selector request stays an IPPR
    candidate, so requests it deferred in that pass reuse it instead of
    launching a new node."""
    spec = IPPRGroupSpec(
        min_cpu=1,
        max_cpu=4,
        min_memory=1024**3,
        max_memory=8 * 1024**3,
        resize_timeout=60,
    )
    node_type_configs = {
        accel: NodeTypeConfig(
            name=accel,
            resources={"CPU": 1, **({"special": 2} if accel == "A" else {})},
            min_worker_nodes=0,
            max_worker_nodes=10,
            labels={"accel": accel},
        )
        for accel in ("A", "B")
    }
    instances = [
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name=accel,
                available_resources={"CPU": 0, **extra},
                total_resources={"CPU": 1, **extra},
                labels={"accel": accel},
                node_id=f"r-{accel}".encode(),
            ),
            im_instance=Instance(
                instance_type=accel,
                status=Instance.RAY_RUNNING,
                instance_id=f"i-{accel}",
                node_id=f"r-{accel}",
            ),
            cloud_instance_id=f"pod-{accel}",
        )
        for accel, extra in (("A", {"special": 2}), ("B", {}))
    ]
    in_op = LabelSelectorOperator.LABEL_OPERATOR_IN
    selector = lambda accel: [("accel", in_op, [accel])]  # noqa: E731
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(
            node_type_configs=node_type_configs,
            resource_requests=[
                # pod-A wins the first pass (2 matching resource types) and defers
                # the fallback request and the unlabeled request.
                ResourceRequestUtil.make(
                    {"CPU": 2, "special": 1}, label_selectors=[selector("A")]
                ),
                # Lands on its primary pod-B in the second pass.
                ResourceRequestUtil.make(
                    {"CPU": 1}, label_selectors=[selector("B"), selector("A")]
                ),
                # Only pod-A has `special`, so it needs pod-A to stay a candidate.
                ResourceRequestUtil.make({"CPU": 1, "special": 1}),
            ],
            instances=instances,
            ippr_specs=IPPRSpecs(groups={"A": spec, "B": spec}),
            ippr_statuses={
                f"pod-{accel}": IPPRStatus(
                    cloud_instance_id=f"pod-{accel}",
                    spec=spec,
                    current_cpu=1,
                    current_memory=1024**3,
                    desired_cpu=1,
                    desired_memory=1024**3,
                )
                for accel in ("A", "B")
            },
        )
    )
    assert sorted((s.cloud_instance_id, s.desired_cpu) for s in reply.to_ippr) == [
        ("pod-A", 4),
        ("pod-B", 4),
    ]
    assert reply.to_launch == []
    assert not reply.infeasible_resource_requests


@pytest.mark.parametrize("max_num_nodes", [1, 2, None])
@pytest.mark.parametrize(
    "l_labels,r2_selectors",
    [
        # Cross-selector bin-packing: R1 requires x=1 and R2 requires y=1, both
        # satisfied at primary priority by node type L.
        (
            {"x": "1", "y": "1"},
            [[("y", LabelSelectorOperator.LABEL_OPERATOR_IN, ["1"])]],
        ),
        # Cross-tier bin-packing when R2's preferred selector (y=1) is only on an
        # undersized node type (Tiny_Y: 0.5 CPU), so x=1 is the best feasible match.
        (
            {"x": "1"},
            [
                [("y", LabelSelectorOperator.LABEL_OPERATOR_IN, ["1"])],
                [("x", LabelSelectorOperator.LABEL_OPERATOR_IN, ["1"])],
            ],
        ),
        # Unlabeled request co-packing: R2 has no label selectors and fills L
        # alongside R1 in the same pass instead of losing to smaller node type S.
        (
            {"x": "1"},
            [],
        ),
    ],
)
def test_label_selector_cross_selector_bin_packing(
    max_num_nodes, l_labels, r2_selectors
):
    """Requests with different, fallback, or empty label selectors pack onto a shared
    node type L instead of greedily picking a smaller single-request node type S."""
    node_type_configs = {
        "Tiny_Y": NodeTypeConfig(
            name="Tiny_Y",
            resources={"CPU": 0.5},
            min_worker_nodes=0,
            max_worker_nodes=10,
            labels={"y": "1"},
        ),
        "S": NodeTypeConfig(
            name="S",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=10,
            labels={"x": "1"},
        ),
        "L": NodeTypeConfig(
            name="L",
            resources={"CPU": 3},
            min_worker_nodes=0,
            max_worker_nodes=10,
            labels=l_labels,
        ),
    }
    requests = [
        ResourceRequestUtil.make(
            {"CPU": 2},
            label_selectors=[[("x", LabelSelectorOperator.LABEL_OPERATOR_IN, ["1"])]],
        ),
        ResourceRequestUtil.make({"CPU": 1}, label_selectors=r2_selectors),
    ]
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(
            node_type_configs=node_type_configs,
            resource_requests=requests,
            max_num_nodes=max_num_nodes,
        )
    )
    assert _launch_and_terminate(reply)[0] == {"L": 1}
    assert not reply.infeasible_resource_requests


def test_label_selector_dynamic_depletion_and_fallback_reservation():
    """Verify (1) exhausting a primary node type promotes fallback matches on the
    next pass, and (2) unlabeled requests do not steal capacity from deferred
    fallback requests."""
    in_op = LabelSelectorOperator.LABEL_OPERATOR_IN
    scheduler = ResourceDemandScheduler(event_logger)

    # 1. Once Y (max_worker_nodes=1) is consumed by R0 ([y=1]), R2 ([y=1, x=1])
    # can no longer schedule on Y and co-packs with R1 ([x=1]) on L.
    reply = scheduler.schedule(
        sched_request(
            node_type_configs={
                "Y": NodeTypeConfig(
                    name="Y",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=1,
                    labels={"y": "1"},
                ),
                "S": NodeTypeConfig(
                    name="S",
                    resources={"CPU": 2},
                    min_worker_nodes=0,
                    max_worker_nodes=10,
                    labels={"x": "1"},
                ),
                "L": NodeTypeConfig(
                    name="L",
                    resources={"CPU": 3},
                    min_worker_nodes=0,
                    max_worker_nodes=10,
                    labels={"x": "1"},
                ),
            },
            resource_requests=[
                ResourceRequestUtil.make(
                    {"CPU": 4}, label_selectors=[[("y", in_op, ["1"])]]
                ),
                ResourceRequestUtil.make(
                    {"CPU": 2}, label_selectors=[[("x", in_op, ["1"])]]
                ),
                ResourceRequestUtil.make(
                    {"CPU": 1},
                    label_selectors=[[("y", in_op, ["1"])], [("x", in_op, ["1"])]],
                ),
            ],
            max_num_nodes=2,
        )
    )
    assert _launch_and_terminate(reply)[0] == {"Y": 1, "L": 1}
    assert not reply.infeasible_resource_requests

    # 2. When B (4 CPU) schedules R_B (2 CPU, [B]) and defers R_CB (2 CPU, [C, B]),
    # an unlabeled request R_u (2 CPU, []) must not jump over R_CB and steal B's
    # remaining 2 CPUs.
    reply = scheduler.schedule(
        sched_request(
            node_type_configs={
                "B": NodeTypeConfig(
                    name="B",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=1,
                    labels={"accel": "B"},
                ),
                "C": NodeTypeConfig(
                    name="C",
                    resources={"CPU": 2},
                    min_worker_nodes=0,
                    max_worker_nodes=1,
                    labels={"accel": "C"},
                ),
                "U": NodeTypeConfig(
                    name="U",
                    resources={"CPU": 2},
                    min_worker_nodes=0,
                    max_worker_nodes=1,
                    labels={},
                ),
            },
            resource_requests=[
                ResourceRequestUtil.make(
                    {"CPU": 2}, label_selectors=[[("accel", in_op, ["C"])]]
                ),
                ResourceRequestUtil.make(
                    {"CPU": 2}, label_selectors=[[("accel", in_op, ["B"])]]
                ),
                ResourceRequestUtil.make(
                    {"CPU": 2},
                    label_selectors=[
                        [("accel", in_op, ["C"])],
                        [("accel", in_op, ["B"])],
                    ],
                ),
                ResourceRequestUtil.make({"CPU": 2}),
            ],
        )
    )
    assert _launch_and_terminate(reply)[0] == {"B": 1, "C": 1, "U": 1}
    assert not reply.infeasible_resource_requests


def test_label_selector_with_dynamic_anti_affinity_labels():
    """Verify label selectors that read anti-affinity labels neither double-schedule
    cached nodes nor corrupt pre-mutation NOT_IN or newly unlocked IN label scores."""
    cfg = NodeTypeConfig(
        name="t", resources={"CPU": 4}, min_worker_nodes=0, max_worker_nodes=2
    )
    instances = [
        make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name="t",
                available_resources={"CPU": cpu},
                total_resources={"CPU": cpu},
                node_id=f"r{i}".encode(),
                labels=labels,
            ),
            im_instance=Instance(
                instance_type="t",
                status=Instance.RAY_RUNNING,
                instance_id=f"i{i}",
                node_id=f"r{i}",
            ),
            cloud_instance_id=f"c{i}",
        )
        for i, (cpu, labels) in enumerate(
            [(1, {"req": "true", "pg": "1"}), (4, {"req": "true"})]
        )
    ]
    anti = [(ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY, "pg", "1")]
    req_b = ResourceRequestUtil.make(
        {"CPU": 1},
        constraints=anti,
        label_selectors=[[("req", LabelSelectorOperator.LABEL_OPERATOR_IN, ["true"])]],
    )
    req_a = ResourceRequestUtil.make(
        {"CPU": 1},
        label_selectors=[[("pg", LabelSelectorOperator.LABEL_OPERATOR_IN, ["1"])]],
    )
    ctx = ResourceDemandScheduler.ScheduleContext(
        nodes=[SchedulingNode.new(inst, {"t": cfg}, True) for inst in instances],
        node_type_configs={"t": cfg},
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
        disable_launch_config_check=True,
        max_num_nodes=2,
    )
    nodes, infeasible = ResourceDemandScheduler._try_schedule(
        ctx, [req_b, req_a], ResourceRequestSource.PENDING_DEMAND
    )
    assert not infeasible
    assert (
        sum(
            len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in nodes
        )
        == 2
    )

    node = SchedulingNode.from_node_config(
        cfg, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )
    remaining, score = node.try_schedule(
        [
            ResourceRequestUtil.make(
                {"CPU": 1},
                constraints=anti,
                label_selectors=[
                    [("pg", LabelSelectorOperator.LABEL_OPERATOR_NOT_IN, ["1"])]
                ],
            )
        ],
        ResourceRequestSource.PENDING_DEMAND,
    )
    assert not remaining and score[0] == 1.0

    # An unlabeled anti-affinity request that adds pg=1 unlocks requests that
    # select pg=1 on the same node within one _try_schedule call.
    req_b_unlabeled = ResourceRequestUtil.make({"CPU": 1}, constraints=anti)
    ctx_unlabeled = ResourceDemandScheduler.ScheduleContext(
        nodes=[
            SchedulingNode.from_node_config(
                cfg, status=SchedulingNodeStatus.SCHEDULABLE, node_kind=NodeKind.WORKER
            )
        ],
        node_type_configs={"t": cfg},
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
        disable_launch_config_check=True,
        max_num_nodes=1,
    )
    nodes, infeasible = ResourceDemandScheduler._try_schedule(
        ctx_unlabeled,
        [req_a, req_b_unlabeled, req_a],
        ResourceRequestSource.PENDING_DEMAND,
    )
    assert not infeasible
    assert [
        len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)) for n in nodes
    ] == [3]


@pytest.mark.parametrize("mode", ["pinned", "slice", "spread", "co_pack", "fallback"])
def test_label_selector_attempt_cache_linear_deepcopy(mode, monkeypatch):
    """Scheduling N label-constrained bundles across N candidate nodes performs O(N)
    SchedulingNode deepcopies instead of O(N^2), including STRICT_SPREAD anti-affinity,
    unlabeled co-packing, and distinct fallback pools."""
    in_op = LabelSelectorOperator.LABEL_OPERATOR_IN
    num_hosts = 64
    cfg = NodeTypeConfig(
        name="tpu",
        resources={"CPU": 8, "TPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=num_hosts * 2,
    )
    nodes = []
    for i in range(num_hosts):
        node = SchedulingNode.from_node_config(
            cfg, status=SchedulingNodeStatus.SCHEDULABLE, node_kind=NodeKind.WORKER
        )
        node.labels = {
            "ray.io/tpu-worker-id": str(i),
            "ray.io/tpu-slice-name": "slice-0",
        }
        nodes.append(node)
    if mode == "fallback":
        for i in range(num_hosts):
            fb = SchedulingNode.from_node_config(
                cfg, status=SchedulingNodeStatus.SCHEDULABLE, node_kind=NodeKind.WORKER
            )
            fb.labels = {
                "ray.io/tpu-worker-id": str(i),
                "ray.io/tpu-slice-name": "slice-1",
            }
            nodes.append(fb)

    pinned = mode in ("pinned", "fallback")
    constraints = (
        [(ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY, "_PG_1", "")]
        if mode == "spread"
        else []
    )
    if pinned:
        requests = [
            ResourceRequestUtil.make(
                {"TPU": 4},
                label_selectors=[
                    [
                        ("ray.io/tpu-slice-name", in_op, ["slice-0"]),
                        ("ray.io/tpu-worker-id", in_op, [str(i)]),
                    ],
                    *(
                        [[("ray.io/tpu-slice-name", in_op, ["slice-1"])]]
                        if mode == "fallback"
                        else []
                    ),
                ],
            )
            for i in range(num_hosts)
        ]
    else:
        requests = [
            ResourceRequestUtil.make(
                {"TPU": 4},
                constraints=constraints,
                label_selectors=[[("ray.io/tpu-slice-name", in_op, ["slice-0"])]],
            )
        ] * num_hosts + (
            [ResourceRequestUtil.make({"CPU": 8})] * num_hosts
            if mode == "co_pack"
            else []
        )
    ctx = ResourceDemandScheduler.ScheduleContext(
        nodes=nodes,
        node_type_configs={"tpu": cfg},
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
        disable_launch_config_check=True,
    )

    deepcopy_count = 0
    real_deepcopy = copy.deepcopy

    def counting_deepcopy(obj, memo=None):
        nonlocal deepcopy_count
        if isinstance(obj, SchedulingNode):
            deepcopy_count += 1
        return real_deepcopy(obj, memo)

    monkeypatch.setattr(copy, "deepcopy", counting_deepcopy)
    out_nodes, infeasible = ResourceDemandScheduler._try_schedule(
        ctx, requests, ResourceRequestSource.PENDING_DEMAND
    )
    assert not infeasible
    expected_per_primary = 2 if mode == "co_pack" else 1
    scheduled_counts = sorted(
        len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
        for n in out_nodes
    )
    expected_counts = ([0] * num_hosts if mode == "fallback" else []) + [
        expected_per_primary
    ] * num_hosts
    assert scheduled_counts == expected_counts
    assert deepcopy_count == num_hosts


def test_label_selector_attempt_cache_prefix_and_tier_invalidation():
    """Verify the attempt cache invalidates whenever a node's accepted top-tier
    prefix, blocking fallback tier, or unlabeled tier prefix shifts across passes."""
    in_op = LabelSelectorOperator.LABEL_OPERATOR_IN
    notin_op = LabelSelectorOperator.LABEL_OPERATOR_NOT_IN
    scheduler = ResourceDemandScheduler(event_logger)

    def running_inst(nid, ntype, total_cpu, avail_cpu, labels):
        return make_autoscaler_instance(
            ray_node=NodeState(
                ray_node_type_name=ntype,
                available_resources={"CPU": avail_cpu},
                total_resources={"CPU": total_cpu},
                node_id=nid.encode(),
                labels=labels,
            ),
            im_instance=Instance(
                instance_type=ntype,
                status=Instance.RAY_RUNNING,
                instance_id=f"i-{nid}",
                node_id=nid,
            ),
            cloud_instance_id=f"c-{nid}",
        )

    # 1. Interleaved gang [A, B, A]: n2 (x=1, y=1, 1 CPU) caches scheduling A on pass 1
    # after n1 (x=1, 3 total / 1 avail CPU) wins A; pass 2 must re-simulate n2 on [B, A]
    # so n2 takes B and n3 (x=1, 1 CPU) takes the remaining A without spurious scale-up.
    gang_cfgs = {
        "nx": NodeTypeConfig(
            name="nx",
            resources={"CPU": 3},
            min_worker_nodes=0,
            max_worker_nodes=1,
            labels={"x": "1"},
        ),
        "nxy": NodeTypeConfig(
            name="nxy",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=1,
            labels={"x": "1", "y": "1"},
        ),
        "nx1": NodeTypeConfig(
            name="nx1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=1,
            labels={"x": "1"},
        ),
        "ny_extra": NodeTypeConfig(
            name="ny_extra",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=5,
            labels={"y": "1"},
        ),
    }
    req_x = ResourceRequestUtil.make(
        {"CPU": 1}, label_selectors=[[("x", in_op, ["1"])]]
    )
    req_y = ResourceRequestUtil.make(
        {"CPU": 1}, label_selectors=[[("y", in_op, ["1"])]]
    )
    for max_num_nodes in (None, 3):
        reply = scheduler.schedule(
            sched_request(
                node_type_configs=gang_cfgs,
                instances=[
                    running_inst("n1", "nx", 3, 1, {"x": "1"}),
                    running_inst("n2", "nxy", 1, 1, {"x": "1", "y": "1"}),
                    running_inst("n3", "nx1", 1, 1, {"x": "1"}),
                ],
                gang_resource_requests=[[req_x, req_y, req_x]],
                max_num_nodes=max_num_nodes,
            )
        )
        assert _launch_and_terminate(reply)[0] == {}
        assert not reply.infeasible_gang_resource_requests

    # 2. Prefix multiplicity shift [S0, S0, S1, S1, S0] and cross-tier blocker removal:
    # verify cached multi-pass _sched_best_node matches uncached simulation exactly.
    s0 = ResourceRequestUtil.make(
        {"CPU": 1}, label_selectors=[[("y", notin_op, ["0"])]]
    )
    s1 = ResourceRequestUtil.make(
        {"CPU": 1},
        label_selectors=[
            [("y", notin_op, ["0"]), ("z", notin_op, ["1"])],
            [("y", notin_op, ["1"]), ("z", notin_op, ["1"])],
        ],
    )
    u0 = ResourceRequestUtil.make({"CPU": 1})
    scenarios = [
        # Consuming one front S0 turns [S0, S0, S1, S1, S0] into [S0, S1, S1, S0]:
        # total count(S0) is still 2 and unique/run-length order still starts with
        # (S0, S1), yet p1 (2 CPU, y=1) must switch from [S0, S0] to [S0, S1] so p2
        # (y=0) takes the second S1 and p3 (y=1, z=1) takes the trailing S0.
        (
            [
                ("p0", 4, 1, {"y": "1", "z": "1"}),
                ("p1", 2, 2, {"y": "1"}),
                ("p2", 1, 1, {"y": "0"}),
                ("p3", 1, 1, {"y": "1", "z": "1"}),
            ],
            [s0, s0, s1, s1, s0],
        ),
        # Consuming a fallback blocker on b0 frees spare capacity on primary node b1
        # so b1 co-packs the unlabeled request u0 positioned earlier in `requests`.
        (
            [
                ("b0", 4, 1, {"x": "0"}),
                ("b1", 2, 2, {"x": "1"}),
            ],
            [
                u0,
                ResourceRequestUtil.make(
                    {"CPU": 1}, label_selectors=[[("x", in_op, ["1"])]]
                ),
                ResourceRequestUtil.make(
                    {"CPU": 1},
                    label_selectors=[
                        [("x", in_op, ["0"])],
                        [("x", in_op, ["1"])],
                    ],
                ),
            ],
        ),
    ]
    for node_specs, reqs in scenarios:
        cfgs = {
            nid: NodeTypeConfig(
                name=nid,
                resources={"CPU": tot},
                min_worker_nodes=0,
                max_worker_nodes=1,
                labels=lbls,
            )
            for nid, tot, _, lbls in node_specs
        }
        base_nodes = [
            SchedulingNode.new(running_inst(nid, nid, tot, av, lbls), cfgs, True)
            for nid, tot, av, lbls in node_specs
        ]

        def run_passes(use_cache):
            ns = copy.deepcopy(base_nodes)
            rs = list(reqs)
            attempts = {} if use_cache else None
            placements = []
            while rs and ns:
                best, rs, ns = ResourceDemandScheduler._sched_best_node(
                    rs, ns, ResourceRequestSource.PENDING_DEMAND, {}, {}, None, attempts
                )
                if best is None:
                    break
                placements.append(
                    (
                        best.ray_node_id,
                        [
                            r.SerializeToString(deterministic=True)
                            for r in best.get_sched_requests(
                                ResourceRequestSource.PENDING_DEMAND
                            )
                        ],
                    )
                )
                if any(
                    best._can_schedule_one(r, ResourceRequestSource.PENDING_DEMAND)
                    for r in rs
                ):
                    ns.append(best)
            return placements, [r.SerializeToString(deterministic=True) for r in rs]

        cached = run_passes(True)
        uncached = run_passes(False)
        assert cached == uncached
        assert not cached[1]

    # Staggered 2-replica materialization (1 busy running pod in rep-0, 1 idle
    # allocated pod in rep-1, and 6 REQUESTED pods) commits 8 slots (not 12) and
    # launches a 3rd replica under max_worker_nodes=12 when 11 tasks are pending.
    cfg_12 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4, "TPU": 4},
            labels={"ray.io/tpu-slice-name": "", "ray.io/tpu-worker-id": ""},
            min_worker_nodes=0,
            max_worker_nodes=12,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key="ray.io/tpu-slice-name",
                worker_index_label_key="ray.io/tpu-worker-id",
            ),
        ),
        "cpu": NodeTypeConfig(
            name="cpu", resources={"CPU": 4}, min_worker_nodes=0, max_worker_nodes=4
        ),
    }
    insts_r4 = [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="r0-0",
                instance_type="tpu",
                status=Instance.RAY_RUNNING,
                node_id="n0",
                cloud_instance_id="c0-0",
            ),
            ray_node=NodeState(
                node_id=b"n0",
                instance_id="c0-0",
                ray_node_type_name="tpu",
                status=NodeStatus.RUNNING,
                total_resources={"CPU": 4, "TPU": 4},
                available_resources={"CPU": 0, "TPU": 0},
                labels={"ray.io/tpu-slice-name": "rep-0", "ray.io/tpu-worker-id": "0"},
            ),
            cloud_instance_id="c0-0",
            cloud_instance_replica_id="rep-0",
        ),
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="r1-0",
                instance_type="tpu",
                status=Instance.ALLOCATED,
                cloud_instance_id="c1-0",
            ),
            cloud_instance_id="c1-0",
            cloud_instance_replica_id="rep-1",
        ),
        *(
            AutoscalerInstance(
                im_instance=Instance(
                    instance_id=f"req-{i}",
                    instance_type="tpu",
                    status=Instance.REQUESTED,
                    launch_request_id="L-init",
                )
            )
            for i in range(6)
        ),
    ]
    reply_r4 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfg_12,
            current_instances=insts_r4,
            disable_launch_config_check=True,
            resource_requests=[
                ResourceRequestByCount(
                    request=ResourceRequest(resources_bundle={"TPU": 4}), count=11
                )
            ],
        )
    )
    assert {r.instance_type: r.count for r in reply_r4.to_launch} == {"tpu": 4}

    # RAY_STOP_REQUESTED keeps occupying its slot (the stop can still be rejected)
    # for multi-host replicas exactly like single-host nodes.
    assert (
        SchedulingNode.new(
            AutoscalerInstance(
                im_instance=Instance(
                    instance_id="mh-stop",
                    instance_type="tpu",
                    status=Instance.RAY_STOP_REQUESTED,
                ),
                ray_node=NodeState(
                    node_id=b"mhs",
                    ray_node_type_name="tpu",
                    status=NodeStatus.IDLE,
                    total_resources={"CPU": 4, "TPU": 4},
                    available_resources={"CPU": 4, "TPU": 4},
                ),
            ),
            cfg_12,
            True,
        )
        is not None
    )
    assert (
        SchedulingNode.new(
            AutoscalerInstance(
                im_instance=Instance(
                    instance_id="sh-stop",
                    instance_type="cpu",
                    status=Instance.RAY_STOP_REQUESTED,
                ),
                ray_node=NodeState(
                    node_id=b"shs",
                    ray_node_type_name="cpu",
                    status=NodeStatus.IDLE,
                    total_resources={"CPU": 4},
                    available_resources={"CPU": 4},
                ),
            ),
            cfg_12,
            True,
        )
        is not None
    )


def _make_lifecycle_replica(
    rep: str,
    status: Instance.InstanceStatus,
    size: int,
    busy: bool,
    launch_config_hash: str = "",
    count: int = 4,
    with_provider_replica_id: bool = True,
) -> List[AutoscalerInstance]:
    instances = []
    for h in range(count):
        nid = f"{rep}-{h}".encode().ljust(28, b"0")
        inst = Instance(
            instance_id=f"{rep}-{h}",
            instance_type="tpu",
            status=status,
            node_id=nid.hex(),
            cloud_instance_id=f"c-{rep}-{h}",
            node_kind=NodeKind.WORKER,
            launch_config_hash=launch_config_hash,
            status_history=[
                Instance.StatusHistory(
                    instance_status=status, timestamp_ns=time.time_ns()
                )
            ],
        )
        avail = 0 if busy else 4
        ray_node = NodeState(
            node_id=nid,
            instance_id=f"c-{rep}-{h}",
            ray_node_type_name="tpu",
            status=NodeStatus.RUNNING if busy else NodeStatus.IDLE,
            total_resources={"CPU": 4, "TPU": 4},
            available_resources={"CPU": avail, "TPU": avail},
            labels=(
                {"ray.io/tpu-slice-name": rep, "ray.io/tpu-worker-id": str(h)}
                if size > 1
                else {}
            ),
        )
        instances.append(
            AutoscalerInstance(
                im_instance=inst,
                ray_node=ray_node,
                cloud_instance_id=f"c-{rep}-{h}",
                cloud_instance_replica_id=(
                    rep if (size > 1 and with_provider_replica_id) else None
                ),
            )
        )
    return instances


@pytest.mark.parametrize("workers_per_replica", [1, 4])
def test_stop_requested_replica_occupancy_and_min_workers(workers_per_replica):
    cfg = NodeTypeConfig(
        name="tpu",
        resources={"CPU": 4, "TPU": 4},
        labels={},
        min_worker_nodes=0,
        max_worker_nodes=8,
        replica_config=(
            WorkerReplicaConfig(
                workers_per_replica=workers_per_replica,
                domain_label_key="ray.io/tpu-slice-name",
                worker_index_label_key="ray.io/tpu-worker-id",
            )
            if workers_per_replica > 1
            else None
        ),
    )
    # Draining replica still occupies max_worker_nodes until stop is accepted.
    busy_and_draining = _make_lifecycle_replica(
        "rep-0", Instance.RAY_RUNNING, workers_per_replica, busy=True
    ) + _make_lifecycle_replica(
        "rep-1", Instance.RAY_STOP_REQUESTED, workers_per_replica, busy=False
    )
    launch_reply = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs={"tpu": cfg},
            current_instances=busy_and_draining,
            disable_launch_config_check=True,
            resource_requests=[
                ResourceRequestByCount(
                    request=ResourceRequest(resources_bundle={"TPU": 4}), count=4
                )
            ],
        )
    )
    assert launch_reply.to_launch == []

    # Draining replica must not count toward min_worker_nodes during idle scale-down.
    idle_and_draining = _make_lifecycle_replica(
        "rep-0", Instance.RAY_STOP_REQUESTED, workers_per_replica, busy=False
    ) + _make_lifecycle_replica(
        "rep-1", Instance.RAY_RUNNING, workers_per_replica, busy=False
    )
    for inst in idle_and_draining:
        inst.ray_node.idle_duration_ms = 60_000
    cfg_min = copy.deepcopy(cfg)
    cfg_min.min_worker_nodes = 4
    idle_reply = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs={"tpu": cfg_min},
            current_instances=idle_and_draining,
            disable_launch_config_check=True,
            idle_timeout_s=10,
        )
    )
    assert idle_reply.to_terminate == []
    assert idle_reply.to_launch == []


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
