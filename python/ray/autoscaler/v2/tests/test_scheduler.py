import copy
import itertools
import os
import random
import sys
import time
from contextlib import contextmanager

# coding: utf-8
from typing import Dict, List, Mapping, Optional, Tuple
from unittest.mock import patch

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
    ClusterResourceConstraint,
    GangResourceRequest,
    NodeState,
    NodeStatus,
    ResourceRequest,
)
from ray.core.generated.common_pb2 import LabelSelectorOperator
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
        (1, True, 1, 1.0, 1.0),
    )

    # Matching a constrained fallback selector (index 1 of 3) yields binary score 1.
    assert try_schedule_ls(
        {"CPU": 1}, {"ray.io/accelerator-type": "B200"}, label_selector_1
    ) == (
        [],
        (1, True, 1, 1.0, 1.0),
    )

    # Matching the first selector (index 0) yields binary score 1,
    # regardless of how many fallback selectors follow.
    assert try_schedule_ls(
        {"CPU": 1}, {"ray.io/accelerator-type": "TPU-v6e"}, label_selector_1
    ) == (
        [],
        (1, True, 1, 1.0, 1.0),
    )
    single_selector = [
        [("ray.io/accelerator-type", LabelSelectorOperator.LABEL_OPERATOR_IN, ["A100"])]
    ]
    assert try_schedule_ls({"CPU": 1}, labels, single_selector) == (
        [],
        (1, True, 1, 1.0, 1.0),
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
    # binary label score of requests scheduled in a subsequent pass.
    cfg = NodeTypeConfig(
        name="type_1",
        resources={"CPU": 3},
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
    assert score_1[0] == 1

    req_fallback = ResourceRequestUtil.make(
        {"CPU": 1}, label_selectors=label_selector_1
    )
    infeasible, score_2 = node.try_schedule([req_fallback], source)
    assert infeasible == []
    assert score_2[0] == 1

    req_unlabeled = ResourceRequestUtil.make({"CPU": 1})
    infeasible, score_3 = node.try_schedule([req_unlabeled], source)
    assert infeasible == []
    assert score_3[0] == 0


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


_IN = LabelSelectorOperator.LABEL_OPERATOR_IN
_NOT_IN = LabelSelectorOperator.LABEL_OPERATOR_NOT_IN
_ANTI = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
_A_THEN_B = [[("accel", _IN, ["A"])], [("accel", _IN, ["B"])]]
_B_THEN_A = _A_THEN_B[::-1]


def _nt(
    name: str,
    resources: Mapping[str, float],
    labels: Optional[Dict[str, str]] = None,
    max_workers: int = 10,
    priority: int = 0,
) -> NodeTypeConfig:
    return NodeTypeConfig(
        name=name,
        resources=dict(resources),
        min_worker_nodes=0,
        max_worker_nodes=max_workers,
        labels=labels or {},
        priority=priority,
    )


def _nts(*specs) -> Dict[NodeType, NodeTypeConfig]:
    out: Dict[NodeType, NodeTypeConfig] = {}
    for spec in specs:
        name, cpu = spec[0], spec[1]
        accel = spec[2] if len(spec) > 2 else None
        max_w = spec[3] if len(spec) > 3 else 10
        prio = spec[4] if len(spec) > 4 else 0
        res = cpu if isinstance(cpu, dict) else {"CPU": cpu}
        lbls = accel if isinstance(accel, dict) else ({"accel": accel} if accel else {})
        out[name] = _nt(name, res, lbls, max_w, prio)
    return out


def _req(
    cpu: float = 1,
    *accels: str,
    extra: Optional[ResourceMap] = None,
    constraints=None,
    selectors=None,
) -> ResourceRequest:
    sels = (
        selectors
        if selectors is not None
        else [[("accel", _IN, [a])] if a else [] for a in accels]
    )
    return ResourceRequestUtil.make(
        {"CPU": cpu, **(extra or {})},
        constraints=constraints,
        label_selectors=sels,
    )


def _res_dict(val, default_cpu: float = 2) -> Dict[str, float]:
    if val is None:
        return {"CPU": float(default_cpu)}
    if isinstance(val, (int, float)):
        return {"CPU": float(val)}
    return {k: float(v) for k, v in val.items()}


def _make_running_instance(
    name: str,
    node_type: Optional[str] = None,
    total=None,
    avail=None,
    labels: Optional[Dict[str, str]] = None,
    dynamic_labels: Optional[Dict[str, str]] = None,
    idle_ms: int = 0,
    cloud_id: Optional[str] = None,
) -> AutoscalerInstance:
    ntype = node_type or name
    tot = _res_dict(total)
    av = dict(tot) if avail is None else _res_dict(avail)
    return make_autoscaler_instance(
        ray_node=NodeState(
            ray_node_type_name=ntype,
            available_resources=av,
            total_resources=tot,
            labels=labels or {},
            dynamic_labels=dynamic_labels or {},
            idle_duration_ms=idle_ms,
            node_id=f"r-{name}".encode(),
        ),
        im_instance=Instance(
            instance_type=ntype,
            status=Instance.RAY_RUNNING,
            instance_id=f"i-{name}",
            node_id=f"r-{name}",
        ),
        cloud_instance_id=cloud_id or f"c-{name}",
    )


def _make_running_node(
    nid: str,
    total,
    avail=None,
    labels: Optional[Dict[str, str]] = None,
) -> SchedulingNode:
    tot = _res_dict(total)
    av = dict(tot) if avail is None else _res_dict(avail)
    return SchedulingNode(
        node_type=nid,
        total_resources=tot,
        available_resources=av,
        labels=dict(labels or {}),
        status=SchedulingNodeStatus.SCHEDULABLE,
        im_instance_id=f"im-{nid}",
        im_instance_status=Instance.RAY_RUNNING,
        ray_node_id=nid,
    )


@contextmanager
def _spy_deepcopy(monkeypatch, target_node_id: Optional[str] = None):
    count = [0]
    real_deepcopy = copy.deepcopy

    def wrapped(obj, memo=None):
        if isinstance(obj, SchedulingNode) and (
            target_node_id is None or obj.ray_node_id == target_node_id
        ):
            count[0] += 1
        return real_deepcopy(obj, memo)

    with monkeypatch.context() as m:
        m.setattr(copy, "deepcopy", wrapped)
        yield count


def _run_best_node_rounds(
    nodes: List[SchedulingNode],
    requests: List[ResourceRequest],
    use_cache: bool,
    monkeypatch=None,
    spy_node_id: Optional[str] = None,
):
    src = ResourceRequestSource.PENDING_DEMAND
    ns = copy.deepcopy(nodes)
    rs = list(requests)
    shape_keys = {id(r): r.SerializeToString(deterministic=True) for r in rs}
    attempts = {} if use_cache else None
    placements = []

    def drive():
        nonlocal ns, rs
        while rs and ns:
            best, rs, ns = ResourceDemandScheduler._sched_best_node(
                rs, ns, src, {}, {}, shape_keys, attempts
            )
            if best is None:
                break
            placements.append(
                (
                    best.ray_node_id,
                    [
                        r.SerializeToString(deterministic=True)
                        for r in best.get_sched_requests(src)
                    ],
                )
            )
            avail = best.get_available_resources(src)
            if any(best._schedulable_selector(r, avail) is not None for r in rs):
                ns.append(best)

    if monkeypatch is not None and spy_node_id is not None:
        with _spy_deepcopy(monkeypatch, spy_node_id) as copies:
            drive()
        spy_count = copies[0]
    else:
        drive()
        spy_count = 0
    return (
        placements,
        [r.SerializeToString(deterministic=True) for r in rs],
        spy_count,
    )


@pytest.mark.parametrize(
    "node_cpu,max_type_b,max_num_nodes,request_cpus,expected_launch",
    [
        (2, 10, None, [1], {"type_a": 1, "type_b": 1}),
        (2, 0, None, [1], {"type_a": 1}),
        (2, 10, 1, [1], {"type_a": 1}),
        (4, 10, None, [2, 1], {"type_a": 1, "type_b": 1}),
    ],
)
def test_label_selector_fallback_new_nodes(
    node_cpu, max_type_b, max_num_nodes, request_cpus, expected_launch
):
    cfgs = _nts(("type_a", node_cpu, "A"), ("type_b", node_cpu, "B", max_type_b))
    requests = [
        _req(cpu, selectors=s) for cpu in request_cpus for s in (_A_THEN_B, _B_THEN_A)
    ]
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(cfgs, max_num_nodes=max_num_nodes, resource_requests=requests)
    )
    assert _launch_and_terminate(reply)[0] == expected_launch
    assert not reply.infeasible_resource_requests


def test_label_selector_existing_nodes_fallback_and_revisit():
    """Verify running nodes honor primary preference, preserve idle primaries over
    empty fallbacks, fall back when primaries fill, and revisit deferred capacity."""
    src = ResourceRequestSource.PENDING_DEMAND
    scheduler = ResourceDemandScheduler(event_logger)

    # 1. Opposing [A, B] and [B, A] each land on their running primary node.
    cfgs_ab = _nts(("type_a", 2, "A"), ("type_b", 2, "B"))
    reply = scheduler.schedule(
        sched_request(
            node_type_configs=cfgs_ab,
            resource_requests=[_req(1, selectors=s) for s in (_A_THEN_B, _B_THEN_A)],
            instances=[
                _make_running_instance(
                    a, f"type_{a.lower()}", 2, labels={"accel": a}, idle_ms=10
                )
                for a in ("A", "B")
            ],
            idle_timeout_s=0,
        )
    )
    assert _launch_and_terminate(reply) == ({}, [])
    assert not reply.infeasible_resource_requests

    # 2. [accel=A, {}] lands on idle primary A (2 CPU) rather than larger B (4 CPU),
    # spills onto B when A is full, and leaves a strict [accel=A] request infeasible.
    cfgs_idle = _nts(("A", 2, "A", 1), ("B", 4, "B", 1))
    insts_idle = [
        _make_running_instance("A", "A", 2, labels={"accel": "A"}, idle_ms=10),
        _make_running_instance("B", "B", 4, labels={"accel": "B"}, idle_ms=10),
    ]
    r_a_any, r_a_strict, r_b = _req(2, "A", ""), _req(2, "A"), _req(2, "B")
    sreq_idle = sched_request(
        cfgs_idle,
        resource_requests=[r_b, r_a_any],
        instances=insts_idle,
        idle_timeout_s=0,
    )
    assert _launch_and_terminate(scheduler.schedule(sreq_idle)) == ({}, [])

    for req_list, expected_inf, expected_per_node in [
        ([r_b, r_a_any, r_a_any], [], {"A": 1, "B": 2}),
        ([r_b, r_a_strict, r_a_strict], [r_a_strict], {"A": 1, "B": 1}),
    ]:
        ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(sreq_idle)
        nodes_out, inf_out = ResourceDemandScheduler._try_schedule(ctx, req_list, src)
        assert inf_out == expected_inf
        assert {
            n.node_type: len(n.get_sched_requests(src)) for n in nodes_out
        } == expected_per_node

    # 3. Existing node A picked in Round 1 stays a candidate via _can_host_more and
    # hosts a deferred [B, A] fallback request once running node B is full.
    res_revisit = {"A": {"CPU": 2, "special": 1}, "B": {"CPU": 1}}
    reply_revisit = scheduler.schedule(
        sched_request(
            node_type_configs={
                k: _nt(k, res, {"accel": k}, max_workers=1)
                for k, res in res_revisit.items()
            },
            instances=[
                _make_running_instance(k, k, res, labels={"accel": k})
                for k, res in res_revisit.items()
            ],
            resource_requests=[
                _req(1, "A", extra={"special": 1}),
                _req(1, "B", "A"),
                _req(1, "B", "A"),
            ],
        )
    )
    assert _launch_and_terminate(reply_revisit)[0] == {}
    assert not reply_revisit.infeasible_resource_requests


@pytest.mark.parametrize(
    "pod_specs,reqs,expected_ippr,expected_cluster_cpu",
    [
        # 1. Resized pod-A stays a candidate and hosts `special` work in Round 3.
        (
            [("A", 4, {"special": 2}, {"accel": "A"}), ("B", 4, {}, {"accel": "B"})],
            [
                _req(2, "A", extra={"special": 1}),
                _req(1, "B", "A"),
                _req(1, extra={"special": 1}),
            ],
            [("pod-A", 4), ("pod-B", 4)],
            8,
        ),
        # 2. Early loop exit retains pod-A in resized_ippr and restores unchosen pod-C.
        (
            [
                ("A", 4, {"special": 2}, {"accel": "A"}),
                ("B", 4, {}, {"accel": "B"}),
                ("C", 4, {}, {"accel": "C"}),
            ],
            [_req(2, "A", extra={"special": 1}), _req(1, "B", "A")],
            [("pod-A", 4), ("pod-B", 4)],
            9,
        ),
        # 3. Already-resized pod-A (max 16 CPU) beats unresized pod-U (max 5 CPU, 100%
        # util) at equal score[0] == 0 for held-back unlabeled work in Round 3, while
        # unselected pod-U is restored to 1 CPU.
        (
            [
                ("A", 16, {}, {"accel": "A"}),
                ("C", 4, {}, {"accel": "C"}),
                ("U", 5, {}, {}),
            ],
            [_req(5, "A"), _req(1, "C", "A"), _req(4)],
            [("pod-A", 16), ("pod-C", 4)],
            21,
        ),
    ],
)
def test_ippr_label_selector_revisit_and_committed_preference(
    pod_specs, reqs, expected_ippr, expected_cluster_cpu
):
    cfgs, instances, groups, statuses = {}, [], {}, {}
    for name, max_cpu, extra, labels in pod_specs:
        spec = IPPRGroupSpec(
            min_cpu=1,
            max_cpu=max_cpu,
            min_memory=1024**3,
            max_memory=8 * 1024**3,
            resize_timeout=60,
        )
        cfgs[name] = _nt(name, {"CPU": 1, **extra}, labels)
        instances.append(
            _make_running_instance(
                name,
                name,
                {"CPU": 1, **extra},
                {"CPU": 0, **extra},
                labels,
                cloud_id=f"pod-{name}",
            )
        )
        groups[name] = spec
        statuses[f"pod-{name}"] = IPPRStatus(
            cloud_instance_id=f"pod-{name}",
            spec=spec,
            current_cpu=1,
            current_memory=1024**3,
            desired_cpu=1,
            desired_memory=1024**3,
        )
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(
            node_type_configs=cfgs,
            resource_requests=reqs,
            instances=instances,
            ippr_specs=IPPRSpecs(groups=groups),
            ippr_statuses=statuses,
        )
    )
    assert (
        sorted((s.cloud_instance_id, s.desired_cpu) for s in reply.to_ippr)
        == expected_ippr
    )
    assert reply.cluster_resources["CPU"] == expected_cluster_cpu
    assert reply.to_launch == []
    assert not reply.infeasible_resource_requests


@pytest.mark.parametrize("max_num_nodes", [1, 2, None])
@pytest.mark.parametrize(
    "l_labels,r2_selectors",
    [
        ({"x": "1", "y": "1"}, [[("y", _IN, ["1"])]]),
        ({"x": "1"}, [[("y", _IN, ["1"])], [("x", _IN, ["1"])]]),
        ({"x": "1"}, []),
    ],
)
def test_label_selector_cross_selector_bin_packing(
    max_num_nodes, l_labels, r2_selectors
):
    cfgs = _nts(("Tiny_Y", 0.5, {"y": "1"}), ("S", 2, {"x": "1"}), ("L", 3, l_labels))
    reqs = [_req(2, selectors=[[("x", _IN, ["1"])]]), _req(1, selectors=r2_selectors)]
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(cfgs, resource_requests=reqs, max_num_nodes=max_num_nodes)
    )
    assert _launch_and_terminate(reply)[0] == {"L": 1}
    assert not reply.infeasible_resource_requests


@pytest.mark.parametrize(
    "cfgs,reqs,max_num_nodes,expected_launch,expected_infeasible",
    [
        # Primary preference preserved: [A100] + [H100, A100] launches both primaries.
        (
            _nts(("a100", 8, "A100"), ("h100", 8, "H100")),
            [_req(2, "A100"), _req(2, "H100", "A100")],
            None,
            {"a100": 1, "h100": 1},
            0,
        ),
        # Held-back unlabeled request reuses already-launched type_a (2 VMs, not 3).
        (
            _nts(
                ("type_a", {"CPU": 16, "memory": 64}, "A"),
                ("type_c", {"CPU": 2, "memory": 8}, "C"),
                ("type_u", {"CPU": 4, "memory": 16}),
            ),
            [_req(6, "A"), _req(1, "C", "A"), _req(4)],
            None,
            {"type_a": 1, "type_c": 1},
            0,
        ),
        # Equal score[0] == 1 fallback reuses launched type_a instead of launching
        # smaller type_a_small and exhausting max_num_nodes=3 before type_d.
        (
            _nts(
                ("type_a", 64, "A"),
                ("type_a_small", 8, "A"),
                ("type_c", {"CPU": 8, "memory": 64}, "C", 1),
                ("type_d", {"CPU": 2, "memory": 8}, "D", 1),
            ),
            [_req(16, "A", "Z"), _req(8, "C"), _req(8, "C", "A"), _req(2, "D")],
            3,
            {"type_a": 1, "type_c": 1, "type_d": 1},
            0,
        ),
        # Exact utilization tie fills already-launched A before launching fresh A.
        (
            _nts(("A", 4, "A", 2), ("B", 1, "B", 1), ("U", 3, None, 1)),
            [_req(3, "A"), _req(1, "B"), _req(1, "B", "A"), _req(3)],
            None,
            {"A": 1, "B": 1, "U": 1},
            0,
        ),
        # Primary Y (max=1) depletion promotes [y=1, x=1] to co-pack on L.
        (
            _nts(("Y", 4, {"y": "1"}, 1), ("S", 2, {"x": "1"}), ("L", 3, {"x": "1"})),
            [
                _req(4, selectors=[[("y", _IN, ["1"])]]),
                _req(2, selectors=[[("x", _IN, ["1"])]]),
                _req(1, selectors=[[("y", _IN, ["1"])], [("x", _IN, ["1"])]]),
            ],
            2,
            {"Y": 1, "L": 1},
            0,
        ),
        # Post-Pass-1 fit check: deferred 2-CPU [C, B] does not fit remaining 1 CPU on
        # B_big (3 CPU), so Pass 2 co-packs 1-CPU unlabeled work onto B_big.
        (
            _nts(
                ("B_small", 2, "B", 1),
                ("B_big", 3, "B", 1),
                ("C", 4, "C", 1),
                ("U", 1, None, 1),
            ),
            [_req(2, "C"), _req(2, "B"), _req(2, "C", "B"), _req(1)],
            None,
            {"B_big": 1, "C": 1},
            0,
        ),
        # 3-selector [A, B, C] active-index progression (missing A -> B=1 -> C=2).
        (
            _nts(
                ("type_b", 2, "B", 1), ("type_c", 2, "C", 1, 10), ("type_u", 1, None, 1)
            ),
            [_req(2, "B"), _req(1, "C"), _req(1, "A", "B", "C"), _req(1, "")],
            None,
            {"type_b": 1, "type_c": 1, "type_u": 1},
            0,
        ),
        # max_num_nodes=2 reached mid-loop packs deferred [B, A] and [[]] onto type_a.
        (
            _nts(("type_a", 4, "A", 2, 10), ("type_b", 4, "B", 2)),
            [_req(2, "A"), _req(4, "B"), _req(1, "B", "A"), _req(1, "")],
            2,
            {"type_a": 1, "type_b": 1},
            0,
        ),
        # Dead 2-constraint primary + {} fallback does not steal B from [B].
        (
            _nts(("B", 2, "B", 1), ("U", 2, None, 1)),
            [
                _req(2, selectors=[[("zone", _IN, ["q"]), ("rack", _IN, ["r"])], []]),
                _req(2, "B"),
            ],
            None,
            {"B": 1, "U": 1},
            0,
        ),
        # Empty-selector truncation in _sort_resource_request keeps 3-CPU [[], [A]]
        # ahead of 2-CPU [[]] requests so 5-CPU + 2-CPU nodes fit all three.
        (
            _nts(("n5", 5, None, 1), ("n2", 2, None, 1)),
            [_req(3, "", "A"), _req(2, ""), _req(2, "")],
            None,
            {"n5": 1, "n2": 1},
            0,
        ),
        # Paired negative vs fallback: [missing] fails while [missing, []] lands on U.
        (
            _nts(("type_u", 2, None, 1)),
            [_req(1, "missing"), _req(1, "missing", "")],
            None,
            {"type_u": 1},
            1,
        ),
    ],
)
def test_label_selector_planner_table(
    cfgs, reqs, max_num_nodes, expected_launch, expected_infeasible
):
    reply = ResourceDemandScheduler(event_logger).schedule(
        sched_request(cfgs, max_num_nodes=max_num_nodes, resource_requests=reqs)
    )
    assert _launch_and_terminate(reply)[0] == expected_launch
    assert len(reply.infeasible_resource_requests) == expected_infeasible


def test_label_selector_fallback_reservation_permutations_and_contracts():
    """Verify fallback reservation across all 24 request permutations, MRV fallback
    ordering, exhausted-primary equivalence, and cluster resource constraints."""
    scheduler = ResourceDemandScheduler(event_logger)

    cfgs_perm = _nts(("B", 4, "B", 1), ("C", 2, "C", 1), ("U", 2, None, 1))
    for unlabeled_sels, extra_constraints in [
        (None, None),
        ([[]], None),
        ([[], []], None),
        ([[], [("accel", _IN, ["C"])]], None),
        ([[("accel", _IN, ["A_missing"])], []], None),
        ([[("accel", _IN, ["A_missing"]), ("zone", _IN, ["z_missing"])], []], None),
        ([[], [("accel", _IN, ["C"])]], [(_ANTI, "_PG_1", "")]),
    ]:
        base_four = [
            _req(2, "C"),
            _req(2, "B"),
            _req(2, "C", "B"),
            _req(2, constraints=extra_constraints, selectors=unlabeled_sels),
        ]
        for perm in itertools.permutations(base_four):
            reply = scheduler.schedule(
                sched_request(cfgs_perm, resource_requests=list(perm))
            )
            assert _launch_and_terminate(reply)[0] == {"B": 1, "C": 1, "U": 1}
            assert not reply.infeasible_resource_requests

    # MRV fallback tie-break: [C, B] takes C while [C, {}] falls back onto U.
    r_cb, r_c_any = _req(2, "C", "B"), _req(2, "C", "")
    cfgs_mrv = _nts(("B", 2, "B", 0), ("C", 2, "C", 1), ("U", 2, None, 1))
    for req_order in ([r_c_any, r_cb], [r_cb, r_c_any]):
        reply_mrv = scheduler.schedule(
            sched_request(cfgs_mrv, resource_requests=req_order)
        )
        assert _launch_and_terminate(reply_mrv)[0] == {"C": 1, "U": 1}
        assert not reply_mrv.infeasible_resource_requests

    # Exhausted primary [{zone: q}, {}] plans identically to [] and [[]].
    cfgs_eq = _nts(("big", 8, "B", 1), ("small", 2, "A", 3))
    req_ba_1cpu = _req(1, "B", "A")
    outcomes = [
        (
            _launch_and_terminate(rep)[0],
            len(rep.infeasible_resource_requests),
        )
        for sels in ([], [[]], [[("zone", _IN, ["q"])], []])
        for r4 in (_req(4, selectors=sels),)
        for rep in (
            scheduler.schedule(
                sched_request(cfgs_eq, resource_requests=[r4, r4, req_ba_1cpu])
            ),
        )
    ]
    assert outcomes[0] == outcomes[1] == outcomes[2]

    # CLUSTER_RESOURCE_CONSTRAINT with multi-selector fallbacks (feasible + excess).
    crc_cfgs = _nts(("type_a", 2, "A", 1), ("type_b", 2, "B", 1))
    crc_ok = [_req(2, "A"), _req(2, "A", "B")]
    rep_crc = scheduler.schedule(
        sched_request(crc_cfgs, cluster_resource_constraints=crc_ok)
    )
    assert _launch_and_terminate(rep_crc)[0] == {"type_a": 1, "type_b": 1}
    assert not rep_crc.infeasible_cluster_resource_constraints

    req_crc_bad = sched_request(
        crc_cfgs, cluster_resource_constraints=crc_ok + [_req(2, "A", "B")]
    )
    rep_crc_bad = scheduler.schedule(req_crc_bad)
    assert (
        rep_crc_bad.infeasible_cluster_resource_constraints
        == req_crc_bad.cluster_resource_constraints
    )


def test_label_selector_empty_and_operator_semantics():
    """Verify binary integer label scores on empty/mixed selectors, empty b'' request
    shape keys, unknown operators, empty value lists, and absent vs '' under NOT_IN."""
    src = ResourceRequestSource.PENDING_DEMAND
    unspec_op = LabelSelectorOperator.LABEL_OPERATOR_UNSPECIFIED
    node_a = SchedulingNode.from_node_config(
        _nt("a", {"CPU": 2}, {"accel": "A"}, 1),
        SchedulingNodeStatus.SCHEDULABLE,
        NodeKind.WORKER,
    )
    node_u = SchedulingNode.from_node_config(
        _nt("u", {"CPU": 2}, max_workers=1),
        SchedulingNodeStatus.SCHEDULABLE,
        NodeKind.WORKER,
    )

    empty_req = ResourceRequest()
    rem_zero, score_zero = copy.deepcopy(node_u).try_schedule(
        [empty_req], src, {id(empty_req): b""}
    )
    assert not rem_zero and score_zero[0] == 0

    for req, node, expected_score in [
        (_req(1, ""), node_u, 0),
        (_req(1, "", ""), node_u, 0),
        (_req(1, "A", ""), node_a, 1),
        (_req(1, "A", ""), node_u, 0),
        (_req(1, "", "A"), node_a, 0),
        (_req(1, "", "A"), node_u, 0),
    ]:
        rem, score = copy.deepcopy(node).try_schedule([req], src)
        assert not rem and score[0] == expected_score

    nodes = {
        k: SchedulingNode.from_node_config(
            _nt("n", {"CPU": 2}, lbls),
            SchedulingNodeStatus.SCHEDULABLE,
            NodeKind.WORKER,
        )
        for k, lbls in [("val", {"k": "v"}), ("empty", {"k": ""}), ("absent", {})]
    }
    for bad_op in (unspec_op, 99):
        req_bad = _req(1, selectors=[[("k", bad_op, ["v"])]])
        assert nodes["val"]._first_matching_selector(req_bad) is None
        assert (
            nodes["val"]._schedulable_selector(
                req_bad, nodes["val"].get_available_resources(src)
            )
            is None
        )
    assert (
        nodes["val"]._schedulable_selector(
            _req(1, selectors=[[("k", _IN, ["v"])]]), {"CPU": 0}
        )
        is None
    )

    for n_key, op, vals, expected in [
        ("val", _IN, ["v"], (0, True)),
        ("val", _IN, ["other", "v"], (0, True)),
        ("val", _IN, [], None),
        ("empty", _IN, [], None),
        ("absent", _IN, [], None),
        ("val", _NOT_IN, [], (0, True)),
        ("empty", _NOT_IN, [], (0, True)),
        ("absent", _NOT_IN, [], (0, True)),
        ("absent", _NOT_IN, [""], (0, True)),
        ("empty", _NOT_IN, [""], None),
        ("absent", _NOT_IN, ["v"], (0, True)),
        ("empty", _NOT_IN, ["v"], (0, True)),
        ("val", _NOT_IN, ["v"], None),
    ]:
        req = _req(1, selectors=[[("k", op, vals)]])
        assert nodes[n_key]._first_matching_selector(req) == expected
        assert (
            nodes[n_key]._schedulable_selector(
                req, nodes[n_key].get_available_resources(src)
            )
            == expected
        )


def test_label_selector_spread_anti_affinity_depletion():
    """Verify STRICT_SPREAD anti-affinity depletion on multi-GPU primary nodes advances
    active_selectors to fallback nodes, paired with pre-blocked counter-cases."""
    src = ResourceRequestSource.PENDING_DEMAND
    scheduler = ResourceDemandScheduler(event_logger)
    cfgs = {k: _nt(k, {"CPU": 4, "GPU": 4}, {"accel": k}, 1) for k in ("A", "B")}
    spread_bundle = _req(
        1,
        "A",
        "B",
        extra={"GPU": 1},
        constraints=[(_ANTI, "_PG_SPREAD", "v1")],
    )
    insts_ok = [
        _make_running_instance("A", "A", {"CPU": 4, "GPU": 4}, labels={"accel": "A"}),
        _make_running_instance("B", "B", {"CPU": 4, "GPU": 4}, labels={"accel": "B"}),
    ]
    ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        sched_request(cfgs, instances=insts_ok)
    )
    nodes, infeasible = ResourceDemandScheduler._try_schedule(
        ctx, [spread_bundle, spread_bundle], src
    )
    assert not infeasible
    assert {n.node_type: len(n.get_sched_requests(src)) for n in nodes} == {
        "A": 1,
        "B": 1,
    }
    assert not scheduler.schedule(
        sched_request(
            cfgs,
            instances=insts_ok,
            gang_resource_requests=[[spread_bundle, spread_bundle]],
        )
    ).infeasible_gang_resource_requests

    insts_pre_a = [
        _make_running_instance(
            "A",
            "A",
            {"CPU": 4, "GPU": 4},
            labels={"accel": "A"},
            dynamic_labels={"_PG_SPREAD": "v1"},
        ),
        _make_running_instance("B", "B", {"CPU": 4, "GPU": 4}, labels={"accel": "B"}),
    ]
    ctx_pre = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        sched_request(cfgs, instances=insts_pre_a)
    )
    nodes_pre, inf_pre = ResourceDemandScheduler._try_schedule(
        ctx_pre, [spread_bundle], src
    )
    assert not inf_pre
    assert {n.node_type: len(n.get_sched_requests(src)) for n in nodes_pre} == {
        "A": 0,
        "B": 1,
    }

    insts_pre_b = [
        _make_running_instance("A", "A", {"CPU": 4, "GPU": 4}, labels={"accel": "A"}),
        _make_running_instance(
            "B",
            "B",
            {"CPU": 4, "GPU": 4},
            labels={"accel": "B"},
            dynamic_labels={"_PG_SPREAD": "v1"},
        ),
    ]
    reply_blocked = scheduler.schedule(
        sched_request(
            cfgs,
            instances=insts_pre_b,
            gang_resource_requests=[[spread_bundle, spread_bundle]],
        )
    )
    assert len(reply_blocked.infeasible_gang_resource_requests) == 1


def test_label_selector_dynamic_anti_affinity_overlap():
    """Verify label selectors reading anti-affinity keys restart Pass 1 on label growth,
    promote ms[0] < act, allow Pass 2 mutators when Pass 1 scheduled nothing, skip
    infeasible 0-request nodes, and match uncached multi-pass scheduling."""
    src = ResourceRequestSource.PENDING_DEMAND
    scheduler = ResourceDemandScheduler(event_logger)
    anti_pg = [(_ANTI, "_PG_1", "")]

    req_pg1 = _req(1, selectors=[[("_PG_1", _IN, [""])]])
    req_pg2 = _req(1, selectors=[[("_PG_2", _IN, [""])]])
    req_mut_l = _req(1, constraints=anti_pg, selectors=[[("req", _IN, ["true"])]])
    req_mut_u = _req(1, constraints=anti_pg, selectors=[])
    req_u = _req(1, "")

    cfg_2cpu = _nt("n1", {"CPU": 2}, {"req": "true"}, 1)
    for cfg, mut, first, expected_rem in [
        (cfg_2cpu, req_mut_l, req_pg1, [req_u]),
        (_nt("nu", {"CPU": 2}, max_workers=1), req_mut_u, req_pg1, [req_u]),
        (cfg_2cpu, req_mut_l, req_pg2, [req_pg2]),
    ]:
        node = SchedulingNode.from_node_config(
            cfg, SchedulingNodeStatus.SCHEDULABLE, NodeKind.WORKER
        )
        rem, score = node.try_schedule([first, mut, req_u], src)
        assert rem == expected_rem and score[0] == 1

    # NOT_IN evaluated before anti-affinity label mutation still scores 1.
    node_notin = SchedulingNode.from_node_config(
        _nt("t", {"CPU": 4}, max_workers=2),
        SchedulingNodeStatus.TO_LAUNCH,
        NodeKind.WORKER,
    )
    rem_ni, score_ni = node_notin.try_schedule(
        [
            _req(
                1,
                constraints=[(_ANTI, "pg", "1")],
                selectors=[[("pg", _NOT_IN, ["1"])]],
            )
        ],
        src,
    )
    assert not rem_ni and score_ni[0] == 1

    # Per-node placement across running r-0 (1 CPU, pg=1) and r-1 (4 CPU): each hosts 1.
    cfg_t = _nt("t", {"CPU": 4}, max_workers=2)
    ctx_run = ResourceDemandScheduler.ScheduleContext(
        nodes=[
            _make_running_node("r-0", 4, 1, {"req": "true", "pg": "1"}),
            _make_running_node("r-1", 4, 4, {"req": "true"}),
        ],
        node_type_configs={"t": cfg_t},
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
        disable_launch_config_check=True,
        max_num_nodes=2,
    )
    req_anti_req = _req(
        1, constraints=[(_ANTI, "pg", "1")], selectors=[[("req", _IN, ["true"])]]
    )
    req_pg_1 = _req(1, selectors=[[("pg", _IN, ["1"])]])
    nodes_run, inf_run = ResourceDemandScheduler._try_schedule(
        ctx_run, [req_anti_req, req_pg_1], src
    )
    assert not inf_run
    assert {n.ray_node_id: len(n.get_sched_requests(src)) for n in nodes_run} == {
        "r-0": 1,
        "r-1": 1,
    }

    # Selector promotion (ms[0] == 0 < act == 1) co-packs onto big (2 CPU).
    cfgs_promo = _nts(("big", 2, {"role": "w"}, 1), ("small", 1, {"role": "w"}, 1))
    req_anti_role = _req(
        1, constraints=[(_ANTI, "pg", "1")], selectors=[[("role", _IN, ["w"])]]
    )
    rep_promo = scheduler.schedule(
        sched_request(cfgs_promo, resource_requests=[req_anti_role, req_pg_1])
    )
    assert _launch_and_terminate(rep_promo)[0] == {"big": 1}
    assert not rep_promo.infeasible_resource_requests

    # Pass 2 runs when scheduled_constrained is False despite fitting deferred fallback.
    best_b5, rem_b5, _ = ResourceDemandScheduler._sched_best_node(
        [
            _req(1, constraints=[(_ANTI, "pg", "1")]),
            _req(1, selectors=[[("pg", _IN, ["1"])], [("alt", _IN, ["1"])]]),
        ],
        [
            _make_running_node("r-unlock", 2, 2, {"alt": "1"}),
            _make_running_node("r-pre", 3, 2, {"pg": "1"}),
        ],
        src,
        {},
        {},
    )
    assert best_b5 is not None and best_b5.ray_node_id == "r-unlock" and rem_b5 == []

    # Uncached score[0] == 1 beats committed score[0] == 0 even at 100% utilization.
    best_s70, _, _ = ResourceDemandScheduler._sched_best_node(
        [_req(1, "B"), _req(1)],
        [
            _make_running_node("r-comm", 1, 1),
            _make_running_node("r-uncomm", 4, 4, {"accel": "B"}),
        ],
        src,
        {},
        {},
        attempts=None,
        num_committed=1,
    )
    assert best_s70 is not None and best_s70.ray_node_id == "r-uncomm"

    # Infeasible dynamic-overlap request does not launch a spurious 0-request node.
    req_inf = _req(
        4, constraints=[(_ANTI, "pg", "1")], selectors=[[("pg", _IN, ["1"])]]
    )
    rep_inf = scheduler.schedule(
        sched_request(_nts(("n1", 1, None, 1)), resource_requests=[req_inf])
    )
    assert rep_inf.to_launch == [] and len(rep_inf.infeasible_resource_requests) == 1

    # Capped single node (max_num_nodes=1): unlocked constrained request beats req_u.
    req_strict_first = _req(
        1,
        constraints=[(_ANTI, "_PG_OTHER", "")],
        selectors=[[("_PG_1", _IN, [""]), ("req", _IN, ["true"])]],
    )
    rep_cap = scheduler.schedule(
        sched_request(
            {"n1": cfg_2cpu},
            max_num_nodes=1,
            resource_requests=[req_strict_first, req_mut_l, req_u],
        )
    )
    assert _launch_and_terminate(rep_cap)[0] == {"n1": 1}
    assert rep_cap.infeasible_resource_requests == [req_u]

    # Multi-pass attempts={} vs attempts=None equivalence with explicit placement check.
    req_anti_w = _req(
        1, constraints=[(_ANTI, "foo", "bar")], selectors=[[("role", _IN, ["w"])]]
    )
    req_only_foo = _req(1, selectors=[[("foo", _IN, ["bar"])]])
    req_fb_foo = _req(1, selectors=[[("foo", _IN, ["bar"])], []])
    req_anti_u = _req(1, constraints=[(_ANTI, "foo", "bar")])
    for nodes_mp, reqs_mp, expected_first_node in [
        (
            [_make_running_node("n_w", 2, labels={"role": "w"})],
            [req_anti_w, req_only_foo],
            "n_w",
        ),
        (
            [
                _make_running_node("n_w", 3, labels={"role": "w"}),
                _make_running_node("n_foo", 1, labels={"foo": "bar"}),
            ],
            [req_anti_w, req_fb_foo],
            "n_foo",
        ),
        (
            [
                _make_running_node("n_w", 2),
                _make_running_node("n_foo", 2, labels={"foo": "bar"}),
            ],
            [req_anti_u, req_only_foo],
            "n_w",
        ),
    ]:
        cached = _run_best_node_rounds(nodes_mp, reqs_mp, True)[:2]
        uncached = _run_best_node_rounds(nodes_mp, reqs_mp, False)[:2]
        assert cached == uncached and not cached[1]
        assert cached[0][0][0] == expected_first_node


@pytest.mark.parametrize(
    "mode",
    [
        "pinned",
        "slice",
        "spread",
        "co_pack",
        "multi_shape",
        "fallback",
        "shared_fallback",
    ],
)
def test_label_selector_attempt_cache_linear_deepcopy(mode, monkeypatch):
    """Scheduling N label-constrained bundles across N candidate nodes performs O(N)
    SchedulingNode deepcopies across pinned, spread, co-pack, and fallback pools."""
    num_hosts = 64
    cfg = _nt("tpu", {"CPU": 8, "TPU": 4}, max_workers=num_hosts * 2)
    nodes = []
    for s_idx in (0, 1) if mode == "fallback" else (0,):
        for i in range(num_hosts):
            node = SchedulingNode.from_node_config(
                cfg, SchedulingNodeStatus.SCHEDULABLE, NodeKind.WORKER
            )
            node.labels = {
                "ray.io/tpu-worker-id": str(i),
                "ray.io/tpu-slice-name": f"slice-{s_idx}",
            }
            nodes.append(node)

    if mode in ("pinned", "fallback"):
        fb = (
            [[("ray.io/tpu-slice-name", _IN, ["slice-1"])]]
            if mode == "fallback"
            else []
        )
        requests = [
            ResourceRequestUtil.make(
                {"TPU": 4},
                label_selectors=[
                    [
                        ("ray.io/tpu-slice-name", _IN, ["slice-0"]),
                        ("ray.io/tpu-worker-id", _IN, [str(i)]),
                    ],
                    *fb,
                ],
            )
            for i in range(num_hosts)
        ]
    elif mode == "shared_fallback":
        requests = [
            ResourceRequestUtil.make(
                {"TPU": 4, f"job_{i}": 0},
                label_selectors=[
                    [("ray.io/tpu-worker-id", _IN, [str(i)])],
                    [("ray.io/tpu-slice-name", _IN, ["slice-0"])],
                ],
            )
            for i in range(num_hosts)
        ]
    else:
        slice_sel = [[("ray.io/tpu-slice-name", _IN, ["slice-0"])]]
        constraints = [(_ANTI, "_PG_1", "")] if mode == "spread" else []
        extra_reqs = (
            [_req(8)] * num_hosts
            if mode == "co_pack"
            else [ResourceRequestUtil.make({"CPU": 8}, label_selectors=slice_sel)]
            * num_hosts
            if mode == "multi_shape"
            else []
        )
        requests = [
            ResourceRequestUtil.make(
                {"TPU": 4},
                constraints=constraints,
                label_selectors=slice_sel,
            )
        ] * num_hosts + extra_reqs

    ctx = ResourceDemandScheduler.ScheduleContext(
        nodes=nodes,
        node_type_configs={"tpu": cfg},
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
        disable_launch_config_check=True,
    )
    with _spy_deepcopy(monkeypatch) as copies:
        out_nodes, infeasible = ResourceDemandScheduler._try_schedule(
            ctx, requests, ResourceRequestSource.PENDING_DEMAND
        )
    assert not infeasible
    expected_per_primary = 2 if mode in ("co_pack", "multi_shape") else 1
    scheduled_counts = sorted(
        len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
        for n in out_nodes
    )
    expected_counts = ([0] * num_hosts if mode == "fallback" else []) + [
        expected_per_primary
    ] * num_hosts
    assert scheduled_counts == expected_counts and copies[0] <= num_hosts


def test_label_selector_pending_node_dedup_and_shared_fallback_latency(monkeypatch):
    """Identical QUEUED/ALLOCATED pending nodes are deduplicated by NodeStateCache
    before label scoring, keeping deepcopy and label-check counts bounded."""
    num_nodes = 64
    cfg = _nt("tpu", {"CPU": 4, "TPU": 4}, {"accel": "A"}, max_workers=num_nodes)
    instances = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_id=f"pending-{i}",
                instance_type="tpu",
                status=Instance.QUEUED if i % 2 == 0 else Instance.ALLOCATED,
            ),
            cloud_instance_id=f"c-{i}",
        )
        for i in range(num_nodes)
    ]
    requests = [
        ResourceRequestUtil.make(
            {"TPU": 4, f"job_{i}": 0},
            label_selectors=[
                [("accel", _IN, ["A", f"alt_{i}"])],
                [("accel", _IN, ["B"])],
            ],
        )
        for i in range(num_nodes)
    ]
    orig_first = SchedulingNode._first_matching_selector
    with _spy_deepcopy(monkeypatch) as copies, patch.object(
        SchedulingNode,
        "_first_matching_selector",
        autospec=True,
        side_effect=orig_first,
    ) as mock_first:
        reply = ResourceDemandScheduler(event_logger).schedule(
            sched_request({"tpu": cfg}, resource_requests=requests, instances=instances)
        )
    assert _launch_and_terminate(reply) == ({}, [])
    assert not reply.infeasible_resource_requests
    assert copies[0] <= num_nodes
    assert 0 < mock_first.call_count <= num_nodes * (num_nodes + 1) // 2 + num_nodes


def test_selected_node_leaves_attempt_cache():
    """Evicting the selected node from `attempts` prevents CPython id() reuse from
    inheriting a stale _NodeAttempt."""
    src = ResourceRequestSource.PENDING_DEMAND
    nodes = [
        _make_running_node(f"r{i}", {"CPU": 2}, labels={"accel": "A"}) for i in range(3)
    ]
    requests = [_req(1, "A")] * 4
    attempts = {}
    while requests:
        best, requests, nodes = ResourceDemandScheduler._sched_best_node(
            requests, nodes, src, {}, {}, None, attempts
        )
        assert best is not None and set(attempts) <= {id(n) for n in nodes}


def test_label_selector_attempt_cache_invalidation_and_blockers(monkeypatch):
    """Verify _NodeAttempt invalidation, pruning, and blocker caching across gang
    interleaving, duplicate-run prefix shifts, intermediate vs tail rejections,
    dual blockers, unconstrained pruning, and active-blocker hit/miss boundaries."""
    scheduler = ResourceDemandScheduler(event_logger)
    req_x = _req(1, selectors=[[("x", _IN, ["1"])]])
    req_y = _req(1, selectors=[[("y", _IN, ["1"])]])
    gang_cfgs = _nts(
        ("nx", 3, {"x": "1"}, 1),
        ("nxy", 1, {"x": "1", "y": "1"}, 1),
        ("nx1", 1, {"x": "1"}, 1),
        ("ny_extra", 1, {"y": "1"}, 5),
    )
    for max_num_nodes in (None, 3):
        rep_gang = scheduler.schedule(
            sched_request(
                gang_cfgs,
                max_num_nodes=max_num_nodes,
                instances=[
                    _make_running_instance("n1", "nx", 3, 1, {"x": "1"}),
                    _make_running_instance("n2", "nxy", 1, 1, {"x": "1", "y": "1"}),
                    _make_running_instance("n3", "nx1", 1, 1, {"x": "1"}),
                ],
                gang_resource_requests=[[req_x, req_y, req_x]],
            )
        )
        assert _launch_and_terminate(rep_gang)[0] == {}
        assert not rep_gang.infeasible_gang_resource_requests

    s0 = _req(1, selectors=[[("y", _NOT_IN, ["0"])]])
    s1 = _req(
        1,
        selectors=[
            [("y", _NOT_IN, ["0"]), ("z", _NOT_IN, ["1"])],
            [("y", _NOT_IN, ["1"]), ("z", _NOT_IN, ["1"])],
        ],
    )
    r_u = _req(1)
    for nodes_case, reqs_case, spy_nid, expected_copies in [
        # Duplicate-run prefix shift [s0, s0, s1, s1, s0, s0] -> [s0, s1, s1, s0, s0].
        (
            [
                _make_running_node("p0", 4, 1, {"y": "1", "z": "1"}),
                _make_running_node("p1", 2, 2, {"y": "1"}),
                _make_running_node("p2", 1, 1, {"y": "0"}),
                _make_running_node("p3", 2, 2, {"y": "1", "z": "1"}),
            ],
            [s0, s0, s1, s1, s0, s0],
            "p1",
            2,
        ),
        # Blocker removal frees primary node b1 to co-pack earlier unlabeled r_u.
        (
            [
                _make_running_node("b0", 4, 1, {"x": "0"}),
                _make_running_node("b1", 2, 2, {"x": "1"}),
            ],
            [r_u, req_x, _req(1, selectors=[[("x", _IN, ["0"])], [("x", _IN, ["1"])]])],
            "b1",
            2,
        ),
        # Intermediate multi-resource rejection invalidates n_mix (2 copies).
        (
            [
                _make_running_node(
                    "n_gpu", {"CPU": 8, "GPU": 8}, {"CPU": 1, "GPU": 1}, {"accel": "A"}
                ),
                _make_running_node(
                    "n_mix", {"CPU": 4, "GPU": 1}, {"CPU": 4, "GPU": 1}, {"accel": "A"}
                ),
                _make_running_node("n_cpu", 3, 3, {"accel": "A"}),
            ],
            [_req(1, "A", extra={"GPU": 1}), _req(4, "A"), _req(3, "A")],
            "n_mix",
            2,
        ),
        # Post-last tail rejection keeps n_mix cached while unrelated r_o drains first.
        (
            [
                _make_running_node(
                    "n_other",
                    {"CPU": 16, "GPU": 16, "tail": 16},
                    {"CPU": 1, "GPU": 1, "tail": 1},
                    {"accel": "O"},
                ),
                _make_running_node(
                    "n_tail",
                    {"CPU": 8, "GPU": 8, "tail": 8},
                    {"CPU": 1, "GPU": 1, "tail": 1},
                    {"accel": "A"},
                ),
                _make_running_node(
                    "n_mix",
                    {"CPU": 4, "GPU": 1, "tail": 1},
                    {"CPU": 4, "GPU": 1, "tail": 1},
                    {"accel": "A"},
                ),
            ],
            [
                _req(1, "O", extra={"GPU": 1, "tail": 1}),
                _req(2, "A", extra={"GPU": 1}),
                _req(2, "A"),
                _req(1, "A", extra={"GPU": 1, "tail": 1}),
            ],
            "n_mix",
            1,
        ),
        # Empty u_prefix=() with rejected r_u still present on Round 2 hits cache.
        (
            [
                _make_running_node("n_o", 8, 1, {"accel": "O"}),
                _make_running_node("n_a", 2, 2, {"accel": "A"}),
                _make_running_node("n_u", 2, 1),
            ],
            [_req(1, "O"), _req(2, "A"), r_u],
            "n_a",
            1,
        ),
        # Split runs [r_a, r_o, r_a] coalesce to (r_a, 2) in stream_for (1 copy).
        (
            [
                _make_running_node("n_o", 8, 1, {"accel": "O"}),
                _make_running_node("n_a", 2, 2, {"accel": "A"}),
            ],
            [_req(1, "A"), _req(1, "O"), _req(1, "A")],
            "n_a",
            1,
        ),
        # Exhausted run growing [r_a, r_mid, r_a, r_mid] -> [(r_a, 2), (r_mid, 1)]
        # invalidates exact prefix on n_spy (2 copies).
        (
            [
                _make_running_node("n_first", 16, 1, {"tag": "1"}),
                _make_running_node("n_spy", 8, 2, {"accel": "A", "tag": "1"}),
                _make_running_node("n_rest", 4, 1, {"accel": "A", "tag": "1"}),
            ],
            [
                _req(1, "A"),
                _req(1, selectors=[[("tag", _IN, ["1"])]]),
                _req(1, "A"),
                _req(1, selectors=[[("tag", _IN, ["1"])]]),
            ],
            "n_spy",
            2,
        ),
        # Held-back blocker hits cache in Round 2 with active r_u present (2 copies).
        (
            [
                _make_running_node("a1", 8, 1, {"accel": "A"}),
                _make_running_node("a2", 4, 1, {"accel": "A"}),
                _make_running_node("n_c", 2, 2, {"accel": "C"}),
            ],
            [_req(1, "C"), r_u, _req(1, "A", ""), _req(1, "A", "")],
            "n_c",
            2,
        ),
        # Active shape (act == m_idx) fitting spare capacity is not a blocker (1 copy).
        (
            [
                _make_running_node("a1", 8, 8, {"accel": "A"}),
                _make_running_node("n_b", 3, 3, {"accel": "B"}),
            ],
            [_req(8, "A"), _req(1, "B")],
            "n_b",
            1,
        ),
    ]:
        c_out, rem_out, copies_out = _run_best_node_rounds(
            nodes_case, reqs_case, True, monkeypatch, spy_nid
        )
        u_out, rem_u, _ = _run_best_node_rounds(nodes_case, reqs_case, False)
        assert c_out == u_out and not rem_out and not rem_u
        assert copies_out == expected_copies

    # Dual blockers r_ab and r_cb on n_b: c_rejected invalidates n_b in Round 2.
    reqs_dual = [_req(2, "B"), _req(1, "A", "B"), _req(1, "A", "B"), _req(1, "C", "B")]
    nodes_dual = [
        _make_running_node("n_a", 8, 1, {"accel": "A"}),
        _make_running_node("n_c", 2, 1, {"accel": "C"}),
        _make_running_node("n_b", 3, 3, {"accel": "B"}),
        _make_running_node("n_b2", 2, 2, {"accel": "B"}),
    ]
    c_dual, rem_dual, _ = _run_best_node_rounds(nodes_dual, reqs_dual, True)
    assert c_dual == _run_best_node_rounds(nodes_dual, reqs_dual, False)[0]
    assert not rem_dual and [nid for nid, _ in c_dual] == ["n_a", "n_b", "n_c"]

    # Unconstrained-only nodes are pruned (0 copies on 32 unlabeled nodes).
    ctx_prune = ResourceDemandScheduler.ScheduleContext(
        nodes=[_make_running_node("n_con", {"CPU": 2}, labels={"accel": "A"})]
        + [_make_running_node(f"n_un_{i}", {"CPU": 2}) for i in range(32)],
        node_type_configs={"n_con": _nt("n_con", {"CPU": 2}, {"accel": "A"})},
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
        disable_launch_config_check=True,
    )
    with _spy_deepcopy(monkeypatch) as prune_copies:
        _, inf_prune = ResourceDemandScheduler._try_schedule(
            ctx_prune, [_req(1, "A"), r_u], ResourceRequestSource.PENDING_DEMAND
        )
    assert not inf_prune and prune_copies[0] == 1


def _random_scenario(rng: random.Random):
    accels = ["A", "B", "C"]
    use_dyn_overlap = rng.random() < 0.2
    nodes = [
        _make_running_node(
            f"n{i}",
            {"CPU": 4.0},
            {"CPU": float(rng.randint(1, 4))},
            {"accel": rng.choice(accels)} if rng.random() < 0.85 else {},
        )
        for i in range(rng.randint(2, 6))
    ]
    requests = []
    for _ in range(rng.randint(2, 6)):
        selectors = [
            []
            if rng.random() < 0.2
            else [("_PG_1", _IN, ["v1"])]
            if (use_dyn_overlap and rng.random() < 0.35)
            else [("accel", _IN, [rng.choice(accels)])]
            for _ in range(rng.randint(0, 3))
        ]
        constraints = [(_ANTI, "_PG_1", "v1")] if rng.random() < 0.25 else None
        requests += [
            _req(rng.choice([1, 1, 2]), constraints=constraints, selectors=selectors)
        ] * rng.randint(1, 4)
    rng.shuffle(requests)
    return nodes, requests


@pytest.mark.parametrize("seed", range(300))
def test_attempt_cache_matches_uncached_scheduling(seed):
    nodes, requests = _random_scenario(random.Random(seed))
    assert (
        _run_best_node_rounds(nodes, requests, True)[:2]
        == _run_best_node_rounds(nodes, requests, False)[:2]
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
