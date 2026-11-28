import copy
import itertools
import os
import random
import sys
import time
from contextlib import contextmanager

# coding: utf-8
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from unittest.mock import patch

import pytest

import ray
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.common import (
    _ACTIVE_CID_STATUSES,
    _TERMINAL_INSTANCE_STATUSES,
    _MultiHostReplicaView,
)
from ray.autoscaler.v2.instance_manager.config import (
    AutoscalingConfig,
    WorkerReplicaConfig,
)
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import CloudInstance
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
    CloudResourceMonitor,
)
from ray.autoscaler.v2.scheduler import (
    IResourceScheduler,
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
    AutoscalingState,
    BundleSelector,
    ClusterResourceConstraint,
    ClusterResourceState,
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


def _synthetic_replica_view(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    instances: Sequence[AutoscalerInstance],
) -> Optional[_MultiHostReplicaView]:
    """Build a `_MultiHostReplicaView` from standalone test `AutoscalerInstance`s,
    matching `Reconciler._scale_cluster` for direct `ResourceDemandScheduler` tests."""
    replica_sizes = {
        nt: cfg.workers_per_replica
        for nt, cfg in node_type_configs.items()
        if cfg.is_multi_worker_replica
    }
    if not replica_sizes or not instances:
        return None
    active_nt_cids = {
        (inst.im_instance.instance_type, inst.im_instance.cloud_instance_id)
        for inst in instances
        if inst.im_instance is not None
        and inst.im_instance.node_kind != NodeKind.HEAD
        and inst.im_instance.instance_type in replica_sizes
        and inst.im_instance.cloud_instance_id
        and inst.im_instance.status in _ACTIVE_CID_STATUSES
    }
    active_nt_rids = {
        (inst.im_instance.instance_type, inst.cloud_instance_replica_id)
        for inst in instances
        if inst.im_instance is not None
        and inst.im_instance.node_kind != NodeKind.HEAD
        and inst.im_instance.instance_type in replica_sizes
        and inst.im_instance.status not in _TERMINAL_INSTANCE_STATUSES
        and inst.cloud_instance_replica_id
    }
    im_instances = []
    cloud_instances = {}
    for inst in instances:
        im = inst.im_instance
        if im is None:
            continue
        im_instances.append(im)
        cid = inst.cloud_instance_id or im.cloud_instance_id
        if (
            not cid
            or im.node_kind == NodeKind.HEAD
            or im.instance_type not in replica_sizes
        ):
            continue
        prov_rid = inst.cloud_instance_replica_id or None
        if im.status not in _TERMINAL_INSTANCE_STATUSES or (
            prov_rid is not None
            and (im.instance_type, prov_rid) in active_nt_rids
            and (im.instance_type, cid) not in active_nt_cids
        ):
            existing = cloud_instances.get(cid)
            if (
                existing is None
                or im.status not in _TERMINAL_INSTANCE_STATUSES
                or (existing.replica_id is None and prov_rid is not None)
            ):
                cloud_instances[cid] = CloudInstance(
                    cloud_instance_id=cid,
                    node_type=im.instance_type,
                    node_kind=im.node_kind,
                    is_running=True,
                    replica_id=prov_rid,
                )
    return _MultiHostReplicaView.build(im_instances, cloud_instances, replica_sizes)


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
        replica_view=_synthetic_replica_view(node_type_configs, instances),
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


def test_max_num_nodes_cancels_only_needed_min_worker_launches():
    node_type_configs = {
        "head": NodeTypeConfig(
            name="head", resources={"CPU": 1}, min_worker_nodes=0, max_worker_nodes=0
        ),
        "type_1": NodeTypeConfig(
            name="type_1", resources={"CPU": 1}, min_worker_nodes=0, max_worker_nodes=5
        ),
        "type_2": NodeTypeConfig(
            name="type_2", resources={"CPU": 1}, min_worker_nodes=2, max_worker_nodes=2
        ),
    }
    instances = []
    for i, (node_type, kind) in enumerate(
        [("head", NodeKind.HEAD)] + [("type_1", NodeKind.WORKER)] * 4
    ):
        instances.append(
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type=node_type,
                    status=Instance.RAY_RUNNING,
                    instance_id=str(i),
                    node_id=f"r{i}",
                    node_kind=kind,
                ),
                ray_node=NodeState(
                    ray_node_type_name=node_type,
                    available_resources={"CPU": 0},
                    total_resources={"CPU": 1},
                    node_id=f"r{i}".encode(),
                ),
            )
        )

    # 5 nodes are running and type_2 needs 2 more, but the cluster holds at most 6.
    request = sched_request(
        node_type_configs=node_type_configs, max_num_nodes=6, instances=instances
    )
    reply = ResourceDemandScheduler(event_logger).schedule(request)

    to_launch, to_terminate = _launch_and_terminate(reply)
    assert to_launch == {"type_2": 1}
    assert to_terminate == []


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
    rep: Optional[str] = None,
    lid: Optional[str] = None,
) -> AutoscalerInstance:
    used = used or {}
    inst = make_autoscaler_instance(
        im_instance=Instance(
            instance_type=nt,
            status=Instance.RAY_RUNNING,
            instance_id=iid,
            node_id=iid.encode(),
            launch_request_id=lid or "",
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
    inst.cloud_instance_replica_id = rep
    return inst


def _pending_node(
    nt: str,
    iid: str,
    status=Instance.REQUESTED,
    rep: Optional[str] = None,
    lid: Optional[str] = None,
) -> AutoscalerInstance:
    inst = make_autoscaler_instance(
        im_instance=Instance(
            instance_type=nt,
            status=status,
            instance_id=iid,
            launch_request_id=lid or "",
        ),
        cloud_instance_id=f"cloud-{iid}" if rep else None,
    )
    inst.cloud_instance_replica_id = rep
    return inst


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
    placeholder_ttl_s: Optional[float] = None,
    unneeded: Optional[Dict[NodeType, List[float]]] = None,
    avail: Optional[Dict[NodeType, float]] = None,
    ippr_specs: Optional[IPPRSpecs] = None,
    ippr_statuses: Optional[Dict[str, IPPRStatus]] = None,
    sched: Optional[ResourceDemandScheduler] = None,
) -> SchedulingReply:
    s = sched or ResourceDemandScheduler(event_logger)
    if unneeded is not None:
        s._pending_unneeded_since_s = {k: list(v) for k, v in unneeded.items()}
    cur_insts = insts or []
    return s.schedule(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=cfgs,
            current_instances=cur_insts,
            gang_resource_requests=gangs or [],
            resource_requests=reqs or [],
            cluster_resource_constraints=crc or [],
            max_num_nodes=max_nodes,
            idle_timeout_s=idle_s,
            pending_idle_timeout_s=pending_idle_s,
            backfill_placeholder_ttl_s=placeholder_ttl_s,
            cloud_resource_availabilities=avail or {},
            ippr_specs=ippr_specs,
            ippr_statuses=ippr_statuses or {},
            replica_view=_synthetic_replica_view(cfgs, cur_insts),
        )
    )


def _schedule_gangs(
    cfgs, gangs, insts=None, avail=None
) -> Tuple[Dict[NodeType, int], int]:
    rep = _sched(cfgs, insts, gangs=gangs, avail=avail)
    return _launch_and_terminate(rep)[0], len(rep.infeasible_gang_resource_requests)


def _mk_ctx(cfgs, insts, gangs=None, max_num_nodes=None):
    req = SchedulingRequest(
        disable_launch_config_check=True,
        node_type_configs=cfgs,
        current_instances=insts,
        gang_resource_requests=gangs or [],
        max_num_nodes=max_num_nodes,
        replica_view=_synthetic_replica_view(cfgs, insts),
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
        WorkerReplicaConfig(
            workers_per_replica=workers_per_replica,
            domain_label_key=_SLICE,
            worker_index_label_key=_WID,
            head_resources=heads,
        )
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
    rep: Optional[str] = None,
    **extra: float,
) -> AutoscalerInstance:
    res = {"CPU": 4, "TPU": 4, **({"TPU-v6e-16-head": 1} if head else {}), **extra}
    lbls = {**TPU_LABELS, _SLICE: s, _WID: str(w)}
    return _run_node("tpu_4x4", iid, res, lbls, used, idle_ms, rep=rep)


def _pending_tpu(
    iid: str,
    status=Instance.ALLOCATED,
    nt: str = "tpu_4x4",
    rep: Optional[str] = None,
) -> AutoscalerInstance:
    return _pending_node(nt, iid, status, rep=rep)


def _booting_slice(idx: int, with_rep: bool = True) -> List[AutoscalerInstance]:
    u = {"CPU": 1, "object_store_memory": 100}
    rep = f"slice-{idx}" if with_rep else None
    return [
        _tpu_host(
            f"s{idx}-0",
            f"slice-{idx}",
            0,
            u,
            object_store_memory=1000,
            rep=rep,
            **{f"slice-{idx}": 1},
        ),
        _tpu_host(f"s{idx}-1", f"slice-{idx}", 1, u, object_store_memory=1000, rep=rep),
        _pending_tpu(f"s{idx}-p2", rep=rep),
        _pending_tpu(f"s{idx}-p3", rep=rep),
    ]


_DEG_B0 = [
    _tpu_host("s0-0", "slice-0", 0, {"TPU": 4}, rep="slice-0"),
    _tpu_host("s0-1", "slice-0", 1, rep="slice-0"),
]
_DEG_B1 = [
    _tpu_host("s1-0", "slice-1", 0, {"TPU": 4}, rep="slice-1"),
    _tpu_host("s1-1", "slice-1", 1, rep="slice-1"),
]
_DEG_B_IDLE = _DEG_B0 + [
    _tpu_host("s1-0", "slice-1", 0, {"CPU": 1}, rep="slice-1"),
    _tpu_host("s1-1", "slice-1", 1, {"CPU": 1}, rep="slice-1"),
    _pending_tpu("p0", rep="slice-1"),
    _pending_tpu("p1", rep="slice-1"),
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
        (TPU_CONFIGS, [_tpu_gang()], _booting_slice(0, with_rep=False), ({}, 0)),
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
            _DEG_B0 + [_pending_tpu(f"p{w}", rep="slice-0") for w in range(2)],
            ({}, 0),
        ),
        (
            TPU_CONFIGS,
            [_tpu_gang((2, 3), pinned="slice-0")],
            [
                _tpu_host("s0-0", "slice-0", 0, {"TPU": 4}),
                _tpu_host("s0-1", "slice-0", 1),
            ]
            + _P4[:2],
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
            [
                _tpu_host("s0-0", "slice-0", 0, {"TPU": 4}),
                _tpu_host("s0-1", "slice-0", 1),
                _tpu_host("s1-0", "slice-1", 0, {"TPU": 4}),
                _tpu_host("s1-1", "slice-1", 1),
            ]
            + _P4,
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
            [_tpu_host("s1-0", "slice-1", 0, idle_ms=5000, rep="slice-1")]
            + [
                _tpu_host(f"s2-{w}", "slice-2", w, idle_ms=5000, rep="slice-2")
                for w in range(3)
            ]
            + [_pending_tpu(f"p{w}", rep="slice-1") for w in range(3)]
            + [_pending_tpu("p3", rep="slice-2")],
            ({}, 0),
        ),
    ],
    ids=[
        "scale_zero_1",
        "scale_zero_2",
        "staggered_1",
        "staggered_1_legacy_no_rep",
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
        "pinned_deg_pairs_boot_legacy_no_rep",
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
        "staggered_2_slices_idle_gt_0",
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
        "idle_scale_down_whole_and_min_workers",
        "pending_idle_priority_and_timestamp_release",
        "staggered_boot_protects_busy_slice",
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

    elif case == "idle_scale_down_whole_and_min_workers":
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

    elif case == "pending_idle_priority_and_timestamp_release":
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

    elif case == "staggered_boot_protects_busy_slice":
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
    else:
        raise AssertionError(f"Unhandled case: {case}")


@pytest.mark.parametrize(
    "case",
    [
        "slot_norm_and_wid_selectors",
        "staggered_subslice_and_cross_gang_tiers",
        "slot_progression_and_head_normalization",
        "fallback_capacity_and_provider_boundary",
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
        stag = (
            [
                _tpu_host(
                    f"s{s}-{w}",
                    f"slice-{s}",
                    w,
                    {"TPU": 4} if (s, w) not in ((0, 2), (2, 3)) else None,
                    head=(w == 0),
                    rep=f"slice-{s}",
                )
                for s, ws in ((0, (0, 1, 2)), (1, (1, 2)), (2, (1, 2, 3)))
                for w in ws
            ]
            + [_pending_tpu("p0", rep="slice-0")]
            + [_pending_tpu("p1", rep="slice-1"), _pending_tpu("p2", rep="slice-1")]
            + [_pending_tpu("p3", rep="slice-2")]
        )
        assert _schedule_gangs(
            {"tpu_4x4": _tpu_config(max_workers=12)},
            [_tpu_gang((None, None))] * 3,
            stag,
        ) == ({}, 0)
        assert _schedule_gangs(TPU_CONFIGS, [_tpu_gang((None, None))] * 3, stag) == (
            {},
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

    elif case == "slot_progression_and_head_normalization":
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

    elif case == "fallback_capacity_and_provider_boundary":
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
    else:
        raise AssertionError(f"Unhandled case: {case}")


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
    else:
        raise AssertionError(f"Unhandled case: {case}")


@pytest.mark.parametrize(
    "case",
    [
        "divisibility_and_bounds",
        "npu_tier_vs_running",
        "domainless_replica_group_scaling",
    ],
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
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key="v/rack",
                worker_index_label_key="v/slot",
                head_resources=("ldr",),
            ),
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

    elif case == "domainless_replica_group_scaling":

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
        assert _launch_and_terminate(_sched(_lws(mn=12), da)) == ({"lws": 6}, [])

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
    else:
        raise AssertionError(f"Unhandled case: {case}")


@pytest.mark.skipif(sys.platform == "win32", reason="KubeRay config skipped on Windows")
def test_kuberay_cr_to_scheduler_and_reconciler_full_stack_seam(monkeypatch):
    """Unmocked seam test: `get_basic_ray_cr` -> `_derive_autoscaling_config_from_ray_cr`
    -> `AutoscalingConfig` -> `ResourceDemandScheduler` -> `Reconciler`."""
    from collections import Counter, defaultdict

    from ray.autoscaler._private.kuberay.autoscaling_config import (
        _derive_autoscaling_config_from_ray_cr,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
    from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
    from ray.autoscaler.v2.instance_manager.node_provider import (
        CloudInstance,
        ICloudInstanceProvider,
    )
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
    from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
    from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
        CloudResourceMonitor,
    )
    from ray.core.generated.autoscaler_pb2 import ClusterResourceState
    from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

    forbidden_proto_accesses: List[str] = []

    class _ZeroProtoInstanceProxy:
        """Blocks reads of removed proto fields so local stale pb2 cannot hide bugs."""

        def __init__(self, wrapped):
            self._wrapped = wrapped

        def __getattr__(self, name):
            if name == "cloud_instance_replica_id":
                forbidden_proto_accesses.append(name)
                raise AttributeError(
                    "Instance proto has no 'cloud_instance_replica_id' field"
                )
            return getattr(self._wrapped, name)

    class _StateProxy:
        def __init__(self, state):
            self.instances = [_ZeroProtoInstanceProxy(i) for i in state.instances]
            self.version = state.version

    class _ReplyProxy:
        def __init__(self, reply):
            self.status = reply.status
            self.state = _StateProxy(reply.state)

    class _ZeroProtoInstanceManagerProxy:
        def __init__(self, wrapped: InstanceManager):
            self._wrapped = wrapped

        def get_instance_manager_state(self, request=None):
            return _ReplyProxy(
                self._wrapped.get_instance_manager_state(request=request)
            )

        def update_instance_manager_state(self, request):
            return self._wrapped.update_instance_manager_state(request)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    class _SeamCloudProvider(ICloudInstanceProvider):
        def __init__(self, instances: Dict[str, CloudInstance]):
            self._instances = instances
            self.begin_steps = 0
            self.end_steps = 0

        def reconcile_step(self, step_ctx):
            from contextlib import contextmanager

            @contextmanager
            def _cm():
                self.begin_steps += 1
                try:
                    yield
                finally:
                    self.end_steps += 1

            return _cm()

        def update_reconcile_step(self, step_ctx) -> None:
            self.begin_steps += 1

        def get_non_terminated(self):
            return dict(self._instances)

        def terminate(self, ids, request_id):
            return None

        def launch(self, shape, request_id):
            return None

        def poll_errors(self):
            return []

    class _SpyScheduler(ResourceDemandScheduler):
        def __init__(self):
            super().__init__()
            self.seen_requests: List[SchedulingRequest] = []

        def schedule(self, request: SchedulingRequest) -> SchedulingReply:
            self.seen_requests.append(request)
            return super().schedule(request)

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
    monkeypatch.delenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", raising=False)
    assert acfg.get_max_concurrent_launches() == sys.maxsize
    monkeypatch.setenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", "10")
    assert acfg.get_max_concurrent_launches() == 10
    nt_cfgs = acfg.get_node_type_configs()
    assert nt_cfgs["v6e-16"].replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key=_SLICE,
        domain_exclusive=True,
        worker_index_label_key=_WID,
        head_resources=("TPU-v6e-16-head",),
    )
    assert nt_cfgs["v6e-16"].runtime_label_keys == (_SLICE, _WID)
    assert nt_cfgs["tpu7x-ds"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2,
        domain_label_key=_SLICE,
        domain_exclusive=True,
        worker_index_label_key=_WID,
        head_resources=("TPU-v7x-16-head",),
    )
    assert nt_cfgs["v6e-4"].replica_config == WorkerReplicaConfig(
        workers_per_replica=1,
        domain_label_key=_SLICE,
        domain_exclusive=True,
        worker_index_label_key=_WID,
        head_resources=("TPU-v6e-4-head",),
    )
    assert nt_cfgs["gpu-lws"].replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key=None,
        domain_exclusive=True,
        worker_index_label_key=None,
        head_resources=(),
    )
    assert nt_cfgs["gpu-lws"].runtime_label_keys == ()

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
    im_proxy = _ZeroProtoInstanceManagerProxy(im)
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
    provider = _SeamCloudProvider(cloud)
    spy_scheduler = _SpyScheduler()
    p_idx = 0
    rep_seq: Dict[str, int] = defaultdict(int)

    def _tick(ns_list, demand=True):
        crs = ClusterResourceState(
            node_states=ns_list,
            pending_gang_resource_requests=gangs if demand else [],
            pending_resource_requests=tasks if demand else [],
        )
        Reconciler.reconcile(
            instance_manager=im_proxy,
            scheduler=spy_scheduler,
            cloud_provider=provider,
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
        by_batch: Dict[Tuple[str, str], List[Instance]] = defaultdict(list)
        for i in sorted(insts.values(), key=lambda x: x.instance_id):
            if i.status == Instance.REQUESTED:
                by_batch[(i.instance_type, i.launch_request_id)].append(i)
        for (nt, lid), group in sorted(by_batch.items()):
            size = nt_cfgs[nt].replica_config.workers_per_replica
            for start in range(0, len(group), size):
                rep_id = f"{nt}-rep-{rep_seq[nt]}"
                rep_seq[nt] += 1
                for inst in group[start : start + size]:
                    p_idx += 1
                    cid = f"{nt}-p{p_idx}"
                    cloud[cid] = CloudInstance(
                        cid,
                        nt,
                        NodeKind.WORKER,
                        True,
                        request_id=lid or None,
                        replica_id=rep_id,
                    )

    i1 = _tick([hns])
    assert _cnt(i1, Instance.REQUESTED) == {"v6e-16": 8, "tpu7x-ds": 2}
    assert _cnt(i1, Instance.QUEUED) == {"gpu-lws": 4, "v6e-4": 1}
    v6e_lids = Counter(
        i.launch_request_id
        for i in i1.values()
        if i.instance_type == "v6e-16" and i.status == Instance.REQUESTED
    )
    assert all(lid and cnt % 4 == 0 for lid, cnt in v6e_lids.items())
    _alloc(i1)
    i2 = _tick([hns])
    assert _cnt(i2, Instance.ALLOCATED) == {"v6e-16": 8, "tpu7x-ds": 2}
    assert _cnt(i2, Instance.REQUESTED) == {"gpu-lws": 4, "v6e-4": 1}
    # Verify Reconciler forwarded CloudInstance.replica_id in memory onto
    # AutoscalerInstance.cloud_instance_replica_id on Tick 2 without proto reads.
    tick2_alloc = [
        ai
        for ai in spy_scheduler.seen_requests[-1].current_instances
        if ai.im_instance is not None
        and ai.im_instance.status == Instance.ALLOCATED
        and ai.im_instance.node_kind == NodeKind.WORKER
    ]
    assert len(tick2_alloc) == 10
    assert all(
        ai.cloud_instance_replica_id == cloud[ai.cloud_instance_id].replica_id
        and bool(ai.cloud_instance_replica_id)
        for ai in tick2_alloc
    )

    _alloc(i2)
    i3 = _tick([hns])
    tick3_alloc = [
        ai
        for ai in spy_scheduler.seen_requests[-1].current_instances
        if ai.im_instance is not None
        and ai.im_instance.status == Instance.ALLOCATED
        and ai.im_instance.node_kind == NodeKind.WORKER
    ]
    assert len(tick3_alloc) == 15
    assert all(
        ai.cloud_instance_replica_id == cloud[ai.cloud_instance_id].replica_id
        and bool(ai.cloud_instance_replica_id)
        for ai in tick3_alloc
    )
    cids: Dict[str, List[str]] = defaultdict(list)
    for i in sorted(
        i3.values(),
        key=lambda x: (
            (cloud[x.cloud_instance_id].replica_id or "")
            if x.cloud_instance_id in cloud
            else "",
            x.cloud_instance_id,
        ),
    ):
        if i.node_kind != NodeKind.HEAD and i.status == Instance.ALLOCATED:
            cids[i.instance_type].append(i.cloud_instance_id)

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
    assert provider.begin_steps >= 4
    assert provider.end_steps == 4
    assert forbidden_proto_accesses == []


@pytest.mark.skipif(sys.platform == "win32", reason="KubeRay config skipped on Windows")
def test_kuberay_cr_shared_clique_and_kuberay_provider_seam():
    """End-to-end unmocked seam: multi-host RayCluster CR with labelRefs
    -> KubeRayProvider.get_non_terminated() -> Reconciler -> ResourceDemandScheduler."""
    from collections import Counter
    from unittest.mock import MagicMock

    from ray.autoscaler._private.kuberay.autoscaling_config import (
        _derive_autoscaling_config_from_ray_cr,
    )
    from ray.autoscaler._private.kuberay.node_provider import (
        KUBERAY_KIND_HEAD,
        KUBERAY_KIND_WORKER,
        KUBERAY_LABEL_KEY_KIND,
        KUBERAY_LABEL_KEY_TYPE,
        IKubernetesHttpApiClient,
    )
    from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
        KUBERAY_LABEL_KEY_REPLICA_NAME,
        KubeRayProvider,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
    from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
    from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
    from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
        CloudResourceMonitor,
    )
    from ray.core.generated.autoscaler_pb2 import ClusterResourceState
    from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

    forbidden_proto_accesses: List[str] = []

    class _ZeroProtoInstanceProxy:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def __getattr__(self, name):
            if name == "cloud_instance_replica_id":
                forbidden_proto_accesses.append(name)
                raise AttributeError(
                    "Instance proto has no 'cloud_instance_replica_id' field"
                )
            return getattr(self._wrapped, name)

    class _StateProxy:
        def __init__(self, state):
            self.instances = [_ZeroProtoInstanceProxy(i) for i in state.instances]
            self.version = state.version

    class _ReplyProxy:
        def __init__(self, reply):
            self.status = reply.status
            self.state = _StateProxy(reply.state)

    class _ZeroProtoInstanceManagerProxy:
        def __init__(self, wrapped: InstanceManager):
            self._wrapped = wrapped

        def get_instance_manager_state(self, request=None):
            return _ReplyProxy(
                self._wrapped.get_instance_manager_state(request=request)
            )

        def update_instance_manager_state(self, request):
            return self._wrapped.update_instance_manager_state(request)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    class _InMemoryK8sClient(IKubernetesHttpApiClient):
        def __init__(self, cr: Dict[str, Any], pods: List[Dict[str, Any]]):
            self._cr = cr
            self._pods = pods
            self.patches: List[Any] = []

        def get(self, path: str) -> Dict[str, Any]:
            if "pods" in path:
                return {"metadata": {"resourceVersion": "1"}, "items": list(self._pods)}
            if "rayclusters" in path:
                return copy.deepcopy(self._cr)
            raise NotImplementedError(path)

        def patch(self, path: str, patches, content_type="application/json-patch+json"):
            self.patches.append((path, patches))
            for op in patches:
                parts = op["path"].strip("/").split("/")
                if len(parts) == 4 and parts[1] == "workerGroupSpecs":
                    idx = int(parts[2])
                    if parts[3] == "replicas":
                        self._cr["spec"]["workerGroupSpecs"][idx]["replicas"] = op[
                            "value"
                        ]
            return copy.deepcopy(self._cr)

        def delete(self, path: str) -> Dict[str, Any]:
            return {}

    class _SpyScheduler(ResourceDemandScheduler):
        def __init__(self):
            super().__init__()
            self.seen_requests: List[SchedulingRequest] = []

        def schedule(self, request: SchedulingRequest) -> SchedulingReply:
            self.seen_requests.append(request)
            return super().schedule(request)

    ray_cr = get_basic_ray_cr()
    ray_cr["metadata"]["name"] = "shared-clique-cluster"
    ray_cr["metadata"]["namespace"] = "default"
    ray_cr["spec"]["headGroupSpec"]["rayStartParams"] = {"num-cpus": "0"}
    base = ray_cr["spec"]["workerGroupSpecs"][0]

    gpu_wg = copy.deepcopy(base)
    gpu_wg.update(
        groupName="gpu-clique",
        replicas=0,
        minReplicas=0,
        maxReplicas=3,
        numOfHosts=2,
        rayStartParams={},
        labelRefs=[
            {
                "name": "ray.io/gpu-clique",
                "valueFrom": {
                    "nodeRef": {"fieldPath": "metadata.labels['nvidia.com/gpu.clique']"}
                },
            }
        ],
    )
    gpu_wg["template"].setdefault("metadata", {})["annotations"] = {}
    gpu_wg["template"]["spec"]["nodeSelector"] = {}
    gpu_wg["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "4"},
        "requests": {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "4"},
    }

    cpu_wg = copy.deepcopy(base)
    cpu_wg.update(
        groupName="cpu",
        replicas=0,
        minReplicas=0,
        maxReplicas=4,
        numOfHosts=1,
        rayStartParams={},
    )
    cpu_wg["template"].setdefault("metadata", {})["annotations"] = {}
    cpu_wg["template"]["spec"]["nodeSelector"] = {}
    cpu_wg["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"cpu": "8", "memory": "16Gi"},
        "requests": {"cpu": "8", "memory": "16Gi"},
    }

    ray_cr["spec"]["workerGroupSpecs"] = [gpu_wg, cpu_wg]
    acfg = AutoscalingConfig(
        configs=_derive_autoscaling_config_from_ray_cr(ray_cr), skip_content_hash=True
    )
    nt_cfgs = acfg.get_node_type_configs()
    assert nt_cfgs["gpu-clique"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2
    )
    assert nt_cfgs["gpu-clique"].runtime_label_keys == ("ray.io/gpu-clique",)
    assert nt_cfgs["cpu"].runtime_label_keys == ()

    def _pod(name: str, kind: str, nt: str, replica_name: Optional[str] = None):
        labels = {
            KUBERAY_LABEL_KEY_KIND: kind,
            KUBERAY_LABEL_KEY_TYPE: nt,
        }
        if replica_name is not None:
            labels[KUBERAY_LABEL_KEY_REPLICA_NAME] = replica_name
        return {
            "metadata": {"name": name, "labels": labels},
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {"name": "ray-worker", "ready": True, "state": {"running": {}}}
                ],
            },
        }

    pods = [_pod("hpod", KUBERAY_KIND_HEAD, "headgroup")]
    k8s_client = _InMemoryK8sClient(ray_cr, pods)
    provider = KubeRayProvider(
        cluster_name="shared-clique-cluster",
        provider_config={"namespace": "default", "head_node_type": "headgroup"},
        gcs_client=MagicMock(),
        k8s_api_client=k8s_client,
    )

    storage = InstanceStorage(cluster_id="shared-seam", storage=InMemoryStorage())
    im = InstanceManager(
        instance_storage=storage, instance_status_update_subscribers=[]
    )
    im_proxy = _ZeroProtoInstanceManagerProxy(im)
    spy_scheduler = _SpyScheduler()
    hnid = b"h".ljust(28, b"\0")
    assert storage.batch_upsert_instances(
        [
            Instance(
                instance_id="h0",
                instance_type="headgroup",
                status=Instance.RAY_RUNNING,
                node_kind=NodeKind.HEAD,
                node_id=hnid.hex(),
                cloud_instance_id="hpod",
                version=0,
            )
        ],
        expected_storage_version=0,
    )[0]
    hns = NodeState(
        node_id=hnid,
        instance_id="hpod",
        ray_node_type_name="headgroup",
        status=NodeStatus.RUNNING,
    )

    def _clique_gang(pg_name: str) -> GangResourceRequest:
        reqs = []
        for _ in range(2):
            r = ResourceRequest(resources_bundle={"CPU": 8.0})
            pc = r.placement_constraints.add()
            pc.anti_affinity.label_name = f"_pg_{pg_name}"
            pc.anti_affinity.label_value = ""
            reqs.append(r)
        return GangResourceRequest(
            bundle_selectors=[
                BundleSelector(
                    resource_requests=reqs,
                    locality_requirement=LocalityRequirement(
                        locality_constraint=LocalityConstraint(
                            label_name="ray.io/gpu-clique",
                            placement_strategy=PlacementStrategy.STRICT_PACK,
                        )
                    ),
                )
            ]
        )

    # Tick 1: Two 2-host CPU-only gangs requesting ray.io/gpu-clique must launch
    # 4 gpu-clique workers (2 replicas) and 0 cpu workers.
    cloud1 = provider.get_non_terminated()
    Reconciler.reconcile(
        instance_manager=im_proxy,
        scheduler=spy_scheduler,
        cloud_provider=provider,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_cluster_resource_state=ClusterResourceState(
            node_states=[hns],
            pending_gang_resource_requests=[_clique_gang("pg0"), _clique_gang("pg1")],
        ),
        non_terminated_cloud_instances=cloud1,
        cloud_provider_errors=[],
        ray_install_errors=[],
        autoscaling_config=acfg,
    )
    i1 = storage.get_instances()[0]
    assert Counter(
        i.instance_type
        for i in i1.values()
        if i.node_kind != NodeKind.HEAD and i.status == Instance.REQUESTED
    ) == {"gpu-clique": 4}

    # Materialize 2 provider replicas in Kubernetes pods with KUBERAY_LABEL_KEY_REPLICA_NAME.
    for rep_idx in range(2):
        for host_idx in range(2):
            pods.append(
                _pod(
                    f"gc-r{rep_idx}-w{host_idx}",
                    KUBERAY_KIND_WORKER,
                    "gpu-clique",
                    replica_name=f"clique-rep-{rep_idx}",
                )
            )
    cloud2 = provider.get_non_terminated()
    assert cloud2["gc-r0-w0"].replica_id == "clique-rep-0"
    assert cloud2["gc-r0-w1"].replica_id == "clique-rep-0"
    assert cloud2["gc-r1-w0"].replica_id == "clique-rep-1"
    assert cloud2["gc-r1-w1"].replica_id == "clique-rep-1"

    # Tick 2: Both replicas join the SAME shared clique "clique-0"; clique-rep-0 is
    # busy while clique-rep-1 is idle past timeout. Only clique-rep-1 scales down.
    m16 = float(16 * 1024**3)
    res = {"CPU": 8.0, "GPU": 4.0, "memory": m16}
    ns_list = [hns]
    for seq, (cid, busy) in enumerate(
        [
            ("gc-r0-w0", True),
            ("gc-r0-w1", False),
            ("gc-r1-w0", False),
            ("gc-r1-w1", False),
        ],
        start=1,
    ):
        ns_list.append(
            NodeState(
                node_id=f"c{seq}".encode().ljust(28, b"\0"),
                instance_id=cid,
                ray_node_type_name="gpu-clique",
                status=NodeStatus.RUNNING if busy else NodeStatus.IDLE,
                idle_duration_ms=0 if busy else 600_000,
                total_resources=res,
                available_resources={**res, **({"GPU": 0.0} if busy else {})},
                labels={"ray.io/gpu-clique": "clique-0"},
            )
        )
    Reconciler.reconcile(
        instance_manager=im_proxy,
        scheduler=spy_scheduler,
        cloud_provider=provider,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_cluster_resource_state=ClusterResourceState(node_states=ns_list),
        non_terminated_cloud_instances=cloud2,
        cloud_provider_errors=[],
        ray_install_errors=[],
        autoscaling_config=acfg,
    )
    i2 = storage.get_instances()[0]
    stopped = {
        i.cloud_instance_id
        for i in i2.values()
        if i.status == Instance.RAY_STOP_REQUESTED
    }
    running = {
        i.cloud_instance_id
        for i in i2.values()
        if i.node_kind != NodeKind.HEAD and i.status == Instance.RAY_RUNNING
    }
    assert stopped == {"gc-r1-w0", "gc-r1-w1"}
    assert running == {"gc-r0-w0", "gc-r0-w1"}
    assert forbidden_proto_accesses == []


@pytest.mark.skipif(sys.platform == "win32", reason="KubeRay config skipped on Windows")
def test_kuberay_cr_single_host_gpu_clique_label_refs_seam():
    """Single-host (numOfHosts=1) RayCluster CR with labelRefs scales from zero,
    packs into a live clique without over-provisioning, and scales down per-node."""
    from collections import Counter
    from unittest.mock import MagicMock

    from ray.autoscaler._private.kuberay.autoscaling_config import (
        _derive_autoscaling_config_from_ray_cr,
    )
    from ray.autoscaler._private.kuberay.node_provider import (
        KUBERAY_KIND_HEAD,
        KUBERAY_KIND_WORKER,
        KUBERAY_LABEL_KEY_KIND,
        KUBERAY_LABEL_KEY_TYPE,
        IKubernetesHttpApiClient,
    )
    from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
        KubeRayProvider,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
    from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
    from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
    from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
        CloudResourceMonitor,
    )
    from ray.core.generated.autoscaler_pb2 import ClusterResourceState
    from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

    class _InMemoryK8sClient(IKubernetesHttpApiClient):
        def __init__(self, cr: Dict[str, Any], pods: List[Dict[str, Any]]):
            self._cr = cr
            self._pods = pods

        def get(self, path: str) -> Dict[str, Any]:
            if "pods" in path:
                return {"metadata": {"resourceVersion": "1"}, "items": list(self._pods)}
            if "rayclusters" in path:
                return copy.deepcopy(self._cr)
            raise NotImplementedError(path)

        def patch(self, path: str, patches, content_type="application/json-patch+json"):
            for op in patches:
                parts = op["path"].strip("/").split("/")
                if len(parts) == 4 and parts[1] == "workerGroupSpecs":
                    idx = int(parts[2])
                    if parts[3] == "replicas":
                        self._cr["spec"]["workerGroupSpecs"][idx]["replicas"] = op[
                            "value"
                        ]
            return copy.deepcopy(self._cr)

        def delete(self, path: str) -> Dict[str, Any]:
            return {}

    ray_cr = get_basic_ray_cr()
    ray_cr["metadata"]["name"] = "single-host-clique-cluster"
    ray_cr["metadata"]["namespace"] = "default"
    ray_cr["spec"]["headGroupSpec"]["rayStartParams"] = {"num-cpus": "0"}
    base = ray_cr["spec"]["workerGroupSpecs"][0]

    gpu_wg = copy.deepcopy(base)
    gpu_wg.update(
        groupName="gpu-clique",
        replicas=0,
        minReplicas=0,
        maxReplicas=4,
        numOfHosts=1,
        rayStartParams={},
        labelRefs=[
            {
                "name": "ray.io/gpu-clique",
                "valueFrom": {
                    "nodeRef": {"fieldPath": "metadata.labels['nvidia.com/gpu.clique']"}
                },
            }
        ],
    )
    gpu_wg["template"].setdefault("metadata", {})["annotations"] = {}
    gpu_wg["template"]["spec"]["nodeSelector"] = {}
    gpu_wg["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "4"},
        "requests": {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "4"},
    }

    cpu_wg = copy.deepcopy(base)
    cpu_wg.update(
        groupName="cpu",
        replicas=0,
        minReplicas=0,
        maxReplicas=4,
        numOfHosts=1,
        rayStartParams={},
    )
    cpu_wg["template"].setdefault("metadata", {})["annotations"] = {}
    cpu_wg["template"]["spec"]["nodeSelector"] = {}
    cpu_wg["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"cpu": "8", "memory": "16Gi"},
        "requests": {"cpu": "8", "memory": "16Gi"},
    }

    ray_cr["spec"]["workerGroupSpecs"] = [gpu_wg, cpu_wg]
    acfg = AutoscalingConfig(
        configs=_derive_autoscaling_config_from_ray_cr(ray_cr), skip_content_hash=True
    )
    nt_cfgs = acfg.get_node_type_configs()
    assert nt_cfgs["gpu-clique"].replica_config is None
    assert nt_cfgs["gpu-clique"].runtime_label_keys == ("ray.io/gpu-clique",)
    assert nt_cfgs["cpu"].runtime_label_keys == ()

    def _pod(name: str, kind: str, nt: str):
        return {
            "metadata": {
                "name": name,
                "labels": {
                    KUBERAY_LABEL_KEY_KIND: kind,
                    KUBERAY_LABEL_KEY_TYPE: nt,
                },
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {"name": "ray-worker", "ready": True, "state": {"running": {}}}
                ],
            },
        }

    pods = [_pod("hpod", KUBERAY_KIND_HEAD, "headgroup")]
    k8s_client = _InMemoryK8sClient(ray_cr, pods)
    provider = KubeRayProvider(
        cluster_name="single-host-clique-cluster",
        provider_config={"namespace": "default", "head_node_type": "headgroup"},
        gcs_client=MagicMock(),
        k8s_api_client=k8s_client,
    )
    storage = InstanceStorage(
        cluster_id="single-host-clique", storage=InMemoryStorage()
    )
    im = InstanceManager(
        instance_storage=storage, instance_status_update_subscribers=[]
    )
    scheduler = ResourceDemandScheduler()
    hnid = b"h".ljust(28, b"\0")
    assert storage.batch_upsert_instances(
        [
            Instance(
                instance_id="h0",
                instance_type="headgroup",
                status=Instance.RAY_RUNNING,
                node_kind=NodeKind.HEAD,
                node_id=hnid.hex(),
                cloud_instance_id="hpod",
                version=0,
            )
        ],
        expected_storage_version=0,
    )[0]
    hns = NodeState(
        node_id=hnid,
        instance_id="hpod",
        ray_node_type_name="headgroup",
        status=NodeStatus.RUNNING,
    )

    def _clique_gang(pg_name: str, num_bundles: int) -> GangResourceRequest:
        reqs = []
        for _ in range(num_bundles):
            r = ResourceRequest(resources_bundle={"CPU": 8.0})
            pc = r.placement_constraints.add()
            pc.anti_affinity.label_name = f"_pg_{pg_name}"
            pc.anti_affinity.label_value = ""
            reqs.append(r)
        return GangResourceRequest(
            bundle_selectors=[
                BundleSelector(
                    resource_requests=reqs,
                    locality_requirement=LocalityRequirement(
                        locality_constraint=LocalityConstraint(
                            label_name="ray.io/gpu-clique",
                            placement_strategy=PlacementStrategy.STRICT_PACK,
                        )
                    ),
                )
            ]
        )

    # Tick 1: 2-bundle CPU-only gang with ray.io/gpu-clique locality scales
    # 2 single-host gpu-clique workers and 0 cpu workers.
    Reconciler.reconcile(
        instance_manager=im,
        scheduler=scheduler,
        cloud_provider=provider,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_cluster_resource_state=ClusterResourceState(
            node_states=[hns],
            pending_gang_resource_requests=[_clique_gang("pg0", 2)],
        ),
        non_terminated_cloud_instances=provider.get_non_terminated(),
        cloud_provider_errors=[],
        ray_install_errors=[],
        autoscaling_config=acfg,
    )
    i1 = storage.get_instances()[0]
    assert Counter(
        i.instance_type
        for i in i1.values()
        if i.node_kind != NodeKind.HEAD and i.status == Instance.REQUESTED
    ) == {"gpu-clique": 2}

    # Materialize the 2 workers in clique "clique-0" (g0 busy, g1 idle with room).
    pods.extend(
        [
            _pod("g0", KUBERAY_KIND_WORKER, "gpu-clique"),
            _pod("g1", KUBERAY_KIND_WORKER, "gpu-clique"),
        ]
    )
    cloud2 = provider.get_non_terminated()
    m16 = float(16 * 1024**3)
    res = {"CPU": 8.0, "GPU": 4.0, "memory": m16}
    ns_tick2 = [
        hns,
        NodeState(
            node_id=b"g0".ljust(28, b"\0"),
            instance_id="g0",
            ray_node_type_name="gpu-clique",
            status=NodeStatus.RUNNING,
            idle_duration_ms=0,
            total_resources=res,
            available_resources={**res, "CPU": 0.0, "GPU": 0.0},
            labels={"ray.io/gpu-clique": "clique-0"},
        ),
        NodeState(
            node_id=b"g1".ljust(28, b"\0"),
            instance_id="g1",
            ray_node_type_name="gpu-clique",
            status=NodeStatus.IDLE,
            idle_duration_ms=0,
            total_resources=res,
            available_resources=res,
            labels={"ray.io/gpu-clique": "clique-0"},
        ),
    ]

    # Tick 2: A 1-bundle gang with ray.io/gpu-clique locality packs onto existing
    # g1 in clique-0 with zero new launches.
    Reconciler.reconcile(
        instance_manager=im,
        scheduler=scheduler,
        cloud_provider=provider,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_cluster_resource_state=ClusterResourceState(
            node_states=ns_tick2,
            pending_gang_resource_requests=[_clique_gang("pg1", 1)],
        ),
        non_terminated_cloud_instances=cloud2,
        cloud_provider_errors=[],
        ray_install_errors=[],
        autoscaling_config=acfg,
    )
    i2 = storage.get_instances()[0]
    assert Counter(i.status for i in i2.values() if i.node_kind != NodeKind.HEAD) == {
        Instance.RAY_RUNNING: 2
    }

    # Tick 3: With no pending gangs and g1 idle past timeout while g0 stays busy,
    # single-host g1 scales down independently while g0 remains running.
    ns_tick3 = [
        ns_tick2[0],
        ns_tick2[1],
        NodeState(
            node_id=b"g1".ljust(28, b"\0"),
            instance_id="g1",
            ray_node_type_name="gpu-clique",
            status=NodeStatus.IDLE,
            idle_duration_ms=600_000,
            total_resources=res,
            available_resources=res,
            labels={"ray.io/gpu-clique": "clique-0"},
        ),
    ]
    Reconciler.reconcile(
        instance_manager=im,
        scheduler=scheduler,
        cloud_provider=provider,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_cluster_resource_state=ClusterResourceState(node_states=ns_tick3),
        non_terminated_cloud_instances=cloud2,
        cloud_provider_errors=[],
        ray_install_errors=[],
        autoscaling_config=acfg,
    )
    i3 = storage.get_instances()[0]
    stopped = {
        i.cloud_instance_id
        for i in i3.values()
        if i.status == Instance.RAY_STOP_REQUESTED
    }
    running = {
        i.cloud_instance_id
        for i in i3.values()
        if i.node_kind != NodeKind.HEAD and i.status == Instance.RAY_RUNNING
    }
    assert stopped == {"g1"}
    assert running == {"g0"}


def _make_lifecycle_replica(
    rep: str,
    status: Instance.InstanceStatus,
    size: int,
    busy: bool,
    count: int = 4,
    idle_ms: int = 0,
    with_provider_replica_id: bool = True,
    lid: Optional[str] = None,
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
            launch_request_id=lid or "",
            node_kind=NodeKind.WORKER,
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
            idle_duration_ms=idle_ms,
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


def _lifecycle_tpu_cfg(
    size: int = 4, min_w: int = 0, max_w: int = 16
) -> Dict[str, NodeTypeConfig]:
    return {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4, "TPU": 4},
            labels={"ray.io/tpu-slice-name": "", "ray.io/tpu-worker-id": ""}
            if size > 1
            else {},
            min_worker_nodes=min_w,
            max_worker_nodes=max_w,
            replica_config=(
                WorkerReplicaConfig(
                    workers_per_replica=size,
                    domain_label_key="ray.io/tpu-slice-name",
                    worker_index_label_key="ray.io/tpu-worker-id",
                )
                if size > 1
                else None
            ),
        )
    }


@pytest.mark.parametrize("workers_per_replica", [1, 4])
def test_stop_requested_replica_occupancy_and_min_workers(workers_per_replica):
    cfg = _lifecycle_tpu_cfg(workers_per_replica, max_w=8)
    busy_and_draining = _make_lifecycle_replica(
        "rep-0", Instance.RAY_RUNNING, workers_per_replica, busy=True
    ) + _make_lifecycle_replica(
        "rep-1", Instance.RAY_STOP_REQUESTED, workers_per_replica, busy=False
    )
    assert (
        _sched(
            cfg,
            busy_and_draining,
            reqs=[_resource_request({"TPU": 4}, 4)],
        ).to_launch
        == []
    )
    idle_and_draining = _make_lifecycle_replica(
        "rep-0",
        Instance.RAY_STOP_REQUESTED,
        workers_per_replica,
        busy=False,
        idle_ms=60_000,
    ) + _make_lifecycle_replica(
        "rep-1", Instance.RAY_RUNNING, workers_per_replica, busy=False, idle_ms=60_000
    )
    assert _launch_and_terminate(
        _sched(
            _lifecycle_tpu_cfg(workers_per_replica, min_w=4, max_w=8),
            idle_and_draining,
            idle_s=10,
        )
    ) == ({}, [])


def test_multihost_staggered_materialization_and_degraded_termination():
    # Staggered 2-replica materialization (1 running pod in rep-0, 1 allocated pod
    # in rep-1, 6 REQUESTED siblings matching their launch_request_ids) commits 8
    # slots and launches a 3rd replica under max=12 (both per-replica and shared lid).
    for l0, l1 in (("lid-0", "lid-1"), ("lid-batch", "lid-batch")):
        insts_r4 = (
            _make_lifecycle_replica(
                "rep-0", Instance.RAY_RUNNING, 4, busy=True, count=1, lid=l0
            )
            + [_pending_node("tpu", "r1-0", Instance.ALLOCATED, rep="rep-1", lid=l1)]
            + [
                _pending_node("tpu", f"req-0-{i}", Instance.REQUESTED, lid=l0)
                for i in range(3)
            ]
            + [
                _pending_node("tpu", f"req-1-{i}", Instance.REQUESTED, lid=l1)
                for i in range(3)
            ]
        )
        assert _launch_and_terminate(
            _sched(
                _lifecycle_tpu_cfg(4, max_w=12),
                insts_r4,
                reqs=[_resource_request({"TPU": 4}, 11)],
            )
        ) == ({"tpu": 4}, [])

    # Unattributed booting pods join idle degraded non-provider replica atomically.
    mixed_insts = (
        _make_lifecycle_replica(
            "rep-healthy",
            Instance.RAY_RUNNING,
            4,
            busy=True,
            with_provider_replica_id=False,
        )
        + _make_lifecycle_replica(
            "rep-0",
            Instance.RAY_RUNNING,
            4,
            busy=False,
            count=2,
            idle_ms=60_000,
            with_provider_replica_id=False,
        )
        + [
            _pending_node("tpu", iid, Instance.ALLOCATED)
            for iid in ("a0", "a1", "stray")
        ]
    )
    assert {
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, max_w=12),
            mixed_insts,
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [0.0, 0.0, 0.0]},
        ).to_terminate
    } == {"rep-0-0", "rep-0-1", "a0", "a1", "stray"}

    # Degraded running replica terminates before complete idle replica under max cap.
    full_and_deg = _make_lifecycle_replica(
        "full", Instance.RAY_RUNNING, 4, busy=False, count=4, idle_ms=60_000
    ) + _make_lifecycle_replica(
        "deg", Instance.RAY_RUNNING, 4, busy=False, count=3, idle_ms=60_000
    )
    assert sorted(
        t.instance_id
        for t in _sched(_lifecycle_tpu_cfg(4, max_w=4), full_and_deg).to_terminate
    ) == ["deg-0", "deg-1", "deg-2"]

    # Degraded idle replica (with or without provider_replica_id) is never blocked
    # by in-flight REQUESTED launches for new replicas, while an incomplete
    # running replica waits while its REQUESTED siblings are fresh and terminates
    # atomically with them once materialized and expired.
    req4 = [
        _pending_node("tpu", f"req-{i}", Instance.REQUESTED, lid="lid-new")
        for i in range(4)
    ]
    req2_half = [
        _pending_node("tpu", f"req-h-{i}", Instance.REQUESTED, lid="lid-half")
        for i in range(2)
    ]
    now = time.monotonic()
    for with_rep in (True, False):
        deg = _make_lifecycle_replica(
            "deg",
            Instance.RAY_RUNNING,
            4,
            busy=False,
            count=3,
            idle_ms=60_000,
            with_provider_replica_id=with_rep,
            lid="lid-deg",
        )
        assert sorted(
            t.instance_id
            for t in _sched(_lifecycle_tpu_cfg(), deg + req4, idle_s=10).to_terminate
        ) == ["deg-0", "deg-1", "deg-2"]

        half_running = _make_lifecycle_replica(
            "half",
            Instance.RAY_RUNNING,
            4,
            busy=False,
            count=2,
            idle_ms=60_000,
            with_provider_replica_id=with_rep,
            lid="lid-half",
        )
        assert (
            _sched(
                _lifecycle_tpu_cfg(),
                half_running + req2_half,
                idle_s=10,
                pending_idle_s=10,
            ).to_terminate
            == []
        )
        # Once those 2 in-flight launches materialize as ALLOCATED and expire,
        # all 4 slots of the replica terminate atomically.
        alloc2 = [
            _pending_node(
                "tpu",
                f"a-{i}",
                Instance.ALLOCATED,
                rep="half" if with_rep else None,
                lid="lid-half",
            )
            for i in range(2)
        ]
        assert sorted(
            t.instance_id
            for t in _sched(
                _lifecycle_tpu_cfg(),
                half_running + alloc2,
                idle_s=10,
                pending_idle_s=10,
                unneeded={"tpu": [now - 100.0, now - 100.0]},
            ).to_terminate
        ) == ["a-0", "a-1", "half-0", "half-1"]

    # Idle degraded running replica still scales down independently when a purely
    # booting half-allocated replica absorbs its 2 in-flight REQUESTED siblings.
    deg_prov = _make_lifecycle_replica(
        "deg", Instance.RAY_RUNNING, 4, busy=False, count=3, idle_ms=60_000, lid="lid-d"
    )
    half_alloc = [
        _pending_node("tpu", f"g1-{i}", Instance.ALLOCATED, rep="G1", lid="lid-g1")
        for i in range(2)
    ] + [
        _pending_node("tpu", f"req-g1-{i}", Instance.REQUESTED, lid="lid-g1")
        for i in range(2)
    ]
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, max_w=8),
            deg_prov + half_alloc,
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0, now - 100.0]},
        ).to_terminate
    ) == ["deg-0", "deg-1", "deg-2"]

    # Provider-identified running replica never absorbs an unattributed ALLOCATED
    # pod: busy replica does not pin the stray pod's idle timer, and idle replica
    # waits on its own REQUESTED sibling while the stray pod still terminates.
    stray = _pending_node("tpu", "stray", Instance.ALLOCATED)
    deg_busy = _make_lifecycle_replica(
        "deg", Instance.RAY_RUNNING, 4, busy=True, count=3
    )
    assert [
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, max_w=8),
            deg_busy + [stray],
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0]},
        ).to_terminate
    ] == ["stray"]
    req_deg_sib = _pending_node("tpu", "req-d-0", Instance.REQUESTED, lid="lid-d")
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, max_w=8),
            deg_prov + [stray, req_deg_sib],
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0]},
        ).to_terminate
    ) == ["stray"]

    # Provider-identified degraded replicas scale down independently under min=4 even
    # when an unattributed ALLOCATED pod is idle or fresh; conversely, a half-allocated
    # booting replica shields its REQUESTED siblings when fresh and scales down
    # atomically with them once all slots expire.
    deg_a = _make_lifecycle_replica(
        "deg-a", Instance.RAY_RUNNING, 4, busy=False, count=3, idle_ms=90_000
    )
    deg_b = _make_lifecycle_replica(
        "deg-b", Instance.RAY_RUNNING, 4, busy=False, count=3, idle_ms=60_000
    )
    # With backfill placeholders, deg_b commits 4 slots (>= min_w=4), so both
    # the expired stray ALLOCATED pod and the older idle degraded replica deg_a
    # scale down while deg_b is preserved for min_w=4.
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, min_w=4),
            deg_a + deg_b + [stray],
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [0.0]},
        ).to_terminate
    ) == ["deg-a-0", "deg-a-1", "deg-a-2", "stray"]
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, min_w=4),
            deg_a + deg_b + [stray],
            idle_s=10,
            pending_idle_s=10,
        ).to_terminate
    ) == ["deg-a-0", "deg-a-1", "deg-a-2"]

    # Half-allocated booting replica waits while its REQUESTED siblings are in flight.
    assert (
        _sched(
            _lifecycle_tpu_cfg(),
            half_alloc,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0, now - 100.0]},
        ).to_terminate
        == []
    )
    # Once all 4 pods materialize (or its REQUESTED siblings disappear), G1 scales
    # down cleanly even when another slice has REQUESTED launches in flight.
    full_alloc = [
        _pending_node("tpu", f"g1-{i}", Instance.ALLOCATED, rep="G1") for i in range(4)
    ] + req4
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(),
            full_alloc,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0] * 4},
        ).to_terminate
    ) == ["g1-0", "g1-1", "g1-2", "g1-3"]
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(),
            half_alloc[:2],
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0] * 2},
        ).to_terminate
    ) == ["g1-0", "g1-1"]


def test_unlabeled_locality_gang_tight_max_num_nodes_with_booting_replica():
    """Unlabeled locality fallback must not double-count replica backfill."""
    head_cfg = NodeTypeConfig(
        name="head", resources={"CPU": 4}, min_worker_nodes=0, max_worker_nodes=1
    )
    gpu_cfg = NodeTypeConfig(
        name="gpu",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=8,
        replica_config=WorkerReplicaConfig(workers_per_replica=4),
    )
    head = AutoscalerInstance(
        im_instance=Instance(
            instance_id="head",
            instance_type="head",
            status=Instance.RAY_RUNNING,
            node_id="ray-head",
            cloud_instance_id="pod-head",
            node_kind=NodeKind.HEAD,
        ),
        ray_node=NodeState(
            node_id=b"ray-head".ljust(28, b"\0"),
            status=NodeStatus.IDLE,
            total_resources={"CPU": 4},
            available_resources={"CPU": 4},
        ),
        cloud_instance_id="pod-head",
    )
    half_booting = [
        _pending_node("gpu", f"p-{i}", Instance.ALLOCATED, rep="rep-0")
        for i in range(2)
    ]
    mid_boot_full = [
        _run_node(
            "gpu",
            f"r-{i}",
            {"CPU": 8, "GPU": 4},
            {},
            used={"CPU": 8, "GPU": 4},
            rep="rep-0",
        )
        for i in range(2)
    ] + half_booting
    half_busy = [
        _run_node(
            "gpu",
            f"r-{i}",
            {"CPU": 8, "GPU": 4},
            {},
            used={"CPU": 8, "GPU": 4},
            rep="rep-0",
        )
        for i in range(2)
    ]
    half_draining = [
        _run_node("gpu", f"d-{i}", {"CPU": 8, "GPU": 4}, {}, rep="rep-0")
        for i in range(2)
    ] + [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="d-stop",
                instance_type="gpu",
                status=Instance.RAY_STOPPING,
                cloud_instance_id="pod-d-stop",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="pod-d-stop",
            cloud_instance_replica_id="rep-0",
        )
    ]
    reqs4 = []
    for _ in range(4):
        req = ResourceRequest(resources_bundle={"CPU": 4, "GPU": 4})
        pc = req.placement_constraints.add()
        pc.anti_affinity.label_name = "_pg"
        pc.anti_affinity.label_value = "gpu_pg"
        reqs4.append(req)
    gang = GangResourceRequest(
        bundle_selectors=[
            BundleSelector(
                resource_requests=reqs4,
                locality_requirement=LocalityRequirement(
                    locality_constraint=LocalityConstraint(
                        label_name="ray.io/gpu-domain",
                        placement_strategy=PlacementStrategy.STRICT_PACK,
                    )
                ),
            )
        ]
    )
    cfgs = {"head": head_cfg, "gpu": gpu_cfg}
    # A half-booting idle attributed replica commits all 4 slots via backfill
    # placeholders, so the 4-host gang fits without launching a second replica
    # and cluster_resources excludes the synthetic placeholders.
    for max_nodes in (8, 9):
        rep_boot = _sched(
            cfgs, [head] + half_booting, gangs=[gang], max_nodes=max_nodes
        )
        assert _launch_and_terminate(rep_boot) == ({}, [])
        assert rep_boot.infeasible_gang_resource_requests == []
        assert rep_boot.cluster_resources["CPU"] == 20.0

    # Busy or draining half-replicas cannot satisfy the 4-host gang, so a new
    # 4-host replica requires 1 (head) + 4 (rep-0) + 4 (new) = 9 nodes.
    for workers in (mid_boot_full, half_busy, half_draining):
        rep_tight = _sched(cfgs, [head] + workers, gangs=[gang], max_nodes=8)
        assert _launch_and_terminate(rep_tight) == ({}, [])
        assert len(rep_tight.infeasible_gang_resource_requests) == 1
        rep_fit = _sched(cfgs, [head] + workers, gangs=[gang], max_nodes=9)
        assert _launch_and_terminate(rep_fit) == ({"gpu": 4}, [])
        assert rep_fit.infeasible_gang_resource_requests == []


def test_multihost_draining_spare_priority_and_head_recovery():
    """Verify draining replicas never absorb launches or wedge pending termination,
    active/booting replicas outrank idle degraded replicas for spare pods, and
    domain-only replicas without worker_index_label_key recover missing head resources."""
    now = time.monotonic()

    # 1. Draining RAY_STOP_REQUESTED replica must not absorb REQUESTED siblings from
    # a half-booted slice (neither splitting idle termination nor starving pinned gang).
    stop_rep = [
        _pending_node(
            "tpu", f"stop-{i}", Instance.RAY_STOP_REQUESTED, rep="stop-rep", lid="lid-s"
        )
        for i in range(2)
    ]
    boot_half = _make_lifecycle_replica(
        "boot-1",
        Instance.RAY_RUNNING,
        4,
        busy=False,
        count=2,
        idle_ms=60_000,
        lid="lid-b",
    )
    req2 = [
        _pending_node("tpu", f"req-{i}", Instance.REQUESTED, lid="lid-b")
        for i in range(2)
    ]
    assert (
        _sched(
            _lifecycle_tpu_cfg(4, max_w=8),
            stop_rep + boot_half + req2,
            idle_s=10,
            pending_idle_s=10,
        ).to_terminate
        == []
    )
    pinned_gang = GangResourceRequest(
        requests=[ResourceRequestUtil.make({"CPU": 1, "TPU": 4}) for _ in range(4)],
        bundle_selectors=[
            BundleSelector(
                resource_requests=[
                    ResourceRequestUtil.make({"CPU": 1, "TPU": 4}) for _ in range(4)
                ],
                locality_requirement=LocalityRequirement(
                    locality_constraint=LocalityConstraint(
                        label_name=_SLICE,
                        placement_strategy=PlacementStrategy.STRICT_PACK,
                    ),
                    label_selector=LabelSelector(
                        label_constraints=[
                            LabelSelectorConstraint(
                                label_key=_SLICE,
                                operator=_IN,
                                label_values=["boot-1"],
                            )
                        ]
                    ),
                ),
            )
        ],
    )
    cfg_gang = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4, "TPU": 4},
            labels={},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                worker_index_label_key=_WID,
            ),
        )
    }
    rep_pinned = _sched(
        cfg_gang,
        stop_rep + boot_half + req2,
        gangs=[pinned_gang],
    )
    assert _launch_and_terminate(rep_pinned) == ({}, [])
    assert rep_pinned.infeasible_gang_resource_requests == []

    # Draining provider replica must not block unrelated expired ALLOCATED pods, and
    # non-provider RAY_STOP_REQUESTED must not trap expired unrequested QUEUED pods.
    expired_alloc = [
        _pending_node("tpu", f"exp-{i}", Instance.ALLOCATED) for i in range(4)
    ]
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, max_w=8),
            stop_rep + expired_alloc,
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0] * 4},
        ).to_terminate
    ) == [f"exp-{i}" for i in range(4)]
    stop_unlabeled = [
        _pending_node("tpu", f"s-{i}", Instance.RAY_STOP_REQUESTED) for i in range(2)
    ]
    unreq_q = [_pending_node("tpu", f"uq-{i}", Instance.QUEUED) for i in range(2)]
    assert sorted(
        t.instance_id
        for t in _sched(
            _lifecycle_tpu_cfg(4, max_w=8),
            stop_unlabeled + unreq_q,
            idle_s=10,
            pending_idle_s=10,
            unneeded={"tpu": [now - 100.0] * 2},
        ).to_terminate
    ) == ["uq-0", "uq-1"]

    # 2. Booting/active incomplete replicas outrank all-running and idle-expired
    # degraded replicas when absorbing spare pending pods (provider and non-provider).
    deg = _make_lifecycle_replica(
        "deg", Instance.RAY_RUNNING, 4, busy=True, count=1, idle_ms=0
    ) + _make_lifecycle_replica(
        "deg", Instance.RAY_RUNNING, 4, busy=False, count=2, idle_ms=60_000
    )
    # Fix distinct instance ids on deg's remaining pods.
    for idx, inst in enumerate(deg[1:], start=1):
        nid = f"n-deg-{idx}".encode().ljust(28, b"\0")
        inst.im_instance.instance_id = f"deg-{idx}"
        inst.im_instance.node_id = nid.hex()
        inst.ray_node.node_id = nid
        inst.ray_node.labels["ray.io/tpu-worker-id"] = str(idx)
    boot_slice = _make_lifecycle_replica(
        "boot-slice",
        Instance.RAY_RUNNING,
        4,
        busy=False,
        count=1,
        idle_ms=0,
        lid="lid-b",
    ) + [
        _pending_node(
            "tpu", "boot-a1", Instance.ALLOCATED, rep="boot-slice", lid="lid-b"
        )
    ]
    any_gang = GangResourceRequest(
        requests=[ResourceRequestUtil.make({"CPU": 1, "TPU": 4}) for _ in range(4)],
        bundle_selectors=[
            BundleSelector(
                resource_requests=[
                    ResourceRequestUtil.make({"CPU": 1, "TPU": 4}) for _ in range(4)
                ],
                locality_requirement=LocalityRequirement(
                    locality_constraint=LocalityConstraint(
                        label_name=_SLICE,
                        placement_strategy=PlacementStrategy.STRICT_PACK,
                    )
                ),
            )
        ],
    )
    rep_boot = _sched(
        cfg_gang,
        deg + boot_slice + req2,
        gangs=[any_gang],
        idle_s=10,
    )
    assert _launch_and_terminate(rep_boot) == ({}, [])
    assert rep_boot.infeasible_gang_resource_requests == []

    # Non-provider variant: 2 freshly idle running pods outrank 3 idle-expired ones.
    cfg_pool = copy.deepcopy(cfg_gang)
    cfg_pool["tpu"].labels["pool"] = "new"
    deg_np = _make_lifecycle_replica(
        "deg",
        Instance.RAY_RUNNING,
        4,
        busy=False,
        count=3,
        idle_ms=60_000,
        with_provider_replica_id=False,
    )
    for inst in deg_np:
        inst.ray_node.labels["pool"] = "old"
    half_np = _make_lifecycle_replica(
        "half",
        Instance.RAY_RUNNING,
        4,
        busy=False,
        count=2,
        idle_ms=0,
        with_provider_replica_id=False,
    )
    for inst in half_np:
        inst.ray_node.labels["pool"] = "new"
    alloc2_np = [_pending_node("tpu", f"a-{i}", Instance.ALLOCATED) for i in range(2)]
    new_pool_reqs = [
        ResourceRequestUtil.make(
            {"CPU": 1, "TPU": 4}, label_selectors=[[("pool", _IN, ["new"])]]
        )
        for _ in range(4)
    ]
    new_pool_gang = GangResourceRequest(
        requests=new_pool_reqs,
        bundle_selectors=[
            BundleSelector(
                resource_requests=new_pool_reqs,
                locality_requirement=LocalityRequirement(
                    locality_constraint=LocalityConstraint(
                        label_name=_SLICE,
                        placement_strategy=PlacementStrategy.STRICT_PACK,
                    )
                ),
            )
        ],
    )
    rep_np = _sched(
        cfg_pool,
        deg_np + half_np + alloc2_np,
        gangs=[new_pool_gang],
        idle_s=10,
    )
    assert rep_np.to_launch == []
    assert rep_np.infeasible_gang_resource_requests == []

    # 3. Domain-only replica with worker_index_label_key=None restores missing
    # head_resources onto an absorbed pending slot.
    rcfg = {
        "r4": NodeTypeConfig(
            name="r4",
            resources={"CPU": 4, "NPU": 2, "ldr": 1},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key="v/rack",
                worker_index_label_key=None,
                head_resources=("ldr",),
            ),
        )
    }
    insts = [
        _run_node("r4", f"r-{i}", {"CPU": 4, "NPU": 2}, {"v/rack": "rk0"}, rep="rep-0")
        for i in range(2)
    ] + [
        _pending_node("r4", f"p-{i}", Instance.ALLOCATED, rep="rep-0") for i in range(2)
    ]
    rep_ldr = _sched(
        rcfg,
        insts,
        reqs=[ResourceRequestByCount(request=_mk_req({"ldr": 1}), count=1)],
    )
    assert _launch_and_terminate(rep_ldr) == ({}, [])


def _strict_pack_gang(
    label_name: str,
    reqs: List[ResourceRequest],
    *,
    loc_constraints: Optional[List[Tuple[str, Any, List[str]]]] = None,
    strategy: PlacementStrategy = PlacementStrategy.STRICT_PACK,
) -> GangResourceRequest:
    loc_sel = LabelSelector(
        label_constraints=[
            LabelSelectorConstraint(label_key=k, operator=op, label_values=vals)
            for k, op, vals in (loc_constraints or [])
        ]
    )
    return GangResourceRequest(
        requests=reqs,
        bundle_selectors=[
            BundleSelector(
                resource_requests=reqs,
                locality_requirement=LocalityRequirement(
                    locality_constraint=LocalityConstraint(
                        label_name=label_name,
                        placement_strategy=strategy,
                    ),
                    label_selector=loc_sel,
                ),
            )
        ],
    )


def _spread_reqs(
    res: Dict[str, float],
    count: int,
    pg_key: str,
    *,
    label_selectors=None,
) -> List[ResourceRequest]:
    out = []
    for _ in range(count):
        r = _mk_req(res, label_selectors=label_selectors)
        pc = r.placement_constraints.add()
        pc.anti_affinity.label_name = pg_key
        pc.anti_affinity.label_value = ""
        out.append(r)
    return out


def test_non_locality_requests_on_multihost_replicas():
    """Committed multi-host replica hosts (including slot 0 with
    head_resources) must be consumed before launching a fresh replica, without
    regressing fallback label-tier preference."""
    cfgs = {
        "tpu_4x4": _tpu_config(
            "tpu_4x4",
            max_workers=8,
            resources={"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
        )
    }
    # (a) Regular tasks: 4 tasks -> 1 replica (4), 5 tasks -> 2 replicas (8),
    # 9 tasks -> 2 replicas (8) + 1 infeasible.
    rep4 = _sched(cfgs, [], reqs=[_resource_request({"CPU": 8, "TPU": 4}, 4)])
    assert _launch_and_terminate(rep4) == ({"tpu_4x4": 4}, [])
    assert rep4.infeasible_resource_requests == []
    assert rep4.cluster_resources["TPU-v6e-16-head"] == 1.0

    rep5 = _sched(cfgs, [], reqs=[_resource_request({"CPU": 8, "TPU": 4}, 5)])
    assert _launch_and_terminate(rep5) == ({"tpu_4x4": 8}, [])
    assert rep5.infeasible_resource_requests == []
    assert rep5.cluster_resources["TPU-v6e-16-head"] == 2.0

    rep9 = _sched(cfgs, [], reqs=[_resource_request({"CPU": 8, "TPU": 4}, 9)])
    assert _launch_and_terminate(rep9) == ({"tpu_4x4": 8}, [])
    assert len(rep9.infeasible_resource_requests) == 1

    # (b) ClusterResourceConstraint: 4x -> 1 replica, 8x -> 2 replicas.
    crc4 = ClusterResourceConstraint(
        resource_requests=[_resource_request({"CPU": 8, "TPU": 4}, 4)]
    )
    rep_crc4 = _sched(cfgs, [], crc=[crc4])
    assert _launch_and_terminate(rep_crc4) == ({"tpu_4x4": 4}, [])
    assert rep_crc4.infeasible_cluster_resource_constraints == []

    crc8 = ClusterResourceConstraint(
        resource_requests=[_resource_request({"CPU": 8, "TPU": 4}, 8)]
    )
    rep_crc8 = _sched(cfgs, [], crc=[crc8])
    assert _launch_and_terminate(rep_crc8) == ({"tpu_4x4": 8}, [])
    assert rep_crc8.infeasible_cluster_resource_constraints == []

    # (c) Non-locality STRICT_SPREAD PGs and head_resources conservation.
    pg1 = GangResourceRequest(requests=_spread_reqs({"CPU": 1, "TPU": 4}, 4, "_pg1"))
    pg2 = GangResourceRequest(requests=_spread_reqs({"CPU": 1, "TPU": 4}, 4, "_pg2"))
    rep_pg1 = _sched(cfgs, [], gangs=[pg1])
    assert _launch_and_terminate(rep_pg1) == ({"tpu_4x4": 4}, [])
    assert rep_pg1.infeasible_gang_resource_requests == []

    rep_pg2 = _sched(cfgs, [], gangs=[pg1, pg2])
    assert _launch_and_terminate(rep_pg2) == ({"tpu_4x4": 8}, [])
    assert rep_pg2.infeasible_gang_resource_requests == []

    # 4 non-head tasks + 1 head task still fit on 1 replica (slot 0 serves head).
    rep_mix = _sched(
        cfgs,
        [],
        reqs=[
            _resource_request({"CPU": 4, "TPU": 4}, 4),
            _resource_request({"TPU-v6e-16-head": 1}, 1),
        ],
    )
    assert _launch_and_terminate(rep_mix) == ({"tpu_4x4": 4}, [])
    assert rep_mix.infeasible_resource_requests == []

    # 2 head tasks strictly require 2 replicas (slots 1-3 never get head_resources).
    rep_2head = _sched(cfgs, [], reqs=[_resource_request({"TPU-v6e-16-head": 1}, 2)])
    assert _launch_and_terminate(rep_2head) == ({"tpu_4x4": 8}, [])
    assert rep_2head.cluster_resources["TPU-v6e-16-head"] == 2.0

    # (d) PR 1 fallback label-tier preservation: higher-tier label match on a fresh
    # replica must still beat a lower-tier committed sibling host.
    tier_cfgs = {
        "tpu_fb": _tpu_config(
            "tpu_fb",
            max_workers=8,
            resources={"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
            labels={**TPU_LABELS, "tier": "fb"},
        ),
        "tpu_pref": _tpu_config(
            "tpu_pref",
            max_workers=8,
            resources={"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
            labels={**TPU_LABELS, "tier": "pref"},
        ),
    }
    rep_tier = _sched(
        tier_cfgs,
        [],
        reqs=[
            _resource_request({"CPU": 8, "TPU": 4}, 1, sel=[[("tier", _IN, ["fb"])]]),
            _resource_request(
                {"CPU": 8, "TPU": 4},
                1,
                sel=[[("tier", _IN, ["pref"])], [("tier", _IN, ["fb"])]],
            ),
        ],
    )
    assert _launch_and_terminate(rep_tier) == ({"tpu_fb": 4, "tpu_pref": 4}, [])


def test_partial_replica_slot0_head_resource_gating_and_empty_labels():
    """Partial replicas reserve slot 0 (and head_resources) until
    the final slot, never merge distinct partial replicas, and overwrite empty
    placeholder labels cleanly."""
    cfgs = {
        "tpu_4x4": _tpu_config(
            "tpu_4x4",
            max_workers=16,
            resources={"CPU": 4, "TPU": 4, "TPU-v6e-16-head": 1},
            labels={**TPU_LABELS, _SLICE: "", _WID: ""},
        )
    }
    # (a) Unattributed 1/4, 2/4, 3/4 pending pods grant 0 head_resources; 4/4 grants 1.
    for cnt, expected_heads in ((1, 0.0), (2, 0.0), (3, 0.0), (4, 1.0)):
        insts = [
            _pending_node("tpu_4x4", f"p-{i}", Instance.ALLOCATED) for i in range(cnt)
        ]
        ctx = _mk_ctx(cfgs, insts)
        assert ctx.get_cluster_resources().get("TPU-v6e-16-head", 0.0) == expected_heads
        # Empty string cfg.labels must be populated, never left "".
        for n in ctx.get_nodes():
            assert n.labels.get(_SLICE)
            assert n.labels.get(_WID) in {"0", "1", "2", "3"}
        if cnt < 4:
            assert all(n.labels.get(_WID) != "0" for n in ctx.get_nodes())

    # (b) Non-head running slice (w=1,2) + 1 ALLOCATED (3/4) still has 0 heads;
    # adding the 4th ALLOCATED pod grants slot 0 + head_resources.
    run_non_head = [
        _tpu_host("r-1", "s0", 1, head=False),
        _tpu_host("r-2", "s0", 2, head=False),
    ]
    ctx3 = _mk_ctx(
        cfgs, run_non_head + [_pending_node("tpu_4x4", "p-1", Instance.ALLOCATED)]
    )
    assert ctx3.get_cluster_resources().get("TPU-v6e-16-head", 0.0) == 0.0
    ctx4 = _mk_ctx(
        cfgs,
        run_non_head
        + [_pending_node("tpu_4x4", f"p-{i}", Instance.ALLOCATED) for i in range(2)],
    )
    assert ctx4.get_cluster_resources().get("TPU-v6e-16-head", 0.0) == 1.0

    # (c) N=2 concurrent attributed partial replicas (rep-0: 3/4, rep-1: 3/4):
    # each keeps its own 3 real ALLOCATED pods on slots 1..3 and places slot 0 on
    # its backfill placeholder, so real cluster_resources reports 0 heads while
    # a 4-host gang requiring w=0 fits on rep-0 without launching a 3rd replica.
    two_partials = [
        _pending_node("tpu_4x4", f"r0-p{i}", Instance.ALLOCATED, rep="rep-0")
        for i in range(3)
    ] + [
        _pending_node("tpu_4x4", f"r1-p{i}", Instance.ALLOCATED, rep="rep-1")
        for i in range(3)
    ]
    ctx_two = _mk_ctx(cfgs, two_partials)
    assert ctx_two.get_cluster_resources().get("TPU-v6e-16-head", 0.0) == 0.0
    real_nodes = [n for n in ctx_two.get_nodes() if not n._is_backfill_placeholder]
    ph_nodes = [n for n in ctx_two.get_nodes() if n._is_backfill_placeholder]
    assert len(real_nodes) == 6
    assert len(ph_nodes) == 2
    assert all(n.labels.get(_WID) != "0" for n in real_nodes)
    assert all(n.labels.get(_WID) == "0" for n in ph_nodes)
    rep_gang = _sched(cfgs, two_partials, gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_gang) == ({}, [])
    assert rep_gang.infeasible_gang_resource_requests == []


def test_non_exclusive_shared_clique_multireplica():
    """Non-exclusive shared cliques decouple provider replica_id
    from shared domain labels across gang scheduling, pinning, idle scale-down,
    and draining isolation."""
    clique_key = "ray.io/gpu-clique"
    clique_cfg = NodeTypeConfig(
        name="gpu-clique",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=12,
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=clique_key,
            domain_exclusive=False,
        ),
        runtime_label_keys=(clique_key,),
    )
    cfgs = {"gpu-clique": clique_cfg}

    def _cnode(iid, clique, rep, busy=False, idle_ms=60_000):
        return _run_node(
            "gpu-clique",
            iid,
            {"CPU": 8, "GPU": 4},
            {clique_key: clique},
            used={"GPU": 4} if busy else None,
            idle_ms=0 if busy else idle_ms,
            rep=rep,
        )

    rep0_busy = [
        _cnode("c0-0", "clique-0", "rep-0", busy=True),
        _cnode("c0-1", "clique-0", "rep-0", busy=False),
    ]
    rep0_idle = [
        _cnode("c0-0", "clique-0", "rep-0", busy=False),
        _cnode("c0-1", "clique-0", "rep-0", busy=False),
    ]
    rep1_idle = [
        _cnode("c1-0", "clique-0", "rep-1", busy=False),
        _cnode("c1-1", "clique-0", "rep-1", busy=False),
    ]
    rep2_idle = [
        _cnode("c2-0", "clique-1", "rep-2", busy=False),
        _cnode("c2-1", "clique-1", "rep-2", busy=False),
    ]

    # (a) Independent idle scale-down inside shared clique-0.
    rep_a = _sched(cfgs, rep0_busy + rep1_idle, idle_s=10)
    assert _launch_and_terminate(rep_a)[0] == {}
    assert sorted(t.instance_id for t in rep_a.to_terminate) == ["c1-0", "c1-1"]

    # (b) Multi-replica 4-host gang spans rep-0 + rep-1 inside clique-0.
    g4 = _strict_pack_gang(clique_key, _spread_reqs({"CPU": 4, "GPU": 4}, 4, "_pg_c4"))
    rep_b = _sched(cfgs, rep0_idle + rep1_idle, gangs=[g4], idle_s=10)
    assert _launch_and_terminate(rep_b) == ({}, [])
    assert rep_b.infeasible_gang_resource_requests == []

    # (c) Pinned shared-clique protection while clique-0 has busy sibling;
    # terminates clique-1. When clique-0 is fully complete & idle and still
    # cannot fit a 6-host pinned gang, clique-0 scales down (no leak).
    g4_pin_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 4, "_pg_pin4"),
        loc_constraints=[(clique_key, _IN, ["clique-0"])],
    )
    rep_c1 = _sched(
        cfgs, rep0_busy + rep1_idle + rep2_idle, gangs=[g4_pin_c0], idle_s=10
    )
    assert _launch_and_terminate(rep_c1)[0] == {}
    assert sorted(t.instance_id for t in rep_c1.to_terminate) == ["c2-0", "c2-1"]
    assert len(rep_c1.infeasible_gang_resource_requests) == 1

    g6_pin_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 6, "_pg_pin6"),
        loc_constraints=[(clique_key, _IN, ["clique-0"])],
    )
    rep_c2 = _sched(cfgs, rep0_idle + rep1_idle, gangs=[g6_pin_c0], idle_s=10)
    assert _launch_and_terminate(rep_c2)[0] == {}
    assert sorted(t.instance_id for t in rep_c2.to_terminate) == [
        "c0-0",
        "c0-1",
        "c1-0",
        "c1-1",
    ]

    # (d) Stage 4 booting attach: rep-1 (booting) joins idle rep-0 in clique-0
    # without stealing rep-2's ALLOCATED pod that already belongs to clique-2.
    rep1_boot = [
        _pending_node("gpu-clique", f"b1-{i}", Instance.ALLOCATED, rep="rep-1")
        for i in range(2)
    ]
    rep2_half_c2 = [
        _cnode("c2-0", "clique-2", "rep-2", busy=False),
        _pending_node("gpu-clique", "c2-p1", Instance.ALLOCATED, rep="rep-2"),
    ]
    rep_d = _sched(cfgs, rep0_idle + rep1_boot + rep2_half_c2, gangs=[g4])
    assert _launch_and_terminate(rep_d) == ({}, [])
    assert rep_d.infeasible_gang_resource_requests == []

    # (e) Cold-start multi-replica Step 5 launch and min_worker_nodes combining.
    rep_e1 = _sched(cfgs, [], gangs=[g4])
    assert _launch_and_terminate(rep_e1) == ({"gpu-clique": 4}, [])
    assert rep_e1.infeasible_gang_resource_requests == []

    min4_cfg = copy.deepcopy(clique_cfg)
    min4_cfg.min_worker_nodes = 4
    rep_e2 = _sched({"gpu-clique": min4_cfg}, [], gangs=[g4])
    assert _launch_and_terminate(rep_e2) == ({"gpu-clique": 4}, [])

    # (f) Mixed cluster: exclusive TPU + non-exclusive gpu-clique + cpu.
    mixed_cfgs = {
        "tpu_4x4": _tpu_config(max_workers=8),
        "gpu-clique": clique_cfg,
        "cpu": NodeTypeConfig(
            name="cpu", resources={"CPU": 8}, min_worker_nodes=0, max_worker_nodes=8
        ),
    }
    g2_cpu_clique = _strict_pack_gang(clique_key, _spread_reqs({"CPU": 4}, 2, "_pg_c2"))
    rep_f1 = _sched(mixed_cfgs, [], gangs=[g2_cpu_clique])
    assert _launch_and_terminate(rep_f1) == ({"gpu-clique": 2}, [])

    rep_f2 = _sched(mixed_cfgs, [], gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_f2) == ({"tpu_4x4": 4}, [])

    # (g) Draining rep-0 in clique-0 must NOT poison shared domain clique-0
    # for healthy sibling rep-1 in the same clique-0.
    rep0_draining = [
        _cnode("c0-0", "clique-0", "rep-0", busy=False),
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="c0-1",
                instance_type="gpu-clique",
                status=Instance.RAY_STOPPING,
                cloud_instance_id="cloud-c0-1",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="cloud-c0-1",
            cloud_instance_replica_id="rep-0",
        ),
    ]
    g2_gpu_clique = _strict_pack_gang(
        clique_key, _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_c2g")
    )
    rep_g = _sched(cfgs, rep0_draining + rep1_idle, gangs=[g2_gpu_clique])
    assert _launch_and_terminate(rep_g) == ({}, [])
    assert rep_g.infeasible_gang_resource_requests == []


def test_non_exclusive_shared_domain_without_provider_replica_id():
    """Shared non-exclusive domains without provider_replica_id isolate
    per-replica launch_request_id groups while keeping ambiguous multi-replica
    no-lid batches intact until all hosts are idle, then scaling down in
    whole-replica chunks."""
    clique_key = "ray.io/gpu-clique"
    clique_cfg = NodeTypeConfig(
        name="gpu-clique",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=12,
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=clique_key,
            domain_exclusive=False,
        ),
        runtime_label_keys=(clique_key,),
    )
    cfgs = {"gpu-clique": clique_cfg}

    def _snode(
        iid,
        clique,
        *,
        lid="",
        busy=False,
        idle_ms=60_000,
        status=Instance.RAY_RUNNING,
    ):
        inst = _run_node(
            "gpu-clique",
            iid,
            {"CPU": 8, "GPU": 4},
            {clique_key: clique},
            used={"GPU": 4} if busy else None,
            idle_ms=0 if busy else idle_ms,
            rep=None,
        )
        assert inst.im_instance is not None
        inst.im_instance.launch_request_id = lid
        inst.im_instance.status = status
        return inst

    # (a) Mixed launch_request_id and no-lid nodes across two shared cliques:
    # - clique-0 lid-busy (1 busy + 1 idle) must stay intact;
    # - clique-0 no-lid (2 busy + 2 idle across unknown physical replica
    #   boundaries) must NOT slice idle hosts out of active physical replicas;
    # - clique-1 lid-c1 (2 idle hosts) scales down independently.
    c0_lid_busy = [
        _snode("c0-lb-0", "clique-0", lid="lid-busy", busy=True),
        _snode("c0-lb-1", "clique-0", lid="lid-busy", busy=False),
    ]
    c0_no_lid_mixed = [
        _snode("c0-n-busy-0", "clique-0", busy=True),
        _snode("c0-n-idle-0", "clique-0", busy=False),
        _snode("c0-n-busy-1", "clique-0", busy=True),
        _snode("c0-n-idle-1", "clique-0", busy=False),
    ]
    c1_lid_idle = [
        _snode("c1-0", "clique-1", lid="lid-c1", busy=False),
        _snode("c1-1", "clique-1", lid="lid-c1", busy=False),
    ]
    rep_a = _sched(cfgs, c0_lid_busy + c0_no_lid_mixed + c1_lid_idle, idle_s=10)
    assert _launch_and_terminate(rep_a)[0] == {}
    assert sorted(t.instance_id for t in rep_a.to_terminate) == [
        "c1-0",
        "c1-1",
    ]

    # Once all 4 no-lid nodes in clique-0 are idle, min_worker_nodes=2 still
    # scales down exactly one whole 2-host replica cleanly.
    c0_no_lid_all_idle = [
        _snode(f"c0-n-idle-{i}", "clique-0", busy=False) for i in range(4)
    ]
    min2_cfg = copy.deepcopy(clique_cfg)
    min2_cfg.min_worker_nodes = 2
    rep_a_idle = _sched({"gpu-clique": min2_cfg}, c0_no_lid_all_idle, idle_s=10)
    assert _launch_and_terminate(rep_a_idle)[0] == {}
    assert len(rep_a_idle.to_terminate) == 2

    # (b) Healthy no-lid replica in clique-0 satisfies a 2-host pinned gang
    # while draining lid-isolated chunks (RAY_STOP_REQUESTED and torn-down
    # launch_request_id) are excluded so a 4-host pinned gang stays infeasible.
    healthy_no_lid = [
        _snode("h-0", "clique-0", busy=False, idle_ms=0),
        _snode("h-1", "clique-0", busy=False, idle_ms=0),
    ]
    draining_stop_req = [
        _snode(
            "s-0",
            "clique-0",
            lid="lid-stop",
            busy=False,
            idle_ms=60_000,
            status=Instance.RAY_STOP_REQUESTED,
        ),
        _snode("s-1", "clique-0", lid="lid-stop", busy=False, idle_ms=60_000),
    ]
    draining_by_lid = [
        _snode("d-0", "clique-0", lid="lid-drain", busy=False, idle_ms=0),
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="d-1",
                instance_type="gpu-clique",
                status=Instance.TERMINATING,
                cloud_instance_id="cloud-d-1",
                launch_request_id="lid-drain",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="cloud-d-1",
        ),
    ]

    g2_pin_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_sd2"),
        loc_constraints=[(clique_key, _IN, ["clique-0"])],
    )
    rep_b_ok = _sched(
        cfgs, healthy_no_lid + draining_stop_req + draining_by_lid, gangs=[g2_pin_c0]
    )
    assert _launch_and_terminate(rep_b_ok) == ({}, [])
    assert rep_b_ok.infeasible_gang_resource_requests == []

    g4_pin_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 4, "_pg_sd4"),
        loc_constraints=[(clique_key, _IN, ["clique-0"])],
    )
    rep_b_inf = _sched(
        cfgs, healthy_no_lid + draining_stop_req + draining_by_lid, gangs=[g4_pin_c0]
    )
    assert _launch_and_terminate(rep_b_inf) == ({}, [])
    assert len(rep_b_inf.infeasible_gang_resource_requests) == 1

    # (c) Independent negative counter-cases verifying each draining signal:
    # a 1-host gang pinned to clique-0 cannot land on either the RAY_RUNNING
    # sibling of a RAY_STOP_REQUESTED host, the RAY_RUNNING sibling sharing a
    # torn-down launch_request_id, or an ambiguous no-lid batch containing a
    # RAY_STOP_REQUESTED host (unknown physical pairing).
    g1_pin_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 1, "_pg_sd1"),
        loc_constraints=[(clique_key, _IN, ["clique-0"])],
    )
    rep_c_stop = _sched(cfgs, draining_stop_req, gangs=[g1_pin_c0])
    assert _launch_and_terminate(rep_c_stop) == ({}, [])
    assert len(rep_c_stop.infeasible_gang_resource_requests) == 1

    rep_c_lid = _sched(cfgs, draining_by_lid, gangs=[g1_pin_c0])
    assert _launch_and_terminate(rep_c_lid) == ({}, [])
    assert len(rep_c_lid.infeasible_gang_resource_requests) == 1

    ambiguous_no_lid_draining = healthy_no_lid + [
        _snode(
            "ns-0",
            "clique-0",
            busy=False,
            idle_ms=60_000,
            status=Instance.RAY_STOP_REQUESTED,
        ),
        _snode("ns-1", "clique-0", busy=False, idle_ms=60_000),
    ]
    rep_c_ambig = _sched(cfgs, ambiguous_no_lid_draining, gangs=[g1_pin_c0])
    assert _launch_and_terminate(rep_c_ambig) == ({}, [])
    assert len(rep_c_ambig.infeasible_gang_resource_requests) == 1


def test_draining_exclusion_lrid_matching_and_placeholders():
    """Draining isolation, non-poisoning counter-cases,
    exact launch_request_id attachment, vanished-batch placeholders, outdated
    placeholder cleanup, and degraded min_worker_nodes scale-down."""
    cfgs = {"tpu_4x4": _tpu_config(max_workers=12)}

    # (a) Partially draining replicas (RAY_STOPPING, TERMINATING, unshadowed
    # RAY_STOPPED) are excluded from gang placement -> launches fresh 4-host slice.
    for drain_st in (
        Instance.RAY_STOPPING,
        Instance.TERMINATING,
        Instance.RAY_STOPPED,
    ):
        draining = [_tpu_host(f"d-{w}", "s0", w, rep="rep-0") for w in range(2)] + [
            AutoscalerInstance(
                im_instance=Instance(
                    instance_id=f"d-term-{w}",
                    instance_type="tpu_4x4",
                    status=drain_st,
                    cloud_instance_id=f"cloud-d-term-{w}",
                    node_kind=NodeKind.WORKER,
                ),
                cloud_instance_id=f"cloud-d-term-{w}",
                cloud_instance_replica_id="rep-0",
            )
            for w in range(2)
        ]
        rep = _sched(cfgs, draining, gangs=[_tpu_gang((0, 1, 2, 3))])
        assert _launch_and_terminate(rep) == ({"tpu_4x4": 4}, [])
        assert rep.infeasible_gang_resource_requests == []

    # (b) Non-poisoning counter-cases:
    # 1. Shadowed RAY_STOPPED (active instance shares cloud_instance_id).
    shadowed = [_tpu_host(f"sh-{w}", "s0", w, rep="rep-0") for w in range(4)] + [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="sh-old",
                instance_type="tpu_4x4",
                status=Instance.RAY_STOPPED,
                cloud_instance_id="cloud-sh-0",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="cloud-sh-0",
            cloud_instance_replica_id="rep-0",
        )
    ]
    rep_sh = _sched(cfgs, shadowed, gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_sh) == ({}, [])

    # 2. Stale raylet-restart TERMINATED row sharing cloud_instance_id and
    # launch_request_id does not poison active replica and reattaches prov_lids.
    restarted = []
    for w in range(2):
        inst = _tpu_host(f"rs-{w}", "s0", w, rep="rep-0")
        inst.im_instance.status_history.add(
            instance_status=Instance.ALLOCATED, timestamp_ns=200
        )
        restarted.append(inst)
    old_raylet = AutoscalerInstance(
        im_instance=Instance(
            instance_id="rs-old",
            instance_type="tpu_4x4",
            status=Instance.TERMINATED,
            cloud_instance_id="cloud-rs-0",
            launch_request_id="lr-rs",
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id="cloud-rs-0",
        cloud_instance_replica_id="rep-0",
    )
    old_raylet.im_instance.status_history.add(
        instance_status=Instance.RAY_RUNNING, timestamp_ns=100
    )
    old_raylet.im_instance.status_history.add(
        instance_status=Instance.TERMINATED, timestamp_ns=150
    )
    restarted.append(old_raylet)
    for i in range(2):
        p = _pending_node("tpu_4x4", f"rs-req-{i}", Instance.REQUESTED)
        p.im_instance.launch_request_id = "lr-rs"
        restarted.append(p)
    rep_rs = _sched(cfgs, restarted, gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_rs) == ({}, [])

    # 3. ALLOCATION_FAILED with same launch_request_id does not poison replica.
    alloc_fail_slice = [_tpu_host(f"af-{w}", "s0", w, rep="rep-0") for w in range(4)]
    for inst in alloc_fail_slice:
        inst.im_instance.launch_request_id = "lr-af"
    alloc_fail_slice.append(
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="af-fail",
                instance_type="tpu_4x4",
                status=Instance.ALLOCATION_FAILED,
                launch_request_id="lr-af",
                node_kind=NodeKind.WORKER,
            )
        )
    )
    rep_af = _sched(cfgs, alloc_fail_slice, gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_af) == ({}, [])

    # (c) N=2 concurrent half-running replicas with distinct launch_request_ids:
    # rep-0 (lr-0, busy) + rep-1 (lr-1, idle) -> lr-1 binds to rep-1 cleanly.
    conc = []
    for w in range(2):
        r0 = _tpu_host(f"c0-{w}", "s0", w, used={"TPU": 4}, rep="rep-0")
        r0.im_instance.launch_request_id = "lr-0"
        r1 = _tpu_host(f"c1-{w}", "s1", w, rep="rep-1")
        r1.im_instance.launch_request_id = "lr-1"
        p0 = _pending_node("tpu_4x4", f"p0-{w}", Instance.REQUESTED)
        p0.im_instance.launch_request_id = "lr-0"
        p1 = _pending_node("tpu_4x4", f"p1-{w}", Instance.REQUESTED)
        p1.im_instance.launch_request_id = "lr-1"
        conc.extend([r0, r1, p0, p1])
    rep_conc = _sched(cfgs, conc, gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_conc) == ({}, [])
    assert rep_conc.infeasible_gang_resource_requests == []

    # (d) REQUESTED rows sharing a draining launch_request_id bind to the
    # draining replica and are excluded from healthy placement.
    drain_lrid = [
        _tpu_host("dl-0", "s0", 0, rep="rep-drain"),
    ]
    drain_lrid[0].im_instance.launch_request_id = "lr-drain"
    drain_term = AutoscalerInstance(
        im_instance=Instance(
            instance_id="dl-1",
            instance_type="tpu_4x4",
            status=Instance.TERMINATING,
            cloud_instance_id="cloud-dl-1",
            launch_request_id="lr-drain",
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id="cloud-dl-1",
        cloud_instance_replica_id="rep-drain",
    )
    drain_reqs = []
    for i in range(2):
        pr = _pending_node("tpu_4x4", f"dl-req-{i}", Instance.REQUESTED)
        pr.im_instance.launch_request_id = "lr-drain"
        drain_reqs.append(pr)
    rep_dl = _sched(
        cfgs, drain_lrid + [drain_term] + drain_reqs, gangs=[_tpu_gang((0, 1, 2, 3))]
    )
    assert _launch_and_terminate(rep_dl) == ({"tpu_4x4": 4}, [])

    # (e) Partial vanished batch (2 vanished ALLOCATED + 2 live REQUESTED on same
    # launch_request_id) populates 2 placeholders so min_worker_nodes=4 and a
    # 4-host gang do not double-launch, while cluster_resources counts ONLY the
    # 2 real REQUESTED pods. Counter-case: if all 4 pods are TERMINATED
    # with 0 live rows, no 24h ghost replica is created -> launches 4 fresh hosts.
    vanished_2 = [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id=f"v-{i}",
                instance_type="tpu_4x4",
                status=Instance.TERMINATED,
                cloud_instance_id=f"cloud-v-{i}",
                launch_request_id="lr-vanished",
                node_kind=NodeKind.WORKER,
            )
        )
        for i in range(2)
    ]
    for vi in vanished_2:
        vi.im_instance.status_history.add(
            instance_status=Instance.ALLOCATED, timestamp_ns=100
        )
    req_vanished_2 = []
    for i in range(2):
        pr = _pending_node("tpu_4x4", f"vr-{i}", Instance.REQUESTED)
        pr.im_instance.launch_request_id = "lr-vanished"
        req_vanished_2.append(pr)
    min4_cfgs = {"tpu_4x4": _tpu_config(min_workers=4, max_workers=8)}
    rep_van = _sched(
        min4_cfgs, vanished_2 + req_vanished_2, gangs=[_tpu_gang((0, 1, 2, 3))]
    )
    assert _launch_and_terminate(rep_van) == ({}, [])
    assert rep_van.infeasible_gang_resource_requests == []
    assert rep_van.cluster_resources["TPU"] == 8.0

    vanished_all4 = [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id=f"va-{i}",
                instance_type="tpu_4x4",
                status=Instance.TERMINATED,
                cloud_instance_id=f"cloud-va-{i}",
                launch_request_id="lr-dead",
                node_kind=NodeKind.WORKER,
            )
        )
        for i in range(4)
    ]
    for vi in vanished_all4:
        vi.im_instance.status_history.add(
            instance_status=Instance.ALLOCATED, timestamp_ns=100
        )
    rep_dead = _sched(min4_cfgs, vanished_all4, gangs=[_tpu_gang((0, 1, 2, 3))])
    assert _launch_and_terminate(rep_dead) == ({"tpu_4x4": 4}, [])

    # (f) Outdated running 3/4 replica marks its 1 placeholder TO_TERMINATE
    # without emitting an empty TerminationRequest, and replaces with 4 new hosts.
    outdated_cfg = _tpu_config(min_workers=4, max_workers=8)
    outdated_cfg.launch_config_hash = "hash-new"
    old_hosts = [_tpu_host(f"old-{w}", "s-old", w, rep="rep-old") for w in range(3)]
    for oh in old_hosts:
        oh.im_instance.launch_config_hash = "hash-old"
    rep_out = ResourceDemandScheduler(event_logger).schedule(
        SchedulingRequest(
            disable_launch_config_check=False,
            node_type_configs={"tpu_4x4": outdated_cfg},
            current_instances=old_hosts,
            replica_view=_synthetic_replica_view({"tpu_4x4": outdated_cfg}, old_hosts),
        )
    )
    assert _launch_and_terminate(rep_out)[0] == {"tpu_4x4": 4}
    assert sorted(t.instance_id for t in rep_out.to_terminate) == [
        "old-0",
        "old-1",
        "old-2",
    ]
    assert all(t.instance_id for t in rep_out.to_terminate)

    # (g) 4 x 1/4 degraded idle replicas with min_worker_nodes=4 terminate
    # 3 replicas and keep 1 (4 committed slots >= min_w=4), zero placeholder TRs.
    four_deg = [
        _tpu_host(f"deg-{r}-1", f"sd-{r}", 1, idle_ms=60_000 + r * 1000, rep=f"deg-{r}")
        for r in range(4)
    ]
    rep_n6 = _sched(min4_cfgs, four_deg, idle_s=10)
    assert len(rep_n6.to_terminate) == 3
    assert all(t.instance_id for t in rep_n6.to_terminate)
    assert rep_n6.to_launch == []


def test_step5_runtime_label_keys_and_single_host_tpu():
    """Step 5 filters to node types declaring the locality label, falls
    back when none declares, and handles single-host TPU cleanly."""
    # (a) Higher-priority cpu without runtime_label_keys is excluded when
    # gpu-rack declares runtime_label_keys=("ray.io/rack",).
    cfgs_a = {
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=8,
            priority=100,
        ),
        "gpu-rack": NodeTypeConfig(
            name="gpu-rack",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
            priority=1,
            runtime_label_keys=("ray.io/rack",),
        ),
    }
    g_rack = _strict_pack_gang("ray.io/rack", _spread_reqs({"CPU": 4}, 2, "_pg_rack"))
    rep_a = _sched(cfgs_a, [], gangs=[g_rack])
    assert _launch_and_terminate(rep_a) == ({"gpu-rack": 2}, [])

    # (b) Legacy fallback when NO node type declares custom/zone.
    cfgs_b = {
        "cpu-a": NodeTypeConfig(
            name="cpu-a", resources={"CPU": 8}, min_worker_nodes=0, max_worker_nodes=4
        ),
    }
    g_legacy = _strict_pack_gang("custom/zone", _spread_reqs({"CPU": 4}, 2, "_pg_leg"))
    rep_b = _sched(cfgs_b, [], gangs=[g_legacy])
    assert _launch_and_terminate(rep_b) == ({"cpu-a": 2}, [])

    # (c) Single-host TPU (workers_per_replica=1) declaring _SLICE excludes cpu.
    cfgs_c = {
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=4,
            priority=50,
        ),
        "v6e-4": NodeTypeConfig(
            name="v6e-4",
            resources={"CPU": 4, "TPU": 4, "TPU-v6e-4-head": 1},
            labels={_ACC: "TPU-V6E", _TOPO: "2x2", _POD: "v6e-4"},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=1,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=("TPU-v6e-4-head",),
            ),
            runtime_label_keys=(_SLICE, _WID),
        ),
    }
    g_sh_tpu = _strict_pack_gang(_SLICE, [_mk_req({"CPU": 2})])
    rep_c = _sched(cfgs_c, [], gangs=[g_sh_tpu])
    assert _launch_and_terminate(rep_c) == ({"v6e-4": 1}, [])


def test_sub_replica_queued_remainder_cancellation_and_timer_preservation():
    """Unlaunchable sub-replica QUEUED remainders (1..size-1) cancel
    proactively regardless of demand/min_worker_nodes/pending_idle_timeout_s,
    even with launch_request_id set, without dropping surviving pending timers."""
    cfgs_min4 = _lifecycle_tpu_cfg(4, min_w=4, max_w=12)

    # (a) Remainders r in (1, 2, 3) cancel immediately with active demand and
    # pending_idle_timeout_s=None, while full 4-worker aligned QUEUED survive.
    for r in (1, 2, 3):
        q_only = [_pending_node("tpu", f"q-{i}", Instance.QUEUED) for i in range(r)]
        # Also set launch_request_id on first QUEUED row to verify dead branch removal.
        q_only[0].im_instance.launch_request_id = "lr-stale-q"
        rep_r = _sched(
            cfgs_min4,
            q_only,
            reqs=[_resource_request({"TPU": 4}, 4)],
            pending_idle_s=None,
        )
        assert sorted(t.instance_id for t in rep_r.to_terminate) == [
            f"q-{i}" for i in range(r)
        ]
        assert _launch_and_terminate(rep_r)[0] == {"tpu": 4}

        q_plus_aligned = [
            _pending_node("tpu", f"qa-{i}", Instance.QUEUED) for i in range(4 + r)
        ]
        rep_4r = _sched(
            cfgs_min4,
            q_plus_aligned,
            reqs=[_resource_request({"TPU": 4}, 4)],
            pending_idle_s=None,
        )
        assert len(rep_4r.to_terminate) == r
        assert _launch_and_terminate(rep_4r)[0] == {}

    # (b) 8 QUEUED with 0 demand and expired pending timeout cancel all 8;
    # 5 QUEUED with 4-host demand cancels 1 remainder and keeps 4 aligned.
    now = time.monotonic()
    q8 = [_pending_node("tpu", f"q8-{i}", Instance.QUEUED) for i in range(8)]
    rep_8 = _sched(
        _lifecycle_tpu_cfg(4, min_w=0, max_w=12),
        q8,
        pending_idle_s=10,
        unneeded={"tpu": [now - 100.0] * 8},
    )
    assert len(rep_8.to_terminate) == 8

    # (c) Timer preservation: sub-replica remainder cancellation must not pop
    # surviving _pending_unneeded_since_s entries belonging to an aligned pending replica.
    sched = ResourceDemandScheduler(event_logger)
    old_ts = now - 5.0
    sched._pending_unneeded_since_s = {"tpu": [old_ts] * 4}
    q5_idle = [_pending_node("tpu", f"qt-{i}", Instance.QUEUED) for i in range(5)]
    rep_timer = _sched(
        _lifecycle_tpu_cfg(4, min_w=0, max_w=12),
        q5_idle,
        pending_idle_s=60,
        sched=sched,
    )
    assert len(rep_timer.to_terminate) == 1
    assert len(sched._pending_unneeded_since_s.get("tpu", [])) == 4
    assert all(ts == old_ts for ts in sched._pending_unneeded_since_s["tpu"])


def test_label_snapshot_and_verbatim_restoration():
    """_schedule_in_domain snapshots and restores prior locality labels
    verbatim (preserving absent keys and prior _PENDING_DOMAIN_PREFIX values)
    and clears _saved_locality_label."""
    clique_key = "ray.io/gpu-clique"
    cfgs = {
        "gpu-clique": NodeTypeConfig(
            name="gpu-clique",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=clique_key,
                domain_exclusive=False,
            ),
            runtime_label_keys=(clique_key,),
        )
    }
    rep0 = [
        _run_node(
            "gpu-clique",
            f"r0-{i}",
            {"CPU": 8, "GPU": 4},
            {clique_key: "clique-0", "custom": "keep"},
            rep="rep-0",
        )
        for i in range(2)
    ]
    rep1_boot = [
        _pending_node("gpu-clique", f"p1-{i}", Instance.ALLOCATED, rep="rep-1")
        for i in range(2)
    ]
    rep2_boot = [
        _pending_node("gpu-clique", f"p2-{i}", Instance.ALLOCATED, rep="rep-2")
        for i in range(2)
    ]
    # Gang uses rep-0 (2 hosts) + rep-1 (2 hosts); rep-2 is an unused sibling
    # whose _PENDING_DOMAIN_PREFIX label must be restored verbatim.
    g4 = _strict_pack_gang(clique_key, _spread_reqs({"CPU": 4, "GPU": 4}, 4, "_pg_b3"))
    ctx = _mk_ctx(cfgs, rep0 + rep1_boot + rep2_boot, gangs=[g4])
    orig_rep2_labels = {
        n.im_instance_id: dict(n.labels)
        for n in ctx.get_nodes()
        if n.im_instance_id in {"p2-0", "p2-1"}
    }
    updated, infeasible = ResourceDemandScheduler._try_schedule_gang(ctx, g4)
    assert infeasible == []
    for n in updated:
        assert n._saved_locality_label is None
        if n.im_instance_id in orig_rep2_labels:
            assert n.labels == orig_rep2_labels[n.im_instance_id]

    # Absent-key restoration on unlabeled single-host pending siblings when gang
    # fits using only 1 of 2 unlabeled siblings.
    sh_cfgs = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=4,
        )
    }
    r_sh = _run_node(
        "gpu", "g-0", {"CPU": 8, "GPU": 4}, {clique_key: "clique-0", "keep": "v"}
    )
    p_sh1 = _pending_node("gpu", "gp-1", Instance.ALLOCATED)
    p_sh2 = _pending_node("gpu", "gp-2", Instance.ALLOCATED)
    g2 = _strict_pack_gang(
        clique_key, _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_b3_sh")
    )
    ctx_sh = _mk_ctx(sh_cfgs, [r_sh, p_sh1, p_sh2], gangs=[g2])
    upd_sh, inf_sh = ResourceDemandScheduler._try_schedule_gang(ctx_sh, g2)
    assert inf_sh == []
    unused_p = [
        n
        for n in upd_sh
        if n.im_instance_id in {"gp-1", "gp-2"}
        and not n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
    ]
    assert len(unused_p) == 1
    assert clique_key not in unused_p[0].labels
    assert unused_p[0]._saved_locality_label is None


def test_pinned_locality_domains_intersection_and_not_in():
    """_pinned_locality_domains intersects finite/co-finite filters
    across bundles and fallback tiers, handling NOT_IN and contradictions."""
    cfgs = {"tpu_4x4": _tpu_config(max_workers=12)}
    s0 = [_tpu_host(f"s0-{w}", "s0", w, idle_ms=60_000, rep="s0") for w in range(3)]
    s1 = [_tpu_host(f"s1-{w}", "s1", w, idle_ms=60_000, rep="s1") for w in range(3)]
    s2 = [_tpu_host(f"s2-{w}", "s2", w, idle_ms=60_000, rep="s2") for w in range(3)]
    # Remove provider_replica_id so no placeholders fill slot 0 -> 4-host gangs
    # remain infeasible and exercise _pinned_locality_domains protection.
    for inst in s0 + s1 + s2:
        inst.cloud_instance_replica_id = None

    # (a) IN intersection across bundles: {s0, s1} & {s1, s2} = {s1}.
    g_inter = _strict_pack_gang(
        _SLICE,
        [
            _mk_req(
                {"CPU": 1, "TPU": 4},
                label_selectors=[[(_SLICE, _IN, ["s0", "s1"]), (_WID, _IN, ["0"])]],
            ),
            _mk_req(
                {"CPU": 1, "TPU": 4},
                label_selectors=[[(_SLICE, _IN, ["s1", "s2"]), (_WID, _IN, ["1"])]],
            ),
            _mk_req({"CPU": 1, "TPU": 4}, label_selectors=[[(_WID, _IN, ["2"])]]),
            _mk_req({"CPU": 1, "TPU": 4}, label_selectors=[[(_WID, _IN, ["3"])]]),
        ],
    )
    assert ResourceDemandScheduler._pinned_locality_domains([g_inter], cfgs) == {
        _SLICE: {"s1"}
    }
    rep_a = _sched(cfgs, s0 + s1 + s2, gangs=[g_inter], idle_s=10)
    assert sorted(t.instance_id for t in rep_a.to_terminate) == [
        "s0-0",
        "s0-1",
        "s0-2",
        "s2-0",
        "s2-1",
        "s2-2",
    ]

    # (b) NOT_IN exclusion: IN [s0, s1] AND NOT_IN [s0] -> {s1} only.
    g_not_in = _strict_pack_gang(
        _SLICE,
        [
            _mk_req(
                {"CPU": 1, "TPU": 4},
                label_selectors=[
                    [
                        (_SLICE, _IN, ["s0", "s1"]),
                        (_SLICE, _NOT_IN, ["s0"]),
                        (_WID, _IN, ["0"]),
                    ]
                ],
            ),
            _mk_req({"CPU": 1, "TPU": 4}, label_selectors=[[(_WID, _IN, ["3"])]]),
        ],
    )
    assert ResourceDemandScheduler._pinned_locality_domains([g_not_in], cfgs) == {
        _SLICE: {"s1"}
    }
    rep_b = _sched(cfgs, s0 + s1 + s2, gangs=[g_not_in], idle_s=10)
    assert sorted(t.instance_id for t in rep_b.to_terminate) == [
        "s0-0",
        "s0-1",
        "s0-2",
        "s2-0",
        "s2-1",
        "s2-2",
    ]

    # (c) Contradictory pins (IN [s0] & IN [s1], and IN [s0] & NOT_IN [s0]) -> {}.
    g_contra1 = _strict_pack_gang(
        _SLICE,
        [
            _mk_req({"CPU": 1, "TPU": 4}, label_selectors=[[(_SLICE, _IN, ["s0"])]]),
            _mk_req({"CPU": 1, "TPU": 4}, label_selectors=[[(_SLICE, _IN, ["s1"])]]),
        ],
    )
    g_contra2 = _strict_pack_gang(
        _SLICE,
        [
            _mk_req(
                {"CPU": 1, "TPU": 4},
                label_selectors=[[(_SLICE, _IN, ["s0"]), (_SLICE, _NOT_IN, ["s0"])]],
            )
        ],
    )
    assert ResourceDemandScheduler._pinned_locality_domains([g_contra1], cfgs) == {}
    assert ResourceDemandScheduler._pinned_locality_domains([g_contra2], cfgs) == {}
    rep_c = _sched(cfgs, s0 + s1 + s2, gangs=[g_contra1], idle_s=10)
    assert len(rep_c.to_terminate) == 9
    assert len(rep_c.infeasible_gang_resource_requests) == 1

    # (d) Multi-tier co-finite fallback (tier 1 IN [s0], tier 2 NOT_IN [s0]):
    # union of {s0} and U \ {s0} is U (unpinned!).
    g_cofin = _strict_pack_gang(
        _SLICE,
        [
            _mk_req(
                {"CPU": 1, "TPU": 4},
                label_selectors=[
                    [(_SLICE, _IN, ["s0"])],
                    [(_SLICE, _NOT_IN, ["s0"])],
                ],
            )
        ],
    )
    assert ResourceDemandScheduler._pinned_locality_domains([g_cofin], cfgs) == {}


def test_stop_requested_and_stopping_live_sibling_subtraction_min_workers():
    """All live siblings of draining replicas (both RAY_STOP_REQUESTED
    and RAY_STOPPING) are subtracted from min_worker_nodes headroom so healthy
    replicas are never over-terminated below min_worker_nodes."""
    cfgs = _lifecycle_tpu_cfg(4, min_w=4, max_w=16)

    def _draining_rep(prefix: str, stop_st) -> List[AutoscalerInstance]:
        return [
            AutoscalerInstance(
                im_instance=Instance(
                    instance_id=f"{prefix}-0",
                    instance_type="tpu",
                    status=stop_st,
                    cloud_instance_id=f"cloud-{prefix}-0",
                    node_kind=NodeKind.WORKER,
                ),
                cloud_instance_id=f"cloud-{prefix}-0",
                cloud_instance_replica_id=prefix,
            )
        ] + [
            _run_node(
                "tpu",
                f"{prefix}-{w}",
                {"CPU": 4, "TPU": 4},
                {_SLICE: prefix, _WID: str(w)},
                idle_ms=60_000,
                rep=prefix,
            )
            for w in range(1, 4)
        ]

    rep0_sr = _draining_rep("rep-0", Instance.RAY_STOP_REQUESTED)
    rep1_sp = _draining_rep("rep-1", Instance.RAY_STOPPING)
    rep2_ok = _make_lifecycle_replica(
        "rep-2", Instance.RAY_RUNNING, 4, busy=False, idle_ms=60_000
    )
    rep3_ok = _make_lifecycle_replica(
        "rep-3", Instance.RAY_RUNNING, 4, busy=False, idle_ms=90_000
    )

    # N=3 (2 draining + 1 healthy): rep-2 MUST be preserved for min_w=4.
    rep_3 = _sched(cfgs, rep0_sr + rep1_sp + rep2_ok, idle_s=10)
    assert _launch_and_terminate(rep_3) == ({}, [])

    # N=4 (2 draining + 2 healthy): only the older healthy replica (rep-3) terminates.
    rep_4 = _sched(cfgs, rep0_sr + rep1_sp + rep2_ok + rep3_ok, idle_s=10)
    assert _launch_and_terminate(rep_4)[0] == {}
    assert sorted(t.instance_id for t in rep_4.to_terminate) == [
        "rep-3-0",
        "rep-3-1",
        "rep-3-2",
        "rep-3-3",
    ]


def test_float_tolerance_and_domain_ranking_edge_cases():
    """Verify 1e-9 float tie-break tolerance, _domain_rank 100%-running
    preference, RAY_STOP_REQUESTED exclusion, single-host pinned max-worker
    protection, and empty label_name / non-STRICT_PACK fallback."""
    # Three 3-tier requests on two identical domains summing to 3 * (1/3)
    # vs 1.0 (which differ by FP rounding <= 1e-9) tie-break deterministically
    # by domain rank regardless of input node ordering.
    cfgs_b4 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
        )
    }
    dom_a = _run_node(
        "gpu",
        "ga-0",
        {"CPU": 8, "GPU": 4},
        {"rack": "a", "k1": "t3", "k2": "t3", "k3": "t3"},
    )
    dom_b = _run_node(
        "gpu",
        "gb-0",
        {"CPU": 8, "GPU": 4},
        {"rack": "b", "k1": "t1", "k2": "t1", "k3": "t1"},
    )
    reqs_b4 = [
        _mk_req(
            {"CPU": 1, "GPU": 1},
            label_selectors=[
                [(f"k{i}", _IN, ["t1"])],
                [(f"k{i}", _IN, ["t2"])],
                [(f"k{i}", _IN, ["t3"])],
            ],
        )
        for i in (1, 2, 3)
    ]
    g_b4 = _strict_pack_gang("rack", reqs_b4)
    # dom_b scores 3.0 > dom_a scores 1.0 (diff > 1e-9): dom_b strictly wins
    # in both node orders.
    for order in ([dom_a, dom_b], [dom_b, dom_a]):
        ctx = _mk_ctx(cfgs_b4, order, gangs=[g_b4])
        upd, inf = ResourceDemandScheduler._try_schedule_gang(ctx, g_b4)
        assert inf == []
        used_ids = [
            n.im_instance_id
            for n in upd
            if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
        ]
        assert used_ids == ["gb-0"]

    # Equal-sum case (1/3 + 2/3 + 1/6 vs 2/3 + 1/3 + 1/6): diff <= 1e-9 ->
    # deterministically prefers rack "a" (earlier domain_val) in both orders.
    reqs_tol2 = [
        _mk_req(
            {"CPU": 1, "GPU": 1},
            label_selectors=[[("k1", _IN, [f"t{t}"])] for t in (1, 2, 3)],
        ),
        _mk_req(
            {"CPU": 1, "GPU": 1},
            label_selectors=[[("k2", _IN, [f"t{t}"])] for t in (1, 2, 3)],
        ),
        _mk_req(
            {"CPU": 1, "GPU": 1},
            label_selectors=[[("k3", _IN, [f"t{t}"])] for t in (1, 2, 3, 4, 5, 6)],
        ),
    ]
    dom_ta = _run_node(
        "gpu",
        "ta",
        {"CPU": 8, "GPU": 4},
        {"rack": "a", "k1": "t3", "k2": "t2", "k3": "t6"},
    )
    dom_tb = _run_node(
        "gpu",
        "tb",
        {"CPU": 8, "GPU": 4},
        {"rack": "b", "k1": "t2", "k2": "t3", "k3": "t6"},
    )
    g_tol2 = _strict_pack_gang("rack", reqs_tol2)
    for order in ([dom_ta, dom_tb], [dom_tb, dom_ta]):
        ctx = _mk_ctx(cfgs_b4, order, gangs=[g_tol2])
        upd, inf = ResourceDemandScheduler._try_schedule_gang(ctx, g_tol2)
        assert inf == []
        used_ids = [
            n.im_instance_id
            for n in upd
            if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
        ]
        assert used_ids == ["ta"]

    # 100%-running slice strictly outranks half-booting slice (2 running + 2 ALLOCATED).
    cfgs_tpu = {"tpu_4x4": _tpu_config(max_workers=12)}
    half_boot_slice = [
        _tpu_host("a-0", "slice-a", 0, rep="slice-a"),
        _tpu_host("a-1", "slice-a", 1, rep="slice-a"),
        _pending_tpu("a-p2", Instance.ALLOCATED, rep="slice-a"),
        _pending_tpu("a-p3", Instance.ALLOCATED, rep="slice-a"),
    ]
    full_run_slice = [
        _tpu_host(f"b-{w}", "slice-b", w, rep="slice-b") for w in range(4)
    ]
    g_tpu = _tpu_gang((0, 1, 2, 3))
    ctx_n2 = _mk_ctx(cfgs_tpu, half_boot_slice + full_run_slice, gangs=[g_tpu])
    upd_n2, inf_n2 = ResourceDemandScheduler._try_schedule_gang(ctx_n2, g_tpu)
    assert inf_n2 == []
    scheduled_slices = {
        n.labels.get(_SLICE)
        for n in upd_n2
        if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
    }
    assert scheduled_slices == {"slice-b"}

    # RAY_STOP_REQUESTED nodes are excluded from domains and unlabeled_pending_by_type.
    sr_node = AutoscalerInstance(
        im_instance=Instance(
            instance_id="sr-0",
            instance_type="gpu",
            status=Instance.RAY_STOP_REQUESTED,
            cloud_instance_id="cloud-sr-0",
            node_id="sr-0",
            node_kind=NodeKind.WORKER,
        ),
        ray_node=NodeState(
            node_id=b"sr-0",
            ray_node_type_name="gpu",
            total_resources={"CPU": 8, "GPU": 4},
            available_resources={"CPU": 8, "GPU": 4},
            labels={"rack": "r-stopping"},
            status=NodeStatus.IDLE,
        ),
        cloud_instance_id="cloud-sr-0",
    )
    g_n3 = _strict_pack_gang("rack", [_mk_req({"CPU": 4, "GPU": 4})])
    rep_n3 = _sched(cfgs_b4, [sr_node], gangs=[g_n3])
    assert _launch_and_terminate(rep_n3) == ({"gpu": 1}, [])

    # Single-host node pinned by an unscheduled gang is protected during
    # max_worker_nodes enforcement while an unpinned idle sibling terminates first.
    cfgs_n5 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=1,
        )
    }
    n_pinned = _run_node(
        "gpu",
        "g-pin",
        {"CPU": 8, "GPU": 4},
        {"rack": "rack-pin"},
        idle_ms=90_000,
    )
    n_unpinned = _run_node(
        "gpu",
        "g-free",
        {"CPU": 8, "GPU": 4},
        {"rack": "rack-free"},
        idle_ms=10_000,
    )
    g_n5 = _strict_pack_gang(
        "rack",
        [_mk_req({"CPU": 4, "GPU": 4}) for _ in range(2)],
        loc_constraints=[("rack", _IN, ["rack-pin"])],
    )
    rep_n5 = _sched(cfgs_n5, [n_pinned, n_unpinned], gangs=[g_n5])
    assert _launch_and_terminate(rep_n5)[0] == {}
    assert [t.instance_id for t in rep_n5.to_terminate] == ["g-free"]

    # Empty locality label_name or non-STRICT_PACK falls back to normal
    # non-locality scheduling without error.
    g_empty_lbl = _strict_pack_gang("", [_mk_req({"CPU": 4, "GPU": 4})])
    g_pack = _strict_pack_gang(
        "rack",
        [_mk_req({"CPU": 4, "GPU": 4})],
        strategy=PlacementStrategy.PACK,
    )
    rep_n7 = _sched(cfgs_b4, [], gangs=[g_empty_lbl, g_pack])
    assert _launch_and_terminate(rep_n7) == ({"gpu": 2}, [])
    assert rep_n7.infeasible_gang_resource_requests == []


def test_reused_cid_replica_view_parity_and_gang_scheduling():
    """Verify exact parity between _MultiHostReplicaView.build and
    ScheduleContext.from_schedule_request across reused cloud_instance_id
    incarnations (healthy untagged adopter, RAY_STOPPED teardown, older
    torn-down incarnation, and cross-node-type reuse) and ensure a torn-down
    later batch reusing a vanished pod's ID never marks the earlier open batch
    draining or triggers a duplicate replica launch."""
    from ray.autoscaler.v2.instance_manager.common import _MultiHostReplicaView
    from ray.autoscaler.v2.instance_manager.node_provider import CloudInstance
    from ray.autoscaler.v2.tests.util import create_instance

    cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4, "TPU": 4},
            labels={_TOPO: "4x4", _ACC: "TPU-V6E"},
            min_worker_nodes=0,
            max_worker_nodes=16,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                worker_index_label_key=_WID,
            ),
            runtime_label_keys=(_SLICE, _WID),
        )
    }

    def _to_ai(rows, cloud):
        out = []
        for im in rows:
            cid = im.cloud_instance_id or None
            ci = cloud.get(cid) if cid else None
            ray_node = None
            if im.status == Instance.RAY_RUNNING:
                ray_node = NodeState(
                    node_id=im.instance_id.encode(),
                    ray_node_type_name=im.instance_type,
                    total_resources={"CPU": 4, "TPU": 4},
                    available_resources={"CPU": 4, "TPU": 4},
                    labels={_SLICE: "slice-x", _WID: "0"},
                    status=NodeStatus.RUNNING,
                )
                im.node_id = im.instance_id
            out.append(
                AutoscalerInstance(
                    ray_node=ray_node,
                    im_instance=im,
                    cloud_instance_id=cid,
                    cloud_instance_replica_id=(
                        (ci.replica_id or None)
                        if (
                            ci is not None
                            and im.node_kind != NodeKind.HEAD
                            and ci.node_kind != NodeKind.HEAD
                            and (not ci.node_type or ci.node_type == im.instance_type)
                        )
                        else None
                    ),
                )
            )
        return out

    from unittest.mock import MagicMock

    from ray.autoscaler._private.kuberay.autoscaling_config import (
        _derive_autoscaling_config_from_ray_cr,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
    from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
    from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
    from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
        CloudResourceMonitor,
    )
    from ray.core.generated.autoscaler_pb2 import AutoscalingState, ClusterResourceState
    from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    wg = copy.deepcopy(cr["spec"]["workerGroupSpecs"][0])
    wg.update(
        groupName="tpu",
        replicas=0,
        minReplicas=0,
        maxReplicas=4,
        numOfHosts=4,
        rayStartParams={},
    )
    wg["template"]["spec"]["nodeSelector"] = {
        "cloud.google.com/gke-tpu-accelerator": "tpu-v6e-slice",
        "cloud.google.com/gke-tpu-topology": "4x4",
    }
    wg["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"cpu": "4", "memory": "8Gi", "google.com/tpu": "4"},
        "requests": {"cpu": "4", "memory": "8Gi", "google.com/tpu": "4"},
    }
    cr["spec"]["workerGroupSpecs"] = [wg]
    acfg = AutoscalingConfig(
        configs=_derive_autoscaling_config_from_ray_cr(cr),
        skip_content_hash=True,
    )

    class _CaptureScheduler(ResourceDemandScheduler):
        def __init__(self):
            super().__init__(event_logger)
            self.last_req: Optional[SchedulingRequest] = None
            self.last_reply: Optional[SchedulingReply] = None

        def schedule(self, request: SchedulingRequest) -> SchedulingReply:
            self.last_req = request
            self.last_reply = super().schedule(request)
            return self.last_reply

    def _run_scale_cluster(rows, cl, gang_list):
        storage = InstanceStorage(cluster_id="reused-cid", storage=InMemoryStorage())
        im_mgr = InstanceManager(
            instance_storage=storage, instance_status_update_subscribers=[]
        )
        copied_rows = copy.deepcopy(rows)
        node_states = []
        for idx, r in enumerate(copied_rows):
            r.version = 0
            if r.status == Instance.RAY_RUNNING:
                nid = f"{idx + 1:056x}"
                r.node_id = nid
                node_states.append(
                    NodeState(
                        node_id=bytes.fromhex(nid),
                        instance_id=r.cloud_instance_id,
                        ray_node_type_name=r.instance_type,
                        total_resources={"CPU": 4, "TPU": 4},
                        available_resources={"CPU": 4, "TPU": 4},
                        labels={_SLICE: "slice-x", _WID: "0"},
                        status=NodeStatus.RUNNING,
                    )
                )
        assert storage.batch_upsert_instances(copied_rows, expected_storage_version=0)[
            0
        ]
        cap = _CaptureScheduler()
        Reconciler._scale_cluster(
            autoscaling_state=AutoscalingState(),
            instance_manager=im_mgr,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(
                node_states=node_states,
                pending_gang_resource_requests=gang_list,
            ),
            scheduler=cap,
            autoscaling_config=acfg,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cl,
            view_cache=None,
        )
        assert cap.last_req is not None and cap.last_reply is not None
        return cap.last_req, cap.last_reply

    def _assert_parity(rows, cloud):
        view = _MultiHostReplicaView.build(rows, cloud, {"tpu": 4})
        prod_req, _ = _run_scale_cluster(rows, cloud, [])
        prod_view = prod_req.replica_view
        syn_view = _synthetic_replica_view(cfgs, _to_ai(copy.deepcopy(rows), cloud))
        assert prod_view is not None and syn_view is not None
        for cand in (prod_view, syn_view):
            assert {k: sorted(v) for k, v in view.rep_lids.items() if v} == {
                k: sorted(v) for k, v in cand.rep_lids.items() if v
            }
            assert {
                k: sorted(v) for k, v in view.vanished_cids_by_lid.items() if v
            } == {k: sorted(v) for k, v in cand.vanished_cids_by_lid.items() if v}
            assert view.non_fillable_reps == cand.non_fillable_reps
            assert view.torn_down_lids == cand.torn_down_lids

    live = {
        "p0": CloudInstance(
            cloud_instance_id="p0",
            node_type="tpu",
            is_running=True,
            node_kind=NodeKind.WORKER,
            replica_id="rep-2",
        )
    }
    b3_base = [
        create_instance(
            "r1",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-1",
            status_times=[
                (Instance.ALLOCATED, 100),
                (Instance.RAY_RUNNING, 110),
                (Instance.TERMINATED, 120),
            ],
        ),
        create_instance(
            "r1-req",
            status=Instance.REQUESTED,
            instance_type="tpu",
            launch_request_id="lid-1",
        ),
        create_instance(
            "r2",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-2",
            status_times=[
                (Instance.ALLOCATED, 200),
                (Instance.RAY_RUNNING, 210),
                (Instance.TERMINATED, 220),
            ],
        ),
        create_instance(
            "r2-req",
            status=Instance.REQUESTED,
            instance_type="tpu",
            launch_request_id="lid-2",
        ),
    ]
    u2_healthy = create_instance(
        "u2-healthy",
        status=Instance.RAY_RUNNING,
        instance_type="tpu",
        cloud_instance_id="p0",
        launch_request_id="",
        status_times=[(Instance.ALLOCATED, 220), (Instance.RAY_RUNNING, 230)],
    )
    _assert_parity([*b3_base, u2_healthy], live)

    u2_stopped = create_instance(
        "u2-live",
        status=Instance.RAY_STOPPED,
        instance_type="tpu",
        cloud_instance_id="p0",
        launch_request_id="",
        status_times=[
            (Instance.ALLOCATED, 220),
            (Instance.RAY_RUNNING, 230),
            (Instance.RAY_STOPPED, 240),
        ],
    )
    _assert_parity([*b3_base, u2_stopped], live)

    b1 = [
        create_instance(
            "r1",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-1",
            status_times=[
                (Instance.REQUESTED, 50),
                (Instance.ALLOCATED, 100),
                (Instance.RAY_RUNNING, 110),
                (Instance.TERMINATED, 120),
            ],
        ),
        create_instance(
            "u1",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="",
            status_times=[
                (Instance.ALLOCATED, 120),
                (Instance.TERMINATING, 140),
                (Instance.TERMINATED, 150),
            ],
        ),
        create_instance(
            "r2",
            status=Instance.RAY_RUNNING,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-2",
            status_times=[
                (Instance.REQUESTED, 80),
                (Instance.ALLOCATED, 200),
                (Instance.RAY_RUNNING, 210),
            ],
        ),
        create_instance(
            "r2-req",
            status=Instance.REQUESTED,
            instance_type="tpu",
            launch_request_id="lid-2",
            status_times=[(Instance.REQUESTED, 80)],
        ),
    ]
    _assert_parity(b1, live)

    other = {
        "p0": CloudInstance(
            cloud_instance_id="p0",
            node_type="other",
            is_running=True,
            node_kind=NodeKind.WORKER,
            replica_id="rep-other",
        )
    }
    cross = [
        create_instance(
            "r1",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-1",
            status_times=[
                (Instance.ALLOCATED, 100),
                (Instance.RAY_RUNNING, 110),
                (Instance.TERMINATED, 120),
            ],
        ),
        create_instance(
            "r1-req",
            status=Instance.REQUESTED,
            instance_type="tpu",
            launch_request_id="lid-1",
        ),
    ]
    head_cloud = {
        "p0": CloudInstance(
            cloud_instance_id="p0",
            node_type="tpu",
            is_running=True,
            node_kind=NodeKind.HEAD,
            replica_id="rep-head",
        )
    }
    _assert_parity(cross, other)
    _assert_parity(cross, head_cloud)

    # Observable gang-scheduling effect: open healthy batch lid-1 (3 REQUESTED +
    # 1 vanished pod p0) + later torn-down batch lid-2 reusing p0 (or a foreign
    # node_type / HEAD pod reusing p0) must NOT mark lid-1 draining or launch a
    # second 4-worker replica.
    lid1 = [
        create_instance(
            "r1",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-1",
            status_times=[
                (Instance.REQUESTED, 90),
                (Instance.ALLOCATED, 100),
                (Instance.TERMINATED, 120),
            ],
        ),
        *[
            create_instance(
                f"r1-{sfx}",
                status=Instance.REQUESTED,
                instance_type="tpu",
                launch_request_id="lid-1",
                status_times=[(Instance.REQUESTED, 90)],
            )
            for sfx in ("a", "b", "c")
        ],
    ]
    lid2 = [
        create_instance(
            "r2",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="lid-2",
            status_times=[
                (Instance.REQUESTED, 150),
                (Instance.ALLOCATED, 200),
                (Instance.RAY_RUNNING, 210),
                (Instance.TERMINATED, 220),
            ],
        ),
        u2_stopped,
    ]
    gang_reqs = [
        _mk_req(
            {"CPU": 1, "TPU": 4},
            label_selectors=[
                [(_TOPO, _IN, ["4x4"]), (_ACC, _IN, ["TPU-V6E"]), (_WID, _IN, [str(h)])]
            ],
        )
        for h in range(4)
    ]
    g = _locality_gang(gang_reqs, _SLICE)
    for rows, cl in (
        (lid1, {}),
        (lid1 + lid2, live),
        (lid1, other),
        (lid1, head_cloud),
    ):
        _, prod_rep = _run_scale_cluster(rows, cl, [g])
        assert _launch_and_terminate(prod_rep) == ({}, [])
        assert prod_rep.infeasible_gang_resource_requests == []
        ais = _to_ai(copy.deepcopy(rows), cl)
        view = _MultiHostReplicaView.build(rows, cl, {"tpu": 4})
        for rv in (view, _synthetic_replica_view(cfgs, ais)):
            rep = ResourceDemandScheduler(event_logger).schedule(
                SchedulingRequest(
                    disable_launch_config_check=True,
                    node_type_configs=cfgs,
                    current_instances=ais,
                    gang_resource_requests=[g],
                    replica_view=rv,
                )
            )
            assert _launch_and_terminate(rep) == ({}, [])
            assert rep.infeasible_gang_resource_requests == []

    # When a draining replica's TERMINATED row on a reused pod is listed before
    # a live ALLOCATED row on the same pod, neither _synthetic_replica_view nor
    # an omitted replica_view=None marks the new live batch draining.
    reused_order_ais = [
        AutoscalerInstance(
            ray_node=None,
            im_instance=create_instance(
                "w-r1-stop",
                status=Instance.RAY_STOP_REQUESTED,
                instance_type="tpu",
                cloud_instance_id="pod-y",
                launch_request_id="lid-1",
            ),
            cloud_instance_id="pod-y",
            cloud_instance_replica_id="rep-1",
        ),
        AutoscalerInstance(
            ray_node=None,
            im_instance=create_instance(
                "w-old-term",
                status=Instance.TERMINATED,
                instance_type="tpu",
                cloud_instance_id="pod-x",
                launch_request_id="lid-1",
            ),
            cloud_instance_id="pod-x",
            cloud_instance_replica_id="rep-1",
        ),
        AutoscalerInstance(
            ray_node=None,
            im_instance=create_instance(
                "w-new-live",
                status=Instance.ALLOCATED,
                instance_type="tpu",
                cloud_instance_id="pod-x",
                launch_request_id="lid-2",
            ),
            cloud_instance_id="pod-x",
            cloud_instance_replica_id="rep-2",
        ),
    ]
    syn_reused_view = _synthetic_replica_view(cfgs, reused_order_ais)
    assert syn_reused_view is not None
    assert ("tpu", "lid-2") not in syn_reused_view.torn_down_lids
    for rv in (syn_reused_view, None):
        ctx_standalone = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=reused_order_ais,
                replica_view=rv,
            )
        )
        nodes_by_id = {n.im_instance_id: n for n in ctx_standalone.get_nodes()}
        assert not nodes_by_id["w-new-live"]._is_draining_replica


def test_locality_gang_prefilter_implicit_resources_and_ippr_resize():
    """Verify _try_schedule_gang's aggregate domain prefilter accounts for
    implicit resources (IMPLICIT_RESOURCE_PREFIX -> 1.0 per node) and does not
    prune existing idle domains whose pods can resize up in place via IPPR."""
    from ray._raylet import IMPLICIT_RESOURCE_PREFIX

    imp_key = f"{IMPLICIT_RESOURCE_PREFIX}custom_implicit"
    gb = 1024 * 1024 * 1024
    cfgs = {
        "tpu_4x4": NodeTypeConfig(
            name="tpu_4x4",
            resources={"CPU": 4, "TPU": 4, "memory": 4 * gb},
            labels=dict(TPU_LABELS),
            min_worker_nodes=0,
            max_worker_nodes=16,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                worker_index_label_key=_WID,
            ),
            runtime_label_keys=(_SLICE, _WID),
        )
    }

    def _make_gang(extra=None, cpu=1):
        reqs = []
        for h in range(4):
            lbl = {_TOPO: "4x4", _ACC: "TPU-V6E", _WID: str(h)}
            bundle = {"CPU": cpu, "TPU": 4, **(extra or {})}
            reqs.append(
                _mk_req(
                    bundle,
                    label_selectors=[[(k, _IN, [v]) for k, v in lbl.items()]],
                )
            )
        return _locality_gang(reqs, _SLICE)

    idle = [_tpu_host(f"a{i}", "slice-a", i, None, 0, rep="rep-a") for i in range(4)]
    alloc = [_pending_tpu(f"p{i}", Instance.ALLOCATED, rep="rep-0") for i in range(4)]

    # 1. Implicit resource on locality gang from zero, on idle replica, and on 4 ALLOCATED.
    rep_zero = _sched(cfgs, [], gangs=[_make_gang({imp_key: 1.0})])
    assert _launch_and_terminate(rep_zero) == ({"tpu_4x4": 4}, [])
    assert rep_zero.infeasible_gang_resource_requests == []

    for existing in (idle, alloc):
        for imp_val in (1.0, 0.5):
            rep_ex = _sched(cfgs, existing, gangs=[_make_gang({imp_key: imp_val})])
            assert _launch_and_terminate(rep_ex) == ({}, [])
            assert rep_ex.infeasible_gang_resource_requests == []

    # Negative counter-case: implicit resource > 1.0 per bundle is infeasible.
    rep_over = _sched(cfgs, idle, gangs=[_make_gang({imp_key: 2.0})])
    assert _launch_and_terminate(rep_over) == ({}, [])
    assert len(rep_over.infeasible_gang_resource_requests) == 1

    # 2. IPPR in-place resize on an existing idle locality domain (CPU 4 -> 6).
    specs = IPPRSpecs(
        groups={
            "tpu_4x4": IPPRGroupSpec(
                min_cpu=4,
                max_cpu=8,
                min_memory=4 * gb,
                max_memory=8 * gb,
                resize_timeout=60,
            )
        }
    )
    ippr_insts = [
        _run_node(
            "tpu_4x4",
            f"a{i}",
            {"CPU": 4, "TPU": 4, "memory": 4 * gb},
            {**TPU_LABELS, _SLICE: "slice-a", _WID: str(i)},
            None,
            0,
            rep="rep-a",
        )
        for i in range(4)
    ]
    ippr_statuses = {
        f"cloud-a{i}": IPPRStatus(
            cloud_instance_id=f"cloud-a{i}",
            spec=specs.groups["tpu_4x4"],
            current_cpu=4,
            current_memory=4 * gb,
            desired_cpu=4,
            desired_memory=4 * gb,
        )
        for i in range(4)
    }
    rep_ippr = _sched(
        cfgs,
        ippr_insts,
        gangs=[_make_gang(cpu=6)],
        ippr_specs=specs,
        ippr_statuses=ippr_statuses,
    )
    assert _launch_and_terminate(rep_ippr) == ({}, [])
    assert rep_ippr.infeasible_gang_resource_requests == []
    assert sorted((s.cloud_instance_id, s.desired_cpu) for s in rep_ippr.to_ippr) == [
        (f"cloud-a{i}", 8) for i in range(4)
    ]

    mixed_reqs = [
        _mk_req(
            {"CPU": cpu, "TPU": 4},
            label_selectors=[
                [(_TOPO, _IN, ["4x4"]), (_ACC, _IN, ["TPU-V6E"]), (_WID, _IN, [str(h)])]
            ],
        )
        for h, cpu in enumerate((6, 6, 1, 1))
    ]
    rep_mixed = _sched(
        cfgs,
        ippr_insts,
        gangs=[_locality_gang(mixed_reqs, _SLICE)],
        ippr_specs=specs,
        ippr_statuses={
            f"cloud-a{i}": IPPRStatus(
                cloud_instance_id=f"cloud-a{i}",
                spec=specs.groups["tpu_4x4"],
                current_cpu=4,
                current_memory=4 * gb,
                desired_cpu=4,
                desired_memory=4 * gb,
            )
            for i in range(4)
        },
    )
    assert _launch_and_terminate(rep_mixed) == ({}, [])
    assert rep_mixed.infeasible_gang_resource_requests == []
    assert sorted((s.cloud_instance_id, s.desired_cpu) for s in rep_mixed.to_ippr) == [
        ("cloud-a0", 8),
        ("cloud-a1", 8),
    ]

    # 3. In-progress IPPR resize (is_in_progress=True, current_cpu=4, desired_cpu=6)
    # where can_resize_up is False because resize is already in progress.
    prog_statuses = {
        f"cloud-a{i}": IPPRStatus(
            cloud_instance_id=f"cloud-a{i}",
            spec=specs.groups["tpu_4x4"],
            current_cpu=4,
            current_memory=4 * gb,
            desired_cpu=6,
            desired_memory=4 * gb,
            resizing_at=100,
            k8s_resize_status="inprogress",
            raylet_id=f"a{i}",
        )
        for i in range(4)
    }
    rep_prog = _sched(
        cfgs,
        ippr_insts,
        gangs=[_make_gang(cpu=6)],
        ippr_specs=specs,
        ippr_statuses=prog_statuses,
    )
    assert _launch_and_terminate(rep_prog) == ({}, [])
    assert rep_prog.infeasible_gang_resource_requests == []


def test_max_workers_pending_replica_cancelability_and_completeness_order():
    """Verify max-worker scale-down prioritizes immediately cancelable pending
    states ahead of REQUESTED batches, prefers incomplete pending replicas ahead
    of complete ones at equal cancelability rank, and treats pinned domains as
    non-idle without overriding busy utilization."""
    cfgs_cap4 = {"tpu_4x4": _tpu_config("tpu_4x4", max_workers=4)}

    # 1. Cancelability precedes completeness: 3 ALLOCATED nodes (immediately
    # cancelable) must be terminated ahead of 4 REQUESTED nodes (which
    # Reconciler._scale_cluster cannot cancel until cloud allocation finishes).
    alloc3 = [
        _pending_tpu(f"alloc-{i}", Instance.ALLOCATED, rep="rep-alloc")
        for i in range(3)
    ]
    req4 = []
    for i in range(4):
        inst = make_autoscaler_instance(
            im_instance=Instance(
                instance_type="tpu_4x4",
                status=Instance.REQUESTED,
                instance_id=f"req-{i}",
                launch_request_id="lid-req",
            )
        )
        req4.append(inst)

    rep_cancel = _sched(cfgs_cap4, alloc3 + req4)
    assert rep_cancel.to_launch == []
    assert [r.instance_id for r in rep_cancel.to_terminate] == [
        "alloc-0",
        "alloc-1",
        "alloc-2",
    ]
    assert all(
        r.cause == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE
        for r in rep_cancel.to_terminate
    )

    # 2. Equal cancelability rank: incomplete 3-of-4 ALLOCATED replica is
    # terminated ahead of a complete 4-of-4 ALLOCATED replica.
    alloc4 = [
        _pending_tpu(f"full-{i}", Instance.ALLOCATED, rep="rep-full") for i in range(4)
    ]
    rep_complete = _sched(cfgs_cap4, alloc4 + alloc3)
    assert rep_complete.to_launch == []
    assert [r.instance_id for r in rep_complete.to_terminate] == [
        "alloc-0",
        "alloc-1",
        "alloc-2",
    ]

    # 3. Pinned domains rank as non-idle behind unpinned idle replicas, while
    # among busy replicas lower utilization still terminates first.
    pin_gang = _tpu_gang(wids=(3,), pinned="slice-pin")
    idle_pin = [
        _tpu_host(f"p{i}", "slice-pin", i, None, 60_000, rep="rep-pin")
        for i in range(4)
    ]
    idle_free = [
        _tpu_host(f"f{i}", "slice-free", i, None, 60_000, rep="rep-free")
        for i in range(4)
    ]
    rep_idle_pin = _sched(cfgs_cap4, idle_pin + idle_free, gangs=[pin_gang])
    assert sorted(r.instance_id for r in rep_idle_pin.to_terminate) == [
        "f0",
        "f1",
        "f2",
        "f3",
    ]

    busy_pin_half = [
        _tpu_host(f"bp{i}", "slice-pin", i, {"CPU": 2, "TPU": 2}, 0, rep="rep-pin")
        for i in range(4)
    ]
    busy_free_full = [
        _tpu_host(f"bf{i}", "slice-free", i, {"CPU": 4, "TPU": 4}, 0, rep="rep-free")
        for i in range(4)
    ]
    rep_busy_pin = _sched(cfgs_cap4, busy_pin_half + busy_free_full, gangs=[pin_gang])
    assert sorted(r.instance_id for r in rep_busy_pin.to_terminate) == [
        "bp0",
        "bp1",
        "bp2",
        "bp3",
    ]


def test_pending_domain_dedup_preserves_ippr_and_total_resources():
    """Verify pending locality domain deduplication does not collapse domains
    whose pods differ in IPPR resize eligibility or total capacity."""
    gb = 1024 * 1024 * 1024
    cfgs = {
        "tpu_4x4": NodeTypeConfig(
            name="tpu_4x4",
            resources={"CPU": 4, "TPU": 4, "memory": 4 * gb},
            labels=dict(TPU_LABELS),
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                worker_index_label_key=_WID,
            ),
            runtime_label_keys=(_SLICE, _WID),
        )
    }
    specs = IPPRSpecs(
        groups={
            "tpu_4x4": IPPRGroupSpec(
                min_cpu=4,
                max_cpu=8,
                min_memory=4 * gb,
                max_memory=8 * gb,
                resize_timeout=60,
            )
        }
    )
    rep_a = [_pending_tpu(f"a{i}", Instance.ALLOCATED, rep="rep-a") for i in range(4)]
    rep_b = [_pending_tpu(f"b{i}", Instance.ALLOCATED, rep="rep-b") for i in range(4)]
    statuses = {
        **{
            f"cloud-a{i}": IPPRStatus(
                cloud_instance_id=f"cloud-a{i}",
                spec=specs.groups["tpu_4x4"],
                current_cpu=4,
                current_memory=4 * gb,
                desired_cpu=4,
                desired_memory=4 * gb,
                last_failed_at=100,
                last_failed_reason="infeasible",
            )
            for i in range(4)
        },
        **{
            f"cloud-b{i}": IPPRStatus(
                cloud_instance_id=f"cloud-b{i}",
                spec=specs.groups["tpu_4x4"],
                current_cpu=4,
                current_memory=4 * gb,
                desired_cpu=4,
                desired_memory=4 * gb,
            )
            for i in range(4)
        },
    }
    gang_cpu6 = _locality_gang(
        [
            _mk_req(
                {"CPU": 6, "TPU": 4},
                label_selectors=[
                    [
                        (_TOPO, _IN, ["4x4"]),
                        (_ACC, _IN, ["TPU-V6E"]),
                        (_WID, _IN, [str(h)]),
                    ]
                ],
            )
            for h in range(4)
        ],
        _SLICE,
    )
    reply = _sched(
        cfgs,
        rep_a + rep_b,
        gangs=[gang_cpu6],
        ippr_specs=specs,
        ippr_statuses=statuses,
    )
    assert _launch_and_terminate(reply) == ({}, [])
    assert reply.infeasible_gang_resource_requests == []

    # Shared-clique running nodes assigned _pending_domain_* by prior gangs also
    # emit to_ippr on the resizable domain rather than skipping it via dedup.
    clique_cfgs = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 4, "GPU": 4, "memory": 4 * gb},
            labels={},
            min_worker_nodes=0,
            max_worker_nodes=2,
            runtime_label_keys=("clique",),
        )
    }
    gpu_specs = IPPRSpecs(
        groups={
            "gpu": IPPRGroupSpec(
                min_cpu=4,
                max_cpu=8,
                min_memory=4 * gb,
                max_memory=8 * gb,
                resize_timeout=60,
            )
        }
    )
    clique_nodes = [
        _pending_node("gpu", "g0", Instance.ALLOCATED, rep="rg0"),
        _pending_node("gpu", "g1", Instance.ALLOCATED, rep="rg1"),
    ]
    clique_statuses = {
        "cloud-g0": IPPRStatus(
            cloud_instance_id="cloud-g0",
            spec=gpu_specs.groups["gpu"],
            current_cpu=4,
            current_memory=4 * gb,
            desired_cpu=4,
            desired_memory=4 * gb,
            raylet_id="raylet-g0",
            last_failed_at=100,
            last_failed_reason="infeasible",
        ),
        "cloud-g1": IPPRStatus(
            cloud_instance_id="cloud-g1",
            spec=gpu_specs.groups["gpu"],
            current_cpu=4,
            current_memory=4 * gb,
            desired_cpu=4,
            desired_memory=4 * gb,
            raylet_id="raylet-g1",
        ),
    }
    seed_gang_0 = _locality_gang([_mk_req({"CPU": 1, "GPU": 2})], "clique")
    seed_gang_1 = _locality_gang([_mk_req({"CPU": 1, "GPU": 2})], "clique")
    resize_gang = _locality_gang([_mk_req({"CPU": 5, "GPU": 2})], "clique")
    clique_reply = _sched(
        clique_cfgs,
        clique_nodes,
        gangs=[seed_gang_0, seed_gang_1, resize_gang],
        ippr_specs=gpu_specs,
        ippr_statuses=clique_statuses,
    )
    assert _launch_and_terminate(clique_reply) == ({}, [])
    assert clique_reply.infeasible_gang_resource_requests == []
    assert [(s.cloud_instance_id, s.desired_cpu) for s in clique_reply.to_ippr] == [
        ("cloud-g1", 8)
    ]


def test_locality_gang_bundle_label_domain_pruning_and_fallback(monkeypatch):
    """Verify bundle-level locality-label pins/exclusions prune unrelated
    domains without over-pruning multi-tier fallbacks that retain an unpinned
    tier."""
    cfgs = {"tpu_4x4": _tpu_config(max_workers=12)}
    slice_a = [_tpu_host(f"a{i}", "slice-a", i, None, 0, rep="rep-a") for i in range(4)]
    slice_b = [_tpu_host(f"b{i}", "slice-b", i, None, 0, rep="rep-b") for i in range(4)]

    def _pinned_bundle_gang(selectors_fn, pinned=None):
        reqs = [
            _mk_req({"CPU": 1, "TPU": 4}, label_selectors=selectors_fn(h))
            for h in range(4)
        ]
        return _locality_gang(reqs, _SLICE, pinned=pinned)

    from ray.autoscaler.v2.scheduler import _GangSearch

    evaluated_domains: List[str] = []
    orig_sid = _GangSearch.schedule_in_domain

    def _spy_sid(*args, **kwargs):
        evaluated_domains.append(args[7] if len(args) > 7 else kwargs["domain_val"])
        return orig_sid(*args, **kwargs)

    monkeypatch.setattr(_GangSearch, "schedule_in_domain", staticmethod(_spy_sid))

    # Positive: bundle pins slice-b; slice-a is never evaluated and no new slice launches.
    g_pin_b = _pinned_bundle_gang(
        lambda h: [
            [
                (_TOPO, _IN, ["4x4"]),
                (_ACC, _IN, ["TPU-V6E"]),
                (_WID, _IN, [str(h)]),
                (_SLICE, _IN, ["slice-b"]),
            ]
        ]
    )
    rep_b = _sched(cfgs, slice_a + slice_b, gangs=[g_pin_b])
    assert _launch_and_terminate(rep_b) == ({}, [])
    assert rep_b.infeasible_gang_resource_requests == []
    assert evaluated_domains == ["slice-b"]

    # Positive: bundle pins nonexistent slice-z; neither idle slice nor new replica is tried.
    evaluated_domains.clear()
    g_pin_missing = _pinned_bundle_gang(
        lambda h: [
            [
                (_TOPO, _IN, ["4x4"]),
                (_ACC, _IN, ["TPU-V6E"]),
                (_WID, _IN, [str(h)]),
                (_SLICE, _IN, ["slice-z"]),
            ]
        ]
    )
    rep_missing = _sched(cfgs, slice_a + slice_b, gangs=[g_pin_missing])
    assert _launch_and_terminate(rep_missing) == ({}, [])
    assert len(rep_missing.infeasible_gang_resource_requests) == 1
    assert evaluated_domains == []

    # Negative counter-case: 2-tier fallback where tier 0 pins missing slice-z
    # and tier 1 is unpinned still places on existing idle slice-a.
    evaluated_domains.clear()
    g_fallback = _pinned_bundle_gang(
        lambda h: [
            [
                (_TOPO, _IN, ["4x4"]),
                (_ACC, _IN, ["TPU-V6E"]),
                (_WID, _IN, [str(h)]),
                (_SLICE, _IN, ["slice-z"]),
            ],
            [
                (_TOPO, _IN, ["4x4"]),
                (_ACC, _IN, ["TPU-V6E"]),
                (_WID, _IN, [str(h)]),
            ],
        ]
    )
    rep_fb = _sched(cfgs, slice_a, gangs=[g_fallback])
    assert _launch_and_terminate(rep_fb) == ({}, [])
    assert rep_fb.infeasible_gang_resource_requests == []
    assert "slice-a" in evaluated_domains

    # Negative counter-case: contradictory locality_req pin vs bundle NOT_IN fails fast.
    evaluated_domains.clear()
    g_conflict = _pinned_bundle_gang(
        lambda h: [
            [
                (_TOPO, _IN, ["4x4"]),
                (_ACC, _IN, ["TPU-V6E"]),
                (_WID, _IN, [str(h)]),
                (_SLICE, _NOT_IN, ["slice-a"]),
            ]
        ],
        pinned="slice-a",
    )
    rep_conflict = _sched(cfgs, slice_a + slice_b, gangs=[g_conflict])
    assert _launch_and_terminate(rep_conflict) == ({}, [])
    assert len(rep_conflict.infeasible_gang_resource_requests) == 1
    assert evaluated_domains == []


def test_duplicate_infeasible_gang_dedup_and_invalidation(monkeypatch):
    """Verify _sched_gang_resource_requests deduplicates identical infeasible
    gangs with distinct details while invalidating on context updates and
    preserving distinct anti-affinity / internal-prefix gangs."""
    cfgs = {"tpu_4x4": _tpu_config(max_workers=4)}
    busy_slice = [
        _tpu_host(f"b{i}", "slice-busy", i, {"CPU": 3, "TPU": 4}, 0, rep="rep-b")
        for i in range(4)
    ]

    calls = 0
    orig_tsg = ResourceDemandScheduler._try_schedule_gang

    def _spy_tsg(*args, **kwargs):
        nonlocal calls
        calls += 1
        return orig_tsg(*args, **kwargs)

    monkeypatch.setattr(
        ResourceDemandScheduler, "_try_schedule_gang", staticmethod(_spy_tsg)
    )

    # Positive: 4 identical infeasible gangs with unique PG details -> evaluated once.
    dup_gangs = []
    for i in range(4):
        g = _tpu_gang()
        g.details = f"pg-dup-{i}"
        dup_gangs.append(g)
    rep_dup = _sched(cfgs, busy_slice, gangs=dup_gangs)
    assert len(rep_dup.infeasible_gang_resource_requests) == 4
    assert calls == 1

    # Negative: distinct anti-affinity constraints are never skipped.
    calls = 0
    distinct_gangs = []
    for i in range(3):
        reqs = [
            _mk_req(
                {"CPU": 1, "TPU": 4},
                [(_ANTI, f"_PG_{i}", f"v{i}")],
                label_selectors=[
                    [
                        (_TOPO, _IN, ["4x4"]),
                        (_ACC, _IN, ["TPU-V6E"]),
                        (_WID, _IN, [str(h)]),
                    ]
                ],
            )
            for h in range(4)
        ]
        distinct_gangs.append(_locality_gang(reqs, _SLICE))
    rep_dist = _sched(cfgs, busy_slice, gangs=distinct_gangs)
    assert len(rep_dist.infeasible_gang_resource_requests) == 3
    assert calls == 3

    # Negative: cache clears when an intervening feasible gang updates ctx.
    # All three share identical sort key (0, 4, 4), so stable sort keeps order
    # [g_fail_1, g_succeed, g_fail_2]: g_fail_2 MUST be re-evaluated after ctx.update.
    calls = 0
    g_fail_1 = _tpu_gang()
    g_fail_1.details = "fail-1"
    g_succeed = _locality_gang(
        [_mk_req({"CPU": 0.25}) for _ in range(4)],
        _SLICE,
    )
    g_succeed.details = "succeed-mid"
    g_fail_2 = _tpu_gang()
    g_fail_2.details = "fail-2"
    g_fail_3 = _tpu_gang()
    g_fail_3.details = "fail-3"
    rep_inv = _sched(cfgs, busy_slice, gangs=[g_fail_1, g_succeed, g_fail_2, g_fail_3])
    assert len(rep_inv.infeasible_gang_resource_requests) == 3
    # g_fail_1 (eval) -> g_succeed (eval + clears cache) -> g_fail_2 (re-eval) -> g_fail_3 (deduped)
    assert calls == 3


def test_try_schedule_gang_read_only_nodes_and_backfill_cache():
    """Verify _try_schedule_gang never mutates ScheduleContext nodes and
    ScheduleContext caches missing replica backfill slots accurately across
    init, update, and restricted_to."""
    cfgs = {"tpu_4x4": _tpu_config(max_workers=8)}
    # Partial 3-of-4 replica without provider replica_id -> 1 missing backfill slot.
    partial = [_tpu_host(f"a{i}", "slice-a", i, None, 0, rep=None) for i in range(3)]
    ctx = _mk_ctx(cfgs, partial, max_num_nodes=8)
    assert ctx.get_missing_replica_backfill_slots() == {"tpu_4x4": 1}
    assert ctx.get_headroom() == 4

    # Snapshot peek_nodes() before feasible and infeasible direct calls.
    def _snap(nodes):
        return [
            (
                n.im_instance_id,
                dict(n.labels),
                {k: dict(v) for k, v in n.available_resources_for_sched.items()},
                n.status,
            )
            for n in nodes
        ]

    before = _snap(ctx.peek_nodes())
    # Feasible gang (launches a new 4-host replica since max_workers=8, 3+1=4 used).
    upd, inf = ResourceDemandScheduler._try_schedule_gang(ctx, _tpu_gang())
    assert inf == []
    assert _snap(ctx.peek_nodes()) == before

    # Infeasible gang (pinned to nonexistent slice).
    g_inf = _tpu_gang(pinned="slice-none")
    _, inf2 = ResourceDemandScheduler._try_schedule_gang(ctx, g_inf)
    assert len(inf2) == 4
    assert _snap(ctx.peek_nodes()) == before

    # restricted_to inherits parent headroom and cached backfill without mutating ctx.
    sub_ctx = ctx.restricted_to(
        ctx.peek_nodes()[:2], node_type_available={"tpu_4x4": 4}
    )
    assert sub_ctx.get_missing_replica_backfill_slots() == {"tpu_4x4": 1}
    assert sub_ctx.get_node_type_available() == {"tpu_4x4": 4}
    assert sub_ctx.get_headroom() == 4
    # Parent ctx cache remains intact.
    assert ctx.get_missing_replica_backfill_slots() == {"tpu_4x4": 1}
    assert ctx.get_headroom() == 4

    # update() refreshes the cached backfill count and headroom.
    ctx.update(upd)
    assert ctx.get_missing_replica_backfill_slots() == {"tpu_4x4": 1}
    assert ctx.get_headroom() == 0
    # Updating ctx with a complete 4-host replica clears the missing backfill slot.
    full_ctx = _mk_ctx(
        cfgs,
        partial + [_tpu_host("a3", "slice-a", 3, None, 0, rep=None)],
        max_num_nodes=8,
    )
    assert full_ctx.get_missing_replica_backfill_slots() == {}
    assert full_ctx.get_headroom() == 4
    ctx.update(full_ctx.peek_nodes())
    assert ctx.get_missing_replica_backfill_slots() == {}
    assert ctx.get_headroom() == 4


def test_enforce_min_workers_ippr_keeps_baseline_resources():
    """min_worker_nodes launches on IPPR-enabled types keep baseline cfg.resources,
    while demand-driven launches still apply IPPR max caps."""
    scheduler = ResourceDemandScheduler(event_logger)
    mem_base = 4 * 1024 * 1024 * 1024
    mem_max = 16 * 1024 * 1024 * 1024
    ippr_specs = IPPRSpecs(
        groups={
            "cpu_ippr": IPPRGroupSpec(
                min_cpu=2.0,
                max_cpu=8.0,
                min_memory=mem_base,
                max_memory=mem_max,
                resize_timeout=60,
            )
        }
    )

    # Positive: min_worker_nodes=1 with no demand launches baseline (CPU=2, memory=4GiB),
    # NOT inflated to IPPR max (CPU=8, memory=16GiB).
    min_cfgs = {
        "cpu_ippr": NodeTypeConfig(
            name="cpu_ippr",
            resources={"CPU": 2.0, "memory": float(mem_base)},
            min_worker_nodes=1,
            max_worker_nodes=2,
        )
    }
    reply_min = scheduler.schedule(
        sched_request(
            node_type_configs=min_cfgs,
            instances=[],
            ippr_specs=ippr_specs,
        )
    )
    assert _launch_and_terminate(reply_min) == ({"cpu_ippr": 1}, [])
    assert reply_min.cluster_resources["CPU"] == 2.0
    assert reply_min.cluster_resources["memory"] == float(mem_base)

    # Paired positive: demand exceeding baseline (CPU=6) cannot silently pack onto
    # the un-resized min_workers launch; it scales out a second IPPR-capped node.
    reply_min_and_demand = scheduler.schedule(
        sched_request(
            node_type_configs=min_cfgs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 6.0})],
            instances=[],
            ippr_specs=ippr_specs,
        )
    )
    assert reply_min_and_demand.infeasible_resource_requests == []
    assert _launch_and_terminate(reply_min_and_demand) == ({"cpu_ippr": 2}, [])
    assert reply_min_and_demand.cluster_resources["CPU"] == 10.0

    # Negative counter-case: demand-driven launch with min_worker_nodes=0 still uses
    # IPPR max caps so a 6-CPU request scales from 0 cleanly.
    demand_cfgs = {
        "cpu_ippr": NodeTypeConfig(
            name="cpu_ippr",
            resources={"CPU": 2.0, "memory": float(mem_base)},
            min_worker_nodes=0,
            max_worker_nodes=2,
        )
    }
    reply_demand = scheduler.schedule(
        sched_request(
            node_type_configs=demand_cfgs,
            resource_requests=[ResourceRequestUtil.make({"CPU": 6.0})],
            instances=[],
            ippr_specs=ippr_specs,
        )
    )
    assert reply_demand.infeasible_resource_requests == []
    assert _launch_and_terminate(reply_demand) == ({"cpu_ippr": 1}, [])
    assert reply_demand.cluster_resources["CPU"] == 8.0
    assert reply_demand.cluster_resources["memory"] == float(mem_max)


def test_outdated_multihost_replica_cascades_to_mixed_siblings():
    """Outdated launch_config_hash on a multi-host replica cascades OUTDATED
    termination to all SCHEDULABLE siblings (ALLOCATED and fresh-hash backfilled),
    while preserving fresh replicas, single-host neighbors, and outdated HEAD."""
    scheduler = ResourceDemandScheduler(event_logger)
    head_cfg = NodeTypeConfig(
        name="head",
        resources={"CPU": 2},
        min_worker_nodes=0,
        max_worker_nodes=0,
        launch_config_hash="h2",
    )
    tpu_cfg = _tpu_config(min_workers=4, max_workers=8)
    tpu_cfg.launch_config_hash = "h2"
    cpu_cfg = NodeTypeConfig(
        name="cpu",
        resources={"CPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=4,
        launch_config_hash="h2",
    )
    cfgs = {"head": head_cfg, "tpu_4x4": tpu_cfg, "cpu": cpu_cfg}

    head_inst = make_autoscaler_instance(
        im_instance=Instance(
            instance_id="head-0",
            instance_type="head",
            status=Instance.RAY_RUNNING,
            node_kind=NodeKind.HEAD,
            node_id="head-0",
            launch_config_hash="h1",
        ),
        ray_node=NodeState(
            node_id=b"head-0",
            ray_node_type_name="head",
            total_resources={"CPU": 2},
            available_resources={"CPU": 2},
            status=NodeStatus.RUNNING,
        ),
        cloud_instance_id="head-0",
    )

    # Slice A (rep-old): 2 outdated RAY_RUNNING + 1 ALLOCATED + 1 fresh backfilled RAY_RUNNING.
    old_running_0 = _tpu_host("old-0", "slice-old", 0, rep="rep-old")
    old_running_0.im_instance.launch_config_hash = "h1"
    old_running_1 = _tpu_host("old-1", "slice-old", 1, rep="rep-old")
    old_running_1.im_instance.launch_config_hash = "h1"
    old_alloc_2 = _pending_tpu("old-2", status=Instance.ALLOCATED, rep="rep-old")
    old_alloc_2.im_instance.launch_config_hash = "h1"
    old_backfill_3 = _tpu_host("old-3", "slice-old", 3, rep="rep-old")
    old_backfill_3.im_instance.launch_config_hash = "h2"

    # Slice B (rep-fresh): 4 fresh RAY_RUNNING hosts -> must NOT be terminated.
    fresh_slice = []
    for w in range(4):
        inst = _tpu_host(f"fresh-{w}", "slice-fresh", w, rep="rep-fresh")
        inst.im_instance.launch_config_hash = "h2"
        fresh_slice.append(inst)

    # Single-host cpu neighbors: 1 outdated + 1 fresh RAY_RUNNING + 1 ALLOCATED.
    cpu_old = _run_node("cpu", "cpu-old", {"CPU": 4}, {})
    cpu_old.im_instance.launch_config_hash = "h1"
    cpu_fresh = _run_node("cpu", "cpu-fresh", {"CPU": 4}, {})
    cpu_fresh.im_instance.launch_config_hash = "h2"
    cpu_alloc = _pending_node("cpu", "cpu-alloc", Instance.ALLOCATED)
    cpu_alloc.im_instance.launch_config_hash = "h1"

    all_outdated_insts = [
        head_inst,
        old_running_0,
        old_running_1,
        old_alloc_2,
        old_backfill_3,
        *fresh_slice,
        cpu_old,
        cpu_fresh,
        cpu_alloc,
    ]
    reply = scheduler.schedule(
        SchedulingRequest(
            node_type_configs=cfgs,
            current_instances=all_outdated_insts,
            disable_launch_config_check=False,
            replica_view=_synthetic_replica_view(cfgs, all_outdated_insts),
        )
    )

    terminated_by_id = {t.instance_id: t for t in reply.to_terminate}
    assert set(terminated_by_id.keys()) == {
        "old-0",
        "old-1",
        "old-2",
        "old-3",
        "cpu-old",
    }
    for tid, req in terminated_by_id.items():
        assert req.cause == TerminationRequest.Cause.OUTDATED, (tid, req.cause)
    assert "head-0" not in terminated_by_id
    assert "cpu-fresh" not in terminated_by_id
    assert "cpu-alloc" not in terminated_by_id
    for w in range(4):
        assert f"fresh-{w}" not in terminated_by_id


def test_partial_ray_stop_requested_slice_keeps_single_draining_replica_slot():
    """On stock KubeRay (no provider_replica_id / launch_request_id), a slice
    partially in RAY_STOP_REQUESTED stays grouped as 1 draining replica (charging
    4 slots, not 8) and remains isolated from both unpinned and pinned gangs."""
    mixed_draining_slice = []
    for w in range(4):
        st = Instance.RAY_STOP_REQUESTED if w < 2 else Instance.RAY_RUNNING
        mixed_draining_slice.append(
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_id=f"s0-{w}",
                    instance_type="tpu_4x4",
                    status=st,
                    node_id=f"s0-{w}",
                    node_kind=NodeKind.WORKER,
                ),
                ray_node=NodeState(
                    node_id=f"s0-{w}".encode(),
                    ray_node_type_name="tpu_4x4",
                    total_resources={"CPU": 4, "TPU": 4},
                    available_resources={"CPU": 4, "TPU": 4},
                    labels={**TPU_LABELS, _SLICE: "slice-draining", _WID: str(w)},
                    status=NodeStatus.RUNNING,
                    idle_duration_ms=0,
                ),
                cloud_instance_id=f"pod-s0-{w}",
            )
        )

    # Positive: max_workers=8 -> 1 draining replica uses 4 slots, leaving 4 slots
    # to launch 1 fresh replacement slice for an unpinned gang.
    cfgs_8 = {"tpu_4x4": _tpu_config(max_workers=8)}
    rep_unpinned = _sched(cfgs_8, mixed_draining_slice, gangs=[_tpu_gang()])
    assert rep_unpinned.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_unpinned) == ({"tpu_4x4": 4}, [])

    # Negative counter-case 1: gang pinned to the draining slice MUST stay infeasible
    # with zero launches.
    rep_pinned = _sched(
        cfgs_8, mixed_draining_slice, gangs=[_tpu_gang(pinned="slice-draining")]
    )
    assert len(rep_pinned.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_pinned) == ({}, [])

    # Negative counter-case 2: max_workers=4 -> draining slice still occupies all 4
    # slots so no replacement slice launches past max_worker_nodes.
    cfgs_4 = {"tpu_4x4": _tpu_config(max_workers=4)}
    rep_capped = _sched(cfgs_4, mixed_draining_slice, gangs=[_tpu_gang()])
    assert len(rep_capped.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_capped) == ({}, [])


def test_gang_prefilters_match_oracle_and_reduce_trials():
    """Verify gang prefilters (_domain_fits_resource_totals and
    _pending_domain_signature) preserve exact oracle scheduling outcomes across
    randomized multi-slice, label-tier, and IPPR scenarios while strictly
    reducing _try_schedule trials on multi-replica clusters."""
    import random

    rng = random.Random(42)
    cfgs = {
        "tpu_4x4": _tpu_config(max_workers=40),
        "tpu_2x2x2": NodeTypeConfig(
            name="tpu_2x2x2",
            resources={"CPU": 8, "TPU": 4, "TPU-v7x-8-head": 1},
            labels={
                "ray.io/tpu-topology": "2x2x2",
                "ray.io/accelerator-type": "TPU-V7X",
            },
            min_worker_nodes=0,
            max_worker_nodes=16,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=_SLICE,
                worker_index_label_key=_WID,
                head_resources=("TPU-v7x-8-head",),
            ),
            runtime_label_keys=(_SLICE, _WID),
        ),
        "gpu_ippr": NodeTypeConfig(
            name="gpu_ippr",
            resources={"CPU": 4, "GPU": 2},
            labels={"rack": "r-ippr"},
            min_worker_nodes=0,
            max_worker_nodes=8,
        ),
    }

    def _run_with_mode(
        instances: List[AutoscalerInstance],
        gangs: List[GangResourceRequest],
        use_prefilters: bool,
        ippr_specs: Optional[IPPRSpecs] = None,
        ippr_statuses: Optional[Dict[str, IPPRStatus]] = None,
    ):
        trials = 0
        orig_try = ResourceDemandScheduler._try_schedule

        def _counting_try(*args, **kwargs):
            nonlocal trials
            trials += 1
            return orig_try(*args, **kwargs)

        sig_counter = 0

        def _unique_sig(*args, **kwargs):
            nonlocal sig_counter
            sig_counter += 1
            return (sig_counter,)

        rv = _synthetic_replica_view(cfgs, instances)
        if use_prefilters:
            with patch.object(
                ResourceDemandScheduler,
                "_try_schedule",
                staticmethod(_counting_try),
            ):
                rep = ResourceDemandScheduler(event_logger).schedule(
                    SchedulingRequest(
                        disable_launch_config_check=True,
                        node_type_configs=cfgs,
                        current_instances=instances,
                        gang_resource_requests=gangs,
                        ippr_specs=ippr_specs,
                        ippr_statuses=ippr_statuses or {},
                        replica_view=rv,
                    )
                )
        else:
            with patch.object(
                ResourceDemandScheduler,
                "_try_schedule",
                staticmethod(_counting_try),
            ), patch.object(
                ResourceDemandScheduler,
                "_domain_fits_resource_totals",
                staticmethod(lambda *_a, **_kw: True),
            ), patch.object(
                ResourceDemandScheduler,
                "_pending_domain_signature",
                staticmethod(_unique_sig),
            ):
                rep = ResourceDemandScheduler(event_logger).schedule(
                    SchedulingRequest(
                        disable_launch_config_check=True,
                        node_type_configs=cfgs,
                        current_instances=instances,
                        gang_resource_requests=gangs,
                        ippr_specs=ippr_specs,
                        ippr_statuses=ippr_statuses or {},
                        replica_view=rv,
                    )
                )
        launches = {r.instance_type: r.count for r in rep.to_launch}
        terms = sorted(t.instance_id for t in rep.to_terminate)
        return launches, terms, len(rep.infeasible_gang_resource_requests), trials

    # 1. Deterministic multi-replica cluster: assert prefilters strictly cut trials.
    many_slices = [
        _tpu_host(
            f"s{s}-{w}",
            f"slice-{s}",
            w,
            used={"TPU": 4} if s < 4 else None,
            rep=f"slice-{s}",
        )
        for s in range(6)
        for w in range(4)
    ] + [
        _pending_node("tpu_4x4", f"p{p}-{w}", Instance.ALLOCATED, lid=f"lid-p{p}")
        for p in range(4)
        for w in range(4)
    ]
    heavy_gangs = [
        _tpu_gang((0, 1, 2, 3)),
        _tpu_gang((0, 1)),
        _tpu_gang((2, 3)),
        _strict_pack_gang(
            _SLICE,
            [
                _mk_req(
                    {"CPU": 2, "TPU": 4},
                    label_selectors=[[(_TOPO, _IN, ["2x2x2"])]],
                )
                for _ in range(2)
            ],
        ),
    ]
    opt_res = _run_with_mode(many_slices, heavy_gangs, use_prefilters=True)
    orc_res = _run_with_mode(many_slices, heavy_gangs, use_prefilters=False)
    assert opt_res[:3] == orc_res[:3]
    assert opt_res[3] < orc_res[3]

    # 2. Seeded randomized differential check across running, pending, multi-tier
    # label selectors, and IPPR-resizable nodes.
    ippr_spec = IPPRGroupSpec(
        min_cpu=4.0,
        max_cpu=16.0,
        min_memory=0.0,
        max_memory=0.0,
        resize_timeout=60,
    )
    ippr_specs = IPPRSpecs(groups={"gpu_ippr": ippr_spec})
    for trial in range(40):
        insts: List[AutoscalerInstance] = []
        ippr_statuses: Dict[str, IPPRStatus] = {}
        n_run = rng.randint(0, 4)
        for s in range(n_run):
            for w in range(4):
                used_tpu = 4 if rng.random() < 0.4 else 0
                used_cpu = rng.choice([0, 2, 4])
                insts.append(
                    _tpu_host(
                        f"t{trial}-s{s}-{w}",
                        f"slice-{s}",
                        w,
                        used={"TPU": used_tpu, "CPU": used_cpu}
                        if (used_tpu or used_cpu)
                        else None,
                        rep=f"slice-{s}",
                    )
                )
        n_pend = rng.randint(0, 3)
        for p in range(n_pend):
            for w in range(4):
                insts.append(
                    _pending_node(
                        "tpu_4x4",
                        f"t{trial}-p{p}-{w}",
                        Instance.ALLOCATED,
                        lid=f"lid-{trial}-{p}",
                    )
                )
        if rng.random() < 0.5:
            gid = f"t{trial}-g0"
            insts.append(
                _run_node(
                    "gpu_ippr",
                    gid,
                    {"CPU": 4, "GPU": 2},
                    {"rack": "r-ippr"},
                    used={"CPU": 2},
                )
            )
            ippr_statuses[f"cloud-{gid}"] = IPPRStatus(
                cloud_instance_id=f"cloud-{gid}",
                spec=ippr_spec,
                current_cpu=4.0,
                current_memory=0.0,
                desired_cpu=4.0,
                desired_memory=0.0,
            )
        gangs: List[GangResourceRequest] = []
        for _ in range(rng.randint(1, 5)):
            kind = rng.choice(["tpu4", "tpu2", "tpu_tier", "tpu_v7x", "ippr"])
            if kind == "tpu4":
                gangs.append(_tpu_gang((0, 1, 2, 3)))
            elif kind == "tpu2":
                gangs.append(_tpu_gang(rng.choice([(0, 1), (2, 3)])))
            elif kind == "tpu_tier":
                gangs.append(
                    _strict_pack_gang(
                        _SLICE,
                        [
                            _mk_req(
                                {"CPU": 1, "TPU": 4},
                                label_selectors=[
                                    [(_TOPO, _IN, ["2x2x2"])],
                                    [(_TOPO, _IN, ["4x4"])],
                                ],
                            )
                            for _ in range(2)
                        ],
                    )
                )
            elif kind == "tpu_v7x":
                gangs.append(
                    _strict_pack_gang(
                        _SLICE,
                        [
                            _mk_req(
                                {"CPU": 2, "TPU": 4},
                                label_selectors=[[(_TOPO, _IN, ["2x2x2"])]],
                            )
                            for _ in range(2)
                        ],
                    )
                )
            else:
                gangs.append(
                    _strict_pack_gang(
                        "rack",
                        [_mk_req({"CPU": 8, "GPU": 1}), _mk_req({"CPU": 4, "GPU": 1})],
                    )
                )
        a = _run_with_mode(insts, gangs, True, ippr_specs, ippr_statuses)
        b = _run_with_mode(insts, gangs, False, ippr_specs, ippr_statuses)
        assert a[:3] == b[:3], f"trial {trial}: opt={a} vs oracle={b}"


def test_pg_normalization_dedup_and_ippr_non_cpu_pruning(monkeypatch):
    """Verify normalized _pg_<hex> dedup hits on distinct fresh keys while excluding
    node-present keys (including dynamically scheduled ones), and IPPR domains only
    inflate CPU/memory in `_domain_fits_resource_totals`."""
    from ray.autoscaler.v2.scheduler import _GangSearch

    cfgs = {"tpu_4x4": _tpu_config(max_workers=8)}
    busy_slices = [
        _tpu_host(f"s{s}-{h}", f"slice-{s}", h, {"TPU": 4}, 0, rep=f"rep-{s}")
        for s in range(2)
        for h in range(4)
    ]

    def _spread_pg(pg_name: str, bundles_keys=None):
        keys = bundles_keys or [pg_name] * 4
        reqs = [
            _mk_req(
                {"CPU": 1, "TPU": 4},
                [(_ANTI, k, "")],
                label_selectors=[
                    [
                        (_TOPO, _IN, ["4x4"]),
                        (_ACC, _IN, ["TPU-V6E"]),
                        (_WID, _IN, [str(h)]),
                    ]
                ],
            )
            for h, k in enumerate(keys)
        ]
        return _locality_gang(reqs, _SLICE)

    calls = 0
    orig_tsg = ResourceDemandScheduler._try_schedule_gang

    def _spy_tsg(*args, **kwargs):
        nonlocal calls
        calls += 1
        return orig_tsg(*args, **kwargs)

    monkeypatch.setattr(
        ResourceDemandScheduler, "_try_schedule_gang", staticmethod(_spy_tsg)
    )

    # 1) Unique unseen _pg_<hex> keys across 4 infeasible gangs -> 1 trial.
    fresh_gangs = [_spread_pg(f"_pg_{i:032x}") for i in range(4)]
    rep_fresh = _sched(cfgs, busy_slices, gangs=fresh_gangs)
    assert len(rep_fresh.infeasible_gang_resource_requests) == 4
    assert calls == 1

    # 2) Intra-gang mixed keys vs uniform keys remain distinct.
    calls = 0
    mixed_vs_uniform = [
        _spread_pg(
            "mix", [f"_pg_{1:032x}", f"_pg_{2:032x}", f"_pg_{1:032x}", f"_pg_{1:032x}"]
        ),
        _spread_pg("uni", [f"_pg_{3:032x}"] * 4),
    ]
    rep_mix = _sched(cfgs, busy_slices, gangs=mixed_vs_uniform)
    assert len(rep_mix.infeasible_gang_resource_requests) == 2
    assert calls == 2

    # 3) Already-active node label key is excluded from fresh normalization.
    calls = 0
    pg_existing = f"_pg_{0xAAAA:032x}"
    pg_unseen = f"_pg_{0xBBBB:032x}"
    nodes_with_pg = list(busy_slices)
    nodes_with_pg[0] = _run_node(
        "tpu_4x4",
        "s0-0",
        {"CPU": 4, "TPU": 4},
        {**TPU_LABELS, _SLICE: "slice-0", _WID: "0", pg_existing: ""},
        used={"TPU": 4},
        rep="rep-0",
    )
    rep_active = _sched(
        cfgs, nodes_with_pg, gangs=[_spread_pg(pg_existing), _spread_pg(pg_unseen)]
    )
    assert len(rep_active.infeasible_gang_resource_requests) == 2
    assert calls == 2

    # 4) A feasible gang stamping a fresh _pg key dynamically adds it to active keys.
    # Use 4 bundles on g_sched so all 4 gangs tie on sort key (0, 8, 4) and run in order.
    calls = 0
    pg_dyn = f"_pg_{0xCCCC:032x}"
    g_fail_0 = _spread_pg(f"_pg_{0xDDD0:032x}")
    g_sched = _locality_gang(
        [
            _mk_req(
                {"CPU": 0.25},
                [(_ANTI, pg_dyn, "")],
                label_selectors=[
                    [
                        (_TOPO, _IN, ["4x4"]),
                        (_ACC, _IN, ["TPU-V6E"]),
                        (_WID, _IN, [str(h)]),
                    ]
                ],
            )
            for h in range(4)
        ],
        _SLICE,
    )
    g_fail_dyn = _spread_pg(pg_dyn)
    g_fail_fresh = _spread_pg(f"_pg_{0xDDD1:032x}")
    rep_dyn = _sched(
        cfgs, busy_slices, gangs=[g_fail_0, g_sched, g_fail_dyn, g_fail_fresh]
    )
    assert len(rep_dyn.infeasible_gang_resource_requests) == 3
    assert calls == 4

    # 5) IPPR resize-up only relaxes CPU/memory; exhausted TPU domains prune before schedule_in_domain.
    spec = IPPRGroupSpec(
        min_cpu=4, max_cpu=8, min_memory=1000, max_memory=2000, resize_timeout=60
    )
    ippr_cfgs = {"tpu_4x4": _tpu_config(max_workers=8)}
    ippr_statuses = {
        f"cloud-s{s}-{h}": IPPRStatus(
            cloud_instance_id=f"cloud-s{s}-{h}",
            spec=spec,
            current_cpu=4,
            current_memory=1000,
            desired_cpu=4,
            desired_memory=1000,
        )
        for s in range(2)
        for h in range(4)
    }
    for inst in busy_slices:
        inst.cloud_instance_id = f"cloud-{inst.im_instance.instance_id}"

    sid_calls = 0
    orig_sid = _GangSearch.schedule_in_domain

    def _spy_sid(*args, **kwargs):
        nonlocal sid_calls
        sid_calls += 1
        return orig_sid(*args, **kwargs)

    monkeypatch.setattr(_GangSearch, "schedule_in_domain", staticmethod(_spy_sid))
    rep_tpu_pruned = _sched(
        ippr_cfgs,
        busy_slices,
        gangs=[_tpu_gang()],
        ippr_specs=IPPRSpecs(groups={"tpu_4x4": spec}),
        ippr_statuses=ippr_statuses,
    )
    assert len(rep_tpu_pruned.infeasible_gang_resource_requests) == 1
    assert sid_calls == 0

    cpu_up_gang = _locality_gang(
        [
            _mk_req(
                {"CPU": 8},
                label_selectors=[[(_TOPO, _IN, ["4x4"]), (_WID, _IN, [str(h)])]],
            )
            for h in range(4)
        ],
        _SLICE,
    )
    rep_cpu_ippr = _sched(
        ippr_cfgs,
        busy_slices,
        gangs=[cpu_up_gang],
        ippr_specs=IPPRSpecs(groups={"tpu_4x4": spec}),
        ippr_statuses=ippr_statuses,
    )
    assert rep_cpu_ippr.infeasible_gang_resource_requests == []
    assert len(rep_cpu_ippr.to_ippr) == 4
    assert sid_calls >= 1


@pytest.mark.parametrize("use_replica_cfg", [False, True])
def test_single_host_pinned_domain_idle_protection(use_replica_cfg):
    """Single-host (workers_per_replica=1) idle workers inside a pinned domain stay
    protected while sibling workers in that domain are busy or booting, and scale down
    once all domain workers are idle and expired."""
    clique_key = "ray.io/gpu-clique"
    rc = (
        WorkerReplicaConfig(
            workers_per_replica=1,
            domain_label_key=clique_key,
            domain_exclusive=False,
        )
        if use_replica_cfg
        else None
    )
    cfgs = {
        "gpu-sh": NodeTypeConfig(
            name="gpu-sh",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=6,
            replica_config=rc,
            runtime_label_keys=(clique_key,),
        )
    }
    pin_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_sh_pin"),
        loc_constraints=[(clique_key, _IN, ["c0"])],
    )

    g0_busy = _run_node(
        "gpu-sh",
        "g0",
        {"CPU": 8, "GPU": 4},
        {clique_key: "c0"},
        used={"GPU": 4},
        idle_ms=0,
    )
    g1_idle = _run_node(
        "gpu-sh", "g1", {"CPU": 8, "GPU": 4}, {clique_key: "c0"}, idle_ms=60_000
    )
    g2_other = _run_node(
        "gpu-sh", "g2", {"CPU": 8, "GPU": 4}, {clique_key: "c1"}, idle_ms=60_000
    )
    rep_busy_sib = _sched(
        cfgs, [g0_busy, g1_idle, g2_other], gangs=[pin_c0], idle_s=10.0
    )
    assert [t.instance_id for t in rep_busy_sib.to_terminate] == ["g2"]

    g0_under_timeout = _run_node(
        "gpu-sh", "g0", {"CPU": 8, "GPU": 4}, {clique_key: "c0"}, idle_ms=2_000
    )
    rep_boot_or_warm = _sched(
        cfgs, [g0_under_timeout, g1_idle], gangs=[pin_c0], idle_s=10.0
    )
    assert rep_boot_or_warm.to_terminate == []

    # When all c0 workers are idle past timeout and still cannot fit, scale down cleanly.
    g0_expired = _run_node(
        "gpu-sh", "g0", {"CPU": 8, "GPU": 4}, {clique_key: "c0"}, idle_ms=60_000
    )
    pin_c0_3 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 3, "_pg_sh_pin3"),
        loc_constraints=[(clique_key, _IN, ["c0"])],
    )
    rep_all_idle = _sched(cfgs, [g0_expired, g1_idle], gangs=[pin_c0_3], idle_s=10.0)
    assert sorted(t.instance_id for t in rep_all_idle.to_terminate) == ["g0", "g1"]

    # Single-host node with domain_exclusive=True on its own slice key still protects
    # siblings pinned on an orthogonal shared label (e.g., ray.io/rack).
    if use_replica_cfg:
        slice_key = "ray.io/tpu-slice-name"
        rack_key = "ray.io/rack"
        excl_cfgs = {
            "tpu-sh": NodeTypeConfig(
                name="tpu-sh",
                resources={"CPU": 8, "TPU": 4},
                min_worker_nodes=0,
                max_worker_nodes=6,
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=1,
                    domain_label_key=slice_key,
                    domain_exclusive=True,
                ),
                runtime_label_keys=(slice_key, rack_key),
            )
        }
        pin_rack = _strict_pack_gang(
            rack_key,
            _spread_reqs({"CPU": 4, "TPU": 4}, 2, "_pg_rack_pin"),
            loc_constraints=[(rack_key, _IN, ["rack-0"])],
        )
        t0_busy = _run_node(
            "tpu-sh",
            "t0",
            {"CPU": 8, "TPU": 4},
            {slice_key: "s0", rack_key: "rack-0"},
            used={"TPU": 4},
            idle_ms=0,
        )
        t1_idle = _run_node(
            "tpu-sh",
            "t1",
            {"CPU": 8, "TPU": 4},
            {slice_key: "s1", rack_key: "rack-0"},
            idle_ms=60_000,
        )
        t2_other_rack = _run_node(
            "tpu-sh",
            "t2",
            {"CPU": 8, "TPU": 4},
            {slice_key: "s2", rack_key: "rack-1"},
            idle_ms=60_000,
        )
        rep_rack = _sched(
            excl_cfgs,
            [t0_busy, t1_idle, t2_other_rack],
            gangs=[pin_rack],
            idle_s=10.0,
        )
        assert [t.instance_id for t in rep_rack.to_terminate] == ["t2"]

        # Pinning on the exclusive slice key itself does not protect an idle node.
        pin_slice = _strict_pack_gang(
            slice_key,
            _spread_reqs({"CPU": 4, "TPU": 4}, 2, "_pg_slice_pin"),
            loc_constraints=[(slice_key, _IN, ["s0"])],
        )
        t1_same_slice = _run_node(
            "tpu-sh",
            "t1",
            {"CPU": 8, "TPU": 4},
            {slice_key: "s0", rack_key: "rack-0"},
            idle_ms=60_000,
        )
        rep_slice = _sched(
            excl_cfgs,
            [t0_busy, t1_same_slice],
            gangs=[pin_slice],
            idle_s=10.0,
        )
        assert [t.instance_id for t in rep_slice.to_terminate] == ["t1"]


def test_non_exclusive_shared_clique_tight_max_and_partial_boot_multitick():
    """Shared non-exclusive cliques combine idle spare capacity with bounded launches
    when Step 5 cannot fit under max_workers, and consume unlabeled booting siblings
    in occupied cliques across ticks cleanly."""
    clique_key = "ray.io/gpu-clique"

    def _cnode(iid, clique, rep, *, busy=False, idle_ms=60_000):
        return _run_node(
            "gpu-clique",
            iid,
            {"CPU": 8, "GPU": 4},
            {clique_key: clique},
            used={"GPU": 4} if busy else None,
            idle_ms=0 if busy else idle_ms,
            rep=rep,
        )

    # A) Tight min=2, max=4: 1 idle 2-host replica in clique-0 + avail=2 fits 4-host unpinned gang.
    cfgs_tight = {
        "gpu-clique": NodeTypeConfig(
            name="gpu-clique",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=2,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=clique_key,
                domain_exclusive=False,
            ),
            runtime_label_keys=(clique_key,),
        )
    }
    rep0 = [_cnode(f"r0-{i}", "clique-0", "rep-0", idle_ms=0) for i in range(2)]
    g4 = _strict_pack_gang(clique_key, _spread_reqs({"CPU": 4, "GPU": 4}, 4, "_pg_t4"))
    r_unpinned = _sched(cfgs_tight, rep0, gangs=[g4])
    assert _launch_and_terminate(r_unpinned) == ({"gpu-clique": 2}, [])
    assert r_unpinned.infeasible_gang_resource_requests == []

    # Pinned gang to dynamic clique-0 MUST NOT launch new nodes even when avail=2.
    g4_pin = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 4, "_pg_p4"),
        loc_constraints=[(clique_key, _IN, ["clique-0"])],
    )
    r_pinned = _sched(cfgs_tight, rep0, gangs=[g4_pin])
    assert _launch_and_terminate(r_pinned) == ({}, [])
    assert len(r_pinned.infeasible_gang_resource_requests) == 1

    # B) Multi-tick max=6: rep0_busy + rep1_idle in clique-0 + rep2_boot unlabeled.
    cfgs_6 = {
        "gpu-clique": NodeTypeConfig(
            name="gpu-clique",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=6,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=clique_key,
                domain_exclusive=False,
            ),
            runtime_label_keys=(clique_key,),
        )
    }
    rep0_busy = [_cnode(f"b0-{i}", "clique-0", "rep-0", busy=True) for i in range(2)]
    rep1_idle = [_cnode(f"i1-{i}", "clique-0", "rep-1", busy=False) for i in range(2)]
    rep2_boot = [
        _pending_node("gpu-clique", f"p2-{i}", Instance.ALLOCATED, rep="rep-2")
        for i in range(2)
    ]

    sched = ResourceDemandScheduler()
    tick1 = _sched(
        cfgs_6,
        rep0_busy + rep1_idle + rep2_boot,
        gangs=[g4],
        idle_s=10.0,
        pending_idle_s=10.0,
        sched=sched,
    )
    assert _launch_and_terminate(tick1) == ({}, [])
    assert tick1.infeasible_gang_resource_requests == []
    assert sched._pending_unneeded_since_s.get("gpu-clique", []) == []

    # Tick 2: gang cleared and rep2_boot expires unneeded; rep1_idle & rep2_boot scale down, rep0_busy stays.
    tick2 = _sched(
        cfgs_6,
        rep0_busy + rep1_idle + rep2_boot,
        gangs=[],
        idle_s=10.0,
        pending_idle_s=10.0,
        unneeded={"gpu-clique": [0.0, 0.0]},
    )
    assert sorted(t.instance_id for t in tick2.to_terminate) == [
        "i1-0",
        "i1-1",
        "p2-0",
        "p2-1",
    ]


def test_schedule_in_domain_joint_fallback_tier_retry():
    """Joint domain_nodes + siblings retry recovers gang placement when an earlier
    fallback-tier bundle consumed a restricted zone node needed by a stricter bundle."""
    clique_key = "ray.io/gpu-clique"
    cfgs = {
        "gpu-large": NodeTypeConfig(
            name="gpu-large",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=2,
            runtime_label_keys=(clique_key, "zone"),
            priority=0,
        ),
        "gpu-small": NodeTypeConfig(
            name="gpu-small",
            resources={"CPU": 4, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=2,
            runtime_label_keys=(clique_key, "zone"),
            priority=10,
        ),
    }
    n0 = _run_node(
        "gpu-large", "n0", {"CPU": 8, "GPU": 4}, {clique_key: "c0", "zone": "zone-a"}
    )
    sib0 = _pending_node("gpu-small", "sib0", Instance.ALLOCATED)
    b_fallback = _mk_req(
        {"CPU": 4, "GPU": 4},
        label_selectors=[[("zone", _IN, ["zone-a"]), ("extra", _IN, ["x"])], []],
    )
    b_zone = _mk_req(
        {"CPU": 8, "GPU": 4},
        label_selectors=[[("zone", _IN, ["zone-a"])]],
    )
    gang = _locality_gang([b_fallback, b_zone], clique_key)
    rep_ok = _sched(cfgs, [n0, sib0], gangs=[gang])
    assert _launch_and_terminate(rep_ok) == ({}, [])
    assert rep_ok.infeasible_gang_resource_requests == []

    n0_wrong_zone = _run_node(
        "gpu-large", "n0", {"CPU": 8, "GPU": 4}, {clique_key: "c0", "zone": "zone-b"}
    )
    rep_bad = _sched(cfgs, [n0_wrong_zone, sib0], gangs=[gang])
    assert len(rep_bad.infeasible_gang_resource_requests) == 1


@pytest.mark.parametrize("dom_key", [None, _SLICE])
def test_domainless_and_missing_label_multihost_lrid_idle_scaledown(dom_key):
    """Missing runtime domain labels (or dom_key=None) still scale down complete
    idle launch_request_id batches alongside busy batches without breaking
    unattributed running safety."""
    cfgs = {
        "lws": NodeTypeConfig(
            name="lws",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=12,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2, domain_label_key=dom_key
            ),
        )
    }

    def _lws_node(iid, *, busy=False, lid=""):
        inst = _run_node(
            "lws",
            iid,
            {"CPU": 8, "GPU": 4},
            {},
            used={"GPU": 4} if busy else None,
            idle_ms=0 if busy else 60_000,
        )
        assert inst.im_instance is not None
        inst.im_instance.launch_request_id = lid
        return inst

    # Distinct launch_request_ids: idle lid-1 scales down while busy lid-0 stays whole.
    rep = _sched(
        cfgs,
        [
            _lws_node("a0", busy=True, lid="lid-0"),
            _lws_node("a1", busy=False, lid="lid-0"),
            _lws_node("b0", busy=False, lid="lid-1"),
            _lws_node("b1", busy=False, lid="lid-1"),
        ],
        idle_s=10.0,
    )
    assert sorted(t.instance_id for t in rep.to_terminate) == ["b0", "b1"]

    # Oversized 4-node single-lrid batch with mixed busy+idle hosts (e.g. two
    # physical slices sharing one launch request with missing runtime labels)
    # must NOT tear active slices in half by slicing idle hosts out.
    rep_oversized_mixed = _sched(
        cfgs,
        [
            _lws_node("m0", busy=False, lid="lid-mix"),
            _lws_node("m1", busy=False, lid="lid-mix"),
            _lws_node("m2", busy=False, lid="lid-mix"),
            _lws_node("m3", busy=True, lid="lid-mix"),
        ],
        idle_s=10.0,
    )
    assert rep_oversized_mixed.to_terminate == []

    rep_oversized_two_busy = _sched(
        cfgs,
        [
            _lws_node("a0", busy=True, lid="lid-mix"),
            _lws_node("a1", busy=False, lid="lid-mix"),
            _lws_node("b0", busy=True, lid="lid-mix"),
            _lws_node("b1", busy=False, lid="lid-mix"),
        ],
        idle_s=10.0,
    )
    assert rep_oversized_two_busy.to_terminate == []

    # Once all 4 hosts in the single-lrid batch are idle, scale-down chunks
    # cleanly into size-2 units respecting min_worker_nodes.
    all_idle_4 = [_lws_node(f"m{i}", busy=False, lid="lid-mix") for i in range(4)]
    min2_cfgs = {
        "lws": NodeTypeConfig(
            name="lws",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=2,
            max_worker_nodes=12,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2, domain_label_key=dom_key
            ),
        )
    }
    rep_oversized_min2 = _sched(min2_cfgs, all_idle_4, idle_s=10.0)
    assert len(rep_oversized_min2.to_terminate) == 2
    rep_oversized_all = _sched(cfgs, all_idle_4, idle_s=10.0)
    assert sorted(t.instance_id for t in rep_oversized_all.to_terminate) == [
        "m0",
        "m1",
        "m2",
        "m3",
    ]

    # Unattributed nodes (no lid, no prov, no runtime domain) preserve safety when 1 is busy.
    rep_unattr = _sched(
        cfgs,
        [
            _lws_node("u0"),
            _lws_node("u1"),
            _lws_node("u2", busy=True),
            _lws_node("u3"),
        ],
        idle_s=10.0,
    )
    assert rep_unattr.to_terminate == []


def test_replica_grouper_exclusive_domain_mixed_and_colliding_provider_ids():
    """Mixed non-provider + provider running nodes on one exclusive domain merge
    without overwriting (no phantom backfill placeholders and full draining
    propagation), and two distinct provider_replica_ids reporting the same
    exclusive domain keep separate replica entries and placeholders."""
    tpu2_cfg = NodeTypeConfig(
        name="tpu2",
        resources={"CPU": 4, "TPU": 4, "TPU-v5p-8-head": 1},
        min_worker_nodes=0,
        max_worker_nodes=8,
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=_SLICE,
            domain_exclusive=True,
            worker_index_label_key=_WID,
            head_resources=("TPU-v5p-8-head",),
        ),
    )

    # (a) Mixed non-provider + provider running hosts in 2-host exclusive slice-0:
    # must produce 0 backfill placeholders so a 3-host gang is infeasible while a
    # 2-host gang fits cleanly.
    mixed_2 = [
        _run_node(
            "tpu2",
            "i1",
            {"CPU": 4, "TPU": 4, "TPU-v5p-8-head": 1},
            {_SLICE: "slice-0", _WID: "0"},
            rep=None,
        ),
        _run_node(
            "tpu2",
            "i2",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            rep="prov-1",
        ),
    ]
    g2_pin = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 4, "TPU": 4}, 2, "_pg_m2"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    g3_pin = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 4, "TPU": 4}, 3, "_pg_m3"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    rep_ok = _sched({"tpu2": tpu2_cfg}, mixed_2, gangs=[g2_pin])
    assert _launch_and_terminate(rep_ok) == ({}, [])
    assert rep_ok.infeasible_gang_resource_requests == []
    rep_inf = _sched({"tpu2": tpu2_cfg}, mixed_2, gangs=[g3_pin])
    assert len(rep_inf.infeasible_gang_resource_requests) == 1

    # (b) Draining propagation in both directions on the mixed exclusive slice:
    # prov-1 draining propagates to unattributed i1, and unattributed
    # RAY_STOP_REQUESTED i1 propagates to prov-1 running + booting siblings.
    g1_pin = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 4, "TPU": 4}, 1, "_pg_m1"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    prov_draining = mixed_2 + [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="i3-term",
                instance_type="tpu2",
                status=Instance.TERMINATING,
                cloud_instance_id="c-i3-term",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="c-i3-term",
            cloud_instance_replica_id="prov-1",
        )
    ]
    rep_drain_prov = _sched({"tpu2": tpu2_cfg}, prov_draining, gangs=[g1_pin])
    assert len(rep_drain_prov.infeasible_gang_resource_requests) == 1

    i1_stop = _run_node(
        "tpu2",
        "i1-stop",
        {"CPU": 4, "TPU": 4},
        {_SLICE: "slice-0", _WID: "0"},
        rep=None,
    )
    assert i1_stop.im_instance is not None
    i1_stop.im_instance.status = Instance.RAY_STOP_REQUESTED
    unattr_draining = [
        i1_stop,
        _run_node(
            "tpu2",
            "i2-run",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            rep="prov-1",
        ),
        _pending_node("tpu2", "i3-boot", Instance.ALLOCATED, rep="prov-1"),
    ]
    rep_drain_unattr = _sched({"tpu2": tpu2_cfg}, unattr_draining, gangs=[g1_pin])
    assert len(rep_drain_unattr.infeasible_gang_resource_requests) == 1

    # (c) Two distinct provider_replica_ids both reporting exclusive slice-0
    # keep separate replica entries so neither loses its backfill placeholder
    # (4-host gang fits, 5-host gang infeasible), and draining slice-0 poisons both.
    colliding = [
        _run_node(
            "tpu2",
            "p1-0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            rep="prov-1",
        ),
        _run_node(
            "tpu2",
            "p2-0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            rep="prov-2",
        ),
    ]
    g4_pin = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 4, "TPU": 4}, 4, "_pg_c4"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    g5_pin = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 4, "TPU": 4}, 5, "_pg_c5"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    rep_col_ok = _sched({"tpu2": tpu2_cfg}, colliding, gangs=[g4_pin])
    assert rep_col_ok.infeasible_gang_resource_requests == []
    rep_col_inf = _sched({"tpu2": tpu2_cfg}, colliding, gangs=[g5_pin])
    assert len(rep_col_inf.infeasible_gang_resource_requests) == 1

    rep_col_drain = _sched({"tpu2": tpu2_cfg}, colliding + [i1_stop], gangs=[g1_pin])
    assert len(rep_col_drain.infeasible_gang_resource_requests) == 1

    # (d) Non-provider + provider collision on exclusive slice-0 where combined
    # membership exceeds workers_per_replica (2+2 > 2) must stay separate so:
    # - min_worker_nodes=2 scales down 1 idle replica (does not wedge at 4),
    # - max_worker_nodes=2 terminates only the idle replica and keeps busy prov-1,
    # - 2 busy non-provider + (1 busy + 1 ALLOCATED) prov-1 does not fabricate a
    #   5th phantom backfill placeholder blocking a 3rd replica under max=6.
    n_idle = [
        _run_node(
            "tpu2",
            "n0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            idle_ms=60_000,
        ),
        _run_node(
            "tpu2",
            "n1",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            idle_ms=60_000,
        ),
    ]
    p_idle = [
        _run_node(
            "tpu2",
            "p0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            rep="prov-1",
            idle_ms=60_000,
        ),
        _run_node(
            "tpu2",
            "p1",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            rep="prov-1",
            idle_ms=60_000,
        ),
    ]
    tpu2_min2 = _tpu_config(
        name="tpu2", workers_per_replica=2, min_workers=2, max_workers=6
    )
    rep_min2 = _sched({"tpu2": tpu2_min2}, n_idle + p_idle, idle_s=10.0)
    assert len(rep_min2.to_terminate) == 2

    p_busy = [
        _run_node(
            "tpu2",
            "p0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            used={"TPU": 4},
            rep="prov-1",
        ),
        _run_node(
            "tpu2",
            "p1",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            used={"TPU": 4},
            rep="prov-1",
        ),
    ]
    tpu2_max2 = _tpu_config(
        name="tpu2", workers_per_replica=2, min_workers=0, max_workers=2
    )
    rep_max2 = _sched({"tpu2": tpu2_max2}, n_idle + p_busy, idle_s=10.0)
    assert sorted(t.instance_id for t in rep_max2.to_terminate) == ["n0", "n1"]

    n_busy = [
        _run_node(
            "tpu2",
            "n0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            used={"TPU": 4},
        ),
        _run_node(
            "tpu2",
            "n1",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            used={"TPU": 4},
        ),
    ]
    p_boot = [
        _run_node(
            "tpu2",
            "p0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            used={"TPU": 4},
            rep="prov-1",
        ),
        _pending_node("tpu2", "p1", Instance.ALLOCATED, rep="prov-1"),
    ]
    tpu2_max6 = _tpu_config(
        name="tpu2", workers_per_replica=2, min_workers=0, max_workers=6
    )
    rep_scale3 = _sched(
        {"tpu2": tpu2_max6},
        n_busy + p_boot,
        reqs=[
            _resource_request({"CPU": 4, "TPU": 4}),
            _resource_request({"CPU": 4, "TPU": 4}),
            _resource_request({"CPU": 4, "TPU": 4}),
        ],
    )
    assert rep_scale3.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_scale3) == ({"tpu2": 2}, [])

    # (e) Same-size collision (1+1 == 2) with overlapping worker index "0" must
    # stay separate rather than merging into a corrupted two-head replica:
    # n0 (unattributed, 1 host) + p0 (prov-1, 1 host + 1 backfill placeholder)
    # yield 3 hosts on slice-0 (not 2 if wrongly merged).
    dup_wid = [
        _run_node(
            "tpu2",
            "n0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            rep=None,
        ),
        _run_node(
            "tpu2",
            "p0",
            {"CPU": 4, "TPU": 4},
            {_SLICE: "slice-0", _WID: "0"},
            rep="prov-1",
        ),
    ]
    g3_pin = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 4, "TPU": 4}, 3, "_pg_c3"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    assert (
        _sched(
            {"tpu2": tpu2_cfg}, dup_wid, gangs=[g3_pin]
        ).infeasible_gang_resource_requests
        == []
    )
    assert (
        len(
            _sched(
                {"tpu2": tpu2_cfg}, dup_wid, gangs=[g4_pin]
            ).infeasible_gang_resource_requests
        )
        == 1
    )


@pytest.mark.parametrize("exclusive", [True, False])
def test_multihost_domain_group_propagates_orthogonal_runtime_labels(exclusive):
    """Multi-host groups with non-null domain_label_key propagate uniform
    orthogonal runtime_label_keys onto booting siblings so cross-rack gangs
    cannot steal booting pods and same-rack demands see them without launching."""
    rack_key = "ray.io/rack"
    dom_key = _SLICE if exclusive else "ray.io/gpu-clique"
    cfgs = {
        "accel": NodeTypeConfig(
            name="accel",
            resources={"CPU": 4, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
            runtime_label_keys=(rack_key, dom_key),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=dom_key,
                domain_exclusive=exclusive,
                worker_index_label_key=_WID if exclusive else None,
            ),
        )
    }

    # rep-1 in rack-0: w0 running (busy), w1 ALLOCATED (booting, no rack label yet).
    # rep-2 in rack-1: w2 running (free), w3 running (busy).
    insts = [
        _run_node(
            "accel",
            "w0",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d0", rack_key: "rack-0", **({_WID: "0"} if exclusive else {})},
            used={"TPU": 4},
            rep="rep-1",
        ),
        _pending_node("accel", "w1", Instance.ALLOCATED, rep="rep-1"),
        _run_node(
            "accel",
            "w2",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d1", rack_key: "rack-1", **({_WID: "0"} if exclusive else {})},
            rep="rep-2",
        ),
        _run_node(
            "accel",
            "w3",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d1", rack_key: "rack-1", **({_WID: "1"} if exclusive else {})},
            used={"TPU": 4},
            rep="rep-2",
        ),
    ]

    # Cross-rack negative check: 2-host gang pinned to rack-1 must NOT steal w1
    # (physically in rep-1 / rack-0) to pair with w2.
    g2_rack1 = _strict_pack_gang(
        rack_key,
        _spread_reqs({"CPU": 4, "TPU": 4}, 2, "_pg_r1"),
        loc_constraints=[(rack_key, _IN, ["rack-1"])],
    )
    rep_cross = _sched(cfgs, insts, gangs=[g2_rack1])
    assert len(rep_cross.infeasible_gang_resource_requests) == 1

    # Same-rack positive checks: both a 1-host gang pinned to rack-0 and a plain
    # ResourceRequest selecting rack-0 place onto w1 with 0 launches.
    g1_rack0 = _strict_pack_gang(
        rack_key,
        _spread_reqs({"CPU": 4, "TPU": 4}, 1, "_pg_r0"),
        loc_constraints=[(rack_key, _IN, ["rack-0"])],
    )
    rep_same_gang = _sched(cfgs, insts, gangs=[g1_rack0])
    assert _launch_and_terminate(rep_same_gang) == ({}, [])
    assert rep_same_gang.infeasible_gang_resource_requests == []

    plain_rack0 = _resource_request(
        {"CPU": 4, "TPU": 4}, sel=[[(rack_key, _IN, ["rack-0"])]]
    )
    rep_same_plain = _sched(cfgs, insts, reqs=[plain_rack0])
    assert _launch_and_terminate(rep_same_plain) == ({}, [])
    assert rep_same_plain.infeasible_resource_requests == []

    # Conflicting running rack labels inside one 4-host replica must NOT
    # propagate either rack onto booting siblings.
    cfgs4 = {
        "accel": NodeTypeConfig(
            name="accel",
            resources={"CPU": 4, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=4,
            runtime_label_keys=(rack_key, dom_key),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=dom_key,
                domain_exclusive=exclusive,
            ),
        )
    }
    conflict_insts = [
        _run_node(
            "accel",
            "c0",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d0", rack_key: "rack-0"},
            used={"TPU": 4},
            rep="rep-c",
        ),
        _run_node(
            "accel",
            "c1",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d0", rack_key: "rack-1"},
            used={"TPU": 4},
            rep="rep-c",
        ),
        _pending_node("accel", "c2", Instance.ALLOCATED, rep="rep-c"),
        _pending_node("accel", "c3", Instance.ALLOCATED, rep="rep-c"),
    ]
    rep_conflict = _sched(cfgs4, conflict_insts, reqs=[plain_rack0])
    assert len(rep_conflict.infeasible_resource_requests) == 1

    # Partial missing label (c0 has rack-0, c1 registered without rack) must NOT
    # propagate rack-0 onto booting siblings c2/c3.
    missing_label_insts = [
        _run_node(
            "accel",
            "c0",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d0", rack_key: "rack-0"},
            used={"TPU": 4},
            rep="rep-m",
        ),
        _run_node(
            "accel",
            "c1",
            {"CPU": 4, "TPU": 4},
            {dom_key: "d0"},
            used={"TPU": 4},
            rep="rep-m",
        ),
        _pending_node("accel", "c2", Instance.ALLOCATED, rep="rep-m"),
        _pending_node("accel", "c3", Instance.ALLOCATED, rep="rep-m"),
    ]
    rep_missing = _sched(cfgs4, missing_label_insts, reqs=[plain_rack0])
    assert len(rep_missing.infeasible_resource_requests) == 1


def test_in_progress_ippr_shared_domain_and_pending_dedup():
    """In-progress IPPR resizes (is_in_progress=True, can_resize_up=False) are
    reflected consistently in _has_domain_spare_capacity, _pending_domain_signature,
    and _domain_fits_resource_totals without order-dependent deduplication bugs."""
    rack_key = "ray.io/rack"
    ippr_grp = IPPRGroupSpec(
        min_cpu=2, max_cpu=8, min_memory=2048, max_memory=8192, resize_timeout=300
    )
    cfgs = {
        "w": NodeTypeConfig(
            name="w",
            resources={"CPU": 2, "memory": 2048},
            min_worker_nodes=0,
            max_worker_nodes=2,
            runtime_label_keys=(rack_key,),
        ),
        "big": NodeTypeConfig(
            name="big",
            resources={"CPU": 8, "memory": 8192},
            min_worker_nodes=0,
            max_worker_nodes=1,
            runtime_label_keys=(rack_key,),
        ),
    }
    ippr_specs = IPPRSpecs(groups={"w": ippr_grp})
    d1_in_progress = IPPRStatus(
        cloud_instance_id="cloud-d1",
        spec=ippr_grp,
        current_cpu=2,
        current_memory=2048,
        desired_cpu=8,
        desired_memory=8192,
        resizing_at=100.0,
        k8s_resize_status="inprogress",
        raylet_id="r1",
    )
    assert d1_in_progress.is_in_progress()
    assert not d1_in_progress.can_resize_up()

    d1_inst = _run_node(
        "w", "d1", {"CPU": 2, "memory": 2048}, {rack_key: "rack-0"}, used={"CPU": 2}
    )
    g2_cpu4 = _strict_pack_gang(rack_key, _spread_reqs({"CPU": 4}, 2, "_pg_ippr"))

    # (a) Shared-domain booting sibling fallback sees d1's in-progress 6 CPU spare capacity.
    rep_sib = _sched(
        cfgs,
        [d1_inst, _pending_node("big", "sib1", Instance.ALLOCATED)],
        gangs=[g2_cpu4],
        ippr_specs=ippr_specs,
        ippr_statuses={"cloud-d1": d1_in_progress},
    )
    assert _launch_and_terminate(rep_sib) == ({}, [])
    assert rep_sib.infeasible_gang_resource_requests == []

    # (b) Tight-cap shared-domain launch fallback (max_worker_nodes=2) launches 1 node.
    rep_launch = _sched(
        {"w": cfgs["w"]},
        [d1_inst],
        gangs=[g2_cpu4],
        ippr_specs=ippr_specs,
        ippr_statuses={"cloud-d1": d1_in_progress},
    )
    assert _launch_and_terminate(rep_launch) == ({"w": 1}, [])
    assert rep_launch.infeasible_gang_resource_requests == []

    # (c) Order-invariant pending-domain deduplication between failed IPPR and
    # in-progress IPPR, plus distinct signature vs resize-up-eligible IPPR.
    n0a = _pending_node("w", "n0a", Instance.ALLOCATED, rep="rep-0")
    n0b = _pending_node("w", "n0b", Instance.ALLOCATED, rep="rep-0")
    n1a = _pending_node("w", "n1a", Instance.ALLOCATED, rep="rep-1")
    n1b = _pending_node("w", "n1b", Instance.ALLOCATED, rep="rep-1")
    st_failed = IPPRStatus(
        cloud_instance_id="cloud-n0a",
        spec=ippr_grp,
        current_cpu=2,
        current_memory=2048,
        desired_cpu=2,
        desired_memory=2048,
        last_failed_at=100.0,
        raylet_id="r0a",
    )
    st_failed_b = IPPRStatus(
        cloud_instance_id="cloud-n0b",
        spec=ippr_grp,
        current_cpu=2,
        current_memory=2048,
        desired_cpu=2,
        desired_memory=2048,
        last_failed_at=100.0,
        raylet_id="r0b",
    )
    st_inprog = IPPRStatus(
        cloud_instance_id="cloud-n1a",
        spec=ippr_grp,
        current_cpu=2,
        current_memory=2048,
        desired_cpu=8,
        desired_memory=8192,
        resizing_at=100.0,
        k8s_resize_status="inprogress",
        raylet_id="r1a",
    )
    st_inprog_b = IPPRStatus(
        cloud_instance_id="cloud-n1b",
        spec=ippr_grp,
        current_cpu=2,
        current_memory=2048,
        desired_cpu=8,
        desired_memory=8192,
        resizing_at=100.0,
        k8s_resize_status="inprogress",
        raylet_id="r1b",
    )
    st_can_up = IPPRStatus(
        cloud_instance_id="cloud-n2",
        spec=ippr_grp,
        current_cpu=2,
        current_memory=2048,
        desired_cpu=2,
        desired_memory=2048,
        raylet_id="r2",
    )
    g2_cpu6 = _strict_pack_gang(rack_key, _spread_reqs({"CPU": 6}, 2, "_pg_ippr6"))
    w_cap4 = NodeTypeConfig(
        name="w",
        resources={"CPU": 2, "memory": 2048},
        min_worker_nodes=0,
        max_worker_nodes=4,
        runtime_label_keys=(rack_key,),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=rack_key,
            domain_exclusive=True,
        ),
    )
    ippr_statuses_ord = {
        "cloud-n0a": st_failed,
        "cloud-n0b": st_failed_b,
        "cloud-n1a": st_inprog,
        "cloud-n1b": st_inprog_b,
    }
    for order in ([n0a, n0b, n1a, n1b], [n1a, n1b, n0a, n0b]):
        rep_ord = _sched(
            {"w": w_cap4},
            order,
            gangs=[g2_cpu6],
            ippr_specs=ippr_specs,
            ippr_statuses=ippr_statuses_ord,
        )
        assert rep_ord.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_ord) == ({}, [])

    sn_fail = SchedulingNode.new(n0a, cfgs, True)
    sn_fail.ippr_status = st_failed
    sn_inprog = SchedulingNode.new(n1a, cfgs, True)
    sn_inprog.ippr_status = st_inprog
    sn_up = SchedulingNode.new(
        _run_node("w", "n2", {"CPU": 2, "memory": 2048}, {}), cfgs, True
    )
    sn_up.ippr_status = st_can_up
    sigs = {
        ResourceDemandScheduler._pending_domain_signature([sn_fail], rack_key),
        ResourceDemandScheduler._pending_domain_signature([sn_inprog], rack_key),
        ResourceDemandScheduler._pending_domain_signature([sn_up], rack_key),
    }
    assert len(sigs) == 3

    # (d) Negative counter-case: in-progress resize DOWN (8 -> 2 CPU with 2 CPU
    # already used) leaves 0 available CPU so _has_domain_spare_capacity is False.
    sn_down = SchedulingNode.new(
        _run_node(
            "w",
            "d-down",
            {"CPU": 8, "memory": 8192},
            {rack_key: "rack-0"},
            used={"CPU": 2},
        ),
        cfgs,
        True,
    )
    sn_down.ippr_status = IPPRStatus(
        cloud_instance_id="cloud-d-down",
        spec=ippr_grp,
        current_cpu=8,
        current_memory=8192,
        desired_cpu=2,
        desired_memory=2048,
        resizing_at=100.0,
        k8s_resize_status="inprogress",
        raylet_id="rd",
    )
    assert not ResourceDemandScheduler._has_domain_spare_capacity(
        [sn_down], _spread_reqs({"CPU": 1}, 1, "_pg_down"), 1
    )


def test_flat_unattributed_nodes_active_pinned_domain_protection():
    """Unattributed multi-worker nodes in `flat` honor `active_pinned_domains`
    protection while an active sibling keeps a shared pinned domain alive, and
    scale down cleanly once the pinned domain is fully idle."""
    rack_key = "ray.io/rack"
    cfgs = {
        "sh": NodeTypeConfig(
            name="sh",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=1,
            labels={rack_key: "rack-0"},
        ),
        "mw": NodeTypeConfig(
            name="mw",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=2,
            labels={rack_key: "rack-0"},
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=_SLICE,
                domain_exclusive=True,
            ),
        ),
    }
    g4_rack0 = _strict_pack_gang(
        rack_key,
        _spread_reqs({"CPU": 4}, 4, "_pg_flat_pin"),
        loc_constraints=[(rack_key, _IN, ["rack-0"])],
    )

    sh_busy = _run_node(
        "sh", "sh0", {"CPU": 4}, {rack_key: "rack-0"}, used={"CPU": 4}, idle_ms=0
    )
    sh_idle = _run_node("sh", "sh0", {"CPU": 4}, {rack_key: "rack-0"}, idle_ms=60_000)
    mw0 = _run_node("mw", "mw0", {"CPU": 4}, {rack_key: "rack-0"}, idle_ms=60_000)
    mw1 = _run_node("mw", "mw1", {"CPU": 4}, {rack_key: "rack-0"}, idle_ms=60_000)

    # While sh0 is active in rack-0, idle unattributed mw0/mw1 in flat stay protected.
    rep_active = _sched(cfgs, [sh_busy, mw0, mw1], gangs=[g4_rack0], idle_s=10.0)
    assert rep_active.to_terminate == []
    assert len(rep_active.infeasible_gang_resource_requests) == 1

    # Once sh0 is also idle and rack-0 has no active or incomplete members, all
    # three idle nodes scale down cleanly (no resource leak).
    rep_all_idle = _sched(cfgs, [sh_idle, mw0, mw1], gangs=[g4_rack0], idle_s=10.0)
    assert sorted(t.instance_id for t in rep_all_idle.to_terminate) == [
        "mw0",
        "mw1",
        "sh0",
    ]

    # Incomplete flat (1 of 2 hosts) in pinned rack-0 protects both itself and
    # idle sh0 in the same pinned rack.
    rep_inc = _sched(cfgs, [sh_idle, mw0], gangs=[g4_rack0], idle_s=10.0)
    assert rep_inc.to_terminate == []


def test_gang_dedup_key_preserves_locality_constraint_label_name():
    """On an unlabeled cluster, _gang_dedup_key must not normalize a placement
    constraint key equal to locality_constraint.label_name into fresh_slots,
    preventing a false cache collision with a schedulable custom-key gang."""
    cfgs = {
        "cpu": NodeTypeConfig(
            name="cpu", resources={"CPU": 4}, min_worker_nodes=0, max_worker_nodes=4
        )
    }

    def _two_bundle_gang(loc_key: str, dynamic_key: str) -> GangResourceRequest:
        b0 = _mk_req({"CPU": 1})
        pc = b0.placement_constraints.add()
        pc.anti_affinity.label_name = dynamic_key
        pc.anti_affinity.label_value = "v"
        b1 = _mk_req(
            {"CPU": 1},
            label_selectors=[[(dynamic_key, _IN, ["v"])]],
        )
        return _strict_pack_gang(loc_key, [b0, b1])

    g_infeas = _two_bundle_gang("loc/key", "loc/key")
    g_ok = _two_bundle_gang("loc/key", "cust/key")

    rep_alone = _sched(cfgs, [], gangs=[g_ok])
    assert rep_alone.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_alone) == ({"cpu": 1}, [])

    rep_both = _sched(cfgs, [], gangs=[g_infeas, g_ok])
    assert len(rep_both.infeasible_gang_resource_requests) == 1
    assert rep_both.infeasible_gang_resource_requests[0] == g_infeas
    assert _launch_and_terminate(rep_both) == ({"cpu": 1}, [])


@pytest.mark.parametrize("use_shared_domain", [True, False])
def test_multireplica_midboot_odd_counts_and_staggered_boot(use_shared_domain):
    """Multi-replica shared-lid batches preserve modulo-size deficit math on odd
    mid-boot counts (3 running + 1 booting/vanished/unlabeled) and staggered
    2+2 registration without premature termination or scale-up deadlock."""
    clique_key = "ray.io/gpu-clique"
    rack_key = "ray.io/rack"
    dom_key = clique_key if use_shared_domain else None
    head_res = "clique-head"
    cfgs = {
        "mw": NodeTypeConfig(
            name="mw",
            resources={"CPU": 4, "GPU": 4, head_res: 1},
            min_worker_nodes=0,
            max_worker_nodes=6,
            runtime_label_keys=(rack_key, clique_key)
            if use_shared_domain
            else (rack_key,),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=dom_key,
                domain_exclusive=False,
                head_resources=(head_res,),
            ),
        )
    }
    base_labels = {
        rack_key: "rack-0",
        **({clique_key: "clique-0"} if use_shared_domain else {}),
    }
    r0 = _run_node(
        "mw",
        "r0",
        {"CPU": 4, "GPU": 4, head_res: 1},
        base_labels,
        used={"GPU": 4},
        lid="L1",
    )
    r1 = _run_node(
        "mw", "r1", {"CPU": 4, "GPU": 4}, base_labels, used={"GPU": 4}, lid="L1"
    )
    r2 = _run_node(
        "mw", "r2", {"CPU": 4, "GPU": 4}, base_labels, used={"GPU": 4}, lid="L1"
    )
    b3 = _pending_node("mw", "b3", Instance.ALLOCATED, lid="L1")

    # (a) Booting 4th host attaches to L1 batch, inherits domain + rack + missing
    # 2nd-replica head resource, rejects cross-domain/cross-rack stealing, and
    # allows 3rd-replica scale-up under max_worker_nodes=6 (no phantom 5th slot).
    pin_key = clique_key if use_shared_domain else rack_key
    pin_val = "clique-0" if use_shared_domain else "rack-0"
    other_val = "clique-1" if use_shared_domain else "rack-1"
    g_head_same = _strict_pack_gang(
        pin_key,
        _spread_reqs({"CPU": 4, "GPU": 4, head_res: 1}, 1, "_pg_head0"),
        loc_constraints=[(pin_key, _IN, [pin_val])],
    )
    rep_head = _sched(cfgs, [r0, r1, r2, b3], gangs=[g_head_same])
    assert rep_head.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_head) == ({}, [])

    other_host = _run_node(
        "mw",
        "o0",
        {"CPU": 4, "GPU": 4, head_res: 1},
        {rack_key: "rack-1", **({clique_key: "clique-1"} if use_shared_domain else {})},
        lid="L2",
    )
    other_host_busy = _run_node(
        "mw",
        "o1",
        {"CPU": 4, "GPU": 4},
        {rack_key: "rack-1", **({clique_key: "clique-1"} if use_shared_domain else {})},
        used={"GPU": 4},
        lid="L2",
    )
    g2_other = _strict_pack_gang(
        pin_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_steal"),
        loc_constraints=[(pin_key, _IN, [other_val])],
    )
    rep_steal = _sched(
        cfgs, [r0, r1, r2, b3, other_host, other_host_busy], gangs=[g2_other]
    )
    assert len(rep_steal.infeasible_gang_resource_requests) == 1

    # 3 running (busy) + 1 ALLOCATED (free) + 2 new GPU=4 demands -> 1 fits on b3,
    # 1 triggers 3rd replica launch (2 nodes) cleanly under max_worker_nodes=6.
    rep_scale3 = _sched(
        cfgs,
        [r0, r1, r2, b3],
        reqs=[
            _resource_request({"CPU": 4, "GPU": 4}),
            _resource_request({"CPU": 4, "GPU": 4}),
        ],
    )
    assert rep_scale3.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_scale3) == ({"mw": 2}, [])

    # (b) RAY_STOP_REQUESTED on r0 propagates draining onto attached b3.
    r0_stop = _run_node(
        "mw",
        "r0",
        {"CPU": 4, "GPU": 4, head_res: 1},
        base_labels,
        used={"GPU": 4},
        lid="L1",
    )
    assert r0_stop.im_instance is not None
    r0_stop.im_instance.status = Instance.RAY_STOP_REQUESTED
    g1_any = _strict_pack_gang(
        pin_key, _spread_reqs({"CPU": 4, "GPU": 4}, 1, "_pg_drain")
    )
    cfgs_capped = {
        "mw": NodeTypeConfig(
            name="mw",
            resources={"CPU": 4, "GPU": 4, head_res: 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
            runtime_label_keys=cfgs["mw"].runtime_label_keys,
            replica_config=cfgs["mw"].replica_config,
        )
    }
    rep_drain = _sched(cfgs_capped, [r0_stop, r1, r2, b3], gangs=[g1_any])
    assert len(rep_drain.infeasible_gang_resource_requests) == 1
    assert rep_drain.to_terminate == []

    # (c) Vanished 4th host emits 1 backfill placeholder with head resource + rack.
    rep_bf = _sched(cfgs_capped, [r0, r1, r2], gangs=[g_head_same])
    assert rep_bf.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_bf) == ({}, [])

    # (d) Unlabeled 4th running sibling attaches to L1 group in shared-domain mode.
    if use_shared_domain:
        r3_unlabeled = _run_node(
            "mw", "r3", {"CPU": 4, "GPU": 4, head_res: 1}, {}, used={"GPU": 4}, lid="L1"
        )
        rep_r3 = _sched(
            cfgs,
            [r0, r1, r2, r3_unlabeled],
            reqs=[_resource_request({"CPU": 4, "GPU": 4})],
        )
        assert rep_r3.infeasible_resource_requests == []
        assert _launch_and_terminate(rep_r3) == ({"mw": 2}, [])

    # (e) Staggered boot Case 2A: 2 idle RAY_RUNNING + 2 RAY_INSTALLING on shared
    # lid="lid-shared" with min_worker_nodes=2 must NOT terminate the 2 running hosts.
    cfgs_min2 = {
        "mw": NodeTypeConfig(
            name="mw",
            resources={"CPU": 4, "GPU": 4, head_res: 1},
            min_worker_nodes=2,
            max_worker_nodes=4,
            runtime_label_keys=cfgs["mw"].runtime_label_keys,
            replica_config=cfgs["mw"].replica_config,
        )
    }
    staggered = [
        _run_node(
            "mw",
            "a0",
            {"CPU": 4, "GPU": 4, head_res: 1},
            base_labels,
            idle_ms=20_000,
            lid="lid-shared",
        ),
        _run_node(
            "mw",
            "b0",
            {"CPU": 4, "GPU": 4, head_res: 1},
            base_labels,
            idle_ms=20_000,
            lid="lid-shared",
        ),
        _pending_node("mw", "a1", Instance.RAY_INSTALLING, lid="lid-shared"),
        _pending_node("mw", "b1", Instance.RAY_INSTALLING, lid="lid-shared"),
    ]
    rep_staggered = _sched(cfgs_min2, staggered, idle_s=10.0)
    assert rep_staggered.to_terminate == []


def test_pinned_domain_pending_idle_symmetry_and_multihost_scaleup():
    """All-idle pinned domains with expired pending + running nodes scale down
    cleanly without leaking, while active multi-worker TO_LAUNCH and QUEUED
    replicas in a pinned domain protect co-located idle nodes."""
    rack_key = "ray.io/rack"
    cfgs = {
        "sh": NodeTypeConfig(
            name="sh",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=2,
            labels={rack_key: "rack-0"},
        ),
        "mw": NodeTypeConfig(
            name="mw",
            resources={"CPU": 4, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=2,
            labels={rack_key: "rack-0"},
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=_SLICE,
                domain_exclusive=True,
            ),
        ),
    }
    g_infeas_rack0 = _strict_pack_gang(
        rack_key,
        _spread_reqs({"CPU": 4}, 5, "_pg_inf5"),
        loc_constraints=[(rack_key, _IN, ["rack-0"])],
    )
    g_gpu2_rack0 = _strict_pack_gang(
        rack_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_gpu2"),
        loc_constraints=[(rack_key, _IN, ["rack-0"])],
    )

    sh0_alloc = _pending_node("sh", "sh0", Instance.ALLOCATED)
    sh0_idle = _run_node("sh", "sh0", {"CPU": 4}, {rack_key: "rack-0"}, idle_ms=60_000)
    mw_idle = [
        _run_node(
            "mw",
            "mw0",
            {"CPU": 4, "GPU": 4},
            {_SLICE: "s0", _WID: "0", rack_key: "rack-0"},
            idle_ms=60_000,
        ),
        _run_node(
            "mw",
            "mw1",
            {"CPU": 4, "GPU": 4},
            {_SLICE: "s0", _WID: "1", rack_key: "rack-0"},
            idle_ms=60_000,
        ),
    ]
    mw_busy = [
        _run_node(
            "mw",
            "mw0",
            {"CPU": 4, "GPU": 4},
            {_SLICE: "s0", _WID: "0", rack_key: "rack-0"},
            used={"GPU": 4},
            idle_ms=0,
        ),
        _run_node(
            "mw",
            "mw1",
            {"CPU": 4, "GPU": 4},
            {_SLICE: "s0", _WID: "1", rack_key: "rack-0"},
            used={"GPU": 4},
            idle_ms=0,
        ),
    ]

    # (a) All-idle pinned rack-0 (ALLOCATED sh0 + expired RUNNING mw0/mw1):
    # Tick 1 starts sh0's pending timer and shields mw0/mw1; Tick 2 after
    # pending_idle_timeout_s scales down all three cleanly (zero leak).
    sched_leak = ResourceDemandScheduler()
    now_t = [100.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        r1 = sched_leak.schedule(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=[sh0_alloc] + mw_idle,
                gang_resource_requests=[g_infeas_rack0],
                idle_timeout_s=10.0,
                pending_idle_timeout_s=10.0,
            )
        )
        assert r1.to_terminate == []
        now_t[0] = 220.0
        r2 = sched_leak.schedule(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=[sh0_alloc] + mw_idle,
                gang_resource_requests=[g_infeas_rack0],
                idle_timeout_s=10.0,
                pending_idle_timeout_s=10.0,
            )
        )
        assert sorted(t.instance_id for t in r2.to_terminate) == ["mw0", "mw1", "sh0"]

    # Counter-case: while mw0/mw1 are busy in rack-0, ALLOCATED sh0 stays
    # protected and does not record a stale unneeded timestamp that could
    # prematurely expire a freshly added sibling pending node.
    sh_fresh = _pending_node("sh", "sh_fresh", Instance.ALLOCATED)
    sched_busy = ResourceDemandScheduler()
    now_t[0] = 100.0
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        rb1 = sched_busy.schedule(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=[sh0_alloc] + mw_busy,
                gang_resource_requests=[g_infeas_rack0],
                idle_timeout_s=10.0,
                pending_idle_timeout_s=10.0,
            )
        )
        assert rb1.to_terminate == []
        assert "sh" not in sched_busy._pending_unneeded_since_s
        now_t[0] = 220.0
        rb2 = sched_busy.schedule(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=[sh0_alloc, sh_fresh] + mw_busy,
                gang_resource_requests=[g_infeas_rack0],
                idle_timeout_s=10.0,
                pending_idle_timeout_s=10.0,
            )
        )
        assert rb2.to_terminate == []

    # (b) Multi-worker TO_LAUNCH replica in pinned rack-0 protects co-located idle sh0.
    rep_launch = _sched(
        cfgs, [sh0_idle], gangs=[g_gpu2_rack0, g_infeas_rack0], idle_s=10.0
    )
    assert _launch_and_terminate(rep_launch) == ({"mw": 2}, [])

    # (c) Multi-worker active QUEUED replica in pinned rack-0 protects idle sh0,
    # and once unneeded + expired all three terminate cleanly together.
    mw_queued = [
        _pending_node("mw", "q0", Instance.QUEUED),
        _pending_node("mw", "q1", Instance.QUEUED),
    ]
    rep_q_active = _sched(
        cfgs,
        [sh0_idle] + mw_queued,
        gangs=[g_gpu2_rack0, g_infeas_rack0],
        idle_s=10.0,
    )
    assert _launch_and_terminate(rep_q_active) == ({}, [])

    sched_q_idle = ResourceDemandScheduler()
    now_t[0] = 100.0
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        rq1 = sched_q_idle.schedule(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=[sh0_idle] + mw_queued,
                gang_resource_requests=[g_infeas_rack0],
                idle_timeout_s=10.0,
                pending_idle_timeout_s=10.0,
            )
        )
        assert rq1.to_terminate == []
        now_t[0] = 220.0
        rq2 = sched_q_idle.schedule(
            SchedulingRequest(
                disable_launch_config_check=True,
                node_type_configs=cfgs,
                current_instances=[sh0_idle] + mw_queued,
                gang_resource_requests=[g_infeas_rack0],
                idle_timeout_s=10.0,
                pending_idle_timeout_s=10.0,
            )
        )
        assert sorted(t.instance_id for t in rq2.to_terminate) == ["q0", "q1", "sh0"]


def test_dynamic_domain_prefilter_host_count_and_locality_selector_safety():
    """Schedulable host-count prefilter skips fragmented slices without calling
    _try_schedule, and failed-dynamic-domain deduplication never skips a domain
    referenced by a multi-tier fallback selector on locality_label."""
    # (a) 8 fragmented 4-host slices: 3 hosts have 16 CPU, 1 host has 0 CPU.
    # A 4-bundle 10-CPU spread gang fits aggregate CPU (48 >= 40) but only 3 hosts
    # can fit any bundle (< min_nodes_needed=4) -> 0 _try_schedule calls per slice.
    frag_slices = [
        _run_node(
            "tpu",
            f"s{s}-w{w}",
            {"CPU": 16, "TPU": 4},
            {_SLICE: f"slice-{s}", _WID: str(w)},
            used={"CPU": 16} if w == 3 else None,
        )
        for s in range(8)
        for w in range(4)
    ]
    g_spread4 = _strict_pack_gang(_SLICE, _spread_reqs({"CPU": 10}, 4, "_pg_f4"))
    orig_try = ResourceDemandScheduler._try_schedule
    calls = 0

    def _counting_try(ctx, requests_to_sched, *args, **kwargs):
        nonlocal calls
        if requests_to_sched:
            calls += 1
        return orig_try(ctx, requests_to_sched, *args, **kwargs)

    with patch.object(
        ResourceDemandScheduler, "_try_schedule", staticmethod(_counting_try)
    ):
        rep_frag = _sched(
            {"tpu": _tpu_config(name="tpu", workers_per_replica=4, max_workers=32)},
            frag_slices,
            gangs=[g_spread4],
        )
    assert len(rep_frag.infeasible_gang_resource_requests) == 1
    assert calls == 0

    # (b) Unpinned gangs referencing locality_label via NOT_IN or a multi-tier
    # fallback selector must NOT be deduplicated across identical non-locality
    # dynamic domains:
    # - NOT_IN (_SLICE, ["slice-0"]) fails on slice-0 but succeeds on slice-1.
    # - Tier 1 (_SLICE IN ["slice-1"]) / Tier 2 (tier IN ["ok"]) selects slice-1
    #   (leaving idle slice-0 w0 to idle-terminate while keeping w1 alive).
    sel_cfgs = {
        "w": NodeTypeConfig(
            name="w",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=2,
            runtime_label_keys=(_SLICE, "tier"),
        )
    }
    insts = [
        _run_node(
            "w", "w0", {"CPU": 4}, {_SLICE: "slice-0", "tier": "ok"}, idle_ms=60_000
        ),
        _run_node(
            "w", "w1", {"CPU": 4}, {_SLICE: "slice-1", "tier": "ok"}, idle_ms=60_000
        ),
    ]
    b_not_in = _mk_req({"CPU": 2}, label_selectors=[[(_SLICE, _NOT_IN, ["slice-0"])]])
    g_not_in = _strict_pack_gang(_SLICE, [b_not_in])
    rep_not_in = _sched(sel_cfgs, insts, gangs=[g_not_in], idle_s=10.0)
    assert rep_not_in.infeasible_gang_resource_requests == []
    assert sorted(t.instance_id for t in rep_not_in.to_terminate) == ["w0"]

    b_pref = _mk_req(
        {"CPU": 2},
        label_selectors=[[(_SLICE, _IN, ["slice-1"])], [("tier", _IN, ["ok"])]],
    )
    g_pref = _strict_pack_gang(_SLICE, [b_pref])
    rep_pref = _sched(sel_cfgs, insts, gangs=[g_pref], idle_s=10.0)
    assert rep_pref.infeasible_gang_resource_requests == []
    assert sorted(t.instance_id for t in rep_pref.to_terminate) == ["w0"]


def _terminated_teardown_row(
    nt: str, iid: str, lid: str, cid: str = "cloud-dead"
) -> AutoscalerInstance:
    dead = make_autoscaler_instance(
        im_instance=Instance(
            instance_type=nt,
            status=Instance.TERMINATED,
            instance_id=iid,
            launch_request_id=lid,
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id=cid,
    )
    for idx, st in enumerate(
        (
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
        )
    ):
        dead.im_instance.status_history.add(
            instance_status=st, timestamp_ns=1_000_000 + idx * 1_000
        )
    return dead


def test_unlabeled_teardown_survivors_stay_schedulable_and_refill_at_max():
    """Unlabeled running survivors whose torn-down peer is TERMINATED remain
    schedulable, idle-terminate cleanly, suppress phantom placeholders, and emit a
    single-pod refill launch at max_worker_nodes."""

    def _tpu_cfg(
        max_w: int, dom_key: Optional[str] = _SLICE
    ) -> Dict[str, NodeTypeConfig]:
        return {
            "tpu": NodeTypeConfig(
                name="tpu",
                min_worker_nodes=0,
                max_worker_nodes=max_w,
                resources={"CPU": 8, "TPU": 4},
                runtime_label_keys=(dom_key, _WID) if dom_key else (),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=dom_key,
                    domain_exclusive=True,
                    worker_index_label_key=_WID if dom_key else None,
                ),
            )
        }

    dead = _terminated_teardown_row("tpu", "dead", "L1")
    idle_hosts = [
        _run_node(
            "tpu",
            f"s{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s1", _WID: str(w)},
            idle_ms=600_000,
            lid="L1",
        )
        for w in range(3)
    ]

    rep_idle = _sched(_tpu_cfg(4), idle_hosts + [dead], idle_s=60.0)
    assert sorted(t.instance_id for t in rep_idle.to_terminate) == ["s0", "s1", "s2"]

    rep_task = _sched(
        _tpu_cfg(4),
        idle_hosts + [dead],
        reqs=[_resource_request({"CPU": 1})],
        idle_s=60.0,
    )
    assert rep_task.infeasible_resource_requests == []
    assert rep_task.to_launch == []

    # 4-host demand on 3 idle torn-down survivors must launch (not park on a phantom
    # placeholder) for both exclusive-domain and domainless groups.
    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_td4"))
    for max_w in (4, 8):
        rep_g4 = _sched(_tpu_cfg(max_w), idle_hosts + [dead], gangs=[g4])
        assert rep_g4.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_g4) == ({"tpu": 1}, [])

    domless_idle = [
        _run_node("tpu", f"d{w}", {"CPU": 8, "TPU": 4}, {}, lid="L1") for w in range(3)
    ]
    rep_domless = _sched(
        _tpu_cfg(8, dom_key=None),
        domless_idle + [dead],
        reqs=[_resource_request({"TPU": 4}, count=4)],
    )
    assert rep_domless.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_domless) == ({"tpu": 1}, [])

    # 3 busy survivors at max_worker_nodes=4 refill 1 task, reject a 2nd task, and
    # reject a 4-host gang.
    busy_hosts = [
        _run_node(
            "tpu",
            f"b{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s1", _WID: str(w)},
            used={"TPU": 4},
            lid="L1",
        )
        for w in range(3)
    ]
    rep_refill = _sched(
        _tpu_cfg(4), busy_hosts + [dead], reqs=[_resource_request({"TPU": 4}, count=2)]
    )
    assert _launch_and_terminate(rep_refill) == ({"tpu": 1}, [])
    assert len(rep_refill.infeasible_resource_requests) == 1
    assert rep_refill.cluster_resources["TPU"] == 16.0

    rep_busy_g4 = _sched(_tpu_cfg(4), busy_hosts + [dead], gangs=[g4])
    assert len(rep_busy_g4.infeasible_gang_resource_requests) == 1


def test_shared_domain_complete_stop_requested_chunk_keeps_siblings_schedulable():
    """Complete RAY_STOP_REQUESTED replica multiples in a shared domain do not
    mark healthy sibling multiples in that domain as draining."""
    gpu_cfgs = {
        "gpu": NodeTypeConfig(
            name="gpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 4, "GPU": 1},
            runtime_label_keys=("rack",),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key="rack",
                domain_exclusive=False,
            ),
        )
    }
    rack_nodes = [
        _run_node("gpu", f"g{i}", {"CPU": 4, "GPU": 1}, {"rack": "rack-a"})
        for i in range(6)
    ]
    rack_nodes[0].im_instance.status = Instance.RAY_STOP_REQUESTED
    rack_nodes[1].im_instance.status = Instance.RAY_STOP_REQUESTED
    rep_rack = _sched(gpu_cfgs, rack_nodes, reqs=[_resource_request({"GPU": 1})])
    assert rep_rack.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_rack) == ({}, [])


def test_live_nodes_outrank_backfill_placeholders():
    """Real running and pending nodes outrank backfill placeholders even when live
    hosts carry extra runtime resources like object_store_memory and node:<ip>."""
    head_res = "TPU-v6e-16-head"
    live_base = {"CPU": 8, "TPU": 4, "memory": 64e9, "object_store_memory": 19e9}
    cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=12,
            resources={"CPU": 8, "TPU": 4, "memory": 64e9, head_res: 1},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    r0_live = [
        _run_node(
            "tpu",
            f"a{w}",
            {
                **live_base,
                f"node:10.0.0.{w}": 1,
                **({head_res: 1} if w == 0 else {}),
            },
            {_SLICE: "slice-0", _WID: str(w)},
            idle_ms=600_000,
            rep="R0",
            lid="L0",
        )
        for w in range(4)
    ]
    r1_deg = [
        _run_node(
            "tpu",
            f"b{w}",
            {**live_base, f"node:10.0.1.{w}": 1},
            {_SLICE: "slice-1", _WID: str(w)},
            idle_ms=1_000,
            rep="R1",
            lid="L1",
        )
        for w in (1, 2, 3)
    ]

    rep_tpu = _sched(
        cfgs,
        r0_live + r1_deg,
        reqs=[_resource_request({"TPU": 4})],
        idle_s=60.0,
    )
    assert rep_tpu.infeasible_resource_requests == []
    assert rep_tpu.to_terminate == []

    g_head = GangResourceRequest(requests=[_mk_req({head_res: 1})])
    rep_head = _sched(cfgs, r0_live + r1_deg, gangs=[g_head], idle_s=60.0)
    assert rep_head.infeasible_gang_resource_requests == []
    assert rep_head.to_terminate == []

    # Real pending ALLOCATED replica also outranks a degraded vanished-batch
    # pending domain containing a backfill placeholder (verifying NodeStateCache,
    # _pending_domain_signature, and _GangSearch placeholder tie-breakers).
    tpu2_cfg = {
        "tpu2": NodeTypeConfig(
            name="tpu2",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 4, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    v0 = _pending_node("tpu2", "v0", Instance.ALLOCATED, rep="A_van", lid="L_van")
    p_real0 = _pending_node(
        "tpu2", "p_real0", Instance.ALLOCATED, rep="B_full", lid="L_full"
    )
    p_real1 = _pending_node(
        "tpu2", "p_real1", Instance.ALLOCATED, rep="B_full", lid="L_full"
    )

    req_pend = sched_request(
        tpu2_cfg,
        instances=[v0, p_real0, p_real1],
        pending_idle_timeout_s=10.0,
    )
    req_pend.gang_resource_requests.append(
        _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_pend2"))
    )
    sched_p = ResourceDemandScheduler()
    sched_p._pending_unneeded_since_s = {"tpu2": [0.0]}
    with patch("ray.autoscaler.v2.scheduler.time.monotonic", return_value=100.0):
        rep_p2 = sched_p.schedule(req_pend)
    assert rep_p2.infeasible_gang_resource_requests == []
    assert [t.instance_id for t in rep_p2.to_terminate] == ["v0"]


def test_placeholder_expiration_with_explicit_and_default_timeouts():
    """Backfill placeholders expire after backfill_placeholder_ttl_s when set, fall
    back to request_status_timeout_s when unset, and never couple to pending_idle_timeout_s."""
    min4_cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=4,
            max_worker_nodes=12,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    r1_deg = [
        _run_node(
            "tpu",
            f"b{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            rep="R1",
            lid="L1",
        )
        for w in (1, 2, 3)
    ]

    sched_exp = ResourceDemandScheduler()
    now_t = [100.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        assert _launch_and_terminate(
            _sched(min4_cfgs, r1_deg, placeholder_ttl_s=10.0, sched=sched_exp)
        ) == ({}, [])
        now_t[0] = 120.0
        assert _launch_and_terminate(
            _sched(min4_cfgs, r1_deg, placeholder_ttl_s=10.0, sched=sched_exp)
        ) == ({"tpu": 4}, [])

    # Setting pending_idle_timeout_s (including 0.0 or 10.0) must NOT prematurely
    # expire backfill placeholders before request_status_timeout_s (600s).
    for pending_idle in (0.0, 10.0, None):
        sched_default = ResourceDemandScheduler()
        now_t[0] = 100.0
        with patch(
            "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
        ):
            assert _launch_and_terminate(
                _sched(
                    min4_cfgs, r1_deg, pending_idle_s=pending_idle, sched=sched_default
                )
            ) == ({}, [])
            now_t[0] = 300.0
            assert _launch_and_terminate(
                _sched(
                    min4_cfgs, r1_deg, pending_idle_s=pending_idle, sched=sched_default
                )
            ) == ({}, [])
            now_t[0] = 800.0
            assert _launch_and_terminate(
                _sched(
                    min4_cfgs, r1_deg, pending_idle_s=pending_idle, sched=sched_default
                )
            ) == ({"tpu": 4}, [])

    # Running-worker idle_timeout_s (e.g. KubeRay's 0s, 15s, or 60s default) must
    # never short-circuit request_status_timeout_s when pending_idle_timeout_s is None.
    g_pinned = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_ttl"),
        loc_constraints=[(_SLICE, _IN, ["slice-1"])],
    )
    for running_idle_s in (0.0, 15.0, 60.0):
        sched_kr = ResourceDemandScheduler()
        now_t[0] = 100.0
        with patch(
            "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
        ):
            rep_t0 = _sched(
                min4_cfgs,
                r1_deg,
                gangs=[g_pinned],
                idle_s=running_idle_s,
                sched=sched_kr,
            )
            assert rep_t0.infeasible_gang_resource_requests == []
            assert _launch_and_terminate(rep_t0) == ({}, [])

            now_t[0] = 220.0
            rep_mid = _sched(
                min4_cfgs,
                r1_deg,
                gangs=[g_pinned],
                idle_s=running_idle_s,
                sched=sched_kr,
            )
            assert rep_mid.infeasible_gang_resource_requests == []
            assert _launch_and_terminate(rep_mid) == ({}, [])

            now_t[0] = 800.0
            rep_exp = _sched(
                min4_cfgs,
                r1_deg,
                gangs=[g_pinned],
                idle_s=running_idle_s,
                sched=sched_kr,
            )
            assert len(rep_exp.infeasible_gang_resource_requests) == 1
            assert _launch_and_terminate(rep_exp) == ({"tpu": 4}, [])


def test_idle_termination_prefers_degraded_subreplicas_under_min_workers():
    """Idle termination under min_worker_nodes terminates degraded sub-replica units
    before complete replicas whether placeholders are present, unrounded, or torn down."""
    tpu_cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=4,
            max_worker_nodes=12,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    full_s0 = [
        _run_node(
            "tpu",
            f"a{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            idle_ms=600_000,
            rep="R0",
            lid="L0",
        )
        for w in range(4)
    ]
    ph_deg = [
        _run_node(
            "tpu",
            f"b{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            idle_ms=120_000,
            rep="R1",
            lid="L1",
        )
        for w in (1, 2, 3)
    ]
    rep_ph = _sched(tpu_cfgs, full_s0 + ph_deg, idle_s=60.0)
    assert sorted(t.instance_id for t in rep_ph.to_terminate) == ["b1", "b2", "b3"]

    # Torn-down 2/4 slices (placeholders suppressed) still terminate before 4/4 s0.
    td_s1 = [
        _run_node(
            "tpu",
            f"c{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            idle_ms=120_000,
            lid="L1",
        )
        for w in range(2)
    ]
    td_s2 = [
        _run_node(
            "tpu",
            f"d{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-2", _WID: str(w)},
            idle_ms=120_000,
            lid="L2",
        )
        for w in range(2)
    ]
    rep_td = _sched(
        tpu_cfgs,
        full_s0
        + td_s1
        + td_s2
        + [
            _terminated_teardown_row("tpu", "dead1", "L1", "c-dead1"),
            _terminated_teardown_row("tpu", "dead2", "L2", "c-dead2"),
        ],
        idle_s=60.0,
    )
    assert sorted(t.instance_id for t in rep_td.to_terminate) == [
        "c0",
        "c1",
        "d0",
        "d1",
    ]

    # Cross-rack unrounded 1+1 split terminates before complete 2/2 rack-0.
    gpu_cfgs = {
        "gpu": NodeTypeConfig(
            name="gpu",
            min_worker_nodes=2,
            max_worker_nodes=4,
            resources={"CPU": 4, "GPU": 1},
            runtime_label_keys=("rack",),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key="rack",
                domain_exclusive=False,
            ),
        )
    }
    rack_insts = [
        _run_node(
            "gpu",
            "ok0",
            {"CPU": 4, "GPU": 1},
            {"rack": "rack-0"},
            idle_ms=600_000,
            lid="L0",
        ),
        _run_node(
            "gpu",
            "ok1",
            {"CPU": 4, "GPU": 1},
            {"rack": "rack-0"},
            idle_ms=600_000,
            lid="L0",
        ),
        _run_node(
            "gpu",
            "sp0",
            {"CPU": 4, "GPU": 1},
            {"rack": "rack-a"},
            idle_ms=120_000,
            lid="L1",
        ),
        _run_node(
            "gpu",
            "sp1",
            {"CPU": 4, "GPU": 1},
            {"rack": "rack-b"},
            idle_ms=120_000,
            lid="L1",
        ),
    ]
    rep_split = _sched(gpu_cfgs, rack_insts, idle_s=60.0)
    assert sorted(t.instance_id for t in rep_split.to_terminate) == ["sp0", "sp1"]


def test_nonprovider_shared_domain_cross_rack_replica_no_phantom_placeholders():
    """A non-provider shared-domain replica whose hosts register across two racks
    does not fabricate per-rack phantom placeholders or fake min_worker_nodes capacity."""

    def _gpu_cfg(min_w: int, max_w: int = 8) -> Dict[str, NodeTypeConfig]:
        return {
            "gpu": NodeTypeConfig(
                name="gpu",
                min_worker_nodes=min_w,
                max_worker_nodes=max_w,
                resources={"CPU": 4, "GPU": 1},
                runtime_label_keys=("rack",),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=2,
                    domain_label_key="rack",
                    domain_exclusive=False,
                ),
            )
        }

    for lid in ("L1", None):
        split_busy = [
            _run_node(
                "gpu",
                n,
                {"CPU": 4, "GPU": 1},
                {"rack": r},
                used={"GPU": 1},
                lid=lid,
            )
            for n, r in (("A", "rack-a"), ("B", "rack-b"))
        ]
        rep_tasks = _sched(
            _gpu_cfg(0),
            split_busy,
            reqs=[_resource_request({"GPU": 1}, count=2)],
        )
        assert rep_tasks.infeasible_resource_requests == []
        assert _launch_and_terminate(rep_tasks) == ({"gpu": 2}, [])

        rep_min = _sched(_gpu_cfg(4), split_busy)
        assert _launch_and_terminate(rep_min) == ({"gpu": 2}, [])

        g_rack = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 2, "_pg_rk"))
        rep_gang = _sched(_gpu_cfg(0, max_w=4), split_busy, gangs=[g_rack])
        assert rep_gang.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_gang) == ({"gpu": 2}, [])


def test_exclusive_boot_batch_rejects_oversized_gang():
    """On an exclusive 2-host group, 1 unlabeled running + 3 installing nodes on
    a shared launch ID split into two 2-host synthetic slices and reject a 3-host gang."""
    tpu2 = NodeTypeConfig(
        name="tpu2",
        resources={"CPU": 4, "TPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=4,
        runtime_label_keys=(_SLICE, _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=_SLICE,
            domain_exclusive=True,
            worker_index_label_key=_WID,
        ),
    )
    u0 = _run_node("tpu2", "u0", {"CPU": 4, "TPU": 4}, {}, lid="lid-shared")
    p3 = [
        _pending_node("tpu2", f"p{i}", Instance.RAY_INSTALLING, lid="lid-shared")
        for i in range(3)
    ]
    g3_ex = _strict_pack_gang(_SLICE, _spread_reqs({"CPU": 4, "TPU": 4}, 3, "_pg_ex3"))
    assert (
        len(
            _sched(
                {"tpu2": tpu2}, [u0] + p3, gangs=[g3_ex]
            ).infeasible_gang_resource_requests
        )
        == 1
    )


def test_shared_domain_boot_overflow_isolated_across_cliques():
    """Shared-domain booting pods beyond an already-complete first replica stay
    unlabeled instead of inheriting the first replica's clique/rack labels."""
    clique_key = "ray.io/gpu-clique"
    rack_key = "ray.io/rack"
    mw = NodeTypeConfig(
        name="mw",
        resources={"CPU": 4, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=6,
        runtime_label_keys=(rack_key, clique_key),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=clique_key,
            domain_exclusive=False,
        ),
    )
    a0 = _run_node(
        "mw",
        "a0",
        {"CPU": 4, "GPU": 4},
        {clique_key: "c0", rack_key: "r0"},
        used={"GPU": 4},
        lid="L1",
    )
    a1 = _run_node(
        "mw",
        "a1",
        {"CPU": 4, "GPU": 4},
        {clique_key: "c0", rack_key: "r0"},
        used={"GPU": 4},
        lid="L1",
    )
    b0 = _pending_node("mw", "b0", Instance.ALLOCATED, lid="L1")
    b1 = _pending_node("mw", "b1", Instance.ALLOCATED, lid="L1")
    x0 = _run_node(
        "mw", "x0", {"CPU": 4, "GPU": 4}, {clique_key: "c1", rack_key: "r1"}, lid="L2"
    )
    x1 = _run_node(
        "mw",
        "x1",
        {"CPU": 4, "GPU": 4},
        {clique_key: "c1", rack_key: "r1"},
        used={"GPU": 4},
        lid="L2",
    )
    g2_c1 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_c1"),
        loc_constraints=[(clique_key, _IN, ["c1"])],
    )
    assert (
        _sched(
            {"mw": mw}, [a0, a1, b0, b1, x0, x1], gangs=[g2_c1]
        ).infeasible_gang_resource_requests
        == []
    )

    mw4 = NodeTypeConfig(
        name="mw",
        resources={"CPU": 4, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=4,
        runtime_label_keys=(rack_key, clique_key),
        replica_config=mw.replica_config,
    )
    g2_c0 = _strict_pack_gang(
        clique_key,
        _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_c0"),
        loc_constraints=[(clique_key, _IN, ["c0"])],
    )
    assert (
        len(
            _sched(
                {"mw": mw4}, [a0, a1, b0, b1], gangs=[g2_c0]
            ).infeasible_gang_resource_requests
        )
        == 1
    )

    a0_stop = copy.deepcopy(a0)
    a1_stop = copy.deepcopy(a1)
    a0_stop.im_instance.status = Instance.RAY_STOP_REQUESTED
    a1_stop.im_instance.status = Instance.RAY_STOP_REQUESTED
    g2_unpin = _strict_pack_gang(
        clique_key, _spread_reqs({"CPU": 4, "GPU": 4}, 2, "_pg_u2")
    )
    assert (
        _sched(
            {"mw": mw4}, [a0_stop, a1_stop, b0, b1], gangs=[g2_unpin]
        ).infeasible_gang_resource_requests
        == []
    )

    mw_min4 = NodeTypeConfig(
        name="mw",
        resources={"CPU": 4, "GPU": 4},
        min_worker_nodes=4,
        max_worker_nodes=6,
        runtime_label_keys=(rack_key, clique_key),
        replica_config=mw.replica_config,
    )
    stag3 = [
        _run_node(
            "mw",
            "sa0",
            {"CPU": 4, "GPU": 4},
            {clique_key: "c0", rack_key: "r0"},
            idle_ms=60_000,
            lid="L1",
        ),
        _run_node(
            "mw",
            "sb0",
            {"CPU": 4, "GPU": 4},
            {clique_key: "c0", rack_key: "r0"},
            idle_ms=60_000,
            lid="L1",
        ),
        _run_node(
            "mw",
            "sc0",
            {"CPU": 4, "GPU": 4},
            {clique_key: "c1", rack_key: "r1"},
            used={"GPU": 4},
            lid="L1",
        ),
        _run_node(
            "mw",
            "sc1",
            {"CPU": 4, "GPU": 4},
            {clique_key: "c1", rack_key: "r1"},
            used={"GPU": 4},
            lid="L1",
        ),
        _pending_node("mw", "sa1", Instance.RAY_INSTALLING, lid="L1"),
        _pending_node("mw", "sb1", Instance.RAY_INSTALLING, lid="L1"),
    ]
    assert _sched({"mw": mw_min4}, stag3, idle_s=10.0).to_terminate == []


def test_domainless_stop_requested_propagates_replica_draining():
    """Domainless RAY_STOP_REQUESTED nodes enter Stage 1 and mark incomplete peers draining."""
    mw_domless = NodeTypeConfig(
        name="mw",
        resources={"CPU": 4, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=2,
        replica_config=WorkerReplicaConfig(workers_per_replica=2),
    )
    r0_s = _run_node("mw", "r0", {"CPU": 4, "GPU": 4}, {}, lid="L1")
    r0_s.im_instance.status = Instance.RAY_STOP_REQUESTED
    r1_ok = _run_node("mw", "r1", {"CPU": 4, "GPU": 4}, {}, lid="L1")
    b1_al = _pending_node("mw", "b1", Instance.ALLOCATED, lid="L1")
    for peer in (r1_ok, b1_al):
        rep_d = _sched(
            {"mw": mw_domless},
            [r0_s, peer],
            reqs=[_resource_request({"GPU": 4})],
        )
        assert len(rep_d.infeasible_resource_requests) == 1
        assert rep_d.to_terminate == []


def test_legacy_unattributed_combined_units_expire_and_chunk_cleanly():
    """Legacy unattributed running + pending pools protect incomplete pinned sets,
    reclaim expired complete sets, and chunk multi-replica pools above min_workers."""
    tpu4 = NodeTypeConfig(
        name="tpu4",
        resources={"CPU": 4, "TPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=4,
        runtime_label_keys=(_SLICE, _WID, "rack"),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=4,
            domain_label_key=_SLICE,
            domain_exclusive=True,
            worker_index_label_key=_WID,
        ),
    )
    cpu_helper = NodeTypeConfig(
        name="cpu",
        resources={"CPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=2,
        runtime_label_keys=("rack",),
    )
    s0 = _run_node(
        "tpu4",
        "s0-0",
        {"CPU": 4, "TPU": 4},
        {_SLICE: "slice-0", _WID: "0", "rack": "rack-a"},
        idle_ms=60_000,
    )
    s1 = _run_node(
        "tpu4",
        "s0-1",
        {"CPU": 4, "TPU": 4},
        {_SLICE: "slice-0", _WID: "1", "rack": "rack-a"},
        idle_ms=60_000,
    )
    p2 = _pending_node("tpu4", "p2", Instance.ALLOCATED)
    p3 = _pending_node("tpu4", "p3", Instance.ALLOCATED)
    c0 = _run_node("cpu", "c0", {"CPU": 4}, {"rack": "rack-a"}, idle_ms=60_000)
    g_inf = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"CPU": 16, "TPU": 4}, 4, "_pg_inf4"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )

    sched4 = ResourceDemandScheduler()
    now_t = [100.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        assert (
            _sched(
                {"tpu4": tpu4},
                [s0, s1, p2, p3],
                gangs=[g_inf],
                idle_s=10.0,
                pending_idle_s=10.0,
                sched=sched4,
            ).to_terminate
            == []
        )
        now_t[0] = 120.0
        t2 = _sched(
            {"tpu4": tpu4},
            [s0, s1, p2, p3],
            gangs=[g_inf],
            idle_s=10.0,
            pending_idle_s=10.0,
            sched=sched4,
        )
        assert sorted(t.instance_id for t in t2.to_terminate) == [
            "p2",
            "p3",
            "s0-0",
            "s0-1",
        ]

    # Incomplete 3/4 combined pool (2 running + 1 allocated) protects both itself
    # and a single-host helper sharing pinned rack-a.
    g_rack = _strict_pack_gang(
        "rack",
        _spread_reqs({"CPU": 16, "TPU": 4}, 4, "_pg_rk4"),
        loc_constraints=[("rack", _IN, ["rack-a"])],
    )
    sched3 = ResourceDemandScheduler()
    now_t[0] = 100.0
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        _sched(
            {"tpu4": tpu4, "cpu": cpu_helper},
            [s0, s1, p2, c0],
            gangs=[g_rack],
            idle_s=10.0,
            pending_idle_s=10.0,
            sched=sched3,
        )
        now_t[0] = 120.0
        t3 = _sched(
            {"tpu4": tpu4, "cpu": cpu_helper},
            [s0, s1, p2, c0],
            gangs=[g_rack],
            idle_s=10.0,
            pending_idle_s=10.0,
            sched=sched3,
        )
        assert t3.to_terminate == []

    # Multi-replica combined pool (1 idle running + 3 expired ALLOCATED, size=2, min=2)
    # chunks into two 2-node units and reclaims the excess 2 pending nodes.
    tpu2_min = NodeTypeConfig(
        name="tpu2",
        resources={"CPU": 4, "TPU": 4},
        min_worker_nodes=2,
        max_worker_nodes=4,
        runtime_label_keys=(_SLICE, _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key=_SLICE,
            domain_exclusive=True,
            worker_index_label_key=_WID,
        ),
    )
    r_idle = _run_node(
        "tpu2", "r0", {"CPU": 4, "TPU": 4}, {_SLICE: "s0", _WID: "0"}, idle_ms=120_000
    )
    u_pend = [_pending_node("tpu2", f"up{i}", Instance.ALLOCATED) for i in range(3)]
    rep_chunk = _sched(
        {"tpu2": tpu2_min},
        [r_idle] + u_pend,
        idle_s=60.0,
        pending_idle_s=10.0,
        unneeded={"tpu2": [0.0, 0.0, 0.0]},
    )
    assert len(rep_chunk.to_terminate) == 2


def test_shared_domain_uneven_worker_indices_balance_via_schedule():
    """Shared-domain multi-replica groups with duplicate existing worker indices
    assign balanced missing indices and head resources via public schedule()."""
    head_res = "head-4"
    shared4 = NodeTypeConfig(
        name="mw4",
        resources={"CPU": 4, "TPU": 4, head_res: 1},
        min_worker_nodes=0,
        max_worker_nodes=8,
        runtime_label_keys=("clique", _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=4,
            domain_label_key="clique",
            domain_exclusive=False,
            worker_index_label_key=_WID,
            head_resources=(head_res,),
        ),
    )
    # Two running hosts in clique c0 both carry wid=1 (missing 2x wid=0,2,3 + 2 heads)
    # alongside 6 booting pods on the same launch batch L1.
    existing = [
        _run_node(
            "mw4",
            f"e{i}",
            {"CPU": 4, "TPU": 4},
            {"clique": "c0", _WID: "1"},
            lid="L1",
        )
        for i in range(2)
    ]
    boot6 = [
        _pending_node("mw4", f"b{i}", Instance.ALLOCATED, lid="L1") for i in range(6)
    ]
    reqs = [
        _mk_req(
            {"TPU": 4, **({head_res: 1} if w == 0 else {})},
            label_selectors=[
                [(k, _IN, [v]) for k, v in (("clique", "c0"), (_WID, str(w)))]
            ],
        )
        for _ in range(2)
        for w in range(4)
    ]
    rep = _sched(
        {"mw4": shared4},
        existing + boot6,
        gangs=[
            _strict_pack_gang("clique", reqs[:4]),
            _strict_pack_gang("clique", reqs[4:]),
        ],
    )
    assert rep.infeasible_gang_resource_requests == []
    assert rep.to_launch == []


def test_kuberay_domainless_multihost_cr_seam():
    """Unmocked KubeRay CR -> AutoscalingConfig -> ResourceDemandScheduler seam for
    a non-TPU numOfHosts=2 GPU worker group with labelRefs and default domain_exclusive."""
    from ray.autoscaler._private.kuberay.autoscaling_config import (
        _derive_autoscaling_config_from_ray_cr,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig

    rack_key = "ray.io/rack"
    ray_cr = {
        "metadata": {"name": "seam-test", "namespace": "default"},
        "spec": {
            "headGroupSpec": {
                "rayStartParams": {},
                "template": {
                    "spec": {"containers": [{"resources": {"limits": {"cpu": "1"}}}]}
                },
            },
            "workerGroupSpecs": [
                {
                    "groupName": "gpu-2h",
                    "replicas": 2,
                    "minReplicas": 1,
                    "maxReplicas": 2,
                    "numOfHosts": 2,
                    "labelRefs": [
                        {
                            "valueFrom": {
                                "nodeRef": {
                                    "fieldPath": f"metadata.labels['{rack_key}']"
                                }
                            }
                        }
                    ],
                    "rayStartParams": {},
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        "limits": {"cpu": "4", "nvidia.com/gpu": "1"}
                                    }
                                }
                            ]
                        }
                    },
                },
                {
                    "groupName": "gpu-other",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 2,
                    "numOfHosts": 1,
                    "labelRefs": [
                        {
                            "valueFrom": {
                                "nodeRef": {
                                    "fieldPath": f"metadata.labels['{rack_key}']"
                                }
                            }
                        }
                    ],
                    "rayStartParams": {},
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        "limits": {"cpu": "4", "nvidia.com/gpu": "1"}
                                    }
                                }
                            ]
                        }
                    },
                },
            ],
        },
    }
    acfg = AutoscalingConfig(
        _derive_autoscaling_config_from_ray_cr(ray_cr), skip_content_hash=True
    )
    nt_cfgs = acfg.get_node_type_configs()
    gpu_res = {"CPU": 4, "GPU": 1}

    # (a) 3 busy running in rack-0 + 1 ALLOCATED b3 on L1: b3 inherits rack-0 and
    # rejects cross-rack stealing by a gang pinned to rack-1.
    run3_busy = [
        _run_node(
            "gpu-2h", f"g{i}", gpu_res, {rack_key: "rack-0"}, used={"GPU": 1}, lid="L1"
        )
        for i in range(3)
    ]
    b3 = _pending_node("gpu-2h", "b3", Instance.ALLOCATED, lid="L1")
    r1_nodes = [
        _run_node("gpu-other", "r1-free", gpu_res, {rack_key: "rack-1"}, lid="L2"),
        _run_node(
            "gpu-other",
            "r1-busy",
            gpu_res,
            {rack_key: "rack-1"},
            used={"GPU": 1},
            lid="L2",
        ),
    ]
    g_r1 = _strict_pack_gang(
        rack_key,
        _spread_reqs({"GPU": 1}, 2, "_pg_r1"),
        loc_constraints=[(rack_key, _IN, ["rack-1"])],
    )
    req_a = sched_request(
        {k: nt_cfgs[k] for k in ("gpu-2h", "gpu-other")},
        max_num_nodes=6,
        instances=run3_busy + [b3] + r1_nodes,
        disable_launch_config_check=True,
    )
    req_a.gang_resource_requests.append(g_r1)
    assert (
        len(ResourceDemandScheduler().schedule(req_a).infeasible_gang_resource_requests)
        == 1
    )

    # (b) 3 busy running in rack-0 with vanished 4th pod emits 1 rack-0 placeholder.
    g_r0 = _strict_pack_gang(
        rack_key,
        [_mk_req({"GPU": 1})],
        loc_constraints=[(rack_key, _IN, ["rack-0"])],
    )
    req_b = sched_request(
        {"gpu-2h": nt_cfgs["gpu-2h"]},
        instances=run3_busy,
        disable_launch_config_check=True,
    )
    req_b.gang_resource_requests.append(g_r0)
    assert (
        ResourceDemandScheduler().schedule(req_b).infeasible_gang_resource_requests
        == []
    )

    # (c) 3 idle running + 1 expired ALLOCATED on L1 reclaims 2 excess nodes down to min=2.
    run3_idle = [
        _run_node(
            "gpu-2h", f"gi{i}", gpu_res, {rack_key: "rack-0"}, idle_ms=120_000, lid="L1"
        )
        for i in range(3)
    ]
    sched_c = ResourceDemandScheduler()
    sched_c._pending_unneeded_since_s = {"gpu-2h": [0.0]}
    req_c = sched_request(
        {"gpu-2h": nt_cfgs["gpu-2h"]},
        instances=run3_idle + [b3],
        idle_timeout_s=60.0,
        pending_idle_timeout_s=10.0,
        disable_launch_config_check=True,
    )
    assert len(sched_c.schedule(req_c).to_terminate) == 2


def test_missing_runtime_domain_label_suppresses_placeholders_and_protects_busy_hosts():
    """Running exclusive-domain hosts missing their runtime domain label never
    fabricate per-host placeholders, and max-worker checks never kill real hosts
    because of in-memory placeholders."""
    for max_w in (4, 8):
        cfgs = {
            "tpu": NodeTypeConfig(
                name="tpu",
                min_worker_nodes=0,
                max_worker_nodes=max_w,
                resources={"CPU": 8, "TPU": 4},
                runtime_label_keys=(_SLICE, _WID),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=_SLICE,
                    domain_exclusive=True,
                    worker_index_label_key=_WID,
                ),
            )
        }
        unlabeled_busy = [
            _run_node(
                "tpu", f"s{w}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L1"
            )
            for w in range(3)
        ]
        rep = _sched(cfgs, unlabeled_busy, max_nodes=max_w)
        assert _launch_and_terminate(rep) == ({}, [])

    # With headroom (max_w=8), a pending task launches a real replica rather than
    # landing on a phantom placeholder.
    cfgs8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    unlabeled_busy3 = [
        _run_node("tpu", f"s{w}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L1")
        for w in range(3)
    ]
    rep_launch = _sched(cfgs8, unlabeled_busy3, reqs=[_resource_request({"TPU": 4})])
    assert _launch_and_terminate(rep_launch) == ({"tpu": 5}, [])


def test_cross_slice_launch_batch_boot_completes_slices_without_termination():
    """Concurrent multi-replica boots without provider replica IDs attach still-booting
    pods across incomplete slices when a launch batch spans physical slices."""
    for max_w in (4, 6):
        cfgs = {
            "tpu": NodeTypeConfig(
                name="tpu",
                min_worker_nodes=0,
                max_worker_nodes=max_w,
                resources={"CPU": 8, "TPU": 4},
                runtime_label_keys=(_SLICE, _WID),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=2,
                    domain_label_key=_SLICE,
                    domain_exclusive=True,
                    worker_index_label_key=_WID,
                ),
            )
        }
        g1_pinned = _strict_pack_gang(
            _SLICE,
            _spread_reqs({"TPU": 4}, 2, "_pg_cs1p"),
            loc_constraints=[(_SLICE, _IN, ["slice-0"])],
        )
        g2_pinned = _strict_pack_gang(
            _SLICE,
            _spread_reqs({"TPU": 4}, 2, "_pg_cs2p"),
            loc_constraints=[(_SLICE, _IN, ["slice-1"])],
        )

        # Case 1: batch A spans incomplete slice-0 and slice-1; batch B's 2 ALLOCATED
        # pods complete both pinned slices via fill_all_open=True.
        insts_span = [
            _run_node(
                "tpu",
                "a",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "slice-0", _WID: "0"},
                lid="A",
            ),
            _run_node(
                "tpu",
                "b",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "slice-1", _WID: "1"},
                lid="A",
            ),
            _pending_node("tpu", "c", Instance.ALLOCATED, lid="B"),
            _pending_node("tpu", "d", Instance.ALLOCATED, lid="B"),
        ]
        rep1 = _sched(cfgs, insts_span, gangs=[g1_pinned, g2_pinned], max_nodes=max_w)
        assert rep1.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep1) == ({}, [])

        # Case 2: slice-0 has 1 pod from A and 1 pod from B; remaining A + B pods
        # pool into one complete 2-host pending slice instead of two 1-host fragments.
        g1 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_cs1"))
        g2 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_cs2"))
        insts_mixed = [
            _run_node(
                "tpu",
                "b",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "slice-0", _WID: "0"},
                lid="A",
            ),
            _run_node(
                "tpu",
                "d",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "slice-0", _WID: "1"},
                lid="B",
            ),
            _pending_node("tpu", "a", Instance.ALLOCATED, lid="A"),
            _pending_node("tpu", "c", Instance.ALLOCATED, lid="B"),
        ]
        rep2 = _sched(cfgs, insts_mixed, gangs=[g1, g2], max_nodes=max_w)
        assert rep2.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep2) == ({}, [])

    # Case 3: completed historical multi-slice launches (4/4 on slice-0 and 3/4 busy
    # on slice-1) never cannibalize a fresh 4-pod ALLOCATED batch.
    cfgs4 = {
        "tpu4": NodeTypeConfig(
            name="tpu4",
            min_worker_nodes=0,
            max_worker_nodes=12,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    s0_full = [
        _run_node(
            "tpu4",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L_hist1" if w < 3 else "L_hist2",
        )
        for w in range(4)
    ]
    s1_deg = [
        _run_node(
            "tpu4",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            lid="L_hist1",
        )
        for w in range(3)
    ]
    fresh4 = [
        _pending_node("tpu4", f"n{w}", Instance.ALLOCATED, lid="L_new")
        for w in range(4)
    ]
    g4_new = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_new4"))
    rep3 = _sched(cfgs4, s0_full + s1_deg + fresh4, gangs=[g4_new])
    assert rep3.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep3) == ({}, [])


def test_multi_slice_refill_bounds_and_fresh_replica_co_launch():
    """Multi-slice refill caps per-slice nodes and head_resources at workers_per_replica,
    skips outdated TO_TERMINATE slices, and co-launches refill + fresh replica when
    requests require a fresh replica alongside an unbacked refill gap."""
    head_res = "TPU-v6e-16-head"

    def _excl_cfg(max_w: int, hash_val: str = "") -> Dict[str, NodeTypeConfig]:
        return {
            "tpu": NodeTypeConfig(
                name="tpu",
                min_worker_nodes=0,
                max_worker_nodes=max_w,
                resources={"CPU": 8, "TPU": 4, head_res: 1},
                launch_config_hash=hash_val,
                runtime_label_keys=(_SLICE, _WID, "rack"),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=_SLICE,
                    domain_exclusive=True,
                    worker_index_label_key=_WID,
                    head_resources=(head_res,),
                ),
            )
        }

    dead_a = _terminated_teardown_row("tpu", "dead-a", "L0")
    dead_b = _terminated_teardown_row("tpu", "dead-b", "L1")

    # (a) 2/4 in slice-0 + 3/4 in slice-1 (refill_gap=3) under max_worker_nodes=8.
    s0_busy_mix = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4} if w == 0 else None,
            lid="L0",
        )
        for w in (0, 1)
    ]
    s1_busy_mix = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4} if w == 1 else None,
            lid="L1",
        )
        for w in (1, 2, 3)
    ]
    g4_a = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_ms1"))
    g4_b = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_ms2"))
    rep_imp4 = _sched(
        _excl_cfg(8), s0_busy_mix + s1_busy_mix + [dead_a, dead_b], gangs=[g4_a]
    )
    assert len(rep_imp4.infeasible_gang_resource_requests) == 1
    assert rep_imp4.to_launch == []

    s0_idle = [
        _run_node(
            "tpu",
            f"i0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L0",
        )
        for w in (1, 2)
    ]
    s1_idle = [
        _run_node(
            "tpu",
            f"i1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            lid="L1",
        )
        for w in (1, 2, 3)
    ]
    for over_cnt in (5, 6):
        g_over = _strict_pack_gang(
            _SLICE, _spread_reqs({"TPU": 4}, over_cnt, f"_pg_ov{over_cnt}")
        )
        rep_over = _sched(
            _excl_cfg(8), s0_idle + s1_idle + [dead_a, dead_b], gangs=[g_over]
        )
        assert len(rep_over.infeasible_gang_resource_requests) == 1
        assert rep_over.to_launch == []

    g_two_heads = _strict_pack_gang(
        _SLICE, [_mk_req({head_res: 1}), _mk_req({head_res: 1})]
    )
    rep_2h = _sched(
        _excl_cfg(8), s0_idle + s1_idle + [dead_a, dead_b], gangs=[g_two_heads]
    )
    assert len(rep_2h.infeasible_gang_resource_requests) == 1

    rep_two_g4 = _sched(
        _excl_cfg(8), s0_idle + s1_idle + [dead_a, dead_b], gangs=[g4_a, g4_b]
    )
    assert rep_two_g4.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_two_g4) == ({"tpu": 3}, [])
    assert rep_two_g4.cluster_resources["TPU"] == 32.0
    assert rep_two_g4.cluster_resources[head_res] == 2.0

    # Outdated-hash slice-0 terminated in Step 1 is skipped by make_refill_nodes.
    s0_stale = [
        _run_node(
            "tpu",
            f"st0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L0",
        )
        for w in (1, 2)
    ]
    for inst in s0_stale:
        inst.im_instance.launch_config_hash = "old-hash"
    s1_curr = [
        _run_node(
            "tpu",
            f"cu1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            lid="L1",
        )
        for w in (1, 2, 3)
    ]
    for inst in s1_curr:
        inst.im_instance.launch_config_hash = "new-hash"
    hash_cfgs = _excl_cfg(8, hash_val="new-hash")
    hash_insts = s0_stale + s1_curr + [dead_a, dead_b]
    req_hash = sched_request(
        hash_cfgs,
        instances=hash_insts,
        disable_launch_config_check=False,
    )
    req_hash.replica_view = _synthetic_replica_view(hash_cfgs, hash_insts)
    req_hash.gang_resource_requests.append(g4_a)
    rep_hash = ResourceDemandScheduler().schedule(req_hash)
    assert rep_hash.infeasible_gang_resource_requests == []
    assert sorted(t.instance_id for t in rep_hash.to_terminate) == ["st0-1", "st0-2"]

    # (b) 3/4 busy survivors (worker 3 died, workers 0..2 busy in rack-0): requests
    # needing head_resources, worker-id="0", or rack!="rack-0" co-launch refill + fresh
    # replica at max_worker_nodes=8 (count=8), while failing cleanly at max_worker_nodes=4.
    busy3 = [
        _run_node(
            "tpu",
            f"b{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w), "rack": "rack-0"},
            used={"TPU": 4, **({head_res: 1} if w == 0 else {})},
            lid="L0",
        )
        for w in range(3)
    ]
    rep_head_at4 = _sched(
        _excl_cfg(4), busy3 + [dead_a], reqs=[_resource_request({head_res: 1})]
    )
    assert len(rep_head_at4.infeasible_resource_requests) == 1
    assert rep_head_at4.to_launch == []

    for single_req in (
        _resource_request({head_res: 1}),
        _resource_request({"TPU": 4}, sel=[[(_WID, _IN, ["0"])]]),
        _resource_request({"TPU": 4}, sel=[[("rack", _NOT_IN, ["rack-0"])]]),
    ):
        rep_co = _sched(_excl_cfg(8), busy3 + [dead_a], reqs=[single_req])
        assert rep_co.infeasible_resource_requests == []
        assert _launch_and_terminate(rep_co) == ({"tpu": 5}, [])
        assert rep_co.cluster_resources["TPU"] == 32.0

    rep_gang_co = _sched(_excl_cfg(8), busy3 + [dead_a], gangs=[g4_a])
    assert rep_gang_co.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_gang_co) == ({"tpu": 5}, [])

    # (c) Shared-domain and domainless labelRefs 4-host STRICT_PACK gang refills 1 pod
    # at max_worker_nodes=4, and co-launches refill + fresh replica on rack NOT_IN at 8.
    for dom_k in ("rack", None):
        sh_cfg4 = {
            "gpu": NodeTypeConfig(
                name="gpu",
                min_worker_nodes=0,
                max_worker_nodes=4,
                resources={"CPU": 4, "GPU": 1},
                runtime_label_keys=("rack",),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=dom_k,
                    domain_exclusive=False,
                ),
            )
        }
        sh_idle3 = [
            _run_node(
                "gpu", f"g{w}", {"CPU": 4, "GPU": 1}, {"rack": "rack-0"}, lid="L0"
            )
            for w in range(3)
        ]
        dead_g = _terminated_teardown_row("gpu", "dead-g", "L0")
        g_rack = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 4, "_pg_rk4"))
        rep_sh4 = _sched(sh_cfg4, sh_idle3 + [dead_g], gangs=[g_rack])
        assert rep_sh4.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_sh4) == ({"gpu": 1}, [])

        sh_cfg8 = {
            "gpu": NodeTypeConfig(
                name="gpu",
                min_worker_nodes=0,
                max_worker_nodes=8,
                resources={"CPU": 4, "GPU": 1},
                runtime_label_keys=("rack",),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=dom_k,
                    domain_exclusive=False,
                ),
            )
        }
        g_not_r0 = _strict_pack_gang(
            "rack",
            _spread_reqs({"GPU": 1}, 4, "_pg_nr0"),
            loc_constraints=[("rack", _NOT_IN, ["rack-0"])],
        )
        rep_sh8 = _sched(sh_cfg8, sh_idle3 + [dead_g], gangs=[g_not_r0])
        assert rep_sh8.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_sh8) == ({"gpu": 5}, [])


def test_inflight_unlabeled_refill_attaches_across_boot_ticks_and_torndown_boot_survivor():
    """In-flight unlabeled refill pods on a new launch_request_id attach to torn-down
    replicas across QUEUED, REQUESTED, ALLOCATED, and RAY_INSTALLING without premature
    idle kills or phantom missing_backfill, and torn-down booting survivors refill at max."""
    head_res = "TPU-v6e-16-head"

    def _cfg(min_w: int, max_w: int) -> Dict[str, NodeTypeConfig]:
        return {
            "tpu": NodeTypeConfig(
                name="tpu",
                min_worker_nodes=min_w,
                max_worker_nodes=max_w,
                resources={"CPU": 8, "TPU": 4, head_res: 1},
                runtime_label_keys=(_SLICE, _WID),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=_SLICE,
                    domain_exclusive=True,
                    worker_index_label_key=_WID,
                    head_resources=(head_res,),
                ),
            )
        }

    dead = _terminated_teardown_row("tpu", "dead-0", "L1")
    idle_survivors = [
        _run_node(
            "tpu",
            f"s{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            idle_ms=600_000,
            lid="L1",
        )
        for w in (1, 2, 3)
    ]
    g4_head = _strict_pack_gang(
        _SLICE,
        [
            _mk_req({"TPU": 4, head_res: 1}, label_selectors=[[(_WID, _IN, ["0"])]]),
            *_spread_reqs({"TPU": 4}, 3, "_pg_rf"),
        ],
    )
    for st in (
        Instance.QUEUED,
        Instance.REQUESTED,
        Instance.ALLOCATED,
        Instance.RAY_INSTALLING,
    ):
        u4 = _pending_node("tpu", "u4", st, lid="L2")
        if st in (Instance.ALLOCATED, Instance.RAY_INSTALLING):
            u4.cloud_instance_id = "cloud-u4"
            u4.im_instance.cloud_instance_id = "cloud-u4"
        rep_g = _sched(
            _cfg(0, 4),
            idle_survivors + [dead, u4],
            gangs=[g4_head],
            idle_s=60.0,
            pending_idle_s=300.0,
        )
        assert rep_g.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_g) == ({}, [])

        # Even with zero demand, idle survivors are held while u4 is newly booting.
        rep_hold = _sched(
            _cfg(0, 4),
            idle_survivors + [dead, u4],
            idle_s=60.0,
            pending_idle_s=300.0,
        )
        assert _launch_and_terminate(rep_hold) == ({}, [])

    # Once u4's pending_idle_timeout_s also expires on ALLOCATED, all 4 terminate whole.
    u4_alloc = _pending_node("tpu", "u4", Instance.ALLOCATED, lid="L2")
    u4_alloc.cloud_instance_id = "cloud-u4"
    u4_alloc.im_instance.cloud_instance_id = "cloud-u4"
    sched_exp = ResourceDemandScheduler()
    sched_exp._pending_unneeded_since_s = {"tpu": [0.0]}
    exp_cfgs = _cfg(0, 4)
    exp_insts = idle_survivors + [dead, u4_alloc]
    req_exp = sched_request(
        exp_cfgs,
        instances=exp_insts,
        idle_timeout_s=60.0,
        pending_idle_timeout_s=10.0,
        disable_launch_config_check=True,
    )
    req_exp.replica_view = _synthetic_replica_view(exp_cfgs, exp_insts)
    with patch("ray.autoscaler.v2.scheduler.time.monotonic", return_value=100.0):
        rep_all_idle = sched_exp.schedule(req_exp)
    assert sorted(t.instance_id for t in rep_all_idle.to_terminate) == [
        "s1",
        "s2",
        "s3",
        "u4",
    ]

    # Busy survivors + ALLOCATED u4 on L2 at max_worker_nodes=8 incur zero phantom
    # missing_backfill and allow launching a 2nd 4-host slice immediately.
    busy_survivors = [
        _run_node(
            "tpu",
            f"bs{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L1",
        )
        for w in (1, 2, 3)
    ]
    u4b = _pending_node("tpu", "u4b", Instance.ALLOCATED, lid="L2")
    u4b.cloud_instance_id = "cloud-u4b"
    u4b.im_instance.cloud_instance_id = "cloud-u4b"
    g4_second = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_s2"))
    rep_2nd = _sched(
        _cfg(0, 8),
        busy_survivors + [dead, u4b],
        gangs=[g4_second],
    )
    assert rep_2nd.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_2nd) == ({"tpu": 4}, [])

    # 1 ALLOCATED survivor + TERMINATED sibling on torn-down L_td at min=4, max=4
    # registers no_backfill=True and immediately launches the 3 missing slots.
    a0 = _pending_node("tpu", "a0", Instance.ALLOCATED, lid="L_td")
    a0.cloud_instance_id = "cloud-a0"
    a0.im_instance.cloud_instance_id = "cloud-a0"
    dead_td = _terminated_teardown_row("tpu", "dead-td", "L_td")
    rep_td_boot = _sched(_cfg(4, 4), [a0, dead_td])
    assert _launch_and_terminate(rep_td_boot) == ({"tpu": 3}, [])


def test_duplicate_worker_indices_preserve_head_slot_and_cap_placeholders_and_max_workers():
    """Duplicate worker_index_label_key values preserve slot 0 and head_resources on
    total_target <= size, cap placeholders at max_worker_nodes, and max-worker checks
    ignore legitimate backfill placeholders."""
    head_res = "TPU-v6e-16-head"
    excl_cfg = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=4,
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    dup_run = [
        _run_node(
            "tpu",
            f"d{i}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: "1"},
            lid="L1",
        )
        for i in range(2)
    ]
    alloc2 = [
        _pending_node("tpu", f"p{i}", Instance.ALLOCATED, lid="L1") for i in range(2)
    ]
    g_head0 = _strict_pack_gang(
        _SLICE,
        [
            _mk_req({"TPU": 4, head_res: 1}, label_selectors=[[(_WID, _IN, ["0"])]]),
            *_spread_reqs({"TPU": 4}, 3, "_pg_dup"),
        ],
    )
    rep_dup = _sched(excl_cfg, dup_run + alloc2, gangs=[g_head0])
    assert rep_dup.infeasible_gang_resource_requests == []

    # Shared domain with 4 real pods (2 busy + 2 free, duplicate wid="1") at max_w=4
    # must not emit 4 phantom placeholders above max_worker_nodes.
    shared_cfg = {
        "gpu": NodeTypeConfig(
            name="gpu",
            min_worker_nodes=0,
            max_worker_nodes=4,
            resources={"CPU": 4, "GPU": 1},
            runtime_label_keys=("rack", _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key="rack",
                domain_exclusive=False,
                worker_index_label_key=_WID,
            ),
        )
    }
    sh_dup4 = [
        _run_node(
            "gpu",
            f"g{i}",
            {"CPU": 4, "GPU": 1},
            {"rack": "rack-0", _WID: "1"},
            used={"GPU": 1} if i < 2 else None,
            lid="L1",
        )
        for i in range(4)
    ]
    g_gpu4 = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 4, "_pg_shdup"))
    rep_sh_dup = _sched(shared_cfg, sh_dup4, gangs=[g_gpu4])
    assert len(rep_sh_dup.infeasible_gang_resource_requests) == 1

    # Per-type max_worker_nodes=4 (with 1 RAY_STOP_REQUESTED + 3/4 busy R0 + 1 ph)
    # and global max_num_nodes=5 (head + two 2/4 busy slices + 4 phs at max_w=8).
    leaving = _run_node(
        "tpu",
        "stop-0",
        {"CPU": 8, "TPU": 4},
        {_SLICE: "old-slice", _WID: "0"},
        rep="R_old",
        lid="L_old",
    )
    leaving.im_instance.status = Instance.RAY_STOP_REQUESTED
    r0_3busy = [
        _run_node(
            "tpu",
            f"r0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            rep="R0",
            lid="L0",
        )
        for w in (1, 2, 3)
    ]
    rep_m9 = _sched(excl_cfg, [leaving] + r0_3busy)
    assert rep_m9.to_terminate == []

    excl_cfg8 = {
        "head": NodeTypeConfig(
            name="head", min_worker_nodes=0, max_worker_nodes=0, resources={"CPU": 2}
        ),
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
    }
    head_inst = make_autoscaler_instance(
        im_instance=Instance(
            instance_type="head",
            status=Instance.RAY_RUNNING,
            instance_id="h0",
            node_id=b"h0",
            node_kind=NodeKind.HEAD,
        ),
        ray_node=NodeState(
            node_id=b"h0",
            ray_node_type_name="head",
            total_resources={"CPU": 2},
            available_resources={"CPU": 2},
            status=NodeStatus.IDLE,
        ),
        cloud_instance_id="cloud-h0",
    )
    two_2of4 = [
        _run_node(
            "tpu",
            f"r{r}-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: f"slice-{r}", _WID: str(w)},
            used={"TPU": 4},
            rep=f"R{r}",
            lid=f"L{r}",
        )
        for r in (0, 1)
        for w in (0, 1)
    ]
    rep_m10 = _sched(excl_cfg8, [head_inst] + two_2of4, max_nodes=5)
    assert rep_m10.to_terminate == []


def test_scale_from_zero_torndown_boot_survivor_and_refill_uuid_order():
    """Scale-from-zero torn-down booting survivors refill cleanly for pending gangs across
    exclusive, shared, and domainless modes, attach in-flight refill batches regardless of
    UUID sort order, and fill staggered multi-slice refills without solo idle kills."""
    head_res = "TPU-v6e-16-head"

    # Scale-from-zero min=0, max=4 with 1 ALLOCATED survivor + TERMINATED sibling
    # on L_td schedules a 4-host STRICT_PACK gang across exclusive, shared, and domainless
    # configs, while rejecting a 5-host gang at max_worker_nodes=4.
    for loc_key, dom_key, is_excl in (
        (_SLICE, _SLICE, True),
        ("rack", "rack", False),
        ("rack", None, False),
    ):
        cfgs = {
            "acc": NodeTypeConfig(
                name="acc",
                min_worker_nodes=0,
                max_worker_nodes=4,
                resources={"CPU": 8, "TPU": 4, head_res: 1},
                runtime_label_keys=tuple({loc_key, _WID}),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=dom_key,
                    domain_exclusive=is_excl,
                    worker_index_label_key=_WID,
                    head_resources=(head_res,),
                ),
            )
        }
        a0 = _pending_node("acc", "a0", Instance.ALLOCATED, lid="L_td")
        a0.cloud_instance_id = "cloud-a0"
        a0.im_instance.cloud_instance_id = "cloud-a0"
        dead_td = _terminated_teardown_row("acc", "dead-td", "L_td")

        g4 = _strict_pack_gang(
            loc_key,
            [
                _mk_req({"TPU": 4, head_res: 1}),
                *_spread_reqs({"TPU": 4}, 3, f"_pg_sfz_{dom_key}_{is_excl}"),
            ],
        )
        rep_ok = _sched(cfgs, [a0, dead_td], gangs=[g4])
        assert rep_ok.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_ok) == ({"acc": 3}, [])
        assert rep_ok.cluster_resources["TPU"] == 16.0
        assert rep_ok.cluster_resources[head_res] == 1.0

        g5 = _strict_pack_gang(
            loc_key, _spread_reqs({"TPU": 4}, 5, f"_pg_sfz5_{dom_key}_{is_excl}")
        )
        rep_bad = _sched(cfgs, [a0, dead_td], gangs=[g5])
        assert len(rep_bad.infeasible_gang_resource_requests) == 1
        assert rep_bad.to_launch == []

    # When refill UUID ("L_a_refill") sorts lexicographically before torn-down
    # UUID ("L_z_td"), the 3 ALLOCATED refill pods still attach to a0's torn-down replica.
    excl_cfg4 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=4,
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    a_surv = _pending_node("tpu", "a-surv", Instance.ALLOCATED, lid="L_z_td")
    a_surv.cloud_instance_id = "cloud-a-surv"
    a_surv.im_instance.cloud_instance_id = "cloud-a-surv"
    dead_z = _terminated_teardown_row("tpu", "dead-z", "L_z_td")
    refill3 = []
    for i in range(3):
        rn = _pending_node("tpu", f"r-{i}", Instance.ALLOCATED, lid="L_a_refill")
        rn.cloud_instance_id = f"cloud-r-{i}"
        rn.im_instance.cloud_instance_id = f"cloud-r-{i}"
        refill3.append(rn)
    g4_slice = _strict_pack_gang(
        _SLICE,
        [
            _mk_req({"TPU": 4, head_res: 1}, label_selectors=[[(_WID, _IN, ["0"])]]),
            *_spread_reqs({"TPU": 4}, 3, "_pg_uuid"),
        ],
    )
    rep_uuid = _sched(excl_cfg4, [a_surv, dead_z] + refill3, gangs=[g4_slice])
    assert rep_uuid.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_uuid) == ({}, [])

    # Staggered 5-pod multi-slice refill on L2 where 1 pod registered into torn-down
    # slice-0 (2/4 -> 3/4) and 4 pods remain booting in REQUESTED / ALLOCATED alongside
    # torn-down slice-1 (1/4 expired idle survivor).
    excl_cfg8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    s0_td = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L0",
        )
        for w in (0, 1)
    ]
    l2_reg = _run_node(
        "tpu",
        "s0-2",
        {"CPU": 8, "TPU": 4},
        {_SLICE: "slice-0", _WID: "2"},
        lid="L2",
    )
    s1_idle_exp = _run_node(
        "tpu",
        "s1-0",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {_SLICE: "slice-1", _WID: "0"},
        idle_ms=600_000,
        lid="L1",
    )
    dead_l0 = _terminated_teardown_row("tpu", "dead-l0", "L0")
    dead_l1 = _terminated_teardown_row("tpu", "dead-l1", "L1")
    for boot_st in (Instance.REQUESTED, Instance.ALLOCATED):
        l2_boot4 = []
        for i in range(4):
            bn = _pending_node("tpu", f"l2-b{i}", boot_st, lid="L2")
            if boot_st == Instance.ALLOCATED:
                bn.cloud_instance_id = f"cloud-l2-b{i}"
                bn.im_instance.cloud_instance_id = f"cloud-l2-b{i}"
            l2_boot4.append(bn)
        base_insts = s0_td + [l2_reg, s1_idle_exp, dead_l0, dead_l1] + l2_boot4

        rep_idle_safe = _sched(excl_cfg8, base_insts, idle_s=60.0, pending_idle_s=300.0)
        assert _launch_and_terminate(rep_idle_safe) == ({}, [])

        g_pin_s0 = _strict_pack_gang(
            _SLICE,
            _spread_reqs({"TPU": 4}, 4, "_pg_ps0"),
            loc_constraints=[(_SLICE, _IN, ["slice-0"])],
        )
        rep_pin = _sched(excl_cfg8, base_insts, gangs=[g_pin_s0])
        assert rep_pin.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_pin) == ({}, [])

        g_two_1 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_2s_a"))
        g_two_2 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_2s_b"))
        rep_two = _sched(excl_cfg8, base_insts, gangs=[g_two_1, g_two_2])
        assert rep_two.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_two) == ({}, [])


def test_multi_rack_refill_isolation_and_multi_slice_gang_headroom():
    """Shared-domain and orthogonal-rack launches isolate cross-domain refill pods,
    preserve split-rack head_resources counts, and account for sibling slice refills
    against cluster max_num_nodes headroom."""
    head_res = "TPU-v6e-16-head"

    # Two incomplete exclusive slices (3/4 idle slice-0 + 3/4 busy slice-1,
    # refill_gap=2) with 1 4-host gang + 1 secondary CPU worker task under max_num_nodes=8
    # vs max_num_nodes=9.
    a7_cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=12,
            resources={"CPU": 4, "TPU": 4, head_res: 1},
            runtime_label_keys=(_SLICE, _WID, "rack"),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        ),
        "cpu_worker": NodeTypeConfig(
            name="cpu_worker",
            min_worker_nodes=0,
            max_worker_nodes=4,
            resources={"CPU": 2, "custom_cpu": 1},
        ),
    }
    s0_idle3 = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 4, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L0",
        )
        for w in range(3)
    ]
    s1_busy3 = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 4, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            lid="L1",
        )
        for w in range(3)
    ]
    dead0 = _terminated_teardown_row("tpu", "d0", "L0")
    dead1 = _terminated_teardown_row("tpu", "d1", "L1")
    g4_tpu = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_a71"))
    cpu_req = _resource_request({"CPU": 2, "custom_cpu": 1})

    rep_cap8 = _sched(
        a7_cfgs,
        s0_idle3 + s1_busy3 + [dead0, dead1],
        reqs=[cpu_req],
        gangs=[g4_tpu],
        max_nodes=8,
    )
    assert rep_cap8.infeasible_gang_resource_requests == []
    assert len(rep_cap8.infeasible_resource_requests) == 1
    assert _launch_and_terminate(rep_cap8) == ({"tpu": 2}, [])
    assert rep_cap8.cluster_resources["TPU"] == 32.0
    assert rep_cap8.cluster_resources[head_res] == 2.0

    rep_cap9 = _sched(
        a7_cfgs,
        s0_idle3 + s1_busy3 + [dead0, dead1],
        reqs=[cpu_req],
        gangs=[g4_tpu],
        max_nodes=9,
    )
    assert rep_cap9.infeasible_gang_resource_requests == []
    assert rep_cap9.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_cap9) == ({"tpu": 2, "cpu_worker": 1}, [])

    # Exclusive TPU slices with orthogonal "rack" runtime label at max_w=8:
    # 3/4 busy in rack-0 + 4/4 (3 busy, 1 free) in rack-1 rejects 2-host rack gang;
    # complete slice-1 split across racks (w=0 with head in rack-0, w=1..3 idle in rack-1)
    # never fabricates a 3rd head token.
    tpu_rack8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            runtime_label_keys=(_SLICE, _WID, "rack"),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    r0_s0_busy3 = [
        _run_node(
            "tpu",
            f"r0s0-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w), "rack": "rack-0"},
            used={"TPU": 4, **({head_res: 1} if w == 0 else {})},
            lid="L0",
        )
        for w in range(3)
    ]
    r1_s1_full = [
        _run_node(
            "tpu",
            f"r1s1-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-1", _WID: str(w), "rack": "rack-1"},
            used={"TPU": 4} if w < 3 else None,
            lid="L1_ok",
        )
        for w in range(4)
    ]
    g2_rack = _strict_pack_gang("rack", _spread_reqs({"TPU": 4}, 2, "_pg_r72_tpu2"))
    rep_tpu_r2 = _sched(tpu_rack8, r0_s0_busy3 + r1_s1_full + [dead0], gangs=[g2_rack])
    assert len(rep_tpu_r2.infeasible_gang_resource_requests) == 1
    assert rep_tpu_r2.to_launch == []

    split_s1_full = [
        _run_node(
            "tpu",
            f"sps1-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {
                _SLICE: "slice-1",
                _WID: str(w),
                "rack": "rack-0" if w == 0 else "rack-1",
            },
            used={"TPU": 4, head_res: 1} if w == 0 else None,
            lid="L1_split",
        )
        for w in range(4)
    ]
    g_rack_head = _strict_pack_gang(
        "rack",
        [_mk_req({"TPU": 4, head_res: 1}), _mk_req({"TPU": 4})],
    )
    rep_split_head = _sched(
        tpu_rack8, r0_s0_busy3 + split_s1_full + [dead0], gangs=[g_rack_head]
    )
    assert len(rep_split_head.infeasible_gang_resource_requests) == 1
    assert rep_split_head.to_launch == []
    assert rep_split_head.cluster_resources[head_res] == 2.0

    # Shared-domain and domainless labelRefs cross-rack refill isolation.
    for dom_k in ("rack", None):

        def _gpu_cfg(max_w: int, dk=dom_k) -> Dict[str, NodeTypeConfig]:
            return {
                "gpu": NodeTypeConfig(
                    name="gpu",
                    min_worker_nodes=0,
                    max_worker_nodes=max_w,
                    resources={"CPU": 4, "GPU": 1},
                    runtime_label_keys=("rack", "ib-fabric"),
                    replica_config=WorkerReplicaConfig(
                        workers_per_replica=4,
                        domain_label_key=dk,
                        domain_exclusive=False,
                    ),
                )
            }

        rack1_4of4 = [
            _run_node(
                "gpu",
                f"rk1-{w}",
                {"CPU": 4, "GPU": 1},
                {"rack": "rack-1", "ib-fabric": "fab-1"},
                used={"GPU": 1} if w < 2 else None,
                lid="L_r1",
            )
            for w in range(4)
        ]
        rack2_3of4_busy = [
            _run_node(
                "gpu",
                f"rk2-{w}",
                {"CPU": 4, "GPU": 1},
                {"rack": "rack-2", "ib-fabric": "fab-2"},
                used={"GPU": 1},
                lid="L_r2",
            )
            for w in range(3)
        ]
        dead_r2 = _terminated_teardown_row("gpu", "dead-r2", "L_r2")

        # 3-GPU gang at max_worker_nodes=8 is rejected (rack-1 is 4/4 with 2 free).
        g3_rk = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 3, "_pg_g3rk"))
        rep_3at8 = _sched(
            _gpu_cfg(8), rack1_4of4 + rack2_3of4_busy + [dead_r2], gangs=[g3_rk]
        )
        assert len(rep_3at8.infeasible_gang_resource_requests) == 1
        assert rep_3at8.to_launch == []

        # 6-GPU gang at max_worker_nodes=12 succeeds AND preserves ib-fabric="fab-2" on
        # rack-2's co-launched refill pod (verified by scheduling a fab-2 task onto it),
        # whereas a 7-GPU gang at max_worker_nodes=12 is rejected.
        g6_rk = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 6, "_pg_g6rk"))
        fab2_task = _resource_request({"GPU": 1}, sel=[[("ib-fabric", _IN, ["fab-2"])]])
        rep_6at12 = _sched(
            _gpu_cfg(12),
            rack1_4of4 + rack2_3of4_busy + [dead_r2],
            reqs=[fab2_task],
            gangs=[g6_rk],
        )
        assert rep_6at12.infeasible_gang_resource_requests == []
        assert rep_6at12.infeasible_resource_requests == []
        assert _launch_and_terminate(rep_6at12) == ({"gpu": 5}, [])
        assert rep_6at12.cluster_resources["GPU"] == 12.0

        g7_rk = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 7, "_pg_g7rk"))
        rep_7at12 = _sched(
            _gpu_cfg(12), rack1_4of4 + rack2_3of4_busy + [dead_r2], gangs=[g7_rk]
        )
        assert len(rep_7at12.infeasible_gang_resource_requests) == 1
        assert rep_7at12.to_launch == []

        # Two incomplete racks at max_worker_nodes=8 (2/4 busy rack-0 + 3/4 free rack-1):
        # 4-GPU gang succeeds while 5-GPU gang is rejected.
        rack0_2busy = [
            _run_node(
                "gpu",
                f"rk0b-{w}",
                {"CPU": 4, "GPU": 1},
                {"rack": "rack-0"},
                used={"GPU": 1},
                lid="L_rk0",
            )
            for w in range(2)
        ]
        rack1_3free = [
            _run_node(
                "gpu",
                f"rk1f-{w}",
                {"CPU": 4, "GPU": 1},
                {"rack": "rack-1"},
                lid="L_rk1",
            )
            for w in range(3)
        ]
        dead_rk0 = _terminated_teardown_row("gpu", "dead-rk0", "L_rk0")
        dead_rk1 = _terminated_teardown_row("gpu", "dead-rk1", "L_rk1")
        g4_rk = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 4, "_pg_g4rk2"))
        rep_4ok = _sched(
            _gpu_cfg(8),
            rack0_2busy + rack1_3free + [dead_rk0, dead_rk1],
            gangs=[g4_rk],
        )
        assert rep_4ok.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_4ok) == ({"gpu": 3}, [])

        g5_rk = _strict_pack_gang("rack", _spread_reqs({"GPU": 1}, 5, "_pg_g5rk2"))
        rep_5bad = _sched(
            _gpu_cfg(8),
            rack0_2busy + rack1_3free + [dead_rk0, dead_rk1],
            gangs=[g5_rk],
        )
        assert len(rep_5bad.infeasible_gang_resource_requests) == 1
        assert rep_5bad.to_launch == []


def test_torndown_doomed_requested_siblings_and_post_refill_fresh_batch():
    """Unallocated Instance.REQUESTED siblings on torn-down batches neither skew
    initial unlabeled alignment nor accept gangs, and refilled complete slices with
    multiple historical launch IDs never scramble subsequent fresh ALLOCATED batches."""
    excl_cfg12 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=12,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }

    # 2 busy running survivors + 1 dead worker + 1 doomed REQUESTED sibling on L0
    # at max_worker_nodes=12 co-launches 2 refill + 4 fresh (3 existing rows +
    # 6 TO_LAUNCH nodes = 9 * 4 = 36 TPUs).
    s0_busy2 = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L0",
        )
        for w in (0, 1)
    ]
    dead_l0 = _terminated_teardown_row("tpu", "dead-l0", "L0")
    doomed_r3 = _pending_node("tpu", "r3", Instance.REQUESTED, lid="L0")
    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_r73a"))
    rep_73a = _sched(excl_cfg12, s0_busy2 + [dead_l0, doomed_r3], gangs=[g4])
    assert rep_73a.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_73a) == ({"tpu": 6}, [])
    assert rep_73a.cluster_resources["TPU"] == 36.0

    # Multi-slice torn-down batch L_ms with 2 busy running + 1 ALLOCATED survivor
    # + 4 dead + 1 doomed REQUESTED sibling never places a 2-host gang onto doomed_r7.
    excl_cfg8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    ms_busy2 = [
        _run_node(
            "tpu",
            f"ms-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L_ms",
        )
        for w in (0, 1)
    ]
    ms_alloc = _pending_node("tpu", "ms-a2", Instance.ALLOCATED, lid="L_ms")
    ms_alloc.cloud_instance_id = "cloud-ms-a2"
    ms_alloc.im_instance.cloud_instance_id = "cloud-ms-a2"
    ms_deads = [
        _terminated_teardown_row("tpu", f"ms-dead-{i}", "L_ms") for i in range(4)
    ]
    doomed_r7 = _pending_node("tpu", "ms-r7", Instance.REQUESTED, lid="L_ms")
    g2 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_r73b"))
    rep_73b = _sched(excl_cfg8, ms_busy2 + [ms_alloc, doomed_r7] + ms_deads, gangs=[g2])
    assert rep_73b.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_73b) == ({"tpu": 1}, [])

    # Complete 4/4 refilled slice-0 carrying {L_old, L_refill} + two 2/4 busy
    # vanished-pod slices (total_nonprov_open == 4 == size) + fresh 4-pod ALLOCATED batch
    # L_new keeps L_new intact and schedules a new 4-host gang with zero launches.
    s0_refilled_full = [
        _run_node(
            "tpu",
            f"rf0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L_old" if w < 3 else "L_refill",
        )
        for w in range(4)
    ]
    s12_deg_busy = [
        _run_node(
            "tpu",
            f"dg{s}-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: f"slice-{s}", _WID: str(w)},
            used={"TPU": 4},
            lid=f"L_van_{s}",
        )
        for s in (1, 2)
        for w in (0, 1)
    ]
    fresh_lnew = []
    for w in range(4):
        fn = _pending_node("tpu", f"nw-{w}", Instance.ALLOCATED, lid="L_new")
        fn.cloud_instance_id = f"cloud-nw-{w}"
        fn.im_instance.cloud_instance_id = f"cloud-nw-{w}"
        fresh_lnew.append(fn)
    g4_74 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_r74"))
    rep_74 = _sched(
        excl_cfg12, s0_refilled_full + s12_deg_busy + fresh_lnew, gangs=[g4_74]
    )
    assert rep_74.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_74) == ({}, [])


def test_multi_slice_torndown_refill_non_modulo_and_queued_attachment():
    """Multiple torn-down slices whose open slots sum to >= size (e.g. 2x 2/4 = 4
    with 4 % 4 == 0, or 1/4 + 2/4 = 5) track full unbacked refill gaps, prevent
    duplicate head_resources over-provisioning, attach 4-pod QUEUED refills to
    protect idle survivors, and co-launch aligned refills for multiple gangs."""
    head_key = "TPU-v5p-16-head"
    cfgs8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4, head_key: 1},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_key,),
            ),
        )
    }

    def _slice_member(
        iid: str, sl: str, w: int, lid: str, busy: bool = True, idle_ms: int = 0
    ):
        tot = {"CPU": 8, "TPU": 4, **({head_key: 1} if w == 0 else {})}
        used = {"TPU": 4, **({head_key: 1} if w == 0 else {})} if busy else None
        return _run_node(
            "tpu",
            iid,
            tot,
            {_SLICE: sl, _WID: str(w)},
            used=used,
            idle_ms=idle_ms,
            lid=lid,
        )

    # Case 1: Two 2/4 torn-down slices (4 total survivors -> raw_gap == 0, rep_open_sum == 4).
    s0_2 = [_slice_member(f"s0-{w}", "slice-0", w, "L_td0", busy=True) for w in (0, 1)]
    s1_2 = [_slice_member(f"s1-{w}", "slice-1", w, "L_td1", busy=True) for w in (0, 1)]
    td_rows = [
        _terminated_teardown_row("tpu", "dead-0", "L_td0"),
        _terminated_teardown_row("tpu", "dead-1", "L_td1"),
    ]
    base_2x2 = s0_2 + s1_2 + td_rows
    ctx_2x2 = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=cfgs8,
            current_instances=base_2x2,
            replica_view=_synthetic_replica_view(cfgs8, base_2x2),
        )
    )
    assert ctx_2x2.get_unbacked_refill_gap("tpu") == 4

    # Both worker-0 heads are already busy on slice-0 and slice-1; requesting a 3rd
    # head_resources task at max_worker_nodes=8 is infeasible (cannot mint a 3rd head).
    rep_3rd_head = _sched(cfgs8, base_2x2, reqs=[_resource_request({head_key: 1})])
    assert len(rep_3rd_head.infeasible_resource_requests) == 1
    assert _launch_and_terminate(rep_3rd_head) == ({}, [])

    # Two 4-host rescheduling gangs refill both idle 2/4 slices cleanly (2 + 2 = 4 pods).
    s0_idle = [
        _slice_member(f"s0-{w}", "slice-0", w, "L_td0", busy=False, idle_ms=120_000)
        for w in (0, 1)
    ]
    s1_idle = [
        _slice_member(f"s1-{w}", "slice-1", w, "L_td1", busy=False, idle_ms=120_000)
        for w in (0, 1)
    ]
    idle_2x2 = s0_idle + s1_idle + td_rows
    two_gangs = [
        _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, f"_pg_ms_{g}"))
        for g in range(2)
    ]
    rep_refill_4 = _sched(cfgs8, idle_2x2, gangs=two_gangs)
    assert rep_refill_4.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_refill_4) == ({"tpu": 4}, [])

    # When the 4 refill pods are QUEUED (len(queued_nodes) == 4, 4 % 4 == 0) and the
    # 4 running survivors on slice-0/slice-1 have expired idle_duration_ms, attaching
    # the fresh QUEUED refills protects the survivors from premature idle termination.
    q4 = [_pending_node("tpu", f"q-{i}", Instance.QUEUED) for i in range(4)]
    rep_q4_hold = _sched(cfgs8, s0_idle + s1_idle + q4 + td_rows, idle_s=30.0)
    assert rep_q4_hold.to_terminate == []

    # Case 2: 1/4 + 2/4 torn-down slices (3 total survivors -> raw_gap == 1, rep_open_sum == 5).
    base_1_2 = [s0_idle[0]] + s1_idle + td_rows
    ctx_1_2 = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=cfgs8,
            current_instances=base_1_2,
            replica_view=_synthetic_replica_view(cfgs8, base_1_2),
        )
    )
    assert ctx_1_2.get_unbacked_refill_gap("tpu") == 5
    rep_refill_5 = _sched(cfgs8, base_1_2, gangs=two_gangs)
    assert rep_refill_5.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_refill_5) == ({"tpu": 5}, [])


def test_shared_domain_sibling_adoption_and_heterogeneous_rack_launch():
    """Shared-domain launch search scopes refill gaps to adopted pending siblings
    (exact max_worker_nodes accounting) and considers heterogeneous step5 helper types."""
    rack = "ray.io/rack"

    def _shared_cfgs(max_gpu: int, with_cpu_helper: bool = False):
        cfgs = {
            "gpu": NodeTypeConfig(
                name="gpu",
                min_worker_nodes=0,
                max_worker_nodes=max_gpu,
                resources={"CPU": 4, "GPU": 1},
                runtime_label_keys=(rack,),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=rack,
                    domain_exclusive=False,
                ),
            )
        }
        if with_cpu_helper:
            cfgs["cpu-helper"] = NodeTypeConfig(
                name="cpu-helper",
                min_worker_nodes=0,
                max_worker_nodes=2,
                resources={"CPU": 16},
                runtime_label_keys=(rack,),
            )
        return cfgs

    rack0 = [
        _run_node(
            "gpu",
            f"r0-{i}",
            {"CPU": 4, "GPU": 1},
            {rack: "rack-0"},
            used={"GPU": 1} if i < 3 else None,
            lid="L0",
        )
        for i in range(4)
    ]
    sib_td = _pending_node("gpu", "sib-0", Instance.ALLOCATED, lid="L_td")
    sib_td.cloud_instance_id = "cloud-sib-0"
    sib_td.im_instance.cloud_instance_id = "cloud-sib-0"
    td_dead = _terminated_teardown_row("gpu", "sib-dead", "L_td")
    insts = rack0 + [sib_td, td_dead]

    # At max_worker_nodes=8: 1 free on rack-0 + 1 adopted sibling + 3 refills = 5 GPUs max.
    g5 = _strict_pack_gang(rack, [_mk_req({"GPU": 1}) for _ in range(5)])
    g6 = _strict_pack_gang(rack, [_mk_req({"GPU": 1}) for _ in range(6)])
    rep_5 = _sched(_shared_cfgs(8), insts, gangs=[g5])
    assert rep_5.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_5) == ({"gpu": 3}, [])
    rep_6 = _sched(_shared_cfgs(8), insts, gangs=[g6])
    assert len(rep_6.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_6) == ({}, [])

    # At max_worker_nodes=12: + 4 fresh replica = 9 GPUs feasible; 10 GPUs infeasible.
    g9 = _strict_pack_gang(rack, [_mk_req({"GPU": 1}) for _ in range(9)])
    g10 = _strict_pack_gang(rack, [_mk_req({"GPU": 1}) for _ in range(10)])
    rep_9 = _sched(_shared_cfgs(12), insts, gangs=[g9])
    assert rep_9.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_9) == ({"gpu": 7}, [])
    rep_10 = _sched(_shared_cfgs(12), insts, gangs=[g10])
    assert len(rep_10.infeasible_gang_resource_requests) == 1

    # Heterogeneous gang: 4 GPUs on rack-0 + 1 CPU=16 coordinator launched on cpu-helper.
    rack0_free = [
        _run_node("gpu", f"rf-{i}", {"CPU": 4, "GPU": 1}, {rack: "rack-0"}, lid="L0")
        for i in range(4)
    ]
    g_het = _strict_pack_gang(
        rack, [_mk_req({"GPU": 1}) for _ in range(4)] + [_mk_req({"CPU": 16})]
    )
    rep_het = _sched(_shared_cfgs(4, with_cpu_helper=True), rack0_free, gangs=[g_het])
    assert rep_het.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_het) == ({"cpu-helper": 1}, [])


def test_cross_slice_shared_lid_draining_isolation_and_pinned_refill():
    """A draining slice in a multi-slice launch batch never poisons healthy complete
    or booting sibling slices sharing launch_request_id, and a rescheduling gang
    pinned to a torn-down slice launches its refill without allowing fresh slices."""
    cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }

    # L_batch spans slice-0 (3 running + 1 RAY_STOPPING) and slice-1 (4 running).
    s0_3 = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            idle_ms=90_000,
            lid="L_batch",
        )
        for w in range(3)
    ]
    s0_stopping = make_autoscaler_instance(
        im_instance=Instance(
            instance_id="s0-3",
            instance_type="tpu",
            status=Instance.RAY_STOPPING,
            cloud_instance_id="cloud-s0-3",
            launch_request_id="L_batch",
            node_id=b"s0-3",
        ),
        ray_node=NodeState(
            node_id=b"s0-3",
            ray_node_type_name="tpu",
            total_resources={"CPU": 8, "TPU": 4},
            available_resources={"CPU": 8, "TPU": 4},
            labels={_SLICE: "slice-0", _WID: "3"},
            status=NodeStatus.IDLE,
        ),
        cloud_instance_id="cloud-s0-3",
    )
    s1_4 = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            idle_ms=90_000,
            lid="L_batch",
        )
        for w in range(4)
    ]
    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_c81"))
    rep_sched = _sched(cfgs, s0_3 + [s0_stopping] + s1_4, gangs=[g4])
    assert rep_sched.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_sched) == ({}, [])

    # Healthy slice-1 also remains eligible for whole-slice idle termination.
    rep_idle = _sched(cfgs, s0_3 + [s0_stopping] + s1_4, idle_s=30.0)
    assert sorted(t.instance_id for t in rep_idle.to_terminate) == [
        "s1-0",
        "s1-1",
        "s1-2",
        "s1-3",
    ]

    # slice-0 is draining (3 running + 1 RAY_STOPPING) while slice-1 has 3 running
    # + 1 ALLOCATED pod on L_batch; healthy slice-1 claims its ALLOCATED 4th pod.
    s1_alloc3 = _pending_node("tpu", "s1-3", Instance.ALLOCATED, lid="L_batch")
    s1_alloc3.cloud_instance_id = "cloud-s1-3"
    s1_alloc3.im_instance.cloud_instance_id = "cloud-s1-3"
    rep_boot_sib = _sched(
        cfgs, s0_3 + [s0_stopping] + s1_4[:3] + [s1_alloc3], gangs=[g4]
    )
    assert rep_boot_sib.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_boot_sib) == ({}, [])

    # Pinned gang on 3/4 torn-down slice-0 launches 1-pod refill; pinned gang on
    # nonexistent slice-missing stays infeasible (does not launch a fresh slice).
    cfgs4 = {"tpu": copy.deepcopy(cfgs["tpu"])}
    cfgs4["tpu"].max_worker_nodes = 4
    td_s0 = [
        _run_node(
            "tpu",
            f"p0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L_pin",
        )
        for w in range(3)
    ] + [_terminated_teardown_row("tpu", "p0-dead", "L_pin")]
    g_pin_s0 = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_pin0"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    rep_pin_ok = _sched(cfgs4, td_s0, gangs=[g_pin_s0])
    assert rep_pin_ok.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_pin_ok) == ({"tpu": 1}, [])

    g_pin_miss = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_pin_miss"),
        loc_constraints=[(_SLICE, _IN, ["slice-missing"])],
    )
    rep_pin_miss = _sched(cfgs, td_s0, gangs=[g_pin_miss])
    assert len(rep_pin_miss.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_pin_miss) == ({}, [])

    # Completed solicited scale-down of slice-0 on an 8-pod L_batch (3 TERMINATED +
    # 1 TERMINATING without ray_node, solicited count 4 % 4 == 0) plus an unsolicited
    # pod crash on slice-1 leaves slice-1's 3 surviving hosts schedulable.
    solic_rows = []
    for k in range(4):
        st = Instance.TERMINATED if k < 3 else Instance.TERMINATING
        im_s = Instance(
            instance_id=f"s0-solic-{k}",
            instance_type="tpu",
            status=st,
            cloud_instance_id=f"cloud-s0-solic-{k}",
            launch_request_id="L_batch",
            node_kind=NodeKind.WORKER,
        )
        im_s.status_history.add(instance_status=Instance.RAY_RUNNING)
        im_s.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        im_s.status_history.add(instance_status=st)
        solic_rows.append(
            make_autoscaler_instance(
                im_instance=im_s, cloud_instance_id=im_s.cloud_instance_id
            )
        )
    unsolic_crash = Instance(
        instance_id="s1-crash",
        instance_type="tpu",
        status=Instance.RAY_STOPPED,
        cloud_instance_id="cloud-s1-crash",
        launch_request_id="L_batch",
        node_kind=NodeKind.WORKER,
    )
    unsolic_crash.status_history.add(instance_status=Instance.RAY_RUNNING)
    unsolic_crash.status_history.add(instance_status=Instance.RAY_STOPPED)
    batch_after_drain = (
        solic_rows
        + s1_4[:3]
        + [
            make_autoscaler_instance(
                im_instance=unsolic_crash,
                cloud_instance_id=unsolic_crash.cloud_instance_id,
            )
        ]
    )
    rep_3tasks = _sched(
        cfgs4, batch_after_drain, reqs=[_resource_request({"TPU": 4}, 3)]
    )
    assert rep_3tasks.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_3tasks) == ({}, [])
    rep_refill_s1 = _sched(cfgs, batch_after_drain, gangs=[g4])
    assert rep_refill_s1.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_refill_s1) == ({"tpu": 1}, [])

    # Shared refill batch L_ref spanning two incomplete 3/4 slices with an unscoped
    # solicited TERMINATING sibling (ray_node=None) keeps all 6 running hosts schedulable,
    # whereas a single incomplete 3/4 slice with that unscoped drain is marked draining.
    s0_base2 = [
        _run_node(
            "tpu",
            f"r0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L_s0",
        )
        for w in range(2)
    ]
    s1_base2 = [
        _run_node(
            "tpu",
            f"r1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            lid="L_s1",
        )
        for w in range(2)
    ]
    s0_ref1 = _run_node(
        "tpu", "r0-2", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-0", _WID: "2"}, lid="L_ref"
    )
    s1_ref1 = _run_node(
        "tpu", "r1-2", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-1", _WID: "2"}, lid="L_ref"
    )
    unscoped_drain_im = Instance(
        instance_id="ref-solic-term",
        instance_type="tpu",
        status=Instance.TERMINATING,
        cloud_instance_id="cloud-ref-solic-term",
        launch_request_id="L_ref",
        node_kind=NodeKind.WORKER,
    )
    unscoped_drain_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    unscoped_drain_im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    unscoped_drain_im.status_history.add(instance_status=Instance.TERMINATING)
    unscoped_drain = make_autoscaler_instance(
        im_instance=unscoped_drain_im, cloud_instance_id="cloud-ref-solic-term"
    )
    two_inc_insts = (
        s0_base2
        + [s0_ref1, _terminated_teardown_row("tpu", "d0", "L_s0")]
        + s1_base2
        + [s1_ref1, _terminated_teardown_row("tpu", "d1", "L_s1")]
        + [unscoped_drain]
    )
    rep_6tasks = _sched(cfgs, two_inc_insts, reqs=[_resource_request({"TPU": 4}, 6)])
    assert rep_6tasks.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_6tasks) == ({}, [])

    # Counter-case: single incomplete 3/4 slice on L_ref with an unscoped solicited
    # TERMINATING row marks that slice draining so tasks cannot place without replacement.
    one_inc_insts = s0_base2 + [
        s0_ref1,
        _terminated_teardown_row("tpu", "d0", "L_s0"),
        unscoped_drain,
    ]
    cfgs3_cap = {"tpu": copy.deepcopy(cfgs["tpu"])}
    cfgs3_cap["tpu"].max_worker_nodes = 3
    rep_single_drain = _sched(
        cfgs3_cap, one_inc_insts, reqs=[_resource_request({"TPU": 4}, 1)]
    )
    assert len(rep_single_drain.infeasible_resource_requests) == 1


def test_domainless_lidless_replacement_donation_and_remainder_idle_reclaim():
    """Domainless multi-host groups let operator-recreated lid-less running pods
    complete incomplete lid-attributed replicas without phantom placeholders or
    mutating live GCS head_resources, reclaim sub-replica idle remainders above
    min_worker_nodes while preserving busy-pool safety, and align planned TO_LAUNCH
    counts with Reconciler._scale_cluster across vanished mid-boot batches."""
    res = {"CPU": 8, "GPU": 4}
    cfgs = {
        "gpu-lws": NodeTypeConfig(
            name="gpu-lws",
            min_worker_nodes=2,
            max_worker_nodes=8,
            resources=res,
            replica_config=WorkerReplicaConfig(workers_per_replica=2),
        )
    }

    # Operator-recreated lid-less p5 donates into incomplete L with zero phantom placeholders.
    insts_r1 = [
        _run_node("gpu-lws", "p1", res, {}, idle_ms=500_000),
        _run_node("gpu-lws", "p2", res, {}, idle_ms=500_000),
        _run_node("gpu-lws", "p3", res, {}, idle_ms=500_000, lid="L"),
        _run_node("gpu-lws", "p5", res, {}, idle_ms=500_000),
    ]
    ctx_r1 = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=cfgs,
            current_instances=insts_r1,
            replica_view=_synthetic_replica_view(cfgs, insts_r1),
        )
    )
    assert ctx_r1.get_missing_replica_backfill_slots() == {}
    assert ctx_r1.get_node_type_available()["gpu-lws"] == 4
    rep_r1_idle = _sched(cfgs, insts_r1, idle_s=30.0)
    assert len(rep_r1_idle.to_terminate) == 2
    rep_r1_up = _sched(cfgs, insts_r1, reqs=[_resource_request(res, 8)])
    assert rep_r1_up.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_r1_up) == ({"gpu-lws": 4}, [])

    # Domainless donation preserves live GCS head_resources and worker_index_label_key:
    # two worker-1 survivors keep heads=0.0, and two worker-0 survivors keep heads=2.0.
    head_key = "GPU-head"
    cfgs_head2 = {
        "gpu-lws": NodeTypeConfig(
            name="gpu-lws",
            min_worker_nodes=0,
            max_worker_nodes=2,
            resources={**res, head_key: 1},
            runtime_label_keys=(_WID,),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                worker_index_label_key=_WID,
                head_resources=(head_key,),
            ),
        )
    }
    w1_pair = [
        _run_node("gpu-lws", "w1-a", res, {_WID: "1"}, lid="L_w1"),
        _run_node("gpu-lws", "w1-b", res, {_WID: "1"}),
    ]
    rep_w1 = _sched(cfgs_head2, w1_pair, reqs=[_resource_request({head_key: 1}, 1)])
    assert rep_w1.cluster_resources.get(head_key, 0.0) == 0.0
    assert len(rep_w1.infeasible_resource_requests) == 1
    assert _launch_and_terminate(rep_w1) == ({}, [])

    w0_pair = [
        _run_node("gpu-lws", "w0-a", {**res, head_key: 1}, {_WID: "0"}, lid="L_w0"),
        _run_node("gpu-lws", "w0-b", {**res, head_key: 1}, {_WID: "0"}),
    ]
    rep_w0 = _sched(cfgs_head2, w0_pair, reqs=[_resource_request({head_key: 1}, 2)])
    assert rep_w0.cluster_resources.get(head_key, 0.0) == 2.0
    assert rep_w0.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_w0) == ({}, [])

    # 3 idle lid-less pods at size=2, min=2 reclaim the 1 orphan pod; 1 busy + 3 idle
    # without lid/prov/domain still terminates nothing (atomic busy safety).
    idle3 = [_run_node("gpu-lws", f"n{i}", res, {}, idle_ms=300_000) for i in range(3)]
    rep_r2 = _sched(cfgs, idle3, idle_s=30.0)
    assert len(rep_r2.to_terminate) == 1

    cfgs_min0 = {
        "gpu-lws": NodeTypeConfig(
            name="gpu-lws",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources=res,
            replica_config=WorkerReplicaConfig(workers_per_replica=2),
        )
    }
    busy1_idle3 = [_run_node("gpu-lws", "b0", res, {}, used={"GPU": 4})] + idle3
    assert _sched(cfgs_min0, busy1_idle3, idle_s=30.0).to_terminate == []

    # Genuine mid-boot vanished pod (TERMINATED without teardown history) alongside
    # sibling REQUESTED rows + a 1/4 torn-down survivor at size=4, max_worker_nodes=8:
    # vanished_cids_by_lid is non-empty, and planned TO_LAUNCH (3) equals the exact
    # QUEUED count created by Reconciler._scale_cluster (3).
    cfgs_van8 = {
        "gpu-lws": NodeTypeConfig(
            name="gpu-lws",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources=res,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    p_alloc = _pending_node("gpu-lws", "a0", Instance.ALLOCATED, lid="L_van")
    p_alloc.cloud_instance_id = "cloud-a0"
    p_alloc.im_instance.cloud_instance_id = "cloud-a0"
    p_alloc.im_instance.node_kind = NodeKind.WORKER
    p_req1 = _pending_node("gpu-lws", "r1", Instance.REQUESTED, lid="L_van")
    p_req1.im_instance.node_kind = NodeKind.WORKER
    p_req2 = _pending_node("gpu-lws", "r2", Instance.REQUESTED, lid="L_van")
    p_req2.im_instance.node_kind = NodeKind.WORKER
    van_im = Instance(
        instance_id="a3-van",
        instance_type="gpu-lws",
        status=Instance.TERMINATED,
        cloud_instance_id="cloud-a3",
        launch_request_id="L_van",
        node_kind=NodeKind.WORKER,
    )
    van_im.status_history.add(instance_status=Instance.ALLOCATED)
    van_im.status_history.add(instance_status=Instance.TERMINATED)
    td_surv = _run_node("gpu-lws", "s0", res, {}, used={"GPU": 4}, lid="L_td")
    td_surv.im_instance.node_kind = NodeKind.WORKER
    td_dead = _terminated_teardown_row("gpu-lws", "s1-dead", "L_td")
    td_dead.im_instance.node_kind = NodeKind.WORKER
    insts_van = [
        p_alloc,
        p_req1,
        p_req2,
        make_autoscaler_instance(im_instance=van_im),
        td_surv,
        td_dead,
    ]
    van_view = _synthetic_replica_view(cfgs_van8, insts_van)
    assert van_view is not None
    assert van_view.vanished_cids_by_lid == {("gpu-lws", "L_van"): {"cloud-a3"}}
    assert van_view.unlabeled_backfill_gap("gpu-lws", 4) == 3

    rep_van = _sched(cfgs_van8, insts_van, reqs=[_resource_request(res, 7)])
    assert rep_van.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_van) == ({"gpu-lws": 3}, [])

    head_im = Instance(
        instance_id="head-0",
        instance_type="head",
        status=Instance.RAY_RUNNING,
        cloud_instance_id="c-head",
        node_id=(b"h" * 28).hex(),
        node_kind=NodeKind.HEAD,
    )
    head_rn = NodeState(
        node_id=b"h" * 28,
        instance_id="c-head",
        ray_node_type_name="head",
        status=NodeStatus.RUNNING,
        total_resources={"CPU": 0.0},
        available_resources={"CPU": 0.0},
    )
    im_rows = [head_im] + [x.im_instance for x in insts_van]
    store = InstanceStorage(cluster_id="van-seam", storage=InMemoryStorage())
    store.batch_upsert_instances(im_rows, expected_storage_version=0)
    im_mgr = InstanceManager(
        instance_storage=store, instance_status_update_subscribers=[]
    )
    as_cfg = AutoscalingConfig(
        configs={
            "cluster_name": "van-seam",
            "max_workers": 8,
            "upscaling_speed": 100.0,
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "gpu-lws": {
                    "node_config": {},
                    "resources": res,
                    "min_workers": 0,
                    "max_workers": 8,
                    "replica_config": {"workers_per_replica": 4},
                },
            },
        },
        skip_content_hash=True,
    )

    class _FixedScheduler(IResourceScheduler):
        def schedule(self, request):
            return rep_van

    Reconciler._scale_cluster(
        autoscaling_state=AutoscalingState(),
        instance_manager=im_mgr,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_state=ClusterResourceState(node_states=[head_rn]),
        scheduler=_FixedScheduler(),
        autoscaling_config=as_cfg,
        cloud_provider=None,
        non_terminated_cloud_instances={
            "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
            "cloud-a0": CloudInstance("cloud-a0", "gpu-lws", NodeKind.WORKER, True),
            td_surv.im_instance.cloud_instance_id: CloudInstance(
                td_surv.im_instance.cloud_instance_id,
                "gpu-lws",
                NodeKind.WORKER,
                True,
            ),
        },
        replica_sizes={"gpu-lws": 4},
    )
    created_queued = sum(
        1 for v in store.get_instances()[0].values() if v.status == Instance.QUEUED
    )
    assert created_queued == sum(r.count for r in rep_van.to_launch) == 3


def test_single_host_replica_guards_and_multi_type_refill_isolation():
    """Pins critical scheduler guards: size==1 replica_config grouping, shared-domain
    split-batch extra_lid_backfill, two-multi-host-type refill isolation, max-worker
    termination skipping empty-ID placeholders/TO_LAUNCH, outside-domain node survival,
    failed domain trial with rem_rg > 0, and active-label anti-affinity dedup."""
    # 1. size == 1 single-host TPU replica_config keeps unlabeled running & pending
    # hosts in distinct 1-host replicas rather than collapsing replica_id to None.
    sh_cfgs = {
        "tpu-1h": NodeTypeConfig(
            name="tpu-1h",
            min_worker_nodes=0,
            max_worker_nodes=4,
            resources={"CPU": 4, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=1,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    sh_insts = [
        _run_node("tpu-1h", "sh-run-0", {"CPU": 4, "TPU": 4}, {}),
        _run_node("tpu-1h", "sh-run-1", {"CPU": 4, "TPU": 4}, {}),
        _pending_node("tpu-1h", "sh-pend-0", Instance.ALLOCATED),
    ]
    sh_ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=sh_cfgs,
            current_instances=sh_insts,
        )
    )
    sh_rids = [n.replica_id for n in sh_ctx.peek_nodes()]
    assert len(sh_rids) == 3 and all(r is not None for r in sh_rids)
    assert len(set(sh_rids)) == 3

    # 2. Shared-domain split batch L_split (5 on rack-0 + 2 on rack-1 = 7 of 8)
    # reserves extra_lid_backfill=1 before placeholder TTL and clears after expiry.
    rack = "ray.io/rack"
    split_cfgs = {
        "gpu": NodeTypeConfig(
            name="gpu",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 4, "GPU": 1},
            runtime_label_keys=(rack,),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4, domain_label_key=rack, domain_exclusive=False
            ),
        )
    }
    split_insts = [
        _run_node(
            "gpu",
            f"sp-{i}",
            {"CPU": 4, "GPU": 1},
            {rack: "rack-0" if i < 5 else "rack-1"},
            lid="L_split",
        )
        for i in range(7)
    ]
    split_ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=split_cfgs,
            current_instances=split_insts,
        )
    )
    assert split_ctx.get_missing_replica_backfill_slots() == {"gpu": 1}
    assert split_ctx.get_node_type_available() == {"gpu": 0}
    split_exp_ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=split_cfgs,
            current_instances=split_insts,
        ),
        expired_placeholder_keys={("gpu", "lid", "L_split")},
    )
    assert split_exp_ctx.get_missing_replica_backfill_slots() == {}
    assert split_exp_ctx.get_node_type_available() == {"gpu": 1}

    # 3. Two multi-host types never cross-count QUEUED or placeholders.
    two_nt_cfgs = {
        "tpu-a": NodeTypeConfig(
            name="tpu-a",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
        "tpu-b": NodeTypeConfig(
            name="tpu-b",
            min_worker_nodes=0,
            max_worker_nodes=8,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
    }
    insts_2nt = (
        [
            _run_node(
                "tpu-a",
                f"a-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "sa-0", _WID: str(w)},
                lid="La",
            )
            for w in range(3)
        ]
        + [_terminated_teardown_row("tpu-a", "a-dead", "La")]
        + [
            _run_node(
                "tpu-b",
                f"b-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "sb-0", _WID: str(w)},
                lid="Lb",
            )
            for w in range(2)
        ]
        + [_pending_node("tpu-b", "qb-0", Instance.QUEUED)]
    )
    ctx_2nt = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=two_nt_cfgs,
            current_instances=insts_2nt,
            replica_view=_synthetic_replica_view(two_nt_cfgs, insts_2nt),
        )
    )
    assert ctx_2nt.get_unbacked_refill_gap("tpu-a") == 1
    assert ctx_2nt.get_unbacked_refill_gap("tpu-b") == 0

    # 4. Max-worker termination over a degraded replica (with placeholder) +
    # over-cap TO_LAUNCH/real nodes never emits empty-ID TerminationRequests.
    over_cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=12,
            max_worker_nodes=12,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    deg_prov3 = [
        _run_node(
            "tpu",
            f"dp-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s-dp", _WID: str(w)},
            idle_ms=60_000,
            rep="R_dp",
        )
        for w in range(3)
    ] + [
        _run_node(
            "tpu",
            f"full-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s-full", _WID: str(w)},
            used={"TPU": 4},
            rep="R_full",
        )
        for w in range(4)
    ]
    rep_over = _sched(over_cfgs, deg_prov3, max_nodes=4)
    assert sorted(t.instance_id for t in rep_over.to_terminate) == [
        "dp-0",
        "dp-1",
        "dp-2",
    ]
    assert all(bool(t.instance_id) for t in rep_over.to_terminate)

    # 5. Outside-domain nodes survive successful gang placement in
    # cluster_resources, and a failed candidate-domain trial with rem_rg > 0 never raises.
    mix_cfgs = {
        "tpu": two_nt_cfgs["tpu-a"],
        "cpu": NodeTypeConfig(
            name="cpu", min_worker_nodes=0, max_worker_nodes=4, resources={"CPU": 16}
        ),
    }
    outside_cpu = _run_node("cpu", "c-0", {"CPU": 16}, {}, used={"CPU": 8})
    s0_free = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s0", _WID: str(w)},
            rep="R0",
        )
        for w in range(4)
    ]
    s1_busy = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s1", _WID: str(w)},
            used={"TPU": 4},
            rep="R1",
        )
        for w in range(4)
    ]
    g_s0 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_surv"))
    rep_surv = _sched(mix_cfgs, [outside_cpu] + s0_free + s1_busy, gangs=[g_s0])
    assert rep_surv.infeasible_gang_resource_requests == []
    assert rep_surv.cluster_resources["TPU"] == 32.0
    assert rep_surv.cluster_resources["CPU"] == 80.0

    # Failed domain trial on cand_nodes when rem_rg > 0 (5-host spread gang on 4-host slice).
    td_two_2x2 = (
        [
            _run_node(
                "tpu",
                f"x0-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "sx0", _WID: str(w)},
                lid="Lx0",
            )
            for w in range(2)
        ]
        + [
            _run_node(
                "tpu",
                f"x1-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "sx1", _WID: str(w)},
                lid="Lx1",
            )
            for w in range(2)
        ]
        + [
            _terminated_teardown_row("tpu", "dx0", "Lx0"),
            _terminated_teardown_row("tpu", "dx1", "Lx1"),
        ]
    )
    g5_impossible = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 5, "_pg_imp5"))
    rep_imp5 = _sched({"tpu": two_nt_cfgs["tpu-a"]}, td_two_2x2, gangs=[g5_impossible])
    assert len(rep_imp5.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_imp5) == ({}, [])

    # 6. Active anti-affinity label key on an existing node distinguishes a
    # rescheduling PG (1 bundle already on tg-0 -> 4 more bundles infeasible at max=4)
    # from a fresh PG (0 bundles on s-tg -> all 4 bundles place cleanly on s-tg).
    aa_cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            min_worker_nodes=0,
            max_worker_nodes=4,
            resources={"CPU": 8, "TPU": 4},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    tagged_slice = [
        _run_node(
            "tpu",
            f"tg-{w}",
            {"CPU": 8, "TPU": 4},
            {
                _SLICE: "s-tg",
                _WID: str(w),
                **({"_pg_resched1": ""} if w == 0 else {}),
            },
            rep="R_tg",
        )
        for w in range(4)
    ]

    def _aa_gang(pg_key: str) -> GangResourceRequest:
        return _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, pg_key))

    rep_aa = _sched(
        aa_cfgs,
        tagged_slice,
        gangs=[_aa_gang("_pg_resched1"), _aa_gang("_pg_resched2")],
    )
    assert len(rep_aa.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_aa) == ({}, [])


def test_capped_partial_refills_multi_slice_refill_attach_and_bundle_pin_ordering():
    """Verifies min/max worker refill capping across partial torn-down slices,
    surviving ALLOCATED pod attachment on torn-down multi-slice refill batches,
    max_worker_nodes-forced open-slot attachment after autoscaler restarts, and
    per-bundle label_selectors domain pin ordering ahead of unpinned gangs."""
    head_res = "TPU-head"
    base_rc = WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key=_SLICE,
        domain_exclusive=True,
        worker_index_label_key=_WID,
        head_resources=(head_res,),
    )

    def _mk_cfgs(min_w: int = 0, max_w: int = 8) -> Dict[str, NodeTypeConfig]:
        return {
            "tpu": NodeTypeConfig(
                name="tpu",
                min_worker_nodes=min_w,
                max_worker_nodes=max_w,
                resources={"CPU": 8, "TPU": 4, head_res: 1},
                runtime_label_keys=(_SLICE, _WID),
                replica_config=base_rc,
            )
        }

    # 1. Min-worker deficit=1 with 1/4 + 2/4 torn-down survivors (3 workers, min=4, max=8)
    # emits LaunchRequest(count=4) (1 refill pod) rather than over-launching 5 pods (count=8).
    td_1_and_2 = (
        [
            _run_node(
                "tpu",
                "m0-0",
                {"CPU": 8, "TPU": 4, head_res: 1},
                {_SLICE: "s0", _WID: "0"},
                lid="Lm0",
            ),
            _terminated_teardown_row("tpu", "m0-dead", "Lm0"),
        ]
        + [
            _run_node(
                "tpu",
                f"m1-{w}",
                {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
                {_SLICE: "s1", _WID: str(w)},
                lid="Lm1",
            )
            for w in range(2)
        ]
        + [_terminated_teardown_row("tpu", "m1-dead", "Lm1")]
    )
    rep_min4 = _sched(_mk_cfgs(min_w=4, max_w=8), td_1_and_2)
    assert _launch_and_terminate(rep_min4) == ({"tpu": 1}, [])

    # 6 surviving pods across 3 torn-down slices at min=8, max=8 refill 2 slots
    # (LaunchRequest(count=2)) with zero live-host terminations.
    td_6_pods = (
        td_1_and_2
        + [
            _run_node(
                "tpu",
                f"m2-{w}",
                {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
                {_SLICE: "s2", _WID: str(w)},
                lid="Lm2",
            )
            for w in range(3)
        ]
        + [_terminated_teardown_row("tpu", "m2-dead", "Lm2")]
    )
    rep_min8 = _sched(_mk_cfgs(min_w=8, max_w=8), td_6_pods)
    assert _launch_and_terminate(rep_min8) == ({"tpu": 2}, [])

    # 2. Multi-slice refill batch L_ref spanning 2/4 s0 and 2/4 s1 where 1 pod registered
    # on s0, 1 terminated, and 2 remain ALLOCATED attaches 1 pod each to s0 and s1.
    ms_ref_insts = (
        [
            _run_node(
                "tpu",
                f"ms0-{w}",
                {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
                {_SLICE: "s0", _WID: str(w)},
                lid="L0",
            )
            for w in range(2)
        ]
        + [
            _run_node(
                "tpu",
                "ms0-2",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s0", _WID: "2"},
                lid="L_ref",
            ),
            _terminated_teardown_row("tpu", "ms0-dead", "L0"),
        ]
        + [
            _run_node(
                "tpu",
                f"ms1-{w}",
                {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
                {_SLICE: "s1", _WID: str(w)},
                lid="L1",
            )
            for w in range(2)
        ]
        + [
            _terminated_teardown_row("tpu", "ms1-dead", "L1"),
            _terminated_teardown_row("tpu", "ref-dead", "L_ref"),
        ]
    )
    for k in range(2):
        p_al = _pending_node("tpu", f"ref-al-{k}", Instance.ALLOCATED, lid="L_ref")
        p_al.cloud_instance_id = f"cloud-ref-al-{k}"
        p_al.im_instance.cloud_instance_id = f"cloud-ref-al-{k}"
        ms_ref_insts.append(p_al)
    two_gangs = [
        _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, f"_pg_ms_{i}"))
        for i in range(2)
    ]
    rep_ms_ok = _sched(_mk_cfgs(0, 8), ms_ref_insts, gangs=two_gangs)
    assert rep_ms_ok.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_ms_ok) == ({"tpu": 1}, [])
    three_gangs = two_gangs + [
        _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_ms_2"))
    ]
    rep_ms_over = _sched(_mk_cfgs(0, 8), ms_ref_insts, gangs=three_gangs)
    assert len(rep_ms_over.infeasible_gang_resource_requests) == 1

    # 3. Autoscaler restart at max_worker_nodes=8 with two 2/4 lid-less running slices +
    # 4 lid-less ALLOCATED pods attaches the 4 pods to complete s0 and s1, keeping
    # total head_resources == 2.0, rejecting a 3rd head task, and scheduling 2 gangs.
    restart_insts = [
        _run_node(
            "tpu",
            f"rs{s}-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: f"s{s}", _WID: str(w)},
        )
        for s in range(2)
        for w in range(2)
    ]
    for k in range(4):
        p_nl = _pending_node("tpu", f"rs-al-{k}", Instance.ALLOCATED)
        p_nl.cloud_instance_id = f"cloud-rs-al-{k}"
        p_nl.im_instance.cloud_instance_id = f"cloud-rs-al-{k}"
        restart_insts.append(p_nl)
    rep_rs_gangs = _sched(_mk_cfgs(0, 8), restart_insts, gangs=two_gangs)
    assert rep_rs_gangs.infeasible_gang_resource_requests == []
    assert rep_rs_gangs.cluster_resources[head_res] == 2.0
    assert _launch_and_terminate(rep_rs_gangs) == ({}, [])
    rep_rs_heads = _sched(
        _mk_cfgs(0, 8), restart_insts, reqs=[_resource_request({head_res: 1}, 3)]
    )
    assert len(rep_rs_heads.infeasible_resource_requests) == 1

    # 4. Per-bundle label_selectors domain pin sorts ahead of an unpinned gang even when
    # locality_requirement.label_selector is empty: both 2-bundle gangs place cleanly.
    pin_insts = [
        _run_node(
            "tpu",
            f"bp{s}-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: f"slice-{s}", _WID: str(w)},
            used={"TPU": 4} if w < 2 else None,
            rep=f"R{s}",
        )
        for s in range(2)
        for w in range(4)
    ]
    g_unpinned2 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_unpin2"))
    pinned_bundle_reqs = _spread_reqs(
        {"TPU": 4},
        2,
        "_pg_pin_bundle0",
        label_selectors=[[(_SLICE, _IN, ["slice-0"])]],
    )
    g_bundle_pinned0 = _strict_pack_gang(_SLICE, pinned_bundle_reqs)
    rep_pin_order = _sched(
        _mk_cfgs(0, 8), pin_insts, gangs=[g_unpinned2, g_bundle_pinned0]
    )
    assert rep_pin_order.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_pin_order) == ({}, [])

    miss_bundle_reqs = _spread_reqs(
        {"TPU": 4},
        2,
        "_pg_pin_bundle99",
        label_selectors=[[(_SLICE, _IN, ["slice-99"])]],
    )
    rep_pin_miss99 = _sched(
        _mk_cfgs(0, 8), pin_insts, gangs=[_strict_pack_gang(_SLICE, miss_bundle_reqs)]
    )
    assert len(rep_pin_miss99.infeasible_gang_resource_requests) == 1


def test_shared_vanished_batch_counter_across_multi_slice_placeholders_and_refills():
    """Shared multi-slice launch batches consume vanished_cids_by_lid once across
    incomplete slices rather than over-stamping placeholders or minting extra heads."""
    head_res = "TPU-v6e-16-head"
    cfgs12 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=12,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    s0 = [
        _run_node(
            "tpu",
            f"s0-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L_s0" if w == 0 else "L_van",
        )
        for w in range(2)
    ]
    s1 = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4, **({head_res: 1} if w == 0 else {})},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            lid="L_s1" if w == 0 else "L_van",
        )
        for w in range(2)
    ]
    req_van = _pending_node("tpu", "req-van", Instance.REQUESTED, lid="L_van")
    van_im = Instance(
        instance_id="van-pod",
        instance_type="tpu",
        status=Instance.TERMINATED,
        cloud_instance_id="cloud-van-pod",
        launch_request_id="L_van",
        node_kind=NodeKind.WORKER,
    )
    van_im.status_history.add(instance_status=Instance.ALLOCATED)
    van_im.status_history.add(instance_status=Instance.TERMINATED)
    van_inst = make_autoscaler_instance(
        im_instance=van_im, cloud_instance_id=van_im.cloud_instance_id
    )
    all_insts = s0 + s1 + [req_van, van_inst]
    cloud_map = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in s0 + s1
    }
    view = _MultiHostReplicaView.build(
        [a.im_instance for a in all_insts], cloud_map, {"tpu": 4}
    )
    g3 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_g3"))
    rep_g3 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs12,
            current_instances=all_insts,
            gang_resource_requests=[g3],
            disable_launch_config_check=True,
            replica_view=view,
        )
    )
    assert rep_g3.infeasible_gang_resource_requests == []
    assert [r.count for r in rep_g3.to_launch] == [6]

    # Verify exact Reconciler._scale_cluster alignment (created QUEUED == planned == 6).
    store = InstanceStorage(cluster_id="van-seam", storage=InMemoryStorage())
    im_mgr = InstanceManager(
        instance_storage=store, instance_status_update_subscribers=[]
    )
    head_im = Instance(
        instance_id="head-0",
        instance_type="head",
        status=Instance.RAY_RUNNING,
        node_id=(b"h" * 28).hex(),
        cloud_instance_id="c-head",
        node_kind=NodeKind.HEAD,
    )
    store.batch_upsert_instances(
        [head_im] + [a.im_instance for a in all_insts], expected_storage_version=0
    )
    as_cfg = AutoscalingConfig(
        configs={
            "cluster_name": "van-seam",
            "max_workers": 12,
            "upscaling_speed": 100.0,
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "tpu": {
                    "node_config": {},
                    "resources": {"CPU": 8, "TPU": 4},
                    "min_workers": 0,
                    "max_workers": 12,
                    "runtime_label_keys": [_SLICE, _WID],
                    "replica_config": {
                        "workers_per_replica": 4,
                        "domain_label_key": _SLICE,
                        "domain_exclusive": True,
                        "worker_index_label_key": _WID,
                        "head_resources": [head_res],
                    },
                },
            },
        },
        skip_content_hash=True,
    )

    class _FixedSched(IResourceScheduler):
        def schedule(self, request):
            return rep_g3

    Reconciler._scale_cluster(
        autoscaling_state=AutoscalingState(),
        instance_manager=im_mgr,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_state=ClusterResourceState(
            node_states=[
                NodeState(
                    node_id=b"h" * 28,
                    instance_id="c-head",
                    ray_node_type_name="head",
                    status=NodeStatus.RUNNING,
                )
            ]
            + [a.ray_node for a in s0 + s1]
        ),
        scheduler=_FixedSched(),
        autoscaling_config=as_cfg,
        cloud_provider=None,
        non_terminated_cloud_instances={
            "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
            **cloud_map,
        },
        replica_sizes={"tpu": 4},
    )
    created = sum(
        1 for v in store.get_instances()[0].values() if v.status == Instance.QUEUED
    )
    assert created == sum(r.count for r in rep_g3.to_launch) == 6

    # v6e-32 (size=8, max_worker_nodes=24): two 4-of-8 slices sharing L_van8 (1 REQUESTED +
    # 4 vanished) + one full 8-of-8 slice never mints a phantom 4th TPU-v6e-32-head.
    head32 = "TPU-v6e-32-head"
    cfgs24 = {
        "tpu32": NodeTypeConfig(
            name="tpu32",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=24,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=8,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head32,),
            ),
        )
    }
    insts32 = []
    for s_idx, cnt in ((0, 4), (1, 4), (2, 8)):
        for w in range(cnt):
            insts32.append(
                _run_node(
                    "tpu32",
                    f"v32-{s_idx}-{w}",
                    {"CPU": 8, "TPU": 4, **({head32: 1} if w == 0 else {})},
                    {_SLICE: f"slice32-{s_idx}", _WID: str(w)},
                    lid=f"L32_{s_idx}" if w == 0 or s_idx == 2 else "L_van8",
                )
            )
    req_van8 = _pending_node("tpu32", "req-van8", Instance.REQUESTED, lid="L_van8")
    van8 = []
    for k in range(4):
        vi = Instance(
            instance_id=f"van8-{k}",
            instance_type="tpu32",
            status=Instance.TERMINATED,
            cloud_instance_id=f"cloud-van8-{k}",
            launch_request_id="L_van8",
            node_kind=NodeKind.WORKER,
        )
        vi.status_history.add(instance_status=Instance.ALLOCATED)
        vi.status_history.add(instance_status=Instance.TERMINATED)
        van8.append(
            make_autoscaler_instance(
                im_instance=vi, cloud_instance_id=vi.cloud_instance_id
            )
        )
    cmap32 = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu32", NodeKind.WORKER, True
        )
        for a in insts32
    }
    all32 = insts32 + [req_van8] + van8
    view32 = _MultiHostReplicaView.build(
        [a.im_instance for a in all32], cmap32, {"tpu32": 8}
    )
    rep32 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs24,
            current_instances=all32,
            resource_requests=[_resource_request({head32: 1}, 4)],
            disable_launch_config_check=True,
            replica_view=view32,
        )
    )
    assert rep32.cluster_resources[head32] == 3.0
    assert len(rep32.infeasible_resource_requests) == 1


def test_refill_attachment_prioritizes_no_backfill_slices_before_placeholder_backed():
    """Torn-down no_backfill slices receive QUEUED and unattributed ALLOCATED refill
    pods ahead of busy placeholder-backed slices."""
    cfgs12 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=12,
            idle_timeout_s=30.0,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    # slice-0: 3-of-4 expired-idle torn-down slice (no_backfill=True, priority=4).
    td0 = [
        _run_node(
            "tpu",
            f"td0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            idle_ms=60_000,
            lid="L_td0",
        )
        for w in range(3)
    ]
    td0_dead = Instance(
        instance_id="td0-dead",
        instance_type="tpu",
        status=Instance.TERMINATED,
        cloud_instance_id="cloud-td0-dead",
        launch_request_id="L_td0",
        node_kind=NodeKind.WORKER,
    )
    td0_dead.status_history.add(instance_status=Instance.RAY_RUNNING)
    td0_dead.status_history.add(instance_status=Instance.TERMINATING)
    td0_dead.status_history.add(instance_status=Instance.TERMINATED)
    # slice-1: 2-of-4 busy + 1 ALLOCATED pod (no_backfill=False, priority=2).
    s1_alloc = _pending_node("tpu", "s1-alloc", Instance.ALLOCATED, lid="L_s1")
    s1_alloc.cloud_instance_id = "cloud-s1-alloc"
    s1_alloc.im_instance.cloud_instance_id = "cloud-s1-alloc"
    s1 = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            lid="L_s1",
        )
        for w in range(2)
    ] + [s1_alloc]
    # slice-2: 1-of-4 busy pod (no_backfill=False).
    s2 = [
        _run_node(
            "tpu",
            "s2-0",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-2", _WID: "0"},
            used={"TPU": 4},
            lid="L_s2",
        )
    ]
    q_ref = _pending_node("tpu", "q-ref-0", Instance.QUEUED)
    live_pods = td0 + s1 + s2
    all_insts = live_pods + [
        make_autoscaler_instance(
            im_instance=td0_dead, cloud_instance_id=td0_dead.cloud_instance_id
        ),
        q_ref,
    ]
    cloud_map = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in live_pods
    }
    view = _MultiHostReplicaView.build(
        [a.im_instance for a in all_insts], cloud_map, {"tpu": 4}
    )
    rep_idle = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs12,
            current_instances=all_insts,
            idle_timeout_s=30.0,
            disable_launch_config_check=True,
            replica_view=view,
        )
    )
    assert rep_idle.to_terminate == []

    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_td0_gang"))
    rep_gang = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs12,
            current_instances=all_insts,
            gang_resource_requests=[g4],
            idle_timeout_s=30.0,
            disable_launch_config_check=True,
            replica_view=view,
        )
    )
    assert rep_gang.infeasible_gang_resource_requests == []
    assert rep_gang.to_launch == []

    # Unattributed ALLOCATED pod (lid=None) also prioritizes torn-down td0 ahead of s1/s2
    # via _attach_unattributed_booting so a 4-host gang fits on td0 + u_alloc without launch.
    u_alloc = _pending_node("tpu", "u-alloc-0", Instance.ALLOCATED)
    u_alloc.cloud_instance_id = "cloud-u-alloc-0"
    u_alloc.im_instance.cloud_instance_id = "cloud-u-alloc-0"
    u_live = td0 + s1 + s2 + [u_alloc]
    u_insts = u_live + [
        make_autoscaler_instance(
            im_instance=td0_dead, cloud_instance_id=td0_dead.cloud_instance_id
        )
    ]
    u_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in u_live
    }
    u_view = _MultiHostReplicaView.build(
        [a.im_instance for a in u_insts], u_cmap, {"tpu": 4}
    )
    rep_u_alloc = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs12,
            current_instances=u_insts,
            gang_resource_requests=[g4],
            idle_timeout_s=30.0,
            disable_launch_config_check=True,
            replica_view=u_view,
        )
    )
    assert rep_u_alloc.infeasible_gang_resource_requests == []
    assert rep_u_alloc.to_launch == []
    assert rep_u_alloc.to_terminate == []


def test_solicited_boot_teardown_post_crash_sibling_drain_and_rejected_stop_history():
    """Solicited ALLOCATED->TERMINATING transitions, post-crash sibling drains, and
    rejected RAY_STOP_REQUESTED->RAY_RUNNING histories preserve exact draining scoping."""
    cfgs8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    s1_3 = [
        _run_node(
            "tpu",
            f"s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            lid="L_8",
        )
        for w in range(3)
    ]
    s1_crash_im = Instance(
        instance_id="s1-crash",
        instance_type="tpu",
        status=Instance.TERMINATED,
        cloud_instance_id="cloud-s1-crash",
        launch_request_id="L_8",
        node_kind=NodeKind.WORKER,
    )
    s1_crash_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    s1_crash_im.status_history.add(instance_status=Instance.RAY_STOPPED)
    s1_crash_im.status_history.add(instance_status=Instance.TERMINATING)
    s1_crash_im.status_history.add(instance_status=Instance.TERMINATED)
    s1_crash = make_autoscaler_instance(
        im_instance=s1_crash_im, cloud_instance_id=s1_crash_im.cloud_instance_id
    )
    g_pin_s1 = _strict_pack_gang(
        _SLICE,
        _spread_reqs(
            {"TPU": 4},
            3,
            "_pg_pin_s1",
            label_selectors=[[(_SLICE, _IN, ["slice-1"])]],
        ),
    )

    # 1. slice-0 had 3 RAY_RUNNING solicited stops + 1 ALLOCATED->TERMINATING pod.
    s0_boot_solic = []
    for k in range(3):
        im_k = Instance(
            instance_id=f"s0-run-term-{k}",
            instance_type="tpu",
            status=Instance.TERMINATING,
            cloud_instance_id=f"cloud-s0-rt-{k}",
            launch_request_id="L_8",
            node_kind=NodeKind.WORKER,
        )
        im_k.status_history.add(instance_status=Instance.RAY_RUNNING)
        im_k.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        im_k.status_history.add(instance_status=Instance.TERMINATING)
        s0_boot_solic.append(
            make_autoscaler_instance(
                im_instance=im_k, cloud_instance_id=im_k.cloud_instance_id
            )
        )
    im_alloc_term = Instance(
        instance_id="s0-alloc-term",
        instance_type="tpu",
        status=Instance.TERMINATING,
        cloud_instance_id="cloud-s0-alloc-term",
        launch_request_id="L_8",
        node_kind=NodeKind.WORKER,
    )
    im_alloc_term.status_history.add(instance_status=Instance.ALLOCATED)
    im_alloc_term.status_history.add(instance_status=Instance.TERMINATING)
    s0_boot_solic.append(
        make_autoscaler_instance(
            im_instance=im_alloc_term, cloud_instance_id=im_alloc_term.cloud_instance_id
        )
    )
    rep_2a = _sched(cfgs8, s0_boot_solic + s1_3 + [s1_crash], gangs=[g_pin_s1])
    assert rep_2a.infeasible_gang_resource_requests == []

    # 2. slice-0 had 1 prior crash + 3 solicited TERMINATING pods alongside 3/4 slice-1.
    s0_prior_crash_im = Instance(
        instance_id="s0-prior-crash",
        instance_type="tpu",
        status=Instance.TERMINATED,
        cloud_instance_id="cloud-s0-prior-crash",
        launch_request_id="L_8",
        node_kind=NodeKind.WORKER,
    )
    s0_prior_crash_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    s0_prior_crash_im.status_history.add(instance_status=Instance.RAY_STOPPED)
    s0_prior_crash_im.status_history.add(instance_status=Instance.TERMINATING)
    s0_prior_crash_im.status_history.add(instance_status=Instance.TERMINATED)
    s0_prior_crash = make_autoscaler_instance(
        im_instance=s0_prior_crash_im,
        cloud_instance_id=s0_prior_crash_im.cloud_instance_id,
    )
    rep_2b = _sched(
        cfgs8,
        s0_boot_solic[:3] + [s0_prior_crash] + s1_3 + [s1_crash],
        gangs=[g_pin_s1],
    )
    assert rep_2b.infeasible_gang_resource_requests == []

    # 3. Rejected RAY_STOP_REQUESTED -> RAY_RUNNING followed by unsolicited crash on u-3
    # keeps surviving 3/4 slice-1 schedulable at max_worker_nodes=4.
    from dataclasses import replace

    cfgs4 = {"tpu": replace(cfgs8["tpu"], max_worker_nodes=4)}
    rej_surv = []
    for w in range(3):
        inst = _run_node(
            "tpu",
            f"rej-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            lid="L_rej",
        )
        inst.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
        inst.im_instance.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        inst.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
        rej_surv.append(inst)
    rej_crash_im = Instance(
        instance_id="rej-3",
        instance_type="tpu",
        status=Instance.TERMINATING,
        cloud_instance_id="cloud-rej-3",
        launch_request_id="L_rej",
        node_kind=NodeKind.WORKER,
    )
    rej_crash_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    rej_crash_im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    rej_crash_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    rej_crash_im.status_history.add(instance_status=Instance.RAY_STOPPED)
    rej_crash_im.status_history.add(instance_status=Instance.TERMINATING)
    rep_rej = _sched(
        cfgs4,
        rej_surv
        + [
            make_autoscaler_instance(
                im_instance=rej_crash_im,
                cloud_instance_id=rej_crash_im.cloud_instance_id,
            )
        ],
        gangs=[g_pin_s1],
    )
    assert rep_rej.infeasible_gang_resource_requests == []

    # 4. Counter-case: Domainless 4-worker batch at max_worker_nodes=4 where 3 running
    # peers had rejected drains and 1 peer is actively solicited-draining marks replica draining.
    gpu4 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    domless_run3 = []
    for w in range(3):
        gn = _run_node(
            "gpu", f"dl-{w}", {"CPU": 8, "GPU": 8}, {}, used={"GPU": 8}, lid="L_dl"
        )
        gn.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
        gn.im_instance.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        gn.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
        domless_run3.append(gn)
    dl_term_im = Instance(
        instance_id="dl-3",
        instance_type="gpu",
        status=Instance.TERMINATING,
        cloud_instance_id="cloud-dl-3",
        launch_request_id="L_dl",
        node_kind=NodeKind.WORKER,
    )
    dl_term_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    dl_term_im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    dl_term_im.status_history.add(instance_status=Instance.TERMINATING)
    rep_dl = _sched(
        gpu4,
        domless_run3
        + [
            make_autoscaler_instance(
                im_instance=dl_term_im, cloud_instance_id=dl_term_im.cloud_instance_id
            )
        ],
        reqs=[_resource_request({"GPU": 8}, 1)],
    )
    assert len(rep_dl.infeasible_resource_requests) == 1
    assert rep_dl.to_launch == []

    # 5. 2-slice batch L_8 where slice-1 had historical rejected-stop entries
    # and later crashed 1 pod while slice-0 drains 3 solicited pods + 1 crash does NOT poison slice-1.
    s1_rej_l8 = []
    for w in range(3):
        inst = _run_node(
            "tpu",
            f"s1-rej-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            lid="L_8",
        )
        inst.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
        inst.im_instance.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        inst.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
        s1_rej_l8.append(inst)
    rep_4b = _sched(
        cfgs8,
        s0_boot_solic[:3] + [s0_prior_crash] + s1_rej_l8 + [s1_crash],
        gangs=[g_pin_s1],
    )
    assert rep_4b.infeasible_gang_resource_requests == []

    # 6. Counter-case: slice-0 exited (3 solicited TERMINATING + 1 crash) and slice-1 has
    # 3 RAY_RUNNING + 1 active RAY_STOPPING with live DRAINING ray_node -> slice-1 IS draining.
    s1_stopping_im = Instance(
        instance_id="s1-stopping-3",
        instance_type="tpu",
        status=Instance.RAY_STOPPING,
        node_id=b"s1st3".ljust(28, b"0").hex(),
        cloud_instance_id="cloud-s1-stopping-3",
        launch_request_id="L_8",
        node_kind=NodeKind.WORKER,
    )
    s1_stopping_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    s1_stopping_im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    s1_stopping_im.status_history.add(instance_status=Instance.RAY_STOPPING)
    rep_4a = _sched(
        cfgs8,
        s0_boot_solic[:3]
        + [
            s0_prior_crash,
            make_autoscaler_instance(
                im_instance=s1_stopping_im,
                ray_node=NodeState(
                    node_id=b"s1st3".ljust(28, b"0"),
                    instance_id=s1_stopping_im.cloud_instance_id,
                    ray_node_type_name="tpu",
                    status=NodeStatus.DRAINING,
                    total_resources={"CPU": 8, "TPU": 4},
                    available_resources={"CPU": 8, "TPU": 4},
                    labels={_SLICE: "slice-1", _WID: "3"},
                ),
                cloud_instance_id=s1_stopping_im.cloud_instance_id,
            ),
        ]
        + s1_3,
        gangs=[g_pin_s1],
    )
    assert len(rep_4a.infeasible_gang_resource_requests) == 1

    # 7. 2+2 split refill batch L_co across slice-0 and slice-1 (max_w=8) where slice-1
    # scales down via solicited stop and slice-0 later crashes 1 original L0 pod preserves slice-0.
    g_pin_s0 = _strict_pack_gang(
        _SLICE,
        _spread_reqs(
            {"TPU": 4},
            3,
            "_pg_pin_s0",
            label_selectors=[[(_SLICE, _IN, ["slice-0"])]],
        ),
    )
    s0_l0_live = _run_node(
        "tpu",
        "s0-l0-0",
        {"CPU": 8, "TPU": 4},
        {_SLICE: "slice-0", _WID: "0"},
        lid="L_0",
    )
    s0_l0_crash_im = Instance(
        instance_id="s0-l0-crash",
        instance_type="tpu",
        status=Instance.TERMINATED,
        cloud_instance_id="cloud-s0-l0-crash",
        launch_request_id="L_0",
        node_kind=NodeKind.WORKER,
    )
    s0_l0_crash_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    s0_l0_crash_im.status_history.add(instance_status=Instance.RAY_STOPPED)
    s0_l0_crash_im.status_history.add(instance_status=Instance.TERMINATING)
    s0_l0_crash_im.status_history.add(instance_status=Instance.TERMINATED)
    s0_lco_live = [
        _run_node(
            "tpu",
            f"s0-lco-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L_co",
        )
        for w in (1, 2)
    ]
    s1_lco_solic = []
    for k in range(2):
        im_co = Instance(
            instance_id=f"s1-lco-term-{k}",
            instance_type="tpu",
            status=Instance.TERMINATING,
            cloud_instance_id=f"cloud-s1-lco-{k}",
            launch_request_id="L_co",
            node_kind=NodeKind.WORKER,
        )
        im_co.status_history.add(instance_status=Instance.RAY_RUNNING)
        im_co.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        im_co.status_history.add(instance_status=Instance.TERMINATING)
        s1_lco_solic.append(
            make_autoscaler_instance(
                im_instance=im_co, cloud_instance_id=im_co.cloud_instance_id
            )
        )
    rep_split_co = _sched(
        cfgs8,
        [
            s0_l0_live,
            make_autoscaler_instance(
                im_instance=s0_l0_crash_im,
                cloud_instance_id=s0_l0_crash_im.cloud_instance_id,
            ),
        ]
        + s0_lco_live
        + s1_lco_solic,
        gangs=[g_pin_s0],
    )
    assert rep_split_co.infeasible_gang_resource_requests == []


def test_omitted_stopping_pods_align_scheduler_and_reconciler():
    """Reachable RAY_STOPPING instances (with live DRAINING ray_nodes) occupy capacity
    identically across ScheduleContext, _MultiHostReplicaView, and Reconciler._scale_cluster."""

    def _mk_cfgs(
        max_w: int, min_w: int = 0, head_res: Optional[str] = "TPU-v6e-16-head"
    ):
        res = {"CPU": 8, "TPU": 4}
        if head_res:
            res[head_res] = 1
        return {
            "tpu": NodeTypeConfig(
                name="tpu",
                resources=res,
                min_worker_nodes=min_w,
                max_worker_nodes=max_w,
                runtime_label_keys=(_SLICE, _WID),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=_SLICE,
                    domain_exclusive=True,
                    worker_index_label_key=_WID,
                    head_resources=(head_res,) if head_res else (),
                ),
            )
        }

    def _draining_stop(
        iid: str,
        cid: str,
        slice_val: str,
        wid: int,
        lid: str = "",
        rep: Optional[str] = None,
    ):
        nid = iid.encode().ljust(28, b"s")[:28]
        im = Instance(
            instance_id=iid,
            instance_type="tpu",
            status=Instance.RAY_STOPPING,
            node_id=nid.hex(),
            cloud_instance_id=cid,
            launch_request_id=lid,
            node_kind=NodeKind.WORKER,
        )
        im.status_history.add(instance_status=Instance.RAY_RUNNING)
        im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        im.status_history.add(instance_status=Instance.RAY_STOPPING)
        return AutoscalerInstance(
            im_instance=im,
            ray_node=NodeState(
                node_id=nid,
                instance_id=cid,
                ray_node_type_name="tpu",
                status=NodeStatus.DRAINING,
                total_resources={"CPU": 8, "TPU": 4},
                available_resources={"CPU": 8, "TPU": 4},
                labels={_SLICE: slice_val, _WID: str(wid)},
            ),
            cloud_instance_id=cid,
            cloud_instance_replica_id=rep,
        )

    def _run_seam(cfgs, insts, *, reqs=None, gangs=None, max_w_global=8):
        store = InstanceStorage(cluster_id="seam", storage=InMemoryStorage())
        im_mgr = InstanceManager(
            instance_storage=store, instance_status_update_subscribers=[]
        )
        head_im = Instance(
            instance_id="head-0",
            instance_type="head",
            status=Instance.RAY_RUNNING,
            node_id=(b"u" * 28).hex(),
            cloud_instance_id="c-head",
            node_kind=NodeKind.HEAD,
        )
        head_rn = NodeState(
            node_id=b"u" * 28,
            instance_id="c-head",
            ray_node_type_name="head",
            status=NodeStatus.RUNNING,
        )
        im_copies = []
        for a in insts:
            im_cp = copy.deepcopy(a.im_instance)
            if a.ray_node is not None and a.ray_node.node_id:
                im_cp.node_id = a.ray_node.node_id.hex()
            im_copies.append(im_cp)
        store.batch_upsert_instances([head_im] + im_copies, expected_storage_version=0)
        ant = {"head": {"node_config": {}, "resources": {"CPU": 0}}}
        for nt, c in cfgs.items():
            rc = c.replica_config
            ant[nt] = {
                "node_config": {},
                "resources": dict(c.resources),
                "labels": dict(c.labels),
                "min_workers": c.min_worker_nodes,
                "max_workers": c.max_worker_nodes,
                "runtime_label_keys": list(c.runtime_label_keys),
                "replica_config": {
                    "workers_per_replica": rc.workers_per_replica,
                    "domain_label_key": rc.domain_label_key,
                    "domain_exclusive": rc.domain_exclusive,
                    "worker_index_label_key": rc.worker_index_label_key,
                    "head_resources": list(rc.head_resources),
                },
            }
        acfg = AutoscalingConfig(
            configs={
                "cluster_name": "seam",
                "max_workers": max_w_global,
                "upscaling_speed": 100.0,
                "provider": {"type": "aws", "disable_launch_config_check": True},
                "head_node_type": "head",
                "available_node_types": ant,
            },
            skip_content_hash=True,
        )
        cmap = {
            "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
            **{
                a.im_instance.cloud_instance_id: CloudInstance(
                    a.im_instance.cloud_instance_id,
                    a.im_instance.instance_type,
                    NodeKind.WORKER,
                    True,
                    replica_id=a.cloud_instance_replica_id,
                )
                for a in insts
                if a.im_instance.cloud_instance_id
                and a.im_instance.status
                not in (Instance.TERMINATED, Instance.TERMINATING)
            },
        }
        rs = {
            nt: c.workers_per_replica
            for nt, c in cfgs.items()
            if c.is_multi_worker_replica
        }
        Reconciler._scale_cluster(
            autoscaling_state=AutoscalingState(),
            instance_manager=im_mgr,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(
                node_states=[head_rn]
                + [a.ray_node for a in insts if a.ray_node is not None],
                pending_resource_requests=[
                    ResourceRequestByCount(request=r, count=1) for r in (reqs or [])
                ],
                pending_gang_resource_requests=gangs or [],
            ),
            scheduler=ResourceDemandScheduler(),
            autoscaling_config=acfg,
            cloud_provider=None,
            non_terminated_cloud_instances=cmap,
            replica_sizes=rs,
        )
        return sum(
            1 for v in store.get_instances()[0].values() if v.status == Instance.QUEUED
        )

    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_stop_align"))

    # 1. Provider-labeled: 4 RAY_STOPPING pods on R0 + 4 busy pods on R1 at max=8 rejects 2nd gang.
    r0_stop = [
        _draining_stop(f"r0-stop-{w}", f"c-r0-{w}", "slice-0", w, rep="R0")
        for w in range(4)
    ]
    r1_busy = [
        _run_node(
            "tpu",
            f"r1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            rep="R1",
        )
        for w in range(4)
    ]
    prov_all = r0_stop + r1_busy
    prov_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id,
            "tpu",
            NodeKind.WORKER,
            True,
            replica_id=a.cloud_instance_replica_id,
        )
        for a in prov_all
    }
    rep_prov = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=_mk_cfgs(8),
            current_instances=prov_all,
            gang_resource_requests=[g4],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in prov_all], prov_cmap, {"tpu": 4}
            ),
        )
    )
    assert len(rep_prov.infeasible_gang_resource_requests) == 1
    assert rep_prov.to_launch == []

    # 2. Unlabeled: 3 RAY_STOPPING pods at max=4 launches 0; at max=8 launches 5
    # (1 alignment refill + 4 fresh slice) matching Reconciler._scale_cluster.
    u3 = [
        _draining_stop(f"u-stop-{k}", f"c-ustop-{k}", "slice-0", k, lid="L_s3")
        for k in range(3)
    ]
    u3_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in u3
    }
    u3_view = _MultiHostReplicaView.build(
        [a.im_instance for a in u3], u3_cmap, {"tpu": 4}
    )
    rep_u4 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=_mk_cfgs(4),
            current_instances=u3,
            gang_resource_requests=[g4],
            disable_launch_config_check=True,
            replica_view=u3_view,
        )
    )
    assert len(rep_u4.infeasible_gang_resource_requests) == 1
    assert rep_u4.to_launch == []

    rep_u8 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=_mk_cfgs(8),
            current_instances=u3,
            gang_resource_requests=[g4],
            disable_launch_config_check=True,
            replica_view=u3_view,
        )
    )
    assert rep_u8.infeasible_gang_resource_requests == []
    assert [r.count for r in rep_u8.to_launch] == [5]
    assert rep_u8.cluster_resources.get("TPU-v6e-16-head", 0.0) == 1.0
    assert _run_seam(_mk_cfgs(8), u3, gangs=[g4], max_w_global=8) == 5

    # 3. 3 running + 1 RAY_STOPPING at min_w=4, max_w=4 (0 demand) launches 0 pods.
    mix_run3 = [
        _run_node(
            "tpu",
            f"mix-{w}",
            {"CPU": 8, "TPU": 4, **({"TPU-v6e-16-head": 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L0",
        )
        for w in range(3)
    ]
    stop1_on_l0 = _draining_stop("mix-stop-3", "c-mix-stop-3", "slice-0", 3, lid="L0")
    mix_stop4 = mix_run3 + [stop1_on_l0]
    mix_stop_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in mix_stop4
    }
    rep_stop_min4 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=_mk_cfgs(4, min_w=4),
            current_instances=mix_stop4,
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in mix_stop4], mix_stop_cmap, {"tpu": 4}
            ),
        )
    )
    assert rep_stop_min4.to_launch == []

    # 4. 1 RAY_STOPPING pod alongside 3/4 and 2/4 torn-down slices at max_w=8 (6 cloud pods)
    # refills 2 slots cleanly instead of over-reserving 4 slots for 1 RAY_STOPPING pod.
    td_s0 = [
        _run_node(
            "tpu",
            f"tds0-{w}",
            {"CPU": 8, "TPU": 4, **({"TPU-v6e-16-head": 1} if w == 0 else {})},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L_td_a",
        )
        for w in range(3)
    ]
    td_s1 = [
        _run_node(
            "tpu",
            f"tds1-{w}",
            {"CPU": 8, "TPU": 4, **({"TPU-v6e-16-head": 1} if w == 0 else {})},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            lid="L_td_b",
        )
        for w in range(2)
    ]
    td_deads = [
        _terminated_teardown_row("tpu", "tda-dead", "L_td_a"),
        _terminated_teardown_row("tpu", "tdb-dead", "L_td_b"),
    ]
    c11_live = (
        td_s0
        + td_s1
        + [_draining_stop("u-stop-td", "c-ustop-td", "slice-old", 0, lid="L_old")]
    )
    c11_all = c11_live + td_deads
    c11_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in c11_live
    }
    rep_c11_1a = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=_mk_cfgs(8),
            current_instances=c11_all,
            resource_requests=[_resource_request({"TPU": 4}, 2)],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in c11_all], c11_cmap, {"tpu": 4}
            ),
        )
    )
    assert rep_c11_1a.infeasible_resource_requests == []
    assert [r.count for r in rep_c11_1a.to_launch] == [2]

    # 5. 3 unlabeled RAY_STOP_REQUESTED + 1 TERMINATED pod at max_w=8 scaling up
    # a fresh 4-host gang plans 5 launches (matching Reconciler._scale_cluster) and strips non-head head_resources.
    sr3 = []
    for w in range(3):
        rn = _run_node(
            "tpu",
            f"sr-{w}",
            {"CPU": 8, "TPU": 4, **({"TPU-v6e-16-head": 1} if w == 0 else {})},
            {_SLICE: "slice-sr", _WID: str(w)},
            lid="L_sr",
        )
        rn.im_instance.status = Instance.RAY_STOP_REQUESTED
        sr3.append(rn)
    sr_all = sr3 + [_terminated_teardown_row("tpu", "sr-dead-3", "L_sr")]
    sr_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in sr3
    }
    rep_sr8 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=_mk_cfgs(8),
            current_instances=sr_all,
            gang_resource_requests=[g4],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in sr_all], sr_cmap, {"tpu": 4}
            ),
        )
    )
    assert rep_sr8.infeasible_gang_resource_requests == []
    assert [r.count for r in rep_sr8.to_launch] == [5]
    assert rep_sr8.cluster_resources.get("TPU-v6e-16-head", 0.0) == 2.0
    assert _run_seam(_mk_cfgs(8), sr_all, gangs=[g4], max_w_global=8) == 5


def test_unattributed_unlabeled_partial_replicas_refill_up_to_max_workers():
    """Unattributed unlabeled partial ALLOCATED and domainless RAY_RUNNING groups
    refill to a full replica at max_worker_nodes == size instead of deadlocking."""
    head_res = "TPU-v6e-16-head"
    tpu4 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=(head_res,),
            ),
        )
    }
    alloc2 = [_pending_node("tpu", f"a-{k}", Instance.ALLOCATED) for k in range(2)]
    for k, a in enumerate(alloc2):
        a.cloud_instance_id = f"c-alloc-{k}"
        a.im_instance.cloud_instance_id = f"c-alloc-{k}"
    alloc_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in alloc2
    }
    alloc_view = _MultiHostReplicaView.build(
        [a.im_instance for a in alloc2], alloc_cmap, {"tpu": 4}
    )
    g4_head = _strict_pack_gang(
        _SLICE,
        [_mk_req({"CPU": 8, "TPU": 4, head_res: 1}, [(_ANTI, "_pg_uh", "1")])]
        + _spread_reqs({"CPU": 8, "TPU": 4}, 4, "_pg_uh")[1:],
    )
    rep_alloc = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=tpu4,
            current_instances=alloc2,
            gang_resource_requests=[g4_head],
            disable_launch_config_check=True,
            replica_view=alloc_view,
        )
    )
    assert rep_alloc.infeasible_gang_resource_requests == []
    assert [r.count for r in rep_alloc.to_launch] == [2]

    # Next tick: alloc2 + 2 QUEUED refill pods attach onto the same replica
    # and satisfy g4_head with zero launches and zero sub-replica cancellations.
    queued2 = [
        _pending_node("tpu", f"q-alloc-{k}", Instance.QUEUED, lid="L_qref")
        for k in range(2)
    ]
    rep_alloc_t2 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=tpu4,
            current_instances=alloc2 + queued2,
            gang_resource_requests=[g4_head],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in alloc2 + queued2], alloc_cmap, {"tpu": 4}
            ),
        )
    )
    assert rep_alloc_t2.infeasible_gang_resource_requests == []
    assert rep_alloc_t2.to_launch == []
    assert rep_alloc_t2.to_terminate == []

    # 1 RAY_INSTALLING pod on L2 + 1 doomed QUEUED pod cancels the
    # sub-replica QUEUED remainder without shrinking L2's 4-slot placeholder replica.
    inst_l2 = _pending_node("tpu", "inst-l2-0", Instance.RAY_INSTALLING, lid="L2")
    inst_l2.cloud_instance_id = "c-inst-l2-0"
    inst_l2.im_instance.cloud_instance_id = "c-inst-l2-0"
    doomed_q1 = _pending_node("tpu", "q-doomed-0", Instance.QUEUED, lid="L_q1")
    l2_cmap = {
        "c-inst-l2-0": CloudInstance("c-inst-l2-0", "tpu", NodeKind.WORKER, True)
    }
    rep_2943 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=tpu4,
            current_instances=[inst_l2, doomed_q1],
            gang_resource_requests=[g4_head],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [inst_l2.im_instance, doomed_q1.im_instance], l2_cmap, {"tpu": 4}
            ),
        )
    )
    assert rep_2943.infeasible_gang_resource_requests == []
    assert rep_2943.to_launch == []
    assert [t.instance_id for t in rep_2943.to_terminate] == ["q-doomed-0"]

    # 2 unattributed unlabeled RAY_RUNNING pods (lid="") on domain_label_key
    # type tpu4 refill 2 slots instead of deadlocking at max_w=4.
    dom_run2 = [
        _run_node("tpu", f"dr-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4})
        for k in range(2)
    ]
    dom_run_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in dom_run2
    }
    rep_dom_run = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=tpu4,
            current_instances=dom_run2,
            resource_requests=[_resource_request({"TPU": 4}, 2)],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in dom_run2], dom_run_cmap, {"tpu": 4}
            ),
        )
    )
    assert rep_dom_run.infeasible_resource_requests == []
    assert [r.count for r in rep_dom_run.to_launch] == [2]

    # Domainless size=2, max_w=6 with 1 RAY_RUNNING (lid="") +
    # 1 RAY_STOP_REQUESTED (lid="L1") + 3 GPU tasks aligns planned == created == 4.
    gpu2_6 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=6,
            replica_config=WorkerReplicaConfig(workers_per_replica=2),
        )
    }
    r5_run = _run_node("gpu", "r5-run", {"CPU": 8, "GPU": 8}, {}, used={"GPU": 8})
    r5_sr = _run_node("gpu", "r5-sr", {"CPU": 8, "GPU": 8}, {}, lid="L1")
    r5_sr.im_instance.status = Instance.RAY_STOP_REQUESTED
    r5_insts = [r5_run, r5_sr]
    r5_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "gpu", NodeKind.WORKER, True
        )
        for a in r5_insts
    }
    rep_r5 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=gpu2_6,
            current_instances=r5_insts,
            resource_requests=[_resource_request({"GPU": 8}, 3)],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in r5_insts], r5_cmap, {"gpu": 2}
            ),
        )
    )
    assert rep_r5.infeasible_resource_requests == []
    assert [r.count for r in rep_r5.to_launch] == [4]

    gpu4 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 8},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    run2 = [
        _run_node("gpu", f"g-{k}", {"CPU": 8, "GPU": 8}, {}, used={"GPU": 8})
        for k in range(2)
    ]
    run_cmap = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "gpu", NodeKind.WORKER, True
        )
        for a in run2
    }
    run_view = _MultiHostReplicaView.build(
        [a.im_instance for a in run2], run_cmap, {"gpu": 4}
    )
    rep_gpu = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=gpu4,
            current_instances=run2,
            resource_requests=[_resource_request({"GPU": 8}, 2)],
            disable_launch_config_check=True,
            replica_view=run_view,
        )
    )
    assert rep_gpu.infeasible_resource_requests == []
    assert [r.count for r in rep_gpu.to_launch] == [2]

    # At max_worker_nodes=8, 2 busy domainless RAY_RUNNING pods (lid="") + 4-GPU gang
    # co-launches 2 refill + 4 fresh = 6 TO_LAUNCH nodes (LaunchRequest(count=6)).
    gpu8 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 8},
            labels=GPU_LABELS,
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    rep_gpu8 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=gpu8,
            current_instances=run2,
            gang_resource_requests=[_gpu_gang(4)],
            disable_launch_config_check=True,
            replica_view=run_view,
        )
    )
    assert rep_gpu8.infeasible_gang_resource_requests == []
    assert [r.count for r in rep_gpu8.to_launch] == [6]

    # Unattributed open slots on a torn-down unlabeled partial replica satisfy min_worker_nodes=4
    # without launching extra nodes.
    cfgs_unattr_min4 = {"tpu": _mh_tpu_cfg(min_w=4, max_w=8)}
    unattr_td_insts = [
        _run_node("tpu", f"uo-min-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4})
        for k in range(2)
    ] + [_mh_term("tpu", f"uo-min-d{k}", lid="", with_td=True) for k in range(2)]
    assert _sched(cfgs_unattr_min4, unattr_td_insts).to_launch == []


def test_doomed_requested_caps_leaving_donor_isolation_and_min_worker_alignment():
    """Verify invariants across doomed REQUESTED max_workers caps,
    leaving-node donor-pool isolation, omitted RAY_STOPPING min_workers alignment,
    and partial refill completion in materialize_unlabeled_launch_gaps."""

    def _sched_planned(cfgs, insts, *, reqs=None, gangs=None, max_nodes=None):
        for a in insts:
            if a.cloud_instance_id and not a.im_instance.cloud_instance_id:
                a.im_instance.cloud_instance_id = a.cloud_instance_id
        cmap = {
            a.im_instance.cloud_instance_id: CloudInstance(
                a.im_instance.cloud_instance_id,
                a.im_instance.instance_type,
                NodeKind.WORKER,
                True,
                replica_id=a.cloud_instance_replica_id,
            )
            for a in insts
            if a.im_instance.cloud_instance_id
            and a.im_instance.status != Instance.TERMINATED
        }
        rs = {
            nt: c.workers_per_replica
            for nt, c in cfgs.items()
            if c.is_multi_worker_replica
        }
        view = _MultiHostReplicaView.build([a.im_instance for a in insts], cmap, rs)
        rep = ResourceDemandScheduler().schedule(
            SchedulingRequest(
                node_type_configs=cfgs,
                current_instances=insts,
                resource_requests=reqs or [],
                gang_resource_requests=gangs or [],
                max_num_nodes=max_nodes,
                disable_launch_config_check=True,
                replica_view=view,
            )
        )
        return rep, sum(r.count for r in rep.to_launch)

    # 1. Doomed REQUESTED rows on torn_down_lids do not inflate real_count
    # in _enforce_max_workers_per_type or _enforce_max_workers_global (no Tick-2 livelock),
    # while genuinely exceeding max_worker_nodes still terminates QUEUED nodes.
    def _tpu_excl(mn=0, mx=8):
        return {
            "tpu": NodeTypeConfig(
                name="tpu",
                resources={"CPU": 8, "TPU": 4},
                min_worker_nodes=mn,
                max_worker_nodes=mx,
                runtime_label_keys=(_SLICE, _WID),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=_SLICE,
                    domain_exclusive=True,
                    worker_index_label_key=_WID,
                ),
            )
        }

    sr0 = _run_node("tpu", "sr0", {"CPU": 8, "TPU": 4}, {_SLICE: "s0", _WID: "0"})
    sr0.im_instance.status = Instance.RAY_STOP_REQUESTED
    sr1 = _run_node("tpu", "sr1", {"CPU": 8, "TPU": 4}, {_SLICE: "s0", _WID: "1"})
    sr1.im_instance.status = Instance.RAY_STOP_REQUESTED
    inst_boot = make_autoscaler_instance(
        im_instance=Instance(
            instance_id="ib0",
            instance_type="tpu",
            status=Instance.RAY_INSTALLING,
            cloud_instance_id="c-ib0",
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id="c-ib0",
    )
    stop_td_im = Instance(
        instance_id="stop-td",
        instance_type="tpu",
        status=Instance.RAY_STOPPING,
        node_id=b"stoptd".ljust(28, b"0").hex(),
        cloud_instance_id="c-stop-td",
        launch_request_id="L_td",
        node_kind=NodeKind.WORKER,
    )
    stop_td_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    stop_td_im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    stop_td_im.status_history.add(instance_status=Instance.RAY_STOPPING)
    stop_td = make_autoscaler_instance(
        im_instance=stop_td_im,
        ray_node=NodeState(
            node_id=b"stoptd".ljust(28, b"0"),
            instance_id="c-stop-td",
            ray_node_type_name="tpu",
            status=NodeStatus.DRAINING,
            total_resources={"CPU": 8, "TPU": 4},
            available_resources={"CPU": 8, "TPU": 4},
            labels={_SLICE: "s-td", _WID: "0"},
        ),
        cloud_instance_id="c-stop-td",
    )
    doomed_reqs = [
        _pending_node("tpu", f"dreq-{k}", Instance.REQUESTED, lid="L_td")
        for k in range(2)
    ]
    term_td = Instance(
        instance_id="term-td",
        instance_type="tpu",
        status=Instance.TERMINATED,
        cloud_instance_id="c-term-td",
        launch_request_id="L_td",
        node_kind=NodeKind.WORKER,
    )
    term_td.status_history.add(instance_status=Instance.RAY_RUNNING)
    term_td.status_history.add(instance_status=Instance.RAY_STOPPED)
    term_td.status_history.add(instance_status=Instance.TERMINATING)
    term_td.status_history.add(instance_status=Instance.TERMINATED)
    queued4 = [_pending_tpu(f"q-{k}", Instance.QUEUED, "tpu") for k in range(4)]
    insts_285 = (
        [sr0, sr1, inst_boot, stop_td]
        + doomed_reqs
        + [make_autoscaler_instance(im_instance=term_td, cloud_instance_id="c-term-td")]
        + queued4
    )
    rep_285, _ = _sched_planned(
        _tpu_excl(0, 8),
        insts_285,
        reqs=[_resource_request({"TPU": 4}, 4)],
        max_nodes=8,
    )
    assert rep_285.to_terminate == []
    assert rep_285.to_launch == []
    # Counter-case: at max_worker_nodes=4, 3 real in ctx + 1 omitted + 4 QUEUED = 8 > 4 cancels QUEUED.
    rep_285_cap, _ = _sched_planned(_tpu_excl(0, 4), insts_285)
    assert {t.instance_id for t in rep_285_cap.to_terminate} == {
        f"q-{k}" for k in range(4)
    }

    # 2. Domainless RAY_STOP_REQUESTED (lid="") is never donated into a
    # healthy incomplete replica, preserving L2 placeholders at max_w=12 and aligning
    # 5 TO_LAUNCH nodes (1 alignment + 4 fresh) at max_w=16.
    def _dom4(mn=0, mx=12):
        return {
            "gpu": NodeTypeConfig(
                name="gpu",
                resources={"CPU": 8, "GPU": 8},
                labels=GPU_LABELS,
                min_worker_nodes=mn,
                max_worker_nodes=mx,
                replica_config=WorkerReplicaConfig(workers_per_replica=4),
            )
        }

    def _gpu_stopping(iid: str, cid: str, lid: str = ""):
        nid = iid.encode().ljust(28, b"g")[:28]
        im = Instance(
            instance_id=iid,
            instance_type="gpu",
            status=Instance.RAY_STOPPING,
            node_id=nid.hex(),
            cloud_instance_id=cid,
            launch_request_id=lid,
            node_kind=NodeKind.WORKER,
        )
        im.status_history.add(instance_status=Instance.RAY_RUNNING)
        im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
        im.status_history.add(instance_status=Instance.RAY_STOPPING)
        return make_autoscaler_instance(
            im_instance=im,
            ray_node=NodeState(
                node_id=nid,
                instance_id=cid,
                ray_node_type_name="gpu",
                status=NodeStatus.DRAINING,
                total_resources={"CPU": 8, "GPU": 8},
                available_resources={"CPU": 8, "GPU": 8},
                labels=dict(GPU_LABELS),
            ),
            cloud_instance_id=cid,
        )

    stop_l0 = _gpu_stopping("st-l0", "c-stl0", lid="L0")
    req_l0 = _pending_node("gpu", "req-l0", Instance.REQUESTED, lid="L0")
    sr_l0 = [
        _run_node("gpu", f"srl0-{k}", {"CPU": 8, "GPU": 8}, dict(GPU_LABELS), lid="L0")
        for k in range(2)
    ]
    for x in sr_l0:
        x.im_instance.status = Instance.RAY_STOP_REQUESTED
    stop_unattr = _gpu_stopping("st-u", "c-stu")
    sr_unattr = _run_node("gpu", "sr-u", {"CPU": 8, "GPU": 8}, dict(GPU_LABELS))
    sr_unattr.im_instance.status = Instance.RAY_STOP_REQUESTED
    run_l2 = _run_node(
        "gpu", "r-l2", {"CPU": 8, "GPU": 8}, dict(GPU_LABELS), {"GPU": 8}, lid="L2"
    )
    req_l2 = _pending_node("gpu", "req-l2", Instance.REQUESTED, lid="L2")
    insts_400 = [stop_l0, req_l0, *sr_l0, stop_unattr, sr_unattr, run_l2, req_l2]
    rep_400_12, pl_400_12 = _sched_planned(
        _dom4(0, 12), insts_400, gangs=[_gpu_gang(4), _gpu_gang(8)]
    )
    assert len(rep_400_12.infeasible_gang_resource_requests) == 1
    assert pl_400_12 == 0
    assert rep_400_12.to_launch == []
    rep_400_16, pl_400_16 = _sched_planned(
        _dom4(0, 16), insts_400, gangs=[_gpu_gang(4), _gpu_gang(8)]
    )
    assert rep_400_16.infeasible_gang_resource_requests == []
    assert pl_400_16 == 5
    assert [r.count for r in rep_400_16.to_launch] == [5]

    # 3. min_worker_nodes with unattributed open slots
    # and reachable omitted RAY_STOPPING slots.
    stop_u = _gpu_stopping("stop-u", "c-stopu")
    insts_4258 = [
        _run_node("gpu", f"ru-{k}", {"CPU": 8, "GPU": 8}, dict(GPU_LABELS))
        for k in range(3)
    ] + [stop_u]
    rep_4258, pl_4258 = _sched_planned(_dom4(8, 8), insts_4258)
    assert pl_4258 == 4
    assert [r.count for r in rep_4258.to_launch] == [4]

    dom2_4 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 8},
            labels=GPU_LABELS,
            min_worker_nodes=4,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(workers_per_replica=2),
        )
    }
    sr_2551 = _run_node(
        "gpu", "sr-2551", {"CPU": 8, "GPU": 8}, dict(GPU_LABELS), lid="L0"
    )
    sr_2551.im_instance.status = Instance.RAY_STOP_REQUESTED
    run_2551 = _run_node("gpu", "r-2551", {"CPU": 8, "GPU": 8}, dict(GPU_LABELS))
    rep_2551, pl_2551 = _sched_planned(dom2_4, [sr_2551, run_2551], max_nodes=4)
    assert pl_2551 == 2
    assert [r.count for r in rep_2551.to_launch] == [2]

    alloc_l0 = make_autoscaler_instance(
        im_instance=Instance(
            instance_id="al-l0",
            instance_type="gpu",
            status=Instance.ALLOCATED,
            cloud_instance_id="c-all0",
            launch_request_id="L0",
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id="c-all0",
    )
    insts_1331 = [
        stop_l0,
        req_l0,
        alloc_l0,
        sr_unattr,
        *(
            _run_node(
                "gpu",
                f"r1331-{k}",
                {"CPU": 8, "GPU": 8},
                dict(GPU_LABELS),
                {"GPU": 8},
                lid="L0",
            )
            for k in range(2)
        ),
    ]
    shared4_8 = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 8, "GPU": 8},
            labels=GPU_LABELS,
            min_worker_nodes=8,
            max_worker_nodes=8,
            runtime_label_keys=("ray.io/gpu-domain",),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key="ray.io/gpu-domain",
                domain_exclusive=False,
            ),
        )
    }
    rep_1331, pl_1331 = _sched_planned(
        shared4_8, insts_1331, reqs=[_resource_request({"GPU": 8}, 4)]
    )
    assert pl_1331 == 3
    assert [r.count for r in rep_1331.to_launch] == [3]

    # 4. Partial refill TO_LAUNCH nodes from min_w + task demand complete
    # remaining unlabeled launch gap up to nt_room (planned == count == 5).
    inst_l1 = make_autoscaler_instance(
        im_instance=Instance(
            instance_id="ib-l1",
            instance_type="tpu",
            status=Instance.RAY_INSTALLING,
            cloud_instance_id="c-ibl1",
            launch_request_id="L_td",
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id="c-ibl1",
    )
    stop_tpu_im = Instance(
        instance_id="st-tpu",
        instance_type="tpu",
        status=Instance.RAY_STOPPING,
        node_id=b"sttpu".ljust(28, b"0").hex(),
        cloud_instance_id="c-sttpu",
        launch_request_id="L0",
        node_kind=NodeKind.WORKER,
    )
    stop_tpu_im.status_history.add(instance_status=Instance.RAY_RUNNING)
    stop_tpu_im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    stop_tpu_im.status_history.add(instance_status=Instance.RAY_STOPPING)
    stop_tpu = make_autoscaler_instance(
        im_instance=stop_tpu_im,
        ray_node=NodeState(
            node_id=b"sttpu".ljust(28, b"0"),
            instance_id="c-sttpu",
            ray_node_type_name="tpu",
            status=NodeStatus.DRAINING,
            total_resources={"CPU": 8, "TPU": 4},
            available_resources={"CPU": 8, "TPU": 4},
            labels={_SLICE: "s-l0", _WID: "0"},
        ),
        cloud_instance_id="c-sttpu",
    )
    insts_3022 = [
        stop_tpu,
        inst_l1,
        make_autoscaler_instance(im_instance=term_td, cloud_instance_id="c-term-td"),
        _pending_node("tpu", "req-sh", Instance.REQUESTED, lid="L_sh"),
    ]
    rep_3022, pl_3022 = _sched_planned(
        _tpu_excl(4, 8), insts_3022, reqs=[_resource_request({"TPU": 4}, 5)]
    )
    assert pl_3022 == 5
    assert [r.count for r in rep_3022.to_launch] == [5]


def _mh_tpu_cfg(
    name: str = "tpu",
    min_w: int = 0,
    max_w: int = 4,
    size: int = 4,
    exclusive: bool = True,
    dom_key: Optional[str] = _SLICE,
) -> NodeTypeConfig:
    rt_keys = tuple(k for k in (dom_key, _WID if dom_key == _SLICE else None) if k)
    return NodeTypeConfig(
        name=name,
        resources={"CPU": 8, "TPU": 4},
        min_worker_nodes=min_w,
        max_worker_nodes=max_w,
        runtime_label_keys=rt_keys,
        replica_config=WorkerReplicaConfig(
            workers_per_replica=size,
            domain_label_key=dom_key,
            domain_exclusive=exclusive,
            worker_index_label_key=_WID if dom_key == _SLICE else None,
        ),
    )


def _mh_stopping(
    nt: str,
    iid: str,
    dom_key: Optional[str] = None,
    dom_val: Optional[str] = None,
    lid: str = "",
    solic: bool = True,
    with_ray_node: bool = True,
) -> AutoscalerInstance:
    nid = iid.encode().ljust(28, b"z")[:28]
    cid = f"c-{iid}"
    im = Instance(
        instance_id=iid,
        instance_type=nt,
        status=Instance.RAY_STOPPING,
        node_id=nid.hex() if with_ray_node else "",
        cloud_instance_id=cid,
        launch_request_id=lid,
        node_kind=NodeKind.WORKER,
    )
    im.status_history.add(instance_status=Instance.RAY_RUNNING)
    if solic:
        im.status_history.add(instance_status=Instance.RAY_STOP_REQUESTED)
    im.status_history.add(instance_status=Instance.RAY_STOPPING)
    lbls = {dom_key: dom_val} if dom_key and dom_val else {}
    return make_autoscaler_instance(
        im_instance=im,
        ray_node=(
            NodeState(
                node_id=nid,
                instance_id=cid,
                ray_node_type_name=nt,
                status=NodeStatus.DRAINING,
                total_resources={"CPU": 8, "TPU": 4},
                available_resources={"CPU": 8, "TPU": 4},
                labels=lbls,
            )
            if with_ray_node
            else None
        ),
        cloud_instance_id=cid,
    )


def _mh_term(
    nt: str,
    iid: str,
    lid: str = "",
    with_td: bool = True,
    solic: bool = False,
    status: "Instance.InstanceStatus" = Instance.TERMINATED,
    end_ns: int = 0,
) -> AutoscalerInstance:
    cid = f"c-{iid}"
    im = Instance(
        instance_id=iid,
        instance_type=nt,
        status=status,
        cloud_instance_id=cid,
        launch_request_id=lid,
        node_kind=NodeKind.WORKER,
    )
    t0 = max(0, end_ns - 3_000) if end_ns else 0
    im.status_history.add(instance_status=Instance.RAY_RUNNING, timestamp_ns=t0)
    if solic:
        im.status_history.add(
            instance_status=Instance.RAY_STOP_REQUESTED,
            timestamp_ns=t0 + 1_000 if end_ns else 0,
        )
        im.status_history.add(
            instance_status=Instance.TERMINATING,
            timestamp_ns=t0 + 2_000 if end_ns else 0,
        )
    elif with_td:
        im.status_history.add(
            instance_status=Instance.RAY_STOPPED,
            timestamp_ns=t0 + 1_000 if end_ns else 0,
        )
        im.status_history.add(
            instance_status=Instance.TERMINATING,
            timestamp_ns=t0 + 2_000 if end_ns else 0,
        )
    if status == Instance.TERMINATED:
        im.status_history.add(instance_status=Instance.TERMINATED, timestamp_ns=end_ns)
    return make_autoscaler_instance(im_instance=im, cloud_instance_id=cid)


def _mh_reconciler_created(
    cfgs: Dict[str, NodeTypeConfig],
    insts: List[AutoscalerInstance],
    *,
    reqs=None,
    gangs=None,
    max_workers: int = 8,
    extra_cloud_cids: Optional[Dict[str, str]] = None,
    sched: Optional[ResourceDemandScheduler] = None,
) -> Dict[str, int]:
    store = InstanceStorage(cluster_id="mh-seam", storage=InMemoryStorage())
    im_mgr = InstanceManager(
        instance_storage=store, instance_status_update_subscribers=[]
    )
    head_im = Instance(
        instance_id="head-0",
        instance_type="head",
        status=Instance.RAY_RUNNING,
        node_id=(b"h" * 28).hex(),
        cloud_instance_id="c-head",
        node_kind=NodeKind.HEAD,
    )
    head_rn = NodeState(
        node_id=b"h" * 28,
        instance_id="c-head",
        ray_node_type_name="head",
        status=NodeStatus.RUNNING,
    )
    before_queued = set()
    im_copies = []
    for a in insts:
        im_cp = copy.deepcopy(a.im_instance)
        if a.ray_node is not None and a.ray_node.node_id:
            im_cp.node_id = a.ray_node.node_id.hex()
        if im_cp.status == Instance.QUEUED:
            before_queued.add(im_cp.instance_id)
        im_copies.append(im_cp)
    store.batch_upsert_instances([head_im] + im_copies, expected_storage_version=0)
    ant = {"head": {"node_config": {}, "resources": {"CPU": 0}}}
    for nt, c in cfgs.items():
        rc = c.replica_config
        entry: Dict[str, Any] = {
            "node_config": {},
            "resources": dict(c.resources),
            "min_workers": c.min_worker_nodes,
            "max_workers": c.max_worker_nodes,
            "runtime_label_keys": list(c.runtime_label_keys),
        }
        if rc is not None:
            entry["replica_config"] = {
                "workers_per_replica": rc.workers_per_replica,
                "domain_label_key": rc.domain_label_key,
                "domain_exclusive": rc.domain_exclusive,
                "worker_index_label_key": rc.worker_index_label_key,
            }
        ant[nt] = entry
    acfg = AutoscalingConfig(
        configs={
            "cluster_name": "mh-seam",
            "max_workers": max_workers,
            "upscaling_speed": 100.0,
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": ant,
        },
        skip_content_hash=True,
    )
    cmap = {
        "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
        **{
            a.im_instance.cloud_instance_id: CloudInstance(
                a.im_instance.cloud_instance_id,
                a.im_instance.instance_type,
                NodeKind.WORKER,
                True,
                replica_id=a.cloud_instance_replica_id,
            )
            for a in insts
            if a.im_instance.cloud_instance_id
            and a.im_instance.status
            not in (Instance.TERMINATED, Instance.TERMINATING, Instance.QUEUED)
        },
    }
    for cid, nt in (extra_cloud_cids or {}).items():
        cmap[cid] = CloudInstance(cid, nt, NodeKind.WORKER, True)
    rs = {
        nt: c.workers_per_replica for nt, c in cfgs.items() if c.is_multi_worker_replica
    }
    Reconciler._scale_cluster(
        autoscaling_state=AutoscalingState(),
        instance_manager=im_mgr,
        cloud_resource_monitor=CloudResourceMonitor(),
        ray_state=ClusterResourceState(
            node_states=[head_rn]
            + [a.ray_node for a in insts if a.ray_node is not None],
            pending_resource_requests=reqs or [],
            pending_gang_resource_requests=gangs or [],
        ),
        scheduler=sched or ResourceDemandScheduler(),
        autoscaling_config=acfg,
        cloud_provider=None,
        non_terminated_cloud_instances=cmap,
        replica_sizes=rs,
    )
    out: Dict[str, int] = {}
    for iid, v in store.get_instances()[0].items():
        if v.status == Instance.QUEUED and iid not in before_queued:
            out[v.instance_type] = out.get(v.instance_type, 0) + 1
    return out


def test_lidless_domain_teardown_remembered_domains_and_incarnation_bounds():
    """Lidless exclusive and shared-domain slices refill via untagged teardown,
    remembered instance domains, or TTL expiry while bounding retained terminal rows."""
    dead_lidless = _mh_term("tpu", "lw-3", lid="", with_td=True)

    for exclusive in (True, False):
        cfgs4 = {"tpu": _mh_tpu_cfg(max_w=4, exclusive=exclusive)}
        cfgs8 = {"tpu": _mh_tpu_cfg(max_w=8, exclusive=exclusive)}
        live3 = [
            _run_node(
                "tpu",
                f"lw-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "slice-0", _WID: str(w)},
                used={"TPU": 4},
            )
            for w in range(3)
        ]

        # Degraded adopted slice with untagged teardown refills 1 host at max=4 and max=8.
        for cfgs_m, mw in ((cfgs4, 4), (cfgs8, 8)):
            rep_td = _sched(
                cfgs_m, live3 + [dead_lidless], reqs=[_resource_request({"TPU": 4})]
            )
            assert rep_td.infeasible_resource_requests == []
            assert _launch_and_terminate(rep_td) == ({"tpu": 1}, [])
            assert _mh_reconciler_created(
                cfgs_m,
                live3 + [dead_lidless],
                reqs=[_resource_request({"TPU": 4})],
                max_workers=mw,
            ) == {"tpu": 1}

        # Post-restart lidless slice without teardown waits before TTL, refills after TTL,
        # and keeps its expired state across QUEUED-attach ticks without double-waiting.
        sched_ttl = ResourceDemandScheduler()
        t_clock = [100.0]
        q_refill = _pending_node("tpu", "q-ttl-0", Instance.QUEUED)
        with patch(
            "ray.autoscaler.v2.scheduler.time.monotonic",
            side_effect=lambda: t_clock[0],
        ):
            rep_pre = _sched(
                cfgs4,
                live3,
                reqs=[_resource_request({"TPU": 4})],
                placeholder_ttl_s=10.0,
                sched=sched_ttl,
            )
            assert len(rep_pre.infeasible_resource_requests) == 1
            assert rep_pre.to_launch == []

            t_clock[0] = 115.0
            rep_exp1 = _sched(
                cfgs4,
                live3,
                reqs=[_resource_request({"TPU": 4})],
                placeholder_ttl_s=10.0,
                sched=sched_ttl,
            )
            assert rep_exp1.infeasible_resource_requests == []
            assert _launch_and_terminate(rep_exp1) == ({"tpu": 1}, [])

            t_clock[0] = 116.0
            rep_q_att = _sched(
                cfgs4,
                live3 + [q_refill],
                reqs=[_resource_request({"TPU": 4})],
                placeholder_ttl_s=10.0,
                sched=sched_ttl,
            )
            assert rep_q_att.infeasible_resource_requests == []
            assert rep_q_att.to_launch == []

            t_clock[0] = 117.0
            rep_still_exp = _sched(
                cfgs4,
                live3,
                reqs=[_resource_request({"TPU": 4})],
                placeholder_ttl_s=10.0,
                sched=sched_ttl,
            )
            assert rep_still_exp.infeasible_resource_requests == []
            assert _launch_and_terminate(rep_still_exp) == ({"tpu": 1}, [])

    # Consuming 1 untagged teardown row on slice-a never falsely marks slice-b torn down.
    cfgs8_excl = {"tpu": _mh_tpu_cfg(max_w=8, exclusive=True)}
    slice_a3 = [
        _run_node(
            "tpu", f"sa-{w}", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-a", _WID: str(w)}
        )
        for w in range(3)
    ]
    slice_b3 = [
        _run_node(
            "tpu", f"sb-{w}", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-b", _WID: str(w)}
        )
        for w in range(3)
    ]
    g4_pin_b = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_pin_b"),
        loc_constraints=[(_SLICE, _IN, ["slice-b"])],
    )
    rep_one_td = _sched(
        cfgs8_excl, slice_a3 + slice_b3 + [dead_lidless], gangs=[g4_pin_b]
    )
    assert len(rep_one_td.infeasible_gang_resource_requests) == 1
    assert rep_one_td.to_launch == []

    # Remembered _instance_domains attributes a crashed host to slice-b even when
    # slice-a sorts earlier and also has a missing slot (6 live pods -> gap 2),
    # or emits 1 exact refill when slice-a has all 4 hosts (7 live pods -> gap 1).
    cfgs12_excl = {"tpu": _mh_tpu_cfg(max_w=12, exclusive=True)}
    sched_mem = ResourceDemandScheduler()
    sb_w3_live = _run_node(
        "tpu", "sb-3", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-b", _WID: "3"}
    )
    sb_w3_dead = _mh_term("tpu", "sb-3", lid="", with_td=True)
    _sched(cfgs8_excl, slice_a3 + slice_b3 + [sb_w3_live], sched=sched_mem)
    for cfgs_mem, mw_mem in ((cfgs8_excl, 8), (cfgs12_excl, 12)):
        rep_mem = _sched(
            cfgs_mem,
            slice_a3 + slice_b3 + [sb_w3_dead],
            gangs=[g4_pin_b],
            sched=sched_mem,
        )
        assert rep_mem.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_mem) == ({"tpu": 2}, [])
        assert _mh_reconciler_created(
            cfgs_mem,
            slice_a3 + slice_b3 + [sb_w3_dead],
            gangs=[g4_pin_b],
            max_workers=mw_mem,
            sched=sched_mem,
        ) == {"tpu": 2}

    # Shared-domain remembered _instance_domains restores worker-0 head resources on slice-b.
    sh_head_cfg = NodeTypeConfig(
        name="tpu",
        resources={"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
        min_worker_nodes=0,
        max_worker_nodes=8,
        runtime_label_keys=(_SLICE, _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=4,
            domain_label_key=_SLICE,
            domain_exclusive=False,
            worker_index_label_key=_WID,
            head_resources=("TPU-v6e-16-head",),
        ),
    )
    cfgs8_sh_head = {"tpu": sh_head_cfg}
    sa3_head_busy = [
        _run_node(
            "tpu",
            f"sha-{w}",
            {"CPU": 8, "TPU": 4, **({"TPU-v6e-16-head": 1} if w == 0 else {})},
            {_SLICE: "slice-a", _WID: str(w)},
            used={"TPU": 4, **({"TPU-v6e-16-head": 1} if w == 0 else {})},
        )
        for w in range(3)
    ]
    sb_w0_live = _run_node(
        "tpu",
        "shb-0",
        {"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
        {_SLICE: "slice-b", _WID: "0"},
        used={"TPU": 4, "TPU-v6e-16-head": 1},
    )
    sb_w123_busy = [
        _run_node(
            "tpu",
            f"shb-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-b", _WID: str(w)},
            used={"TPU": 4},
        )
        for w in (1, 2, 3)
    ]
    sb_w0_dead = _mh_term("tpu", "shb-0", lid="", with_td=True)
    sched_sh_mem = ResourceDemandScheduler()
    _sched(
        cfgs8_sh_head,
        sa3_head_busy + [sb_w0_live] + sb_w123_busy,
        sched=sched_sh_mem,
    )
    rep_sh_mem = _sched(
        cfgs8_sh_head,
        sa3_head_busy + sb_w123_busy + [sb_w0_dead],
        reqs=[_resource_request({"TPU": 4, "TPU-v6e-16-head": 1})],
        sched=sched_sh_mem,
    )
    assert rep_sh_mem.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_sh_mem) == ({"tpu": 2}, [])

    sa_w3_live = _run_node(
        "tpu", "sa-3", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-a", _WID: "3"}
    )
    rep_mem_exact = _sched(
        cfgs12_excl,
        slice_a3 + [sa_w3_live] + slice_b3 + [sb_w3_dead],
        gangs=[g4_pin_b],
        sched=sched_mem,
    )
    assert rep_mem_exact.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_mem_exact) == ({"tpu": 1}, [])
    assert _mh_reconciler_created(
        cfgs12_excl,
        slice_a3 + [sa_w3_live] + slice_b3 + [sb_w3_dead],
        gangs=[g4_pin_b],
        max_workers=12,
        sched=sched_mem,
    ) == {"tpu": 1}

    # Retained 24h terminal teardown rows from an older incarnation do not mark a
    # newer staggered 3/4 slice as torn down, whereas a contemporaneous teardown does.
    stale_td = _mh_term("tpu", "old-dead", lid="", with_td=True, end_ns=100_000_000_000)
    new_staggered = []
    for w in range(3):
        inst_n = _run_node(
            "tpu", f"ns-{w}", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-new", _WID: str(w)}
        )
        inst_n.im_instance.status_history.add(
            instance_status=Instance.ALLOCATED, timestamp_ns=3_700_000_000_000
        )
        new_staggered.append(inst_n)
    g4_any = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_inc_bound"))
    rep_stale = _sched(cfgs8_excl, [stale_td] + new_staggered, gangs=[g4_any])
    assert rep_stale.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_stale) == ({"tpu": 5}, [])
    assert _mh_reconciler_created(
        cfgs8_excl, [stale_td] + new_staggered, gangs=[g4_any], max_workers=8
    ) == {"tpu": 5}

    fresh_td = _mh_term(
        "tpu", "cur-dead", lid="", with_td=True, end_ns=3_800_000_000_000
    )
    rep_fresh = _sched(cfgs8_excl, [fresh_td] + new_staggered, gangs=[g4_any])
    assert rep_fresh.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_fresh) == ({"tpu": 1}, [])


def test_shared_domain_mixed_batch_coalescing_and_cross_rack_alignment():
    """Shared-domain complete slices built from mixed batches coalesce without phantom
    refill gaps, align cross-rack launches, and isolate unlabeled running nodes."""
    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_sh_mb"))
    mixed_slice0 = [
        _run_node(
            "tpu",
            f"mb-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
            lid="L1" if w < 3 else "",
        )
        for w in range(4)
    ]
    dead_l1 = _mh_term("tpu", "dead-l1", lid="L1", with_td=True)
    for max_w in (8, 12, 16):
        sh_cfg = {"tpu": _mh_tpu_cfg(max_w=max_w, exclusive=False)}
        rep_mb = _sched(sh_cfg, mixed_slice0 + [dead_l1], gangs=[g4])
        assert rep_mb.infeasible_gang_resource_requests == []
        assert _launch_and_terminate(rep_mb) == ({"tpu": 4}, [])
        assert _mh_reconciler_created(
            sh_cfg, mixed_slice0 + [dead_l1], gangs=[g4], max_workers=max_w
        ) == {"tpu": 4}

    # Two size=2 shared slices each holding 1 older host + 1 host of a shared refill batch.
    sh2_cfg6 = {"tpu": _mh_tpu_cfg(max_w=6, size=2, exclusive=False)}
    two_s2 = [
        _run_node(
            "tpu",
            "s0-old",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s0", _WID: "0"},
            used={"TPU": 4},
            lid="L0",
        ),
        _run_node(
            "tpu",
            "s0-ref",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s0", _WID: "1"},
            used={"TPU": 4},
            lid="Lref",
        ),
        _run_node(
            "tpu",
            "s1-old",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s1", _WID: "0"},
            used={"TPU": 4},
            lid="L1",
        ),
        _run_node(
            "tpu",
            "s1-ref",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s1", _WID: "1"},
            used={"TPU": 4},
            lid="Lref",
        ),
        _mh_term("tpu", "d0", lid="L0", with_td=True),
        _mh_term("tpu", "d1", lid="L1", with_td=True),
    ]
    g2 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_sh2"))
    rep_s2 = _sched(sh2_cfg6, two_s2, gangs=[g2])
    assert rep_s2.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_s2) == ({"tpu": 2}, [])

    # Unbacked healthy_raw aligns planned == created == 5 on lidless exclusive and cross-rack shared.
    excl8 = {"tpu": _mh_tpu_cfg(max_w=8, exclusive=True)}
    lidless3 = [
        _run_node(
            "tpu",
            f"ex-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4},
        )
        for w in range(3)
    ]
    rep_ex5 = _sched(excl8, lidless3, gangs=[g4])
    assert _launch_and_terminate(rep_ex5) == ({"tpu": 5}, [])
    assert _mh_reconciler_created(excl8, lidless3, gangs=[g4], max_workers=8) == {
        "tpu": 5
    }

    sh_rack8 = {"tpu": _mh_tpu_cfg(max_w=8, exclusive=False, dom_key="rack")}
    sh_split3 = [
        _run_node(
            "tpu",
            f"shr-{k}",
            {"CPU": 8, "TPU": 4},
            {"rack": "r0" if k < 2 else "r1"},
            used={"TPU": 4},
            lid="L_sh",
        )
        for k in range(3)
    ]
    g4_rack = _strict_pack_gang("rack", _spread_reqs({"TPU": 4}, 4, "_pg_rack_sh"))
    rep_rk5 = _sched(sh_rack8, sh_split3, gangs=[g4_rack])
    assert _launch_and_terminate(rep_rk5) == ({"tpu": 5}, [])
    assert _mh_reconciler_created(
        sh_rack8, sh_split3, gangs=[g4_rack], max_workers=8
    ) == {"tpu": 5}

    # Unlabeled running node on a shared-domain type neither blocks synthetic QUEUED
    # siblings nor accepts refills when tagged with a torn-down launch_request_id.
    sh_cfg4 = {"tpu": _mh_tpu_cfg(max_w=4, exclusive=False)}
    unlbl_lid_td = [
        _run_node(
            "tpu", f"ul-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L_utd"
        )
        for k in range(3)
    ] + [_mh_term("tpu", "ul-dead", lid="L_utd", with_td=True)]
    rep_ul_td = _sched(sh_cfg4, unlbl_lid_td, reqs=[_resource_request({"TPU": 4})])
    assert len(rep_ul_td.infeasible_resource_requests) == 1
    assert rep_ul_td.to_launch == []

    # Domainless partial launch_request_id group expires via TTL, persists across
    # QUEUED-attach ticks, and refills.
    dl_cfg4 = {"tpu": _mh_tpu_cfg(max_w=4, exclusive=False, dom_key=None)}
    dl_lid3 = [
        _run_node(
            "tpu", f"dll-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L_dl_ttl"
        )
        for k in range(3)
    ]
    q_dl_ttl = _pending_node("tpu", "q-dl-ttl", Instance.QUEUED)
    sched_dl = ResourceDemandScheduler()
    t_dl = [50.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: t_dl[0]
    ):
        assert (
            _sched(
                dl_cfg4,
                dl_lid3,
                reqs=[_resource_request({"TPU": 4})],
                placeholder_ttl_s=10.0,
                sched=sched_dl,
            ).to_launch
            == []
        )
        t_dl[0] = 65.0
        assert (
            _sched(
                dl_cfg4,
                dl_lid3 + [q_dl_ttl],
                reqs=[_resource_request({"TPU": 4})],
                placeholder_ttl_s=10.0,
                sched=sched_dl,
            ).to_launch
            == []
        )
        t_dl[0] = 66.0
        rep_dl_exp = _sched(
            dl_cfg4,
            dl_lid3,
            reqs=[_resource_request({"TPU": 4})],
            placeholder_ttl_s=10.0,
            sched=sched_dl,
        )
        assert _launch_and_terminate(rep_dl_exp) == ({"tpu": 1}, [])

    # A transient unattributed booting node arriving after TTL expiration must not
    # clear the attribution group's expired placeholder entry when it leaves later.
    dl_min4_max8 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=4,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    ttl_run2 = [
        _run_node(
            "tpu", f"tr-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L_tr"
        )
        for k in range(2)
    ]
    ttl_boot_same = _pending_node("tpu", "tb-1", Instance.ALLOCATED, lid="L_tr")
    ttl_boot_same.cloud_instance_id = (
        ttl_boot_same.im_instance.cloud_instance_id
    ) = "cloud-tb-1"
    ttl_boot_unattr = _pending_node("tpu", "tb-2", Instance.ALLOCATED)
    ttl_boot_unattr.cloud_instance_id = (
        ttl_boot_unattr.im_instance.cloud_instance_id
    ) = "cloud-tb-2"
    sched_tr = ResourceDemandScheduler()
    t_tr = [50.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: t_tr[0]
    ):
        assert _launch_and_terminate(
            _sched(
                dl_min4_max8,
                ttl_run2 + [ttl_boot_same],
                placeholder_ttl_s=10.0,
                sched=sched_tr,
            )
        ) == ({}, [])
        t_tr[0] = 65.0
        assert _launch_and_terminate(
            _sched(
                dl_min4_max8,
                ttl_run2 + [ttl_boot_same, ttl_boot_unattr],
                placeholder_ttl_s=10.0,
                sched=sched_tr,
            )
        ) == ({}, [])
        t_tr[0] = 66.0
        assert _launch_and_terminate(
            _sched(
                dl_min4_max8,
                ttl_run2 + [ttl_boot_same],
                placeholder_ttl_s=10.0,
                sched=sched_tr,
            )
        ) == ({"tpu": 1}, [])

    # Shared-domain size=2 cross-rack placement uses an ALLOCATED sibling of a busy
    # unlabeled running pod that lacks a runtime domain label.
    sh2_cfg4 = {"tpu": _mh_tpu_cfg(max_w=4, size=2, exclusive=False)}
    ra_idle = _run_node("tpu", "ra-0", {"CPU": 8, "TPU": 4}, {_SLICE: "rack-a"})
    ul_busy = _run_node(
        "tpu", "ul-0", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L_cr"
    )
    qs_alloc = _pending_node("tpu", "qs-1", Instance.ALLOCATED, lid="L_cr")
    qs_alloc.cloud_instance_id = "c-qs-1"
    qs_alloc.im_instance.cloud_instance_id = "c-qs-1"
    g2_rack = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 2, "_pg_cr_alloc"))
    rep_cr_alloc = _sched(sh2_cfg4, [ra_idle, ul_busy, qs_alloc], gangs=[g2_rack])
    assert rep_cr_alloc.infeasible_gang_resource_requests == []
    assert rep_cr_alloc.to_launch == []


def test_draining_domain_isolation_and_multi_slice_lid_slot_caps():
    """Domain-labeled stopping pods stay isolated to their own slice, slot caps count
    unattributed members, and known-draining sibling slices use live slot counts."""
    cfgs8 = {"tpu": _mh_tpu_cfg(max_w=8)}
    cfgs12 = {"tpu": _mh_tpu_cfg(max_w=12)}
    g3_s1 = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 3, "_pg_s1_pin"),
        loc_constraints=[(_SLICE, _IN, ["slice-1"])],
    )
    s0_term3 = [
        _mh_term("tpu", f"s0-t{k}", lid="L_8", solic=True, status=Instance.TERMINATING)
        for k in range(3)
    ]
    s1_run3 = [
        _run_node(
            "tpu",
            f"s1-r{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(k)},
            lid="L_8",
        )
        for k in range(3)
    ]
    s1_crash1 = _mh_term("tpu", "s1-cr3", lid="L_8", with_td=True, solic=False)
    s0_stopping = _mh_stopping(
        "tpu", "s0-st3", dom_key=_SLICE, dom_val="slice-0", lid="L_8"
    )
    assert (
        _sched(
            cfgs8, s0_term3 + [s0_stopping] + s1_run3 + [s1_crash1], gangs=[g3_s1]
        ).infeasible_gang_resource_requests
        == []
    )
    s0_sr = _run_node(
        "tpu", "s0-sr3", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-0", _WID: "3"}, lid="L_8"
    )
    s0_sr.im_instance.status = Instance.RAY_STOP_REQUESTED
    assert (
        _sched(
            cfgs8, s0_term3 + [s0_sr] + s1_run3 + [s1_crash1], gangs=[g3_s1]
        ).infeasible_gang_resource_requests
        == []
    )

    # _max_lid_slots_on_rep counts unattributed live members in other_live.
    g3_s0 = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 3, "_pg_s0_pin"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )
    s0_unattr2 = [
        _run_node(
            "tpu", f"s0-u{k}", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-0", _WID: str(k)}
        )
        for k in range(2)
    ]
    s0_lco1 = _run_node(
        "tpu",
        "s0-lco2",
        {"CPU": 8, "TPU": 4},
        {_SLICE: "slice-0", _WID: "2"},
        lid="L_co",
    )
    s0_lco_cr = _mh_term("tpu", "s0-lco-cr", lid="L_co", with_td=True, solic=False)
    s1_lco_solic2 = [
        _mh_term(
            "tpu", f"s1-lco-t{k}", lid="L_co", solic=True, status=Instance.TERMINATING
        )
        for k in range(2)
    ]
    assert (
        _sched(
            cfgs8,
            s0_unattr2 + [s0_lco1, s0_lco_cr] + s1_lco_solic2,
            gangs=[g3_s0],
        ).infeasible_gang_resource_requests
        == []
    )

    # Known-draining slice-0 (1 running + 1 RAY_STOPPING + 2 solicited TERMINATING)
    # contributes live_lid (1) rather than cap (4) to sum_cap so slice-1 stays schedulable.
    s0_live1 = _run_node(
        "tpu",
        "kd-s0-0",
        {"CPU": 8, "TPU": 4},
        {_SLICE: "slice-0", _WID: "0"},
        lid="L_kd",
    )
    s0_stop1 = _mh_stopping(
        "tpu", "kd-s0-1", dom_key=_SLICE, dom_val="slice-0", lid="L_kd"
    )
    s0_solic2 = [
        _mh_term(
            "tpu", f"kd-s0-t{k}", lid="L_kd", solic=True, status=Instance.TERMINATING
        )
        for k in range(2)
    ]
    s1_live3_kd = [
        _run_node(
            "tpu",
            f"kd-s1-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(k)},
            lid="L_kd",
        )
        for k in range(3)
    ]
    s1_cr_kd = _mh_term("tpu", "kd-s1-cr", lid="L_kd", with_td=True, solic=False)
    assert (
        _sched(
            cfgs8,
            [s0_live1, s0_stop1] + s0_solic2 + s1_live3_kd + [s1_cr_kd],
            gangs=[g3_s1],
        ).infeasible_gang_resource_requests
        == []
    )

    # 3-slice multi-replica batch (4 exited solicited TERMINATING + 2 live + 2 RAY_STOP_REQUESTED
    # on slice-0 + 3 live + 1 crash on slice-1) uses live_lid on slice-0 in sum_cap so slice-1 refills.
    r0_solic4 = [
        _mh_term(
            "tpu", f"kd3-r0-t{k}", lid="L_kd3", solic=True, status=Instance.TERMINATING
        )
        for k in range(4)
    ]
    r1_live2 = [
        _run_node(
            "tpu",
            f"kd3-r1-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            lid="L_kd3",
        )
        for k in range(2)
    ]
    r1_sr2 = []
    for k in (2, 3):
        n_sr = _run_node(
            "tpu",
            f"kd3-r1-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            lid="L_kd3",
        )
        n_sr.im_instance.status = Instance.RAY_STOP_REQUESTED
        r1_sr2.append(n_sr)
    r2_live3 = [
        _run_node(
            "tpu",
            f"kd3-r2-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(k)},
            lid="L_kd3",
        )
        for k in range(3)
    ]
    r2_cr1 = _mh_term("tpu", "kd3-r2-3", lid="L_kd3", with_td=True, solic=False)
    g4_s1 = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_kd3_s1"),
        loc_constraints=[(_SLICE, _IN, ["slice-1"])],
    )
    rep_kd3 = _sched(
        cfgs12, r0_solic4 + r1_live2 + r1_sr2 + r2_live3 + [r2_cr1], gangs=[g4_s1]
    )
    assert rep_kd3.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_kd3) == ({"tpu": 1}, [])

    # Domainless 2-replica drain check and unlabeled running attachment onto partial slice.
    dl_cfgs8 = {"tpu": _mh_tpu_cfg(max_w=8, exclusive=False, dom_key=None)}
    dl_r3 = [
        _run_node("tpu", f"dl-r{k}", {"CPU": 8, "TPU": 4}, {}, lid="L_8")
        for k in range(3)
    ]
    dl_solic4 = [
        _mh_term("tpu", f"dl-s{k}", lid="L_8", solic=True, status=Instance.TERMINATING)
        for k in range(4)
    ]
    dl_crash1 = _mh_term("tpu", "dl-cr", lid="L_8", with_td=True, solic=False)
    rep_dl = _sched(
        dl_cfgs8,
        dl_r3 + dl_solic4 + [dl_crash1],
        reqs=[_resource_request({"TPU": 4}, 3)],
    )
    assert rep_dl.infeasible_resource_requests == []
    assert rep_dl.to_launch == []

    s0_l12 = [
        _run_node(
            "tpu",
            f"l12-s0-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            lid="L_12",
        )
        for k in range(3)
    ]
    s1_l12 = [
        _run_node(
            "tpu",
            f"l12-s1-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(k)},
            lid="L_12",
        )
        for k in range(2)
    ]
    s1_unlbl = _run_node("tpu", "l12-s1-u2", {"CPU": 8, "TPU": 4}, {}, lid="L_12")
    solic4_l12 = [
        _mh_term(
            "tpu", f"l12-t{k}", lid="L_12", solic=True, status=Instance.TERMINATING
        )
        for k in range(4)
    ]
    cr2_l12 = [
        _mh_term("tpu", f"l12-cr{k}", lid="L_12", with_td=True, solic=False)
        for k in range(2)
    ]
    assert (
        _sched(
            cfgs12,
            s0_l12 + s1_l12 + [s1_unlbl] + solic4_l12 + cr2_l12,
            gangs=[g3_s1],
        ).infeasible_gang_resource_requests
        == []
    )


def test_queued_refill_attachment_doomed_split_and_unlabeled_orphan_caps():
    """Launchable QUEUED refills attach cleanly, doomed tails split before attach,
    and unlabeled running orphans never split whole QUEUED replicas."""
    cfgs4 = {"tpu": _mh_tpu_cfg(max_w=4)}
    cfgs8 = {"tpu": _mh_tpu_cfg(max_w=8)}
    g4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_q_att"))

    k_run2_idle = [
        _run_node(
            "tpu",
            f"kr-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            lid="L_k",
        )
        for k in range(2)
    ]
    k_del2 = [_mh_term("tpu", f"kd-{k}", lid="L_k", with_td=False) for k in range(2)]
    k_q2 = [_pending_node("tpu", f"kq-{k}", Instance.QUEUED) for k in range(2)]
    rep_k4 = _sched(cfgs4, k_run2_idle + k_del2 + k_q2, gangs=[g4])
    assert rep_k4.infeasible_gang_resource_requests == []
    assert rep_k4.to_launch == []
    assert rep_k4.to_terminate == []

    rep_kp = _sched(
        cfgs8,
        k_run2_idle + k_del2 + k_q2,
        pending_idle_s=0.0,
        unneeded={"tpu": [0.0, 0.0]},
    )
    assert rep_kp.to_terminate == []

    k_run2_busy = [
        _run_node(
            "tpu",
            f"krb-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            used={"TPU": 4},
            lid="L_k",
        )
        for k in range(2)
    ]
    rep_k8_2nd = _sched(cfgs8, k_run2_busy + k_del2 + k_q2, gangs=[g4])
    assert rep_k8_2nd.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_k8_2nd) == ({"tpu": 4}, [])

    # Splitting doomed QUEUED tail before attach keeps q-0 attached while q-1 cancels,
    # and rebuild_replicas resets synthetic labels cleanly.
    td_busy3 = [
        _run_node(
            "tpu",
            f"tdb-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            used={"TPU": 4},
            lid="L_tdb",
        )
        for k in range(3)
    ] + [_mh_term("tpu", "tdb-dead", lid="L_tdb", with_td=True)]
    q_pair = [
        _pending_node("tpu", "q-0", Instance.QUEUED),
        _pending_node("tpu", "q-1", Instance.QUEUED),
    ]
    cmap_tdb = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in td_busy3[:3]
    }
    view_tdb = _MultiHostReplicaView.build(
        [a.im_instance for a in td_busy3 + q_pair], cmap_tdb, {"tpu": 4}
    )
    rep_split_q = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs4,
            current_instances=td_busy3 + q_pair,
            resource_requests=[_resource_request({"TPU": 4})],
            disable_launch_config_check=True,
            replica_view=view_tdb,
        )
    )
    assert rep_split_q.infeasible_resource_requests == []
    assert rep_split_q.to_launch == []
    assert [t.instance_id for t in rep_split_q.to_terminate] == ["q-1"]

    # 2/4 busy torn-down slice + 2 unlabeled running orphans + 4 QUEUED pods keeps the
    # 4 QUEUED pods whole so a 4-host STRICT_PACK gang schedules onto the QUEUED slice.
    td2_busy = [
        _run_node(
            "tpu",
            f"td2-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            used={"TPU": 4},
            lid="L_td2",
        )
        for k in range(2)
    ] + [_mh_term("tpu", f"td2-d{k}", lid="L_td2", with_td=True) for k in range(2)]
    unlbl_orphans2 = [
        _run_node("tpu", f"uo-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4})
        for k in range(2)
    ]
    q_whole4 = [_pending_node("tpu", f"qw-{k}", Instance.QUEUED) for k in range(4)]
    sh_cfg8 = {"tpu": _mh_tpu_cfg(max_w=8, exclusive=False)}
    rep_whole_q = _sched(sh_cfg8, td2_busy + unlbl_orphans2 + q_whole4, gangs=[g4])
    assert rep_whole_q.infeasible_gang_resource_requests == []
    assert rep_whole_q.to_launch == []
    assert rep_whole_q.to_terminate == []

    # Shared-domain unlabeled running orphan + 3 QUEUED refills keeps _has_runtime_domain
    # False on attached refills so Step 5 places a 3-host gang onto those 3 QUEUED pods.
    unlbl_orphan1 = unlbl_orphans2[0]
    sh_cfg4 = {"tpu": _mh_tpu_cfg(max_w=4, exclusive=False)}
    q_ref3 = [_pending_node("tpu", f"qr3-{k}", Instance.QUEUED) for k in range(3)]
    g3 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 3, "_pg_uo_sh3"))
    rep_uo_sh = _sched(sh_cfg4, [unlbl_orphan1] + q_ref3, gangs=[g3])
    assert rep_uo_sh.infeasible_gang_resource_requests == []
    assert rep_uo_sh.to_launch == []

    # 4 fresh ALLOCATED pods stay whole rather than splitting 2+2 across a torn-down
    # labeled slice and a torn-down unlabeled running orphan under exclusive domains.
    s0_half = [
        _run_node(
            "tpu",
            f"shf-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            used={"TPU": 4},
            lid="L_shf",
        )
        for k in range(2)
    ] + [_mh_term("tpu", f"shf-d{k}", lid="L_shf", with_td=True) for k in range(2)]
    uo_td2 = unlbl_orphans2 + [
        _mh_term("tpu", f"uo-td{k}", lid="", with_td=True) for k in range(2)
    ]
    b_whole4 = []
    for k in range(4):
        bn = _pending_node("tpu", f"bw-{k}", Instance.ALLOCATED)
        bn.cloud_instance_id = f"c-bw-{k}"
        bn.im_instance.cloud_instance_id = f"c-bw-{k}"
        b_whole4.append(bn)
    rep_b_whole4 = _sched(cfgs8, s0_half + uo_td2 + b_whole4, gangs=[g4])
    assert rep_b_whole4.infeasible_gang_resource_requests == []
    assert rep_b_whole4.to_launch == []

    # Domainless running lid group on a domain-keyed node type never accepts QUEUED
    # refill attachment: 1 QUEUED pod attaches to 3 ALLOCATED siblings while 2 excess
    # expired QUEUED pods idle-terminate instead of attaching to the running group.
    u2_dlid = [
        _run_node(
            "tpu", f"udl-{k}", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L_dlid"
        )
        for k in range(2)
    ]
    b3_dlid = []
    for k in range(3):
        bn = _pending_node("tpu", f"bdl-{k}", Instance.ALLOCATED, lid="L_dlid")
        bn.cloud_instance_id = f"c-bdl-{k}"
        bn.im_instance.cloud_instance_id = f"c-bdl-{k}"
        b3_dlid.append(bn)
    d0_dlid = _mh_term("tpu", "d-dlid", lid="L_dlid", with_td=True)
    q3_dlid = [_pending_node("tpu", f"qdl-{k}", Instance.QUEUED) for k in range(3)]
    rep_dlid = _sched(
        sh_cfg8,
        u2_dlid + b3_dlid + [d0_dlid] + q3_dlid,
        pending_idle_s=10.0,
        unneeded={"tpu": [20.0, 20.0, 20.0]},
    )
    assert {t.instance_id for t in rep_dlid.to_terminate} == {"qdl-1", "qdl-2"}

    # When doomed QUEUED cancellation lowers forced open slots and triggers rebuild_replicas,
    # stale orthogonal runtime labels copied during pass 1 are reset cleanly on pass 2.
    cfg_rk = NodeTypeConfig(
        name="tpu",
        resources={"CPU": 8, "TPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=20,
        runtime_label_keys=(_SLICE, _WID, "rack"),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=4,
            domain_label_key=_SLICE,
            domain_exclusive=True,
            worker_index_label_key=_WID,
        ),
    )
    s_rk_a = [
        _run_node(
            "tpu",
            f"rka-{s}-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: f"slice-{s}", _WID: str(k), "rack": "rack-a"},
            used={"TPU": 4},
            lid=f"L_rka_{s}",
        )
        for s in (0, 1, 3)
        for k in range(3)
    ]
    s_rk_b = [
        _run_node(
            "tpu",
            f"rkb-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-2", _WID: str(k), "rack": "rack-b"},
            used={"TPU": 4},
            lid="L_rkb",
        )
        for k in range(2)
    ] + [_mh_term("tpu", f"rkb-d{k}", lid="L_rkb", with_td=True) for k in range(2)]
    u_rk_b = [
        _run_node(
            "tpu",
            "ukb-0",
            {"CPU": 8, "TPU": 4},
            {"rack": "rack-b"},
            used={"TPU": 4},
            lid="L_ukb",
        ),
        _mh_term("tpu", "ukb-d", lid="L_ukb", with_td=True),
    ]
    q7_rk = [_pending_node("tpu", f"qrk-{k}", Instance.QUEUED) for k in range(7)]
    g4_not_rka = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_not_rka"),
        loc_constraints=[
            ("rack", LabelSelectorOperator.LABEL_OPERATOR_NOT_IN, ["rack-a"])
        ],
    )
    rep_rk_reset = _sched(
        {"tpu": cfg_rk}, s_rk_a + s_rk_b + u_rk_b + q7_rk, gangs=[g4_not_rka]
    )
    assert rep_rk_reset.infeasible_gang_resource_requests == []
    assert rep_rk_reset.to_launch == []
    assert {t.instance_id for t in rep_rk_reset.to_terminate} == {
        "qrk-4",
        "qrk-5",
        "qrk-6",
    }


def test_multitype_headroom_credit_placeholder_trim_and_room_clamps():
    """Multi-type headroom credits unlaunched sibling pads for both gangs and tasks,
    trims over-capacity placeholders, and clamps TO_LAUNCH counts to reconciler room."""
    cfgs_ab = {
        "tpu_a": _mh_tpu_cfg("tpu_a", max_w=8),
        "tpu_b": _mh_tpu_cfg("tpu_b", max_w=8),
    }
    cfgs_ab["tpu_a"].resources["tok_a"] = 1
    cfgs_ab["tpu_b"].resources["tok_b"] = 1
    insts_ab = [
        _mh_stopping("tpu_a", "sa-0", dom_key=_SLICE, dom_val="sa-0", lid="La"),
        _mh_stopping("tpu_b", "sb-0", dom_key=_SLICE, dom_val="sb-0", lid="Lb"),
    ]
    reqs_ab = [
        _resource_request({"TPU": 4, "tok_a": 1}, 4),
        _resource_request({"TPU": 4, "tok_b": 1}, 4),
    ]

    # Paired 2-type gang headroom credit test at max_nodes=9 vs max_nodes=8.
    gang_a = _strict_pack_gang(
        _SLICE, _spread_reqs({"TPU": 4, "tok_a": 1}, 4, "_pg_cred_a")
    )
    rep_gang9 = _sched(cfgs_ab, insts_ab, gangs=[gang_a], max_nodes=9)
    assert rep_gang9.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_gang9) == ({"tpu_a": 7}, [])
    assert _mh_reconciler_created(cfgs_ab, insts_ab, gangs=[gang_a], max_workers=9) == {
        "tpu_a": 7
    }

    rep_gang8 = _sched(cfgs_ab, insts_ab, gangs=[gang_a], max_nodes=8)
    assert len(rep_gang8.infeasible_gang_resource_requests) == 1
    assert rep_gang8.to_launch == []
    assert (
        _mh_reconciler_created(cfgs_ab, insts_ab, gangs=[gang_a], max_workers=8) == {}
    )

    # Exact scheduler and reconciler launch dicts at max_nodes=16 and max_nodes=13.
    rep_ab16 = _sched(cfgs_ab, insts_ab, reqs=reqs_ab, max_nodes=16)
    assert _launch_and_terminate(rep_ab16) == ({"tpu_a": 7, "tpu_b": 7}, [])
    assert _mh_reconciler_created(cfgs_ab, insts_ab, reqs=reqs_ab, max_workers=16) == {
        "tpu_a": 7,
        "tpu_b": 7,
    }

    rep_ab13 = _sched(cfgs_ab, insts_ab, reqs=reqs_ab, max_nodes=13)
    assert _launch_and_terminate(rep_ab13) == ({"tpu_a": 7}, [])
    assert _mh_reconciler_created(cfgs_ab, insts_ab, reqs=reqs_ab, max_workers=13) == {
        "tpu_a": 7
    }

    # Unbacked placeholder conversion with a vanished pod clamps sibling launches at max_workers=11.
    ph_a3 = [
        _run_node(
            "tpu_a",
            f"pa-{k}",
            {"CPU": 8, "TPU": 4, "tok_a": 1},
            {_SLICE: "sa-0", _WID: str(k)},
            used={"TPU": 4, "tok_a": 1},
            lid="La_ph",
        )
        for k in range(3)
    ]
    van_a_im = Instance(
        instance_id="pa-van-3",
        instance_type="tpu_a",
        status=Instance.TERMINATED,
        cloud_instance_id="c-pa-van-3",
        launch_request_id="La_ph",
        node_kind=NodeKind.WORKER,
    )
    van_a_im.status_history.add(instance_status=Instance.ALLOCATED)
    van_a_im.status_history.add(instance_status=Instance.TERMINATED)
    van_a = make_autoscaler_instance(
        im_instance=van_a_im, cloud_instance_id="c-pa-van-3"
    )
    insts_van = ph_a3 + [van_a, insts_ab[1]]
    cmap_van = {
        "c-pa-van-3": CloudInstance("c-pa-van-3", "tpu_a", NodeKind.WORKER, True),
        **{
            a.im_instance.cloud_instance_id: CloudInstance(
                a.im_instance.cloud_instance_id,
                a.im_instance.instance_type,
                NodeKind.WORKER,
                True,
                replica_id=a.cloud_instance_replica_id,
            )
            for a in ph_a3 + [insts_ab[1]]
        },
    }
    view_van = _MultiHostReplicaView.build(
        [a.im_instance for a in insts_van], cmap_van, {"tpu_a": 4, "tpu_b": 4}
    )
    rep_van11 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs_ab,
            current_instances=insts_van,
            resource_requests=reqs_ab,
            max_num_nodes=11,
            disable_launch_config_check=True,
            replica_view=view_van,
        )
    )
    assert _launch_and_terminate(rep_van11) == ({"tpu_a": 5}, [])
    assert _mh_reconciler_created(
        cfgs_ab,
        insts_van,
        reqs=reqs_ab,
        max_workers=11,
        extra_cloud_cids={"c-pa-van-3": "tpu_a"},
    ) == {"tpu_a": 5}

    # Over-capacity non-provider placeholders are trimmed under sibling min_workers + alignment
    # and under same-tick TO_TERMINATE scale-down on a sibling node type.
    cfgs_trim = {
        "tpu_a": _mh_tpu_cfg("tpu_a", min_w=8, max_w=8),
        "tpu_b": _mh_tpu_cfg("tpu_b", min_w=0, max_w=4, exclusive=False),
    }
    req_b = _pending_node("tpu_b", "rb-0", Instance.REQUESTED, lid="Lb_req")
    req_b.cloud_instance_id = ""
    req_b.im_instance.cloud_instance_id = ""
    two_gangs = [
        _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, f"_pg_tr_{i}"))
        for i in range(2)
    ]
    rep_trim = _sched(cfgs_trim, [insts_ab[0], req_b], gangs=two_gangs, max_nodes=10)
    assert len(rep_trim.infeasible_gang_resource_requests) == 1
    assert _launch_and_terminate(rep_trim) == ({"tpu_a": 7}, [])
    assert _mh_reconciler_created(
        cfgs_trim, [insts_ab[0], req_b], gangs=two_gangs, max_workers=10
    ) == {"tpu_a": 7}

    cfgs_td_occ = {
        "tpu_a": _mh_tpu_cfg("tpu_a", min_w=0, max_w=4),
        "tpu_b": _mh_tpu_cfg("tpu_b", min_w=0, max_w=4),
    }
    b8_over = [
        _run_node(
            "tpu_b",
            f"bo-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: f"sb-{k // 4}", _WID: str(k % 4)},
            used={"TPU": 4} if k >= 4 else None,
            lid=f"Lb_{k // 4}",
        )
        for k in range(8)
    ]
    a2_idle = [
        _run_node(
            "tpu_a",
            f"ai-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "sa-0", _WID: str(k)},
            lid="La0",
        )
        for k in range(2)
    ]
    g4_occ = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_occ"))
    rep_occ10 = _sched(cfgs_td_occ, a2_idle + b8_over, gangs=[g4_occ], max_nodes=10)
    assert len(rep_occ10.infeasible_gang_resource_requests) == 1
    assert rep_occ10.to_launch == []
    assert (
        _mh_reconciler_created(
            cfgs_td_occ, a2_idle + b8_over, gangs=[g4_occ], max_workers=10
        )
        == {}
    )

    # Reclaiming per-type excess sibling QUEUED pods before min_workers refills 1 slot on tpu_a.
    cfgs_min_after_max = {
        "tpu_a": _mh_tpu_cfg("tpu_a", min_w=4, max_w=8),
        "tpu_b": _mh_tpu_cfg("tpu_b", min_w=0, max_w=4),
    }
    insts_mam = (
        [
            _run_node(
                "tpu_a",
                f"ma-{k}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "sa-0", _WID: str(k)},
                used={"TPU": 4},
                lid="La_mam",
            )
            for k in range(3)
        ]
        + [_mh_term("tpu_a", "ma-dead", lid="La_mam", with_td=True)]
        + b8_over[4:]
        + [_pending_node("tpu_b", f"qb-{k}", Instance.QUEUED) for k in range(4)]
    )
    rep_mam = _sched(cfgs_min_after_max, insts_mam, max_nodes=8)
    assert [r.count for r in rep_mam.to_launch] == [1]
    assert _mh_reconciler_created(cfgs_min_after_max, insts_mam, max_workers=8) == {
        "tpu_a": 1
    }


def test_max_workers_terminating_occupancy_and_pinned_unalloc_cancellation():
    """Max-worker enforcement accounts for omitted and draining slots, cancels pinned
    unallocated refills before live/allocated replicas, and drops fresh QUEUED slices first."""
    cfgs4 = {"tpu": _mh_tpu_cfg(max_w=4)}
    cfgs8 = {"tpu": _mh_tpu_cfg(max_w=8)}

    insts_stop4_q4 = [
        _mh_stopping("tpu", f"st-{k}", dom_key=_SLICE, dom_val="s0") for k in range(4)
    ] + [_pending_node("tpu", f"sq-{k}", Instance.QUEUED) for k in range(4)]
    rep_stop4 = _sched(cfgs4, insts_stop4_q4)
    assert {t.instance_id for t in rep_stop4.to_terminate} == {
        f"sq-{k}" for k in range(4)
    }

    cfgs_global_stop = {
        "tpu_a": _mh_tpu_cfg("tpu_a", min_w=4, max_w=8),
        "tpu_b": _mh_tpu_cfg("tpu_b", min_w=0, max_w=8),
    }
    insts_stop5 = [
        _mh_stopping("tpu_b", f"sb-st-{k}", dom_key=_SLICE, dom_val=f"sb-{k // 4}")
        for k in range(5)
    ]
    rep_gstop = _sched(cfgs_global_stop, insts_stop5, max_nodes=8)
    assert rep_gstop.to_launch == []
    assert _mh_reconciler_created(cfgs_global_stop, insts_stop5, max_workers=8) == {}

    # Pinned unallocated QUEUED refill on slice-0 cancels ahead of booting ALLOCATED or
    # idle RAY_RUNNING 4-pod sibling replicas when omitted stopping slots cap capacity.
    s0_pinned_td = [
        _run_node(
            "tpu",
            f"p0-{k}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(k)},
            used={"TPU": 4},
            lid="L0",
        )
        for k in range(3)
    ] + [_mh_term("tpu", "p0-dead", lid="L0", with_td=True)]
    q_pin = _pending_node("tpu", "q-pin-0", Instance.QUEUED)
    stop4_s2 = [
        _mh_stopping("tpu", f"s2-st-{k}", lid="L2", with_ray_node=False)
        for k in range(4)
    ]
    g4_pin_s0 = _strict_pack_gang(
        _SLICE,
        _spread_reqs({"TPU": 4}, 4, "_pg_pin_s0_cap"),
        loc_constraints=[(_SLICE, _IN, ["slice-0"])],
    )

    s1_alloc4 = []
    for k in range(4):
        an = _pending_node("tpu", f"s1-al-{k}", Instance.ALLOCATED, lid="L1")
        an.cloud_instance_id = f"c-s1-al-{k}"
        an.im_instance.cloud_instance_id = f"c-s1-al-{k}"
        s1_alloc4.append(an)
    insts_pin_alloc = s0_pinned_td + [q_pin] + s1_alloc4 + stop4_s2
    cmap_pa = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in insts_pin_alloc
        if a.im_instance.cloud_instance_id
        and a.im_instance.status != Instance.TERMINATED
    }
    rep_pin_alloc = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs8,
            current_instances=insts_pin_alloc,
            gang_resource_requests=[g4_pin_s0],
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in insts_pin_alloc], cmap_pa, {"tpu": 4}
            ),
        )
    )
    assert [t.instance_id for t in rep_pin_alloc.to_terminate] == ["q-pin-0"]

    # Standalone 4-pod QUEUED replica (tier=1) cancels before 1-pod QUEUED refill
    # attached to a 3-pod live slice (tier=2) when reclaiming 4 excess pods at max_w=8.
    q_fresh4 = [_pending_node("tpu", f"qf-{k}", Instance.QUEUED) for k in range(4)]
    insts_tier = s0_pinned_td + [q_pin] + q_fresh4 + stop4_s2
    cmap_tier = {
        a.im_instance.cloud_instance_id: CloudInstance(
            a.im_instance.cloud_instance_id, "tpu", NodeKind.WORKER, True
        )
        for a in insts_tier
        if a.im_instance.cloud_instance_id
        and a.im_instance.status not in (Instance.TERMINATED, Instance.QUEUED)
    }
    rep_tier = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            node_type_configs=cfgs8,
            current_instances=insts_tier,
            disable_launch_config_check=True,
            replica_view=_MultiHostReplicaView.build(
                [a.im_instance for a in insts_tier], cmap_tier, {"tpu": 4}
            ),
        )
    )
    assert {t.instance_id for t in rep_tier.to_terminate} == {
        f"qf-{k}" for k in range(4)
    }


def test_heterogeneous_domain_gang_refill_and_non_unlabeled_room_clamp():
    """Heterogeneous domain candidates attribute refill gaps to the launching
    type, and materialize_unlabeled_launch_gaps clamps following single-host
    and labeled multi-host types to remaining global capacity."""
    nt_helper = NodeTypeConfig(
        name="single-helper",
        resources={"CPU": 2, "HELPER": 1},
        min_worker_nodes=0,
        max_worker_nodes=2,
        runtime_label_keys=(_SLICE,),
    )
    nt_tpu = _mh_tpu_cfg("tpu", max_w=8)
    cfgs = {"single-helper": nt_helper, "tpu": nt_tpu}
    helper_live = _run_node(
        "single-helper", "helper-0", {"CPU": 2, "HELPER": 1}, {_SLICE: "slice-0"}
    )
    s0_tpu = [
        _run_node(
            "tpu",
            f"tpu-s0-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            lid="L0",
        )
        for w in range(2)
    ]
    s1_tpu = [
        _run_node(
            "tpu",
            f"tpu-s1-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-1", _WID: str(w)},
            used={"TPU": 4},
            lid="L1",
        )
        for w in range(3)
    ]
    insts = [
        helper_live,
        *s0_tpu,
        *s1_tpu,
        _mh_term("tpu", "td-s0", lid="L0", with_td=True),
        _mh_term("tpu", "td-s1", lid="L1", with_td=True),
    ]
    gang = _strict_pack_gang(
        _SLICE,
        [
            ResourceRequestUtil.make(
                {"TPU": 4},
                label_selectors=[
                    [(_WID, LabelSelectorOperator.LABEL_OPERATOR_IN, [str(w)])]
                ],
            )
            for w in range(4)
        ],
    )
    rep = _sched(
        cfgs,
        insts,
        gangs=[gang],
        reqs=[_resource_request({"HELPER": 1}, 2)],
        max_nodes=9,
    )
    assert rep.infeasible_gang_resource_requests == []
    assert len(rep.infeasible_resource_requests) == 1
    assert _launch_and_terminate(rep) == ({"tpu": 3}, [])

    rep_with_tpu_task = _sched(
        cfgs,
        insts,
        gangs=[gang],
        reqs=[_resource_request({"TPU": 4}, 1)],
        max_nodes=9,
    )
    assert rep_with_tpu_task.infeasible_gang_resource_requests == []
    assert rep_with_tpu_task.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_with_tpu_task) == ({"tpu": 3}, [])


def test_shared_domain_gang_packs_into_single_replica_without_straddling():
    """Shared-domain STRICT_PACK gangs keep bundles on one multi-host replica
    when pods are interleaved and pick already-busy replicas for smaller gangs."""
    nt_shared = NodeTypeConfig(
        name="gpu-shared",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=6,
        idle_timeout_s=60.0,
        runtime_label_keys=("ray.io/nvlink-clique", _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key="ray.io/nvlink-clique",
            domain_exclusive=False,
            worker_index_label_key=_WID,
        ),
    )
    interleaved = [
        _run_node(
            "gpu-shared",
            iid,
            {"CPU": 8, "GPU": 4},
            {"ray.io/nvlink-clique": "clique-0", _WID: str(wid)},
            idle_ms=120_000,
        )
        for iid, wid in (("i-55", 0), ("i-57", 0), ("i-58", 1), ("i-67", 1))
    ]
    for ai, rid in zip(interleaved, ("rep-11", "rep-12", "rep-12", "rep-11")):
        ai.cloud_instance_replica_id = rid
    gang_2 = _strict_pack_gang(
        "ray.io/nvlink-clique", _spread_reqs({"GPU": 4}, 2, "_pg_shared2")
    )
    rep_idle = _sched(
        {"gpu-shared": nt_shared}, interleaved, gangs=[gang_2], idle_s=60.0
    )
    assert rep_idle.infeasible_gang_resource_requests == []
    assert sorted(t.instance_id for t in rep_idle.to_terminate) in (
        ["i-55", "i-67"],
        ["i-57", "i-58"],
    )

    # Asymmetric shared clique: rep-11 has 1 busy + 1 free node, rep-12 has 2 free nodes.
    # A 2-node gang packs onto rep-12 without straddling; a 1-node gang uses rep-11's
    # free node so rep-12 stays completely idle and terminates.
    asym = [
        _run_node(
            "gpu-shared",
            iid,
            {"CPU": 8, "GPU": 4},
            {"ray.io/nvlink-clique": "clique-0", _WID: str(wid)},
            used={"GPU": 4} if busy else None,
            idle_ms=0 if busy else 120_000,
        )
        for iid, wid, busy in (
            ("a-0", 0, True),
            ("b-0", 0, False),
            ("a-1", 1, False),
            ("b-1", 1, False),
        )
    ]
    for ai, rid in zip(asym, ("rep-a", "rep-b", "rep-a", "rep-b")):
        ai.cloud_instance_replica_id = rid
    rep_2node = _sched({"gpu-shared": nt_shared}, asym, gangs=[gang_2], idle_s=60.0)
    assert rep_2node.infeasible_gang_resource_requests == []
    assert rep_2node.to_terminate == []

    gang_1 = _strict_pack_gang(
        "ray.io/nvlink-clique", _spread_reqs({"GPU": 4}, 1, "_pg_shared1")
    )
    rep_1node = _sched({"gpu-shared": nt_shared}, asym, gangs=[gang_1], idle_s=60.0)
    assert rep_1node.infeasible_gang_resource_requests == []
    assert sorted(t.instance_id for t in rep_1node.to_terminate) == ["b-0", "b-1"]


def test_runtime_label_delivery_failure_stops_simulation_and_partial_stays_fail_open():
    """Full RAY_RUNNING replicas missing a declared runtime label stop simulated
    scale-up for that key across ticks, while partial or incomplete replicas stay fail-open."""
    nt_v6e = NodeTypeConfig(
        name="v6e-16",
        resources={"CPU": 8, "TPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=16,
        runtime_label_keys=(_SLICE, _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=4,
            domain_label_key=_SLICE,
            domain_exclusive=True,
            worker_index_label_key=_WID,
        ),
    )
    slice_gang = _strict_pack_gang(
        _SLICE,
        [
            ResourceRequestUtil.make(
                {"TPU": 4},
                label_selectors=[
                    [(_WID, LabelSelectorOperator.LABEL_OPERATOR_IN, [str(w)])]
                ],
            )
            for w in range(4)
        ],
    )
    slice_only_gang = _strict_pack_gang(
        _SLICE, _spread_reqs({"TPU": 4}, 4, "_pg_sonly")
    )

    # Case A: unlabeled <1.6 full running replica without slice or worker-id stops launch loop.
    unlabeled_4 = [
        _run_node("v6e-16", f"u-{w}", {"CPU": 8, "TPU": 4}, {}) for w in range(4)
    ]
    sched_mt = ResourceDemandScheduler()
    for tick_insts in (
        unlabeled_4,
        unlabeled_4
        + [_run_node("v6e-16", f"u2-{w}", {"CPU": 8, "TPU": 4}, {}) for w in range(4)],
    ):
        for g in (slice_gang, slice_only_gang):
            rep = _sched({"v6e-16": nt_v6e}, tick_insts, gangs=[g], sched=sched_mt)
            assert rep.to_launch == []
            assert len(rep.infeasible_gang_resource_requests) == 1

    # Case B: provider replica-named >=1.6 full replica missing runtime labels also fails closed.
    prov_missing = []
    for w in range(4):
        ai = _run_node("v6e-16", f"pm-{w}", {"CPU": 8, "TPU": 4}, {})
        ai.cloud_instance_replica_id = "rep-pm"
        prov_missing.append(ai)
    rep_prov = _sched({"v6e-16": nt_v6e}, prov_missing, gangs=[slice_gang])
    assert rep_prov.to_launch == []
    assert len(rep_prov.infeasible_gang_resource_requests) == 1

    # Case C: slice present on all 4 hosts but worker-id missing on all 4 stays fail-closed for wid gang.
    wid_missing = [
        _run_node("v6e-16", f"wm-{w}", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-wm"})
        for w in range(4)
    ]
    rep_wid = _sched({"v6e-16": nt_v6e}, wid_missing, gangs=[slice_gang])
    assert rep_wid.to_launch == []
    assert len(rep_wid.infeasible_gang_resource_requests) == 1

    # Case D: partial delivery (2/4 pods carry labels) and incomplete (<4 running) stay fail-open.
    partial = [
        _run_node(
            "v6e-16",
            f"p-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-p", _WID: str(w)} if w < 2 else {},
            used={"TPU": 4} if w < 2 else None,
        )
        for w in range(4)
    ]
    for ai in partial:
        ai.cloud_instance_replica_id = "rep-part"
    rep_partial = _sched({"v6e-16": nt_v6e}, partial, gangs=[slice_gang])
    assert rep_partial.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_partial) == ({"v6e-16": 4}, [])

    incomplete_running = [
        _run_node("v6e-16", f"inc-{w}", {"CPU": 8, "TPU": 4}, {}, lid="L_inc")
        for w in range(2)
    ] + [
        _pending_node("v6e-16", f"inc-{w}", Instance.ALLOCATED, lid="L_inc")
        for w in (2, 3)
    ]
    rep_inc = _sched({"v6e-16": nt_v6e}, incomplete_running, gangs=[slice_gang])
    assert rep_inc.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_inc) == ({"v6e-16": 4}, [])

    # Mixed delivery: one fully labeled replica and one unlabeled replica keep slice scaling enabled.
    mixed = []
    for w in range(4):
        labeled = _run_node(
            "v6e-16",
            f"ml-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-ml", _WID: str(w)},
            used={"TPU": 4},
        )
        labeled.cloud_instance_replica_id = "rep-ml"
        unlabeled = _run_node(
            "v6e-16",
            f"mu-{w}",
            {"CPU": 8, "TPU": 4},
            {},
            used={"TPU": 4},
        )
        unlabeled.cloud_instance_replica_id = "rep-mu"
        mixed.extend([labeled, unlabeled])
    rep_mixed = _sched({"v6e-16": nt_v6e}, mixed, gangs=[slice_gang])
    assert rep_mixed.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_mixed) == ({"v6e-16": 4}, [])


def test_shared_domain_and_unlabeled_multireplica_partial_idle_scale_down():
    """Shared-domain and domainless multi-replica groups split healthy multiples so
    1 idle replica scales down cleanly even when siblings host active gangs."""
    gpu_shared = NodeTypeConfig(
        name="gpu-shared",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=6,
        idle_timeout_s=60.0,
        runtime_label_keys=("nvlink", _WID),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=3,
            domain_label_key="nvlink",
            domain_exclusive=False,
            worker_index_label_key=_WID,
        ),
    )
    gang_sh = _strict_pack_gang(
        "nvlink",
        [
            ResourceRequestUtil.make(
                {"GPU": 4},
                label_selectors=[
                    [(_WID, LabelSelectorOperator.LABEL_OPERATOR_IN, [str(w)])]
                ],
            )
            for w in range(3)
        ],
    )
    # 6 idle pods whose instance IDs order w=0,0,1,1,2,2 still pack into balanced 3-node units.
    idle_sh = [
        _run_node(
            "gpu-shared",
            f"w{idx // 2}-p{idx}",
            {"CPU": 8, "GPU": 4},
            {"nvlink": "clique-0", _WID: str(idx // 2)},
            idle_ms=120_000,
        )
        for idx in range(6)
    ]
    rep_sh = _sched(
        {"gpu-shared": gpu_shared},
        idle_sh,
        gangs=[gang_sh],
        idle_s=60.0,
    )
    assert rep_sh.infeasible_gang_resource_requests == []
    assert rep_sh.to_launch == []
    assert sorted(t.instance_id for t in rep_sh.to_terminate) == [
        "w0-p1",
        "w1-p3",
        "w2-p5",
    ]

    # Domainless all-idle 2-replica batch (launch_request_id='L_batch') splits cleanly
    # so 3 GPU tasks keep 1 replica busy while the remaining 3 idle pods scale down.
    gpu_plain = NodeTypeConfig(
        name="gpu-plain",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=6,
        idle_timeout_s=60.0,
        replica_config=WorkerReplicaConfig(workers_per_replica=3),
    )
    plain_nodes = [
        _run_node(
            "gpu-plain",
            f"p-{w}",
            {"CPU": 8, "GPU": 4},
            {},
            idle_ms=120_000,
            lid="L_batch",
        )
        for w in range(6)
    ]
    rep_plain = _sched(
        {"gpu-plain": gpu_plain},
        plain_nodes,
        reqs=[
            ResourceRequestByCount(
                request=ResourceRequestUtil.make({"GPU": 4}), count=3
            )
        ],
        idle_s=60.0,
    )
    assert rep_plain.to_launch == []
    assert sorted(t.instance_id for t in rep_plain.to_terminate) == [
        "p-3",
        "p-4",
        "p-5",
    ]


def test_min_workers_capped_by_per_type_max_workers_post_min():
    """Enforcing max_worker_nodes after min_worker_nodes clamps min-worker additions."""
    cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=4,
            max_worker_nodes=4,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        )
    }
    prov_pod = _run_node(
        "tpu", "p-0", {"CPU": 8, "TPU": 4}, {_SLICE: "slice-0", _WID: "0"}
    )
    prov_pod.cloud_instance_replica_id = "rep-0"
    sched = ResourceDemandScheduler()
    # Tick 1 registers the placeholder TTL; Tick 2 sees expired backfill (`min=4, max=4`),
    # so `_enforce_min_workers_per_type` adds a 4-node replica that post-min
    # `_enforce_max_workers_per_type` must cancel because `1 + 4 > max_worker_nodes=4`.
    rep_t1 = _sched(cfgs, [prov_pod], placeholder_ttl_s=0.0, sched=sched, max_nodes=8)
    assert rep_t1.to_launch == []
    rep_t2 = _sched(cfgs, [prov_pod], placeholder_ttl_s=0.0, sched=sched, max_nodes=8)
    assert rep_t2.to_launch == []
    assert rep_t2.to_terminate == []

    # Canceled min-worker launch candidates do not consume global headroom for
    # sibling node types.
    cfgs_with_helper = {
        **cfgs,
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 4, "HELPER": 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
        ),
    }
    sched_helper = ResourceDemandScheduler()
    _sched(
        cfgs_with_helper,
        [prov_pod],
        placeholder_ttl_s=0.0,
        sched=sched_helper,
        max_nodes=8,
    )
    rep_helper = _sched(
        cfgs_with_helper,
        [prov_pod],
        reqs=[_resource_request({"HELPER": 1}, 4)],
        placeholder_ttl_s=0.0,
        sched=sched_helper,
        max_nodes=8,
    )
    assert rep_helper.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_helper) == ({"cpu": 4}, [])


def test_refill_attachment_skips_skewed_head_expansion_on_full_size_replicas():
    """Refill attachment on Tick 2 matches Tick-1 modular slot math when a full
    size-multiple replica already carries excess head resources."""
    head_res = "TPU-v6e-16-head"
    cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            min_worker_nodes=0,
            max_worker_nodes=6,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=None,
                domain_exclusive=False,
                worker_index_label_key=None,
                head_resources=(head_res,),
            ),
        )
    }
    w1 = _run_node("tpu", "w1", {"CPU": 8, "TPU": 4, head_res: 1}, {}, lid="Ls")
    w2 = _run_node("tpu", "w2", {"CPU": 8, "TPU": 4}, {}, lid="Ls")
    w4 = _run_node(
        "tpu",
        "w4",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {},
        used={"TPU": 4, "CPU": 8, head_res: 1},
    )
    w6 = _run_node(
        "tpu",
        "w6",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {},
        used={"TPU": 4, "CPU": 8, head_res: 1},
        lid="Ls",
    )
    td = _mh_term("tpu", "td", lid="Ls", with_td=True)

    r_no_head = [_mk_req({"CPU": 2, "TPU": 4}), _mk_req({"CPU": 2, "TPU": 4})]
    g_no_head = GangResourceRequest(
        requests=r_no_head,
        bundle_selectors=[BundleSelector(resource_requests=r_no_head)],
    )
    r_with_head = [
        _mk_req({"CPU": 2, "TPU": 4, head_res: 1}),
        _mk_req({"CPU": 2, "TPU": 4}),
    ]
    g_with_head = GangResourceRequest(
        requests=r_with_head,
        bundle_selectors=[BundleSelector(resource_requests=r_with_head)],
    )

    rep_t1 = _sched(cfgs, [w1, w2, w4, w6, td], gangs=[g_no_head, g_with_head])
    assert rep_t1.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_t1) == ({"tpu": 2}, [])

    q2 = [_pending_node("tpu", f"q-{k}", Instance.QUEUED) for k in range(2)]
    rep_t2 = _sched(cfgs, [w1, w2, w4, w6, td] + q2, gangs=[g_no_head, g_with_head])
    assert rep_t2.infeasible_gang_resource_requests == []
    assert rep_t2.to_launch == []
    assert rep_t2.to_terminate == []

    # Same invariant holds when two live busy head nodes form a non-torn-down size-2 group.
    cfgs4 = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=None,
                domain_exclusive=False,
                worker_index_label_key=None,
                head_resources=(head_res,),
            ),
        )
    }
    h0 = _run_node(
        "tpu",
        "h0",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {},
        used={"TPU": 4, "CPU": 8, head_res: 1},
        lid="L0",
    )
    h1 = _run_node(
        "tpu",
        "h1",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {},
        used={"TPU": 4, "CPU": 8, head_res: 1},
        lid="L0",
    )
    q2_live = [_pending_node("tpu", f"ql-{k}", Instance.QUEUED) for k in range(2)]
    rep_live_t1 = _sched(cfgs4, [h0, h1], gangs=[g_with_head])
    assert rep_live_t1.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_live_t1) == ({"tpu": 2}, [])
    rep_live_t2 = _sched(cfgs4, [h0, h1] + q2_live, gangs=[g_with_head])
    assert rep_live_t2.infeasible_gang_resource_requests == []
    assert rep_live_t2.to_launch == []
    assert rep_live_t2.to_terminate == []

    # Shared-domain backfill placeholders must not expand a complete multi-head rack
    # group at the expense of another rack's incomplete replica.
    sh_cfgs = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4, head_res: 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key="rack",
                domain_exclusive=False,
                worker_index_label_key=None,
                head_resources=(head_res,),
            ),
        )
    }
    h_ra0 = _run_node(
        "tpu",
        "hra-0",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {"rack": "rack-a"},
        used={"TPU": 4, "CPU": 8, head_res: 1},
        lid="La",
    )
    h_ra1 = _run_node(
        "tpu",
        "hra-1",
        {"CPU": 8, "TPU": 4, head_res: 1},
        {"rack": "rack-a"},
        used={"TPU": 4, "CPU": 8, head_res: 1},
        lid="La",
    )
    w_rb = _run_node(
        "tpu",
        "wrb-0",
        {"CPU": 8, "TPU": 4},
        {"rack": "rack-b"},
        used={"TPU": 4},
        lid="Lb",
    )
    req_rb = ResourceRequestUtil.make(
        {"TPU": 4},
        label_selectors=[
            [("rack", LabelSelectorOperator.LABEL_OPERATOR_IN, ["rack-b"])]
        ],
    )
    rep_rb = _sched(
        sh_cfgs,
        [h_ra0, h_ra1, w_rb],
        reqs=[ResourceRequestByCount(request=req_rb, count=1)],
    )
    assert rep_rb.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_rb) == ({}, [])


def test_plain_and_shared_cross_rack_refill_bounded_by_type_gap():
    """Domainless and shared-domain non-provider types cap placeholders and refill
    gaps at the cluster pod-count deficit and coalesce materialized refills."""

    def _dead_row(nt: str, lid: str):
        im = Instance(
            instance_id=f"dead-{lid or 'untagged'}",
            instance_type=nt,
            status=Instance.TERMINATED,
            cloud_instance_id=f"c-dead-{lid or 'untagged'}",
            launch_request_id=lid,
            node_kind=NodeKind.WORKER,
        )
        for st in (
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
        ):
            im.status_history.add(instance_status=st)
        return make_autoscaler_instance(
            im_instance=im, cloud_instance_id=im.cloud_instance_id
        )

    def _sched_with_view(cfgs, insts, *, reqs=None, gangs=None):
        cloud = {
            a.cloud_instance_id: CloudInstance(
                a.cloud_instance_id,
                a.im_instance.instance_type,
                NodeKind.WORKER,
                True,
            )
            for a in insts
            if a.cloud_instance_id and a.im_instance.status != Instance.TERMINATED
        }
        view = _MultiHostReplicaView.build(
            [a.im_instance for a in insts],
            cloud,
            {
                nt: c.workers_per_replica
                for nt, c in cfgs.items()
                if c.is_multi_worker_replica
            },
        )
        return ResourceDemandScheduler().schedule(
            SchedulingRequest(
                node_type_configs=cfgs,
                current_instances=insts,
                resource_requests=reqs or [],
                gang_resource_requests=gangs or [],
                disable_launch_config_check=True,
                replica_view=view,
            )
        )

    # 1. Plain multi-host (domain_label_key=None): 3 pods L1 torn down + 1 pod L2 refill
    # is already 4/4 pods; 1, 3, or 4 GPU tasks all launch 4 new workers immediately.
    plain_cfg = {
        "grp": NodeTypeConfig(
            name="grp",
            resources={"CPU": 8, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    plain_refilled = (
        [
            _run_node(
                "grp", f"p{i}", {"CPU": 8, "GPU": 1}, {}, used={"GPU": 1}, lid="L1"
            )
            for i in range(3)
        ]
        + [_run_node("grp", "p3", {"CPU": 8, "GPU": 1}, {}, used={"GPU": 1}, lid="L2")]
        + [_dead_row("grp", "L1")]
    )
    for task_cnt in (1, 3, 4):
        rep_p = _sched_with_view(
            plain_cfg,
            plain_refilled,
            reqs=[
                ResourceRequestByCount(
                    request=ResourceRequestUtil.make({"GPU": 1}), count=task_cnt
                )
            ],
        )
        assert rep_p.infeasible_resource_requests == []
        assert _launch_and_terminate(rep_p) == ({"grp": 4}, [])

    # 2. Shared-domain (domain_exclusive=False): 2 + 1 degraded replica across two racks
    # launches exactly 5 (1 refill + 4 gang) at both max=8 and max=12 for L1 and adopted.
    rack = "rack"
    rack_gang = _strict_pack_gang(rack, _spread_reqs({"GPU": 1}, 4, "_pg_rack_refill"))
    for lid in ("L1", ""):
        split = [
            _run_node(
                "grp",
                f"s{i}",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-a" if i < 2 else "rack-b"},
                used={"GPU": 1},
                lid=lid,
            )
            for i in range(3)
        ] + [_dead_row("grp", lid)]
        for max_w in (8, 12):
            sh_cfg = {
                "grp": NodeTypeConfig(
                    name="grp",
                    resources={"CPU": 8, "GPU": 1},
                    min_worker_nodes=0,
                    max_worker_nodes=max_w,
                    runtime_label_keys=(rack,),
                    replica_config=WorkerReplicaConfig(
                        workers_per_replica=4,
                        domain_label_key=rack,
                        domain_exclusive=False,
                    ),
                )
            }
            rep_sh = _sched_with_view(sh_cfg, split, gangs=[rack_gang])
            assert rep_sh.infeasible_gang_resource_requests == []
            assert _launch_and_terminate(rep_sh) == ({"grp": 5}, [])

    # 3. Shared-domain complete cross-rack refill (3 rack-a L1 td + 1 rack-b L2 refill)
    # emits zero phantom placeholders and schedules 1 rack gang at max=8.
    sh_cfg8 = {
        "grp": NodeTypeConfig(
            name="grp",
            resources={"CPU": 8, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=8,
            runtime_label_keys=(rack,),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=rack,
                domain_exclusive=False,
            ),
        )
    }
    cross_rack_complete = (
        [
            _run_node(
                "grp",
                f"ca{i}",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-a"},
                used={"GPU": 1},
                lid="L1",
            )
            for i in range(3)
        ]
        + [
            _run_node(
                "grp",
                "cb3",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-b"},
                used={"GPU": 1},
                lid="L2",
            )
        ]
        + [_dead_row("grp", "L1")]
    )
    rep_cr = _sched_with_view(sh_cfg8, cross_rack_complete, gangs=[rack_gang])
    assert rep_cr.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_cr) == ({"grp": 4}, [])


def test_reconciler_capacity_parity_and_post_clamp_demand_conservation():
    """Scheduler capacity bounds match Reconciler active counts across partial
    provider crashes, in-flight siblings, same-tick teardowns, and sub-replica
    exits while conserving clamped requests and gangs as infeasible."""
    cfgs_prov = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 4, "HELPER": 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
        ),
    }

    # 1. Partial provider-labeled replica with 3 idle pods + 1 crashed pod holds
    # all 4 slots active in Reconciler, blocking sibling launches at max=8.
    partial_crash = (
        [
            _run_node(
                "tpu",
                f"r0-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s0", _WID: str(w)},
                rep="rep-0",
                lid="L0",
            )
            for w in range(3)
        ]
        + [_mh_term("tpu", "r0-dead", lid="L0", with_td=True)]
        + [
            _run_node(
                "tpu",
                f"r1-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s1", _WID: str(w)},
                used={"CPU": 8, "TPU": 4},
                rep="rep-1",
                lid="L1",
            )
            for w in range(4)
        ]
    )
    helper_req = [_resource_request({"HELPER": 1}, 1)]
    rep_crash = _sched(cfgs_prov, partial_crash, reqs=helper_req, max_nodes=8)
    assert _launch_and_terminate(rep_crash) == ({}, [])
    assert len(rep_crash.infeasible_resource_requests) == 1
    assert (
        _mh_reconciler_created(cfgs_prov, partial_crash, reqs=helper_req, max_workers=8)
        == {}
    )

    # 2. Mid-boot REQUESTED sibling on a provider replica grants no phantom
    # headroom to sibling node types under tight global max_num_nodes.
    mid_boot = (
        [
            _run_node(
                "tpu",
                f"r0-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s0", _WID: str(w)},
                used={"CPU": 8, "TPU": 4},
                rep="rep-0",
                lid="L0",
            )
            for w in range(3)
        ]
        + [_pending_node("tpu", "r0-req", Instance.REQUESTED, lid="L0")]
        + [
            _run_node(
                "tpu",
                f"r1-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s1", _WID: str(w)},
                used={"CPU": 8, "TPU": 4},
                rep="rep-1",
                lid="L1",
            )
            for w in range(4)
        ]
    )
    cpu_reqs3 = [_resource_request({"CPU": 4}, 3)]
    rep_boot = _sched(cfgs_prov, mid_boot, reqs=cpu_reqs3, max_nodes=8)
    assert _launch_and_terminate(rep_boot) == ({}, [])
    assert len(rep_boot.infeasible_resource_requests) == 1
    assert (
        _mh_reconciler_created(cfgs_prov, mid_boot, reqs=cpu_reqs3, max_workers=8) == {}
    )

    # 3. Same-tick TO_TERMINATE accounting: pure ALLOCATED provider teardowns
    # free uncovered slots for sibling types, whereas mixed RAY_RUNNING plus
    # ALLOCATED teardowns defer in Reconciler and keep replica slots active.
    alloc_only = [
        _pending_node("tpu", f"ta-{w}", Instance.ALLOCATED, rep="rep-a", lid="La")
        for w in range(2)
    ]
    ctx_alloc = _mk_ctx({"tpu": cfgs_prov["tpu"]}, alloc_only, max_num_nodes=4)
    nodes_alloc = ctx_alloc.get_nodes()
    for node in nodes_alloc:
        node.status = SchedulingNodeStatus.TO_TERMINATE
    ctx_alloc.update(nodes_alloc)
    by_t_alloc, tot_alloc = ctx_alloc._reconciler_active_counts()
    assert by_t_alloc.get("tpu", 0) == 0 and tot_alloc == 0

    cfgs_shrink = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=0,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 4, "HELPER": 1},
            min_worker_nodes=1,
            max_worker_nodes=4,
        ),
    }
    rep_alloc_term = _sched(cfgs_shrink, alloc_only, max_nodes=4)
    launches_alloc, terms_alloc = _launch_and_terminate(rep_alloc_term)
    assert launches_alloc == {"cpu": 1}
    assert {t[0] for t in terms_alloc} == {"ta-0", "ta-1"}
    assert _mh_reconciler_created(cfgs_shrink, alloc_only, max_workers=4) == {"cpu": 1}

    mixed_drain = [
        _run_node(
            "tpu",
            "mr-0",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "sm", _WID: "0"},
            rep="rep-m",
            lid="Lm",
        ),
        _pending_node("tpu", "ma-1", Instance.ALLOCATED, rep="rep-m", lid="Lm"),
    ]
    rep_mixed = _sched(cfgs_shrink, mixed_drain, max_nodes=4)
    launches_mix, terms_mix = _launch_and_terminate(rep_mixed)
    assert launches_mix == {}
    assert {t[0] for t in terms_mix} == {"mr-0", "ma-1"}
    assert _mh_reconciler_created(cfgs_shrink, mixed_drain, max_workers=4) == {}

    # 4. Mixed unlabeled + provider-labeled draining replica keeps all 4 slots
    # until KubeRay finishes terminating, preserving unlaunched gangs and tasks
    # in infeasible_* rather than dropping them.
    cfgs_mixed = {
        "gpu_unlab": NodeTypeConfig(
            name="gpu_unlab",
            resources={"GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        ),
        "tpu_a": NodeTypeConfig(
            name="tpu_a",
            resources={"TPU_A": 1},
            min_worker_nodes=0,
            max_worker_nodes=12,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
        "tpu_b": NodeTypeConfig(
            name="tpu_b",
            resources={"TPU_B": 1},
            min_worker_nodes=0,
            max_worker_nodes=8,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
    }
    insts_mixed = (
        [
            _run_node("gpu_unlab", f"gu-{k}", {"GPU": 1}, {}, used={"GPU": 1})
            for k in range(4)
        ]
        + [
            _run_node(
                "tpu_a",
                "ta-a0",
                {"TPU_A": 1},
                {_SLICE: "sa0", _WID: "0"},
                used={"TPU_A": 1},
                rep="rep-a0",
                lid="La0",
            ),
            _run_node(
                "tpu_a",
                "ta-a2",
                {"TPU_A": 1},
                {_SLICE: "sa2", _WID: "0"},
                idle_ms=10_000,
                rep="rep-a2",
                lid="La2",
            ),
        ]
        + [
            _run_node(
                "tpu_a",
                f"ta-a1-{w}",
                {"TPU_A": 1},
                {_SLICE: "sa1", _WID: str(w)},
                used={"TPU_A": 1},
                rep="rep-a1",
                lid="La1",
            )
            for w in range(4)
        ]
    )
    gang_b = _strict_pack_gang(_SLICE, _spread_reqs({"TPU_B": 1}, 4, "pg-b"))
    rep_gang_drop = _sched(
        cfgs_mixed, insts_mixed, gangs=[gang_b], max_nodes=16, idle_s=0
    )
    launches_gd, terms_gd = _launch_and_terminate(rep_gang_drop)
    assert launches_gd == {}
    assert [t[0] for t in terms_gd] == ["ta-a2"]
    assert len(rep_gang_drop.infeasible_gang_resource_requests) == 1
    assert (
        _mh_reconciler_created(cfgs_mixed, insts_mixed, gangs=[gang_b], max_workers=16)
        == {}
    )

    rep_task_drop = _sched(
        cfgs_mixed,
        insts_mixed,
        reqs=[_resource_request({"TPU_B": 1}, 4)],
        max_nodes=16,
        idle_s=0,
    )
    launches_td, terms_td = _launch_and_terminate(rep_task_drop)
    assert launches_td == {}
    assert [t[0] for t in terms_td] == ["ta-a2"]
    assert len(rep_task_drop.infeasible_resource_requests) == 4

    # 5. Unlabeled backfill placeholders are trimmed when global room < gap_0,
    # keeping pending tasks in infeasible_resource_requests rather than binding
    # them to unlaunchable placeholders.
    cfgs_gap = {
        "tpu": NodeTypeConfig(
            name="tpu",
            resources={"CPU": 8, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        ),
        "cpu": NodeTypeConfig(
            name="cpu",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=4,
        ),
    }
    insts_gap = [
        _run_node("tpu", "t-0", {"CPU": 8, "TPU": 4}, {}, used={"TPU": 4}, lid="L0"),
        *[
            _run_node("cpu", f"c-{k}", {"CPU": 4}, {}, used={"CPU": 4})
            for k in range(3)
        ],
    ]
    rep_gap = _sched(
        cfgs_gap,
        insts_gap,
        reqs=[_resource_request({"TPU": 4}, 2)],
        max_nodes=6,
    )
    assert _launch_and_terminate(rep_gap) == ({}, [])
    assert len(rep_gap.infeasible_resource_requests) == 2

    # 6. When the last unlabeled pod is TERMINATING while sub-replica and fresh
    # REQUESTED batches remain, post-TTL refill aligns with Reconciler and emits
    # zero sub-replica launches.
    nt_u2 = NodeTypeConfig(
        name="tpu",
        resources={"CPU": 4, "TPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=4,
        replica_config=WorkerReplicaConfig(workers_per_replica=2),
    )
    insts_exit = [
        _pending_node("tpu", "t-term", Instance.TERMINATING, lid="L0"),
        _pending_node("tpu", "q-orphan", Instance.REQUESTED, lid="L1"),
        _pending_node("tpu", "q-fresh-0", Instance.REQUESTED, lid="L2"),
        _pending_node("tpu", "q-fresh-1", Instance.REQUESTED, lid="L2"),
    ]
    sched_exit = ResourceDemandScheduler(event_logger)
    t_exit = [1000.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: t_exit[0]
    ):
        _sched(
            {"tpu": nt_u2},
            insts_exit,
            reqs=[_resource_request({"TPU": 4}, 4)],
            max_nodes=4,
            sched=sched_exit,
        )
        t_exit[0] = 2000.0
        rep_exit = _sched(
            {"tpu": nt_u2},
            insts_exit,
            reqs=[_resource_request({"TPU": 4}, 4)],
            max_nodes=4,
            sched=sched_exit,
        )
    assert _launch_and_terminate(rep_exit) == ({}, [])
    assert len(rep_exit.infeasible_resource_requests) == 1

    # 8. ScheduleContext tracks draining non-QUEUED TO_TERMINATE backfill slots,
    # domain-inherited provider replicas, reconciler headroom and per-type
    # bounds with unmaterialized launch gaps, and rebuild_replicas label resets.
    insts_drain_ctx = [
        *[
            _run_node(
                "tpu",
                f"bd-{i}",
                {"CPU": 8, "TPU": 4},
                {},
                used={"TPU": 4},
                lid="L1",
            )
            for i in range(4)
        ],
        _run_node("tpu", "pd-0", {"CPU": 8, "TPU": 4}, {}, used={}, lid="L0"),
        _mh_term("tpu", "pd-td", lid="L0", with_td=True),
    ]
    ctx_drain = _mk_ctx({"tpu": cfgs_gap["tpu"]}, insts_drain_ctx, max_num_nodes=8)
    nd_drain = ctx_drain.get_nodes()
    for node in nd_drain:
        if node.im_instance_id == "pd-0":
            node.status = SchedulingNodeStatus.TO_TERMINATE
    ctx_drain.update(nd_drain)
    assert ctx_drain.get_missing_replica_backfill_slots() == {"tpu": 3}
    assert ctx_drain.get_headroom() == 0
    assert ctx_drain.get_node_type_available()["tpu"] == 0

    cfgs_rec = {
        "tpu_a": _mh_tpu_cfg("tpu_a", max_w=8, exclusive=True),
        "tpu_b": _mh_tpu_cfg("tpu_b", max_w=8, exclusive=True),
    }
    insts_ip = [
        _run_node(
            "tpu_a",
            "ip-0",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s0", _WID: "0"},
            used={},
            rep="rep-0",
            lid="L0",
        ),
        _run_node(
            "tpu_a",
            "ip-1",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s0", _WID: "1"},
            used={},
            lid="L0",
        ),
    ]
    ctx_ip = _mk_ctx(cfgs_rec, insts_ip, max_num_nodes=8)
    nd_ip = ctx_ip.get_nodes()
    for node in nd_ip:
        if node.im_instance_id in ("ip-0", "ip-1"):
            node.status = SchedulingNodeStatus.TO_TERMINATE
    ctx_ip.update(nd_ip)
    assert ctx_ip.get_missing_replica_backfill_slots() == {"tpu_a": 4}
    assert ctx_ip.get_headroom() == 0

    insts_rec = [
        _run_node(
            "tpu_a",
            "n0",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "s0", _WID: "0"},
            lid="L0",
        ),
        _mh_stopping("tpu_a", "st1", dom_key=_SLICE, dom_val="s1", lid="L0"),
        _pending_node("tpu_a", "n2", Instance.REQUESTED, lid="L1"),
    ]
    ctx_rec = _mk_ctx(cfgs_rec, insts_rec, max_num_nodes=11)
    nd_rec = ctx_rec.get_nodes()
    nd_rec.extend(ctx_rec._make_launch_node("tpu_a") for _ in range(2))
    ctx_rec.update(nd_rec)
    assert ctx_rec.get_headroom() == 3
    assert ctx_rec.get_node_type_available()["tpu_a"] == 0

    ctx_rebuild = _mk_ctx(
        {"tpu": _mh_tpu_cfg("tpu", max_w=8, exclusive=True)},
        [_pending_node("tpu", "q-stale", Instance.QUEUED)],
        max_num_nodes=8,
    )
    n_stale = ctx_rebuild.get_nodes()[0]
    n_stale.labels["rack"] = "rack-stale"
    ctx_rebuild.update([n_stale])
    ctx_rebuild.rebuild_replicas([n_stale])
    assert "rack" not in ctx_rebuild.peek_nodes()[0].labels


def test_completed_subreplica_batches_shared_cross_rack_and_occupied_refill_caps():
    """Completed IM-created refill batches, solicited once-running teardowns,
    shared-domain cross-rack split batches, and occupied partial replicas preserve
    replica alignment across plain, exclusive, and shared domain modes."""

    # 1. Plain multi-host group (no domain key): a 3+1 busy replica completed
    # across two IM batches plans a full 4-node replica immediately for 1, 3, or
    # 4 tasks without stalling on TTL placeholders.
    plain_cfg8 = {
        "grp": NodeTypeConfig(
            name="grp",
            resources={"CPU": 8, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
    }
    plain_refilled = (
        [
            _run_node(
                "grp", f"p-{i}", {"CPU": 8, "GPU": 1}, {}, used={"GPU": 1}, lid="L1"
            )
            for i in range(3)
        ]
        + [_run_node("grp", "p-3", {"CPU": 8, "GPU": 1}, {}, used={"GPU": 1}, lid="L2")]
        + [_mh_term("grp", "p-dead", lid="L1", with_td=True)]
    )
    for task_cnt in (1, 3, 4):
        rep_plain = _sched(
            plain_cfg8,
            plain_refilled,
            reqs=[_resource_request({"GPU": 1}, task_cnt)],
            max_nodes=8,
        )
        assert rep_plain.infeasible_resource_requests == []
        assert _launch_and_terminate(rep_plain) == ({"grp": 4}, [])

    p_im = _run_node("grp", "p-im", {"CPU": 8, "GPU": 1}, {}, used={"GPU": 1}, lid="L2")
    p_im.im_instance.status_history.add(instance_status=Instance.QUEUED)
    p_im.im_instance.status_history.add(instance_status=Instance.RAY_RUNNING)
    plain_with_im_sub = [
        _run_node("grp", f"pf-{i}", {"CPU": 8, "GPU": 1}, {}, used={"GPU": 1}, lid="L1")
        for i in range(4)
    ] + [p_im]
    rep_im_sub = _sched(
        plain_cfg8,
        plain_with_im_sub,
        reqs=[_resource_request({"GPU": 1}, 1)],
        max_nodes=8,
    )
    assert rep_im_sub.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_im_sub) == ({"grp": 3}, [])

    # 2. Exclusive domain: a solicited teardown of a once-running pod records the
    # remembered domain timestamp so a replacement host arriving alone in a new
    # batch refills its remaining slots immediately.
    excl_cfg8 = {"tpu": _mh_tpu_cfg("tpu", max_w=8, exclusive=True)}
    sched_excl = ResourceDemandScheduler(event_logger)
    live_s0 = [
        _run_node(
            "tpu",
            f"ex-{w}",
            {"CPU": 8, "TPU": 4},
            {_SLICE: "slice-0", _WID: str(w)},
            used={"TPU": 4} if w < 3 else {},
            lid="L0",
        )
        for w in range(4)
    ]
    _sched(excl_cfg8, live_s0, sched=sched_excl)
    p3_solic = _mh_term("tpu", "ex-3", lid="L0", solic=True, end_ns=1_000_000_000)
    p4_pad = _run_node(
        "tpu",
        "ex-4",
        {"CPU": 8, "TPU": 4},
        {_SLICE: "slice-0", _WID: "0"},
        used={"TPU": 4},
        lid="L1",
    )
    rep_excl = _sched(
        excl_cfg8,
        [p4_pad, p3_solic],
        reqs=[_resource_request({"TPU": 4}, 2)],
        max_nodes=8,
        sched=sched_excl,
    )
    assert rep_excl.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_excl) == ({"tpu": 3}, [])

    # 3. Shared domain cross-rack split batches: a 2+1 degraded replica across two
    # racks plans 5 nodes (1 refill + 4 gang) for both batch-created and adopted
    # pods, and a 5-pod cross-rack split batch scales cleanly after placeholder TTL.
    rack = "rack"

    def _sh_cfg(max_w: int) -> Dict[str, NodeTypeConfig]:
        return {
            "grp": NodeTypeConfig(
                name="grp",
                resources={"CPU": 8, "GPU": 1},
                min_worker_nodes=0,
                max_worker_nodes=max_w,
                runtime_label_keys=(rack,),
                replica_config=WorkerReplicaConfig(
                    workers_per_replica=4,
                    domain_label_key=rack,
                    domain_exclusive=False,
                ),
            )
        }

    rack_gang = _strict_pack_gang(rack, _spread_reqs({"GPU": 1}, 4, "pg-sh-split"))
    for lid in ("L1", ""):
        split_2_1 = [
            _run_node(
                "grp",
                "ra-0",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-a"},
                used={"GPU": 1},
                lid=lid,
            ),
            _run_node(
                "grp",
                "ra-1",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-a"},
                used={"GPU": 1},
                lid=lid,
            ),
            _run_node(
                "grp",
                "rb-2",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-b"},
                used={"GPU": 1},
                lid=lid,
            ),
            _mh_term("grp", "r-dead", lid=lid, with_td=True),
        ]
        for max_w in (8, 12):
            rep_2_1 = _sched(
                _sh_cfg(max_w), split_2_1, gangs=[rack_gang], max_nodes=max_w
            )
            assert rep_2_1.infeasible_gang_resource_requests == []
            assert _launch_and_terminate(rep_2_1) == ({"grp": 5}, [])

    split_5 = [
        _run_node(
            "grp",
            "s5-0",
            {"CPU": 8, "GPU": 1},
            {rack: "rack-a"},
            used={"CPU": 8, "GPU": 1},
            lid="L1",
        ),
        *[
            _run_node(
                "grp",
                f"s5-{1 + k}",
                {"CPU": 8, "GPU": 1},
                {rack: "rack-b"},
                used={"CPU": 8, "GPU": 1},
                lid="L1",
            )
            for k in range(4)
        ],
    ]
    sched_s5 = ResourceDemandScheduler(event_logger)
    t_s5 = [3000.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: t_s5[0]
    ):
        _sched(
            _sh_cfg(8),
            split_5,
            reqs=[_resource_request({"GPU": 1}, 2)],
            max_nodes=8,
            sched=sched_s5,
        )
        t_s5[0] = 4000.0
        rep_s5 = _sched(
            _sh_cfg(8),
            split_5,
            reqs=[_resource_request({"GPU": 1}, 2)],
            max_nodes=8,
            sched=sched_s5,
        )
    assert rep_s5.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_s5) == ({"grp": 3}, [])

    # 4. Occupied partial replicas (< size open) preserve full size-node pending
    # gang slices instead of shattering them across disjoint occupied islands,
    # while sub-replica remainder pods still attach cleanly.
    occ_cfg = {"tpu": _mh_tpu_cfg("tpu", max_w=12, exclusive=True)}
    gang_4 = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "pg-occ"))
    insts_occ = (
        [
            _run_node(
                "tpu",
                "o0-0",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s0", _WID: "0"},
                used={"TPU": 4},
                lid="L0",
            )
        ]
        + [
            _run_node(
                "tpu",
                f"o1-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s1", _WID: str(w)},
                used={"TPU": 4},
                lid="L1",
            )
            for w in range(2)
        ]
        + [
            _pending_node("tpu", f"g-{k}", Instance.REQUESTED, lid="L2")
            for k in range(4)
        ]
    )
    rep_occ = _sched(occ_cfg, insts_occ, gangs=[gang_4], max_nodes=12)
    assert rep_occ.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_occ) == ({}, [])

    insts_rem = (
        [
            _run_node(
                "tpu",
                f"a0-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s0", _WID: str(w)},
                used={"TPU": 4},
                lid="L0",
            )
            for w in range(3)
        ]
        + [
            _run_node(
                "tpu",
                f"a1-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s1", _WID: str(w)},
                used={"TPU": 4},
                lid="L1",
            )
            for w in range(3)
        ]
        + [_pending_node("tpu", "rem-0", Instance.REQUESTED, lid="L2")]
    )
    rep_rem = _sched(
        occ_cfg, insts_rem, reqs=[_resource_request({"TPU": 4}, 1)], max_nodes=12
    )
    assert rep_rem.infeasible_resource_requests == []
    assert _launch_and_terminate(rep_rem) == ({}, [])

    g4_a = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "pg-occ-a"))
    g4_b = _strict_pack_gang(_SLICE, _spread_reqs({"TPU": 4}, 4, "pg-occ-b"))
    insts_occ_cap = (
        [
            _run_node(
                "tpu",
                "o0-0",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s0", _WID: "0"},
                used={"TPU": 4},
                lid="L0",
            ),
            _pending_node("tpu", "o0-1", Instance.ALLOCATED, lid="L0"),
        ]
        + [_mh_term("tpu", f"o0-td{k}", lid="L0", with_td=True) for k in range(2)]
        + [
            _run_node(
                "tpu",
                "e1-0",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s1", _WID: "0"},
                used={},
                lid="L1",
            )
        ]
        + [_mh_term("tpu", f"e1-td{k}", lid="L1", with_td=True) for k in range(3)]
        + [
            _run_node(
                "tpu",
                f"e2-{w}",
                {"CPU": 8, "TPU": 4},
                {_SLICE: "s2", _WID: str(w)},
                used={},
                lid="L2",
            )
            for w in range(3)
        ]
        + [_mh_term("tpu", "e2-td0", lid="L2", with_td=True)]
        + [_pending_node("tpu", f"qo-{k}", Instance.QUEUED, lid="L3") for k in range(5)]
    )
    rep_occ_cap = _sched(occ_cfg, insts_occ_cap, gangs=[g4_a, g4_b], max_nodes=12)
    assert rep_occ_cap.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_occ_cap)[0] == {}


def test_healed_labeled_replica_with_retained_terminal_sibling_idle_and_demand():
    """A healed labeled multi-host replica retaining a 24h TERMINATED sibling
    row must neither poison active draining checks nor suppress newer batches."""

    def _run_pod(
        nt: str,
        iid: str,
        cid: str,
        *,
        lid: str = "",
        rep: str = "",
        dom_key: str = _SLICE,
        dom_val: str = "slice-0",
        wid: Optional[str] = None,
        res: Optional[Dict[str, float]] = None,
        idle_ms: int = 0,
        free_res: bool = False,
    ) -> AutoscalerInstance:
        nid = iid.encode().ljust(28, b"h")[:28]
        tot = dict(res or {"CPU": 8.0, "TPU": 4.0})
        avail = dict(tot) if (idle_ms or free_res) else {}
        lbls = {dom_key: dom_val}
        if wid is not None:
            lbls[_WID] = wid
        return AutoscalerInstance(
            im_instance=Instance(
                instance_id=iid,
                instance_type=nt,
                status=Instance.RAY_RUNNING,
                node_id=nid.hex(),
                cloud_instance_id=cid,
                launch_request_id=lid,
                node_kind=NodeKind.WORKER,
            ),
            ray_node=NodeState(
                node_id=nid,
                instance_id=cid,
                ray_node_type_name=nt,
                status=NodeStatus.IDLE if idle_ms else NodeStatus.RUNNING,
                idle_duration_ms=idle_ms,
                total_resources=tot,
                available_resources=avail,
                labels=lbls,
            ),
            cloud_instance_id=cid,
            cloud_instance_replica_id=rep,
        )

    def _term_row(
        iid: str, nt: str, cid: str, lid: str, rep: str
    ) -> AutoscalerInstance:
        im = Instance(
            instance_id=iid,
            instance_type=nt,
            status=Instance.TERMINATED,
            cloud_instance_id=cid,
            launch_request_id=lid,
            node_kind=NodeKind.WORKER,
        )
        im.status_history.add(instance_status=Instance.RAY_RUNNING, timestamp_ns=100)
        im.status_history.add(instance_status=Instance.TERMINATING, timestamp_ns=150)
        im.status_history.add(instance_status=Instance.TERMINATED, timestamp_ns=200)
        return AutoscalerInstance(
            im_instance=im,
            cloud_instance_id=cid,
            cloud_instance_replica_id=rep,
        )

    # 1. Exclusive TPU (size=4, max=4): both new-pod heal and same-cid untagged
    # restart retain TERMINATED sibling from L1 without locking rep-0 for 24h.
    excl_cfg = {"tpu": _mh_tpu_cfg("tpu", max_w=4, size=4, exclusive=True)}
    g4 = _locality_gang(
        [ResourceRequest(resources_bundle={"TPU": 4.0}) for _ in range(4)], _SLICE
    )
    for mode_name, healed_cid in (("new-pod", "c-new"), ("same-cid", "c-dead")):
        for idle_ms in (5_000, 0):
            insts = [
                _run_pod(
                    "tpu",
                    f"w-{mode_name}-{w}",
                    f"c-{w}",
                    lid="L1",
                    rep="rep-0",
                    wid=str(w),
                    idle_ms=idle_ms,
                    free_res=(idle_ms == 0),
                )
                for w in range(3)
            ] + [
                _run_pod(
                    "tpu",
                    f"w-{mode_name}-3",
                    healed_cid,
                    lid="",
                    rep="rep-0",
                    wid="3",
                    idle_ms=idle_ms,
                    free_res=(idle_ms == 0),
                ),
                _term_row("t-dead", "tpu", "c-dead", "L1", "rep-0"),
            ]
            view = _synthetic_replica_view(excl_cfg, insts)
            assert view is not None
            assert view.non_fillable_reps == set()
            if mode_name == "new-pod":
                live_cloud = {
                    inst.cloud_instance_id: CloudInstance(
                        cloud_instance_id=inst.cloud_instance_id,
                        node_type="tpu",
                        is_running=True,
                        node_kind=NodeKind.WORKER,
                        replica_id="rep-0",
                    )
                    for inst in insts[:-1]
                }
                missing_view = _MultiHostReplicaView.build(
                    [inst.im_instance for inst in insts],
                    live_cloud,
                    {"tpu": 4},
                )
                assert missing_view.non_fillable_reps == set()
                if idle_ms:
                    rep_missing_idle = ResourceDemandScheduler(event_logger).schedule(
                        SchedulingRequest(
                            disable_launch_config_check=True,
                            node_type_configs=excl_cfg,
                            current_instances=insts[:-1],
                            idle_timeout_s=1.0,
                            replica_view=missing_view,
                        )
                    )
                    assert len(rep_missing_idle.to_terminate) == 4
            ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
                SchedulingRequest(
                    disable_launch_config_check=True,
                    node_type_configs=excl_cfg,
                    current_instances=insts,
                    replica_view=view,
                )
            )
            assert all(not n._is_draining_replica for n in ctx.get_nodes())
            if idle_ms:
                rep_idle = _sched(excl_cfg, insts, idle_s=1.0)
                assert len(rep_idle.to_terminate) == 4
                assert all(
                    t.cause == TerminationRequest.Cause.IDLE
                    for t in rep_idle.to_terminate
                )
            else:
                rep_busy = _sched(excl_cfg, insts, gangs=[g4])
                assert rep_busy.infeasible_gang_resource_requests == []
                assert _launch_and_terminate(rep_busy)[0] == {}

    # 2. Shared-domain GPU (size=3, max=6): healed replica scales down when idle
    # and accepts task demand without over-launching a replacement replica.
    gpu_cfg = {
        "gpu": NodeTypeConfig(
            name="gpu",
            resources={"CPU": 4, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=6,
            runtime_label_keys=("rack",),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=3,
                domain_label_key="rack",
                domain_exclusive=False,
            ),
        )
    }
    for mode_name, healed_cid in (("new-pod", "cg-new"), ("same-cid", "cg-dead")):
        for idle_ms in (5_000, 0):
            gpu_insts = [
                _run_pod(
                    "gpu",
                    f"g-{mode_name}-{w}",
                    f"cg-{w}",
                    lid="Lg1",
                    rep="rep-g",
                    dom_key="rack",
                    dom_val="rack-0",
                    res={"CPU": 4.0, "GPU": 1.0},
                    idle_ms=idle_ms,
                    free_res=(idle_ms == 0),
                )
                for w in range(2)
            ] + [
                _run_pod(
                    "gpu",
                    f"g-{mode_name}-2",
                    healed_cid,
                    lid="",
                    rep="rep-g",
                    dom_key="rack",
                    dom_val="rack-0",
                    res={"CPU": 4.0, "GPU": 1.0},
                    idle_ms=idle_ms,
                    free_res=(idle_ms == 0),
                ),
                _term_row("tg-dead", "gpu", "cg-dead", "Lg1", "rep-g"),
            ]
            if idle_ms:
                rep_g_idle = _sched(gpu_cfg, gpu_insts, idle_s=1.0)
                assert len(rep_g_idle.to_terminate) == 3
            else:
                rep_g_busy = _sched(
                    gpu_cfg,
                    gpu_insts,
                    reqs=[_resource_request({"GPU": 1.0}, 1)],
                )
                assert rep_g_busy.infeasible_resource_requests == []
                assert _launch_and_terminate(rep_g_busy)[0] == {}

    # 3. Retained TERMINATED sibling in L1 keeps L1 non-fillable via torn_down_lids
    # without suppressing a newer healthy backfill batch L2 on the same replica.
    bf_insts = [
        _run_pod("tpu", "b0", "cb0", lid="L1", rep="rep-0", wid="0", free_res=True),
        _run_pod("tpu", "b1", "cb1", lid="L1", rep="rep-0", wid="1", free_res=True),
        _term_row("b-dead", "tpu", "cb-dead", "L1", "rep-0"),
        AutoscalerInstance(
            im_instance=Instance(
                instance_id="b2",
                instance_type="tpu",
                status=Instance.ALLOCATED,
                cloud_instance_id="cb2",
                launch_request_id="L2",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="cb2",
            cloud_instance_replica_id="rep-0",
        ),
        _pending_node("tpu", "b3", Instance.REQUESTED, lid="L2"),
    ]
    bf_view = _synthetic_replica_view(excl_cfg, bf_insts)
    assert bf_view is not None
    assert bf_view.owed_batch_slots("tpu", "rep-0", "L1", 4) == 0
    assert bf_view.owed_batch_slots("tpu", "rep-0", "L2", 4) == 1
    rep_bf = _sched(excl_cfg, bf_insts, gangs=[g4])
    assert rep_bf.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_bf)[0] == {}


def test_pending_termination_deltas_unlabeled_gap_and_single_host_ray_stop_requested():
    """Verify ScheduleContext same-tick termination deltas project unlabeled gap,
    labeled whole-vs-partial replica deferral, and single-host RAY_STOP_REQUESTED."""
    tpu_cfg = {"tpu": _mh_tpu_cfg("tpu", max_w=8, size=4, dom_key=None)}

    # 1. Unlabeled TO_TERMINATE ALLOCATED tears down its sibling REQUESTED batch
    # and drops from unlabeled_backfill_gap (and returns 0 when occ_cnt == 0).
    unlab_insts = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_id="p0",
                instance_type="tpu",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c0",
                launch_request_id="L0",
                node_kind=NodeKind.WORKER,
            ),
            ray_node=NodeState(
                node_id=b"u0".ljust(28, b"0"),
                instance_id="c0",
                ray_node_type_name="tpu",
                status=NodeStatus.RUNNING,
                total_resources={"CPU": 8.0, "TPU": 4.0},
                available_resources={"CPU": 8.0, "TPU": 4.0},
            ),
            cloud_instance_id="c0",
        ),
        make_autoscaler_instance(
            im_instance=Instance(
                instance_id="p1",
                instance_type="tpu",
                status=Instance.ALLOCATED,
                cloud_instance_id="c1",
                launch_request_id="L1",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id="c1",
        ),
        _pending_node("tpu", "p2", Instance.REQUESTED, lid="L1"),
        _pending_node("tpu", "p3", Instance.REQUESTED, lid="L1"),
    ]
    ctx_u = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=tpu_cfg,
            current_instances=unlab_insts,
            replica_view=_synthetic_replica_view(tpu_cfg, unlab_insts),
        )
    )
    assert ctx_u.get_own_pad_credit("tpu", extra_launched_types=("tpu",)) == 0
    pad_insts = [_mh_stopping("tpu", "st0", lid="L0")]
    ctx_pad = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=tpu_cfg,
            current_instances=pad_insts,
            replica_view=_synthetic_replica_view(tpu_cfg, pad_insts),
        )
    )
    assert ctx_pad.get_own_pad_credit("tpu") == 3
    assert ctx_pad.get_own_pad_credit("tpu", extra_launched_types=("tpu",)) == 0
    nodes_u = ctx_u.get_nodes()
    for n in nodes_u:
        if n.im_instance_id == "p1":
            n.status = SchedulingNodeStatus.TO_TERMINATE
    ctx_u.update(nodes_u)
    assert ctx_u.get_unlabeled_backfill_gap("tpu", 4) == 3
    by_nt_u, tot_u = ctx_u._reconciler_active_counts()
    assert by_nt_u.get("tpu", 0) == 1 and tot_u == 1

    # When the sole occupying unlabeled pod terminates, gap drops to 0 even with
    # another unclaimed REQUESTED batch still present.
    solo_insts = [
        unlab_insts[1],
        _pending_node("tpu", "s2", Instance.REQUESTED, lid="L2"),
    ]
    ctx_solo = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=tpu_cfg,
            current_instances=solo_insts,
            replica_view=_synthetic_replica_view(tpu_cfg, solo_insts),
        )
    )
    nodes_s = ctx_solo.get_nodes()
    nodes_s[0].status = SchedulingNodeStatus.TO_TERMINATE
    ctx_solo.update(nodes_s)
    assert ctx_solo.get_unlabeled_backfill_gap("tpu", 4) == 0

    # 2. Labeled mixed RAY_RUNNING + ALLOCATED whole-replica vs partial deferral.
    lbl_cfg = {"tpu": _mh_tpu_cfg("tpu", max_w=8, size=4, exclusive=True)}
    mixed_insts = [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id=f"m{i}",
                instance_type="tpu",
                status=Instance.RAY_RUNNING if i < 2 else Instance.ALLOCATED,
                node_id=f"m{i}".encode().ljust(28, b"0").hex() if i < 2 else "",
                cloud_instance_id=f"cm{i}",
                launch_request_id="Lm0",
                node_kind=NodeKind.WORKER,
            ),
            ray_node=(
                NodeState(
                    node_id=f"m{i}".encode().ljust(28, b"0"),
                    instance_id=f"cm{i}",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                    total_resources={"CPU": 8.0, "TPU": 4.0},
                    available_resources={"CPU": 8.0, "TPU": 4.0},
                    labels={_SLICE: "sm0", _WID: str(i)},
                )
                if i < 2
                else None
            ),
            cloud_instance_id=f"cm{i}",
            cloud_instance_replica_id="rep-m",
        )
        for i in range(4)
    ]
    req_m = SchedulingRequest(
        disable_launch_config_check=True,
        node_type_configs=lbl_cfg,
        current_instances=mixed_insts,
        replica_view=_synthetic_replica_view(lbl_cfg, mixed_insts),
    )
    ctx_whole = ResourceDemandScheduler.ScheduleContext.from_schedule_request(req_m)
    nodes_w = ctx_whole.get_nodes()
    for n in nodes_w:
        n.status = SchedulingNodeStatus.TO_TERMINATE
    ctx_whole.update(nodes_w)
    def_w, drop_w, td_w, nf_w = ctx_whole._pending_termination_deltas()
    assert def_w == {id(n) for n in nodes_w[2:]}
    assert drop_w == set() and td_w == set()
    assert nf_w == {("tpu", "rep-m")}

    ctx_part = ResourceDemandScheduler.ScheduleContext.from_schedule_request(req_m)
    nodes_p = ctx_part.get_nodes()
    nodes_p[0].status = SchedulingNodeStatus.TO_TERMINATE
    ctx_part.update(nodes_p)
    def_p, drop_p, td_p, nf_p = ctx_part._pending_termination_deltas()
    assert def_p == set() and drop_p == set() and td_p == set() and nf_p == set()

    # 3. Single-host RAY_STOP_REQUESTED TO_TERMINATE counts in mixed clusters.
    mix_cfg = {
        "cpu": NodeTypeConfig(
            name="cpu", resources={"CPU": 4}, min_worker_nodes=0, max_worker_nodes=8
        ),
        "tpu": _mh_tpu_cfg("tpu", max_w=8, size=4, exclusive=True),
    }
    cpu_stop = make_autoscaler_instance(
        im_instance=Instance(
            instance_id="cpu-0",
            instance_type="cpu",
            status=Instance.RAY_STOP_REQUESTED,
            cloud_instance_id="cc-0",
            node_kind=NodeKind.WORKER,
        ),
        cloud_instance_id="cc-0",
    )
    ctx_sh = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=mix_cfg,
            current_instances=[cpu_stop],
            replica_view=_synthetic_replica_view(mix_cfg, [cpu_stop]),
        )
    )
    nodes_sh = ctx_sh.get_nodes()
    nodes_sh[0].status = SchedulingNodeStatus.TO_TERMINATE
    ctx_sh.update(nodes_sh)
    sh_counts, sh_total = ctx_sh._reconciler_active_counts()
    assert sh_counts.get("cpu", 0) == 1 and sh_total == 1


def test_global_max_workers_canceling_unlabeled_min_launches_preserves_valid_queued():
    """Canceling speculative unlabeled min-worker launches for global max_num_nodes
    must immediately drop their alignment tax so valid sibling QUEUED batches survive."""
    cfgs = {
        "tpu_a": NodeTypeConfig(
            name="tpu_a",
            resources={"CPU": 1, "TPU_A": 1},
            min_worker_nodes=8,
            max_worker_nodes=12,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        ),
        "tpu_b": NodeTypeConfig(
            name="tpu_b",
            resources={"CPU": 1, "TPU_B": 1},
            min_worker_nodes=0,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=2),
        ),
        "cpu": NodeTypeConfig(
            name="cpu", resources={"CPU": 1}, min_worker_nodes=0, max_worker_nodes=8
        ),
    }
    base_insts = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_id="ta0",
                instance_type="tpu_a",
                status=Instance.RAY_RUNNING,
                node_id=b"ta0".ljust(28, b"0").hex(),
                cloud_instance_id="ca0",
                launch_request_id="La0",
                node_kind=NodeKind.WORKER,
            ),
            ray_node=NodeState(
                node_id=b"ta0".ljust(28, b"0"),
                instance_id="ca0",
                ray_node_type_name="tpu_a",
                status=NodeStatus.RUNNING,
                total_resources={"CPU": 1.0, "TPU_A": 1.0},
                available_resources={},
            ),
            cloud_instance_id="ca0",
        ),
        _mh_term("tpu_a", "ta-td", lid="La0"),
    ] + [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_id=f"c{i}",
                instance_type="cpu",
                status=Instance.RAY_RUNNING,
                node_id=f"c{i}".encode().ljust(28, b"0").hex(),
                cloud_instance_id=f"cc{i}",
                node_kind=NodeKind.WORKER,
            ),
            ray_node=NodeState(
                node_id=f"c{i}".encode().ljust(28, b"0"),
                instance_id=f"cc{i}",
                ray_node_type_name="cpu",
                status=NodeStatus.RUNNING,
                total_resources={"CPU": 1.0},
                available_resources={},
            ),
            cloud_instance_id=f"cc{i}",
        )
        for i in range(3)
    ]
    gang_b = GangResourceRequest(
        requests=[ResourceRequest(resources_bundle={"TPU_B": 1.0}) for _ in range(2)]
    )

    # Tick 1 with tpu_a min=0 queues the 2-worker tpu_b gang cleanly within cap=8.
    cfgs_t1 = dict(cfgs)
    cfgs_t1["tpu_a"] = NodeTypeConfig(
        name="tpu_a",
        resources={"CPU": 1, "TPU_A": 1},
        min_worker_nodes=0,
        max_worker_nodes=12,
        replica_config=WorkerReplicaConfig(workers_per_replica=4),
    )
    rep_t1 = _sched(cfgs_t1, base_insts, gangs=[gang_b], max_nodes=8)
    assert rep_t1.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_t1) == ({"tpu_b": 2}, [])

    # Tick 2 enables tpu_a min_worker_nodes=8 while tpu_b's 2 nodes remain QUEUED;
    # canceling tpu_a's speculative launches must not churn tpu_b's queued nodes.
    tick2_insts = base_insts + [
        _pending_node("tpu_b", "qb0", Instance.QUEUED),
        _pending_node("tpu_b", "qb1", Instance.QUEUED),
    ]
    rep_t2 = _sched(cfgs, tick2_insts, gangs=[gang_b], max_nodes=8)
    assert rep_t2.infeasible_gang_resource_requests == []
    assert _launch_and_terminate(rep_t2) == ({}, [])


def test_gang_domain_search_multiset_dedup_fallback_memo_and_counter_cases(monkeypatch):
    """Verify Gap S1 O(S) multiset deduplication on UUID-permuted booting slices and
    fallback-tier running slices, plus soundness counter-cases (multi-bundle packing,
    overlapping multi-value IN selectors, draining/leaving domain isolation, mixed IPPR
    signatures, and default schedule_in_domain signature)."""
    import random
    import uuid

    from ray.autoscaler.v2.scheduler import _GangSearch
    from ray.autoscaler.v2.schema import IPPRGroupSpec, IPPRStatus

    rng = random.Random(20261121)
    cfgs_8h = {
        "v6e-32": NodeTypeConfig(
            name="v6e-32",
            resources={"CPU": 8, "TPU": 4, "TPU-v6e-32-head": 1},
            min_worker_nodes=0,
            max_worker_nodes=640,
            labels={
                _ACC: "TPU-V6E",
                _TOPO: "4x8",
                "ray.io/tpu-pod-type": "v6e-32",
            },
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=8,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=("TPU-v6e-32-head",),
            ),
        )
    }

    # 1. 60 booting 8-host KubeRay replicas with random UUID im_instance_ids -> O(S)=60 calls.
    boot_instances = []
    for rep_idx in range(60):
        rep_name = f"v6e-32-rep-{rep_idx}"
        for host_idx in range(8):
            uid = str(uuid.UUID(int=rng.getrandbits(128)))
            inst = make_autoscaler_instance(
                im_instance=Instance(
                    instance_id=uid,
                    instance_type="v6e-32",
                    status=Instance.ALLOCATED,
                    cloud_instance_id=f"{rep_name}-pod-{host_idx}",
                    node_kind=NodeKind.WORKER,
                ),
                cloud_instance_id=f"{rep_name}-pod-{host_idx}",
            )
            inst.cloud_instance_replica_id = rep_name
            boot_instances.append(inst)
    gangs_8h = [_tpu_gang(tuple(range(8)), pod_type="v6e-32") for _ in range(60)]

    real_sched_in_dom = _GangSearch.schedule_in_domain
    dom_calls = []

    def _spy_sched_in_dom(*args, **kwargs):
        dom_calls.append(1)
        return real_sched_in_dom(*args, **kwargs)

    monkeypatch.setattr(
        _GangSearch, "schedule_in_domain", staticmethod(_spy_sched_in_dom)
    )
    sched = ResourceDemandScheduler()
    rep_boot = sched.schedule(
        SchedulingRequest(
            node_type_configs=cfgs_8h,
            current_instances=boot_instances,
            gang_resource_requests=gangs_8h,
            disable_launch_config_check=True,
        )
    )
    assert rep_boot.infeasible_gang_resource_requests == []
    assert rep_boot.to_launch == []
    assert len(dom_calls) == 60

    # 2. 60 running fallback-tier 4-host slices (preferred tier=gold, fallback tier=silver) -> O(S)=60 calls.
    cfgs_fb = {
        "v6e-16": NodeTypeConfig(
            name="v6e-16",
            resources={"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
            min_worker_nodes=0,
            max_worker_nodes=320,
            labels={_ACC: "TPU-V6E", _TOPO: "4x4", "tier": "silver"},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=("TPU-v6e-16-head",),
            ),
        )
    }
    run_fb_instances = []
    for rep_idx in range(60):
        sname = f"slice-fb-{rep_idx:03d}"
        for w in range(4):
            nid = f"fb-{rep_idx:03d}-{w}".encode().ljust(28, b"0")
            cid = f"pod-fb-{rep_idx:03d}-{w}"
            inst = make_autoscaler_instance(
                im_instance=Instance(
                    instance_id=f"im-{cid}",
                    instance_type="v6e-16",
                    status=Instance.RAY_RUNNING,
                    node_id=nid.hex(),
                    cloud_instance_id=cid,
                    node_kind=NodeKind.WORKER,
                ),
                ray_node=NodeState(
                    node_id=nid,
                    instance_id=cid,
                    ray_node_type_name="v6e-16",
                    status=NodeStatus.RUNNING,
                    total_resources={
                        "CPU": 8.0,
                        "TPU": 4.0,
                        **({"TPU-v6e-16-head": 1.0} if w == 0 else {}),
                    },
                    available_resources={
                        "CPU": 8.0,
                        "TPU": 4.0,
                        **({"TPU-v6e-16-head": 1.0} if w == 0 else {}),
                    },
                    labels={
                        _ACC: "TPU-V6E",
                        _TOPO: "4x4",
                        "tier": "silver",
                        _SLICE: sname,
                        _WID: str(w),
                    },
                ),
                cloud_instance_id=cid,
            )
            inst.cloud_instance_replica_id = sname
            run_fb_instances.append(inst)
    # Build 60 fallback-tier locality gangs using _tpu_gang's BundleSelector idiom.
    fb_gangs = []
    for _ in range(60):
        g = _tpu_gang((0, 1, 2, 3), pod_type="v6e-16")
        for w, b_req in enumerate(g.bundle_selectors[0].resource_requests):
            del b_req.label_selectors[:]
            s_pref = b_req.label_selectors.add()
            c1 = s_pref.label_constraints.add()
            c1.label_key, c1.operator = _WID, _IN
            c1.label_values.append(str(w))
            c2 = s_pref.label_constraints.add()
            c2.label_key, c2.operator = "tier", _IN
            c2.label_values.append("gold")
            s_fb = b_req.label_selectors.add()
            c3 = s_fb.label_constraints.add()
            c3.label_key, c3.operator = _WID, _IN
            c3.label_values.append(str(w))
            c4 = s_fb.label_constraints.add()
            c4.label_key, c4.operator = "tier", _IN
            c4.label_values.append("silver")
        fb_gangs.append(g)

    dom_calls.clear()
    rep_fb = sched.schedule(
        SchedulingRequest(
            node_type_configs=cfgs_fb,
            current_instances=run_fb_instances,
            gang_resource_requests=fb_gangs,
            disable_launch_config_check=True,
        )
    )
    assert rep_fb.infeasible_gang_resource_requests == []
    assert rep_fb.to_launch == []
    assert len(dom_calls) == 60
    monkeypatch.setattr(
        _GangSearch, "schedule_in_domain", staticmethod(real_sched_in_dom)
    )

    # 3. Counter-case Trap 1: multi-bundle packing on booting domains prefers fewer pending hosts (2 vs 3).
    cfgs_pack = {
        "gpu-4h": NodeTypeConfig(
            name="gpu-4h",
            resources={"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=16,
            runtime_label_keys=("rack",),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key="rack",
                domain_exclusive=False,
            ),
        )
    }
    ctx_pack = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            node_type_configs=cfgs_pack,
            current_instances=[],
            disable_launch_config_check=True,
        )
    )
    # Seed rack-frag (free GPUs: 2, 2, 2, 4 -> needs 3 pending hosts for 4x{GPU:2})
    # before rack-dense (free GPUs: 4, 4, 4, 4 -> packs onto 2 pending hosts).
    frag_nodes = [
        SchedulingNode(
            node_type="gpu-4h",
            total_resources={"CPU": 8.0, "GPU": 4.0},
            available_resources={"CPU": 8.0, "GPU": float(g)},
            labels={"rack": "rack-a"},
            status=SchedulingNodeStatus.SCHEDULABLE,
            im_instance_id=f"frag-{i}",
            im_instance_status=Instance.ALLOCATED,
        )
        for i, g in enumerate([2, 2, 2, 4])
    ]
    dense_nodes = [
        SchedulingNode(
            node_type="gpu-4h",
            total_resources={"CPU": 8.0, "GPU": 4.0},
            available_resources={"CPU": 8.0, "GPU": 4.0},
            labels={"rack": "rack-b"},
            status=SchedulingNodeStatus.SCHEDULABLE,
            im_instance_id=f"dense-{i}",
            im_instance_status=Instance.ALLOCATED,
        )
        for i in range(4)
    ]
    ctx_pack.update(frag_nodes + dense_nodes)
    pack_gang = _tpu_gang((0, 1, 2, 3), pod_type="v6e-16")
    pack_gang.bundle_selectors[
        0
    ].locality_requirement.locality_constraint.label_name = "rack"
    del pack_gang.bundle_selectors[
        0
    ].locality_requirement.label_selector.label_constraints[:]
    del pack_gang.bundle_selectors[0].resource_requests[:]
    for _ in range(4):
        pack_gang.bundle_selectors[0].resource_requests.append(
            ResourceRequest(resources_bundle={"GPU": 2.0})
        )
    after_pack, inf_pack = ResourceDemandScheduler._try_schedule_gang(
        ctx_pack, pack_gang
    )
    assert inf_pack == []
    placed_racks = {
        n.labels["rack"]
        for n in after_pack
        if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
    }
    assert placed_racks == {"rack-b"}

    # 4. Counter-case Trap 2: overlapping multi-value IN selectors must NOT multiset-collapse.
    ctx_ord = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            node_type_configs=cfgs_pack,
            current_instances=[],
            disable_launch_config_check=True,
        )
    )
    # Domain "dom-0" has order ["0", "1"] (Bundle 0 with 2 constraints grabs "0", starving Bundle 1).
    # Domain "dom-1" has order ["1", "0"] (Bundle 0 grabs "1", Bundle 1 grabs "0" -> succeeds!).
    dom0 = [
        SchedulingNode(
            node_type="gpu-4h",
            total_resources={"CPU": 8.0, "GPU": 4.0},
            available_resources={"CPU": 8.0, "GPU": 4.0},
            labels={"rack": "dom-0", "slot": s, "tag": "ok"},
            status=SchedulingNodeStatus.SCHEDULABLE,
            im_instance_id=f"d0-{i}",
            im_instance_status=Instance.RAY_RUNNING,
            ray_node_id=f"rn0-{i}",
        )
        for i, s in enumerate(["0", "1"])
    ]
    dom1 = [
        SchedulingNode(
            node_type="gpu-4h",
            total_resources={"CPU": 8.0, "GPU": 4.0},
            available_resources={"CPU": 8.0, "GPU": 4.0},
            labels={"rack": "dom-1", "slot": s, "tag": "ok"},
            status=SchedulingNodeStatus.SCHEDULABLE,
            im_instance_id=f"d1-{i}",
            im_instance_status=Instance.RAY_RUNNING,
            ray_node_id=f"rn1-{i}",
        )
        for i, s in enumerate(["1", "0"])
    ]
    # Also test Trap 3 (mixed IPPRStatus tuple vs None) and Defect D6 (leaving node in shared domain).
    dom0[0].ippr_status = IPPRStatus(
        cloud_instance_id="p0",
        spec=IPPRGroupSpec(
            min_cpu=4, max_cpu=16, min_memory=1024, max_memory=4096, resize_timeout=60
        ),
        current_cpu=8,
        current_memory=2048,
        desired_cpu=12,
        desired_memory=2048,
    )
    leaving_in_dom1 = SchedulingNode(
        node_type="gpu-4h",
        total_resources={"CPU": 8.0, "GPU": 4.0},
        available_resources={"CPU": 8.0, "GPU": 4.0},
        labels={"rack": "dom-1", "slot": "9", "tag": "ok"},
        status=SchedulingNodeStatus.SCHEDULABLE,
        im_instance_id="d1-leaving",
        im_instance_status=Instance.RAY_STOP_REQUESTED,
        ray_node_id="rn1-leaving",
    )
    ctx_ord.update(dom0 + dom1 + [leaving_in_dom1])
    ord_gang = _tpu_gang((0, 1), pod_type="v6e-8")
    ord_gang.bundle_selectors[
        0
    ].locality_requirement.locality_constraint.label_name = "rack"
    del ord_gang.bundle_selectors[
        0
    ].locality_requirement.label_selector.label_constraints[:]
    del ord_gang.bundle_selectors[0].resource_requests[:]
    ord_gang.bundle_selectors[0].resource_requests.extend(
        [
            _mk_req(
                {"GPU": 4.0},
                [],
                [[("slot", _IN, ["0", "1"]), ("tag", _IN, ["ok"])]],
            ),
            _mk_req(
                {"GPU": 4.0},
                [],
                [[("slot", _IN, ["0"])]],
            ),
        ]
    )
    after_ord, inf_ord = ResourceDemandScheduler._try_schedule_gang(ctx_ord, ord_gang)
    assert inf_ord == []
    assert {
        n.labels["rack"]
        for n in after_ord
        if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
    } == {"dom-1"}

    # Trap 4: default schedule_in_domain(return_delta=False) returns List[SchedulingNode].
    direct_res = _GangSearch.schedule_in_domain(
        ctx_ord,
        ctx_ord.peek_nodes(),
        dom1,
        dom1,
        [],
        list(ord_gang.bundle_selectors[0].resource_requests),
        "rack",
        "dom-1",
    )
    assert isinstance(direct_res, list)


def test_steady_state_zero_deepcopies_and_deepcopy_field_isolation(monkeypatch):
    """Verify Gap S2 eliminates full-cluster get_nodes() deepcopies on steady-state
    production ticks (even with pending_idle_timeout_s enabled), preserves over-cap
    placeholder trimming and reconciler_rem+align caps, and isolates all mutable
    SchedulingNode fields in __deepcopy__ (including _placed_gangs)."""
    from ray.autoscaler.v2.schema import IPPRGroupSpec, IPPRStatus

    cfgs = {
        "head": NodeTypeConfig(
            name="head",
            resources={"CPU": 0},
            min_worker_nodes=0,
            max_worker_nodes=0,
        ),
        "v6e-16": NodeTypeConfig(
            name="v6e-16",
            resources={"CPU": 8, "TPU": 4, "TPU-v6e-16-head": 1},
            min_worker_nodes=0,
            max_worker_nodes=160,
            idle_timeout_s=300.0,
            labels={_ACC: "TPU-V6E", _TOPO: "4x4"},
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=4,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
                head_resources=("TPU-v6e-16-head",),
            ),
        ),
    }
    hnid = b"h".ljust(28, b"0")
    instances = [
        make_autoscaler_instance(
            im_instance=Instance(
                instance_id="im-head",
                instance_type="head",
                status=Instance.RAY_RUNNING,
                node_id=hnid.hex(),
                cloud_instance_id="pod-head",
                node_kind=NodeKind.HEAD,
            ),
            ray_node=NodeState(
                node_id=hnid,
                instance_id="pod-head",
                ray_node_type_name="head",
                status=NodeStatus.RUNNING,
                total_resources={"CPU": 0.0},
                available_resources={"CPU": 0.0},
            ),
            cloud_instance_id="pod-head",
        )
    ]
    for s in range(30):
        sname = f"slice-{s:02d}"
        for w in range(4):
            nid = f"n-{s:02d}-{w}".encode().ljust(28, b"0")
            cid = f"pod-{s:02d}-{w}"
            inst = make_autoscaler_instance(
                im_instance=Instance(
                    instance_id=f"im-{cid}",
                    instance_type="v6e-16",
                    status=Instance.RAY_RUNNING,
                    node_id=nid.hex(),
                    cloud_instance_id=cid,
                    node_kind=NodeKind.WORKER,
                ),
                ray_node=NodeState(
                    node_id=nid,
                    instance_id=cid,
                    ray_node_type_name="v6e-16",
                    status=NodeStatus.RUNNING,
                    total_resources={"CPU": 8.0, "TPU": 4.0},
                    available_resources={"CPU": 4.0, "TPU": 0.0},
                    idle_duration_ms=0,
                    labels={
                        _ACC: "TPU-V6E",
                        _TOPO: "4x4",
                        _SLICE: sname,
                        _WID: str(w),
                    },
                ),
                cloud_instance_id=cid,
            )
            inst.cloud_instance_replica_id = sname
            instances.append(inst)

    real_get_nodes = ResourceDemandScheduler.ScheduleContext.get_nodes
    get_nodes_calls = []

    def _spy_get_nodes(self_ctx):
        get_nodes_calls.append(1)
        return real_get_nodes(self_ctx)

    monkeypatch.setattr(
        ResourceDemandScheduler.ScheduleContext, "get_nodes", _spy_get_nodes
    )
    sched = ResourceDemandScheduler()
    rep = sched.schedule(
        SchedulingRequest(
            node_type_configs=cfgs,
            max_num_nodes=161,
            idle_timeout_s=300.0,
            pending_idle_timeout_s=900.0,
            disable_launch_config_check=True,
            current_instances=instances,
        )
    )
    assert rep.to_launch == []
    assert rep.to_terminate == []
    assert len(get_nodes_calls) == 0
    monkeypatch.setattr(
        ResourceDemandScheduler.ScheduleContext, "get_nodes", real_get_nodes
    )

    # SchedulingNode.__deepcopy__ must isolate every mutable field.
    tr = TerminationRequest(
        id="t1",
        instance_id="im-1",
        instance_type="v6e-16",
        cause=TerminationRequest.Cause.IDLE,
        details="orig",
    )
    orig = SchedulingNode(
        node_type="v6e-16",
        total_resources={"CPU": 8.0, "TPU": 4.0},
        available_resources={"CPU": 8.0, "TPU": 4.0},
        labels={_SLICE: "s0", _WID: "0"},
        status=SchedulingNodeStatus.SCHEDULABLE,
        im_instance_id="im-1",
        im_instance_status=Instance.RAY_RUNNING,
        termination_request=tr,
    )
    orig.ippr_status = IPPRStatus(
        cloud_instance_id="p0",
        spec=IPPRGroupSpec(
            min_cpu=4, max_cpu=16, min_memory=1024, max_memory=4096, resize_timeout=60
        ),
        current_cpu=8,
        current_memory=2048,
        desired_cpu=8,
        desired_memory=2048,
    )
    orig.sched_requests[ResourceRequestSource.PENDING_DEMAND].append(
        ResourceRequest(resources_bundle={"CPU": 1.0})
    )

    cloned = copy.deepcopy(orig)
    cloned.total_resources["CPU"] = 99.0
    cloned.available_resources_for_sched[ResourceRequestSource.PENDING_DEMAND][
        "TPU"
    ] = 0.0
    cloned.sched_requests[ResourceRequestSource.PENDING_DEMAND].append(
        ResourceRequest(resources_bundle={"CPU": 2.0})
    )
    cloned.labels[_SLICE] = "mutated"
    cloned.ippr_status.desired_cpu = 16
    cloned.termination_request.details = "mutated"

    assert orig.total_resources["CPU"] == 8.0
    assert (
        orig.available_resources_for_sched[ResourceRequestSource.PENDING_DEMAND]["TPU"]
        == 4.0
    )
    assert len(orig.sched_requests[ResourceRequestSource.PENDING_DEMAND]) == 1
    assert orig.labels[_SLICE] == "s0"
    assert orig.ippr_status.desired_cpu == 8
    assert orig.termination_request.details == "orig"


def test_shared_domain_multiset_dedup_does_not_suppress_running_replica():
    """Shared-domain STRICT_PACK gang search must prefer replicas with idle
    RAY_RUNNING hosts over unbooted ALLOCATED hosts and must not multiset-collapse
    multi-replica domains with differing replica ordering."""
    in_op = LabelSelectorOperator.LABEL_OPERATOR_IN
    cfg = NodeTypeConfig(
        name="gpu_mh",
        resources={"CPU": 8, "GPU": 4},
        min_worker_nodes=0,
        max_worker_nodes=16,
        labels={"ray.io/accelerator-type": "H100"},
        runtime_label_keys=("ray.io/rack",),
        replica_config=WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key="ray.io/rack",
            domain_exclusive=False,
        ),
    )

    def _run(im_id: str, cid: str, rep_id: str, rack: str, idle: bool):
        return AutoscalerInstance(
            im_instance=Instance(
                instance_id=im_id,
                instance_type="gpu_mh",
                status=Instance.RAY_RUNNING,
                node_kind=NodeKind.WORKER,
                node_id=f"nid-{im_id}",
                cloud_instance_id=cid,
            ),
            ray_node=NodeState(
                node_id=f"nid-{im_id}".encode(),
                instance_id=cid,
                ray_node_type_name="gpu_mh",
                status=NodeStatus.RUNNING,
                total_resources={"CPU": 8, "GPU": 4},
                available_resources={"CPU": 8, "GPU": 4}
                if idle
                else {"CPU": 0, "GPU": 0},
                labels={"ray.io/accelerator-type": "H100", "ray.io/rack": rack},
            ),
            cloud_instance_id=cid,
            cloud_instance_replica_id=rep_id,
        )

    def _alloc(im_id: str, cid: str, rep_id: str):
        return AutoscalerInstance(
            im_instance=Instance(
                instance_id=im_id,
                instance_type="gpu_mh",
                status=Instance.ALLOCATED,
                node_kind=NodeKind.WORKER,
                cloud_instance_id=cid,
            ),
            cloud_instance_id=cid,
            cloud_instance_replica_id=rep_id,
        )

    def _rack(rack: str, pend_first: bool):
        rep_a = [
            _run(f"{rack}-a0", f"pod-{rack}-a0", f"{rack}-rep-a", rack, idle=False),
            _run(f"{rack}-a1-run", f"pod-{rack}-a1", f"{rack}-rep-a", rack, idle=True),
        ]
        rep_b = [
            _run(f"{rack}-b0", f"pod-{rack}-b0", f"{rack}-rep-b", rack, idle=False),
            _alloc(f"{rack}-b1-pend", f"pod-{rack}-b1", f"{rack}-rep-b"),
        ]
        rep_c = [
            _run(f"{rack}-c0", f"pod-{rack}-c0", f"{rack}-rep-c", rack, idle=False),
            _alloc(f"{rack}-c1-pend", f"pod-{rack}-c1", f"{rack}-rep-c"),
        ]
        return (rep_b + rep_c + rep_a) if pend_first else (rep_a + rep_b + rep_c)

    bundles = [
        ResourceRequestUtil.make(
            {"CPU": 2, "GPU": 4},
            label_selectors=[[("ray.io/accelerator-type", in_op, ["H100"])]],
        )
        for _ in range(2)
    ]
    bs = BundleSelector(resource_requests=bundles)
    bs.locality_requirement.locality_constraint.label_name = "ray.io/rack"
    bs.locality_requirement.locality_constraint.placement_strategy = (
        PlacementStrategy.STRICT_PACK
    )
    gang = GangResourceRequest(bundle_selectors=[bs])

    sched = ResourceDemandScheduler()
    req = SchedulingRequest(
        node_type_configs={"gpu_mh": cfg},
        current_instances=_rack("rack-a", pend_first=True)
        + _rack("rack-b", pend_first=False),
        gang_resource_requests=[gang],
        disable_launch_config_check=True,
    )
    ctx = sched._build_context_with_placeholder_ttl(req)
    out, inf = sched._try_schedule_gang(ctx, gang)
    assert not inf
    placed = [
        (n.im_instance_id, n.is_running) for n in out if any(n.sched_requests.values())
    ]
    assert sum(not is_run for _, is_run in placed) == 1

    # Also verify trimmed status_history keeps im_created_lids via first_status_transition_time_ns_fn.
    trimmed_im = Instance(
        instance_id="sub-0",
        instance_type="gpu_mh",
        status=Instance.RAY_RUNNING,
        node_kind=NodeKind.WORKER,
        launch_request_id="L-sub",
        node_id="nid-sub-0",
        cloud_instance_id="pod-sub-0",
    )
    trimmed_inst = AutoscalerInstance(
        im_instance=trimmed_im,
        ray_node=NodeState(
            node_id=b"nid-sub-0",
            instance_id="pod-sub-0",
            ray_node_type_name="gpu_mh",
            status=NodeStatus.RUNNING,
            total_resources={"CPU": 8, "GPU": 4},
            available_resources={"CPU": 8, "GPU": 4},
            labels={"ray.io/accelerator-type": "H100", "ray.io/rack": "r0"},
        ),
        cloud_instance_id="pod-sub-0",
    )
    ctx_trimmed = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
        SchedulingRequest(
            node_type_configs={"gpu_mh": cfg},
            current_instances=[trimmed_inst],
            disable_launch_config_check=True,
            first_status_transition_time_ns_fn=lambda inst, st: (
                100 if st == Instance.QUEUED else None
            ),
        )
    )
    assert ctx_trimmed.is_completed_subreplica_batch("gpu_mh", "L-sub", 1, 2)


def test_unpromoted_pending_refill_consumption_invalidates_attempt_cache():
    """Consuming an unpromoted pending_refills batch in Loop 3 replaces it with a
    full replica offering missing worker-index slots, so cached _NodeAttempt
    entries from before the active_selectors drop must be invalidated."""
    cfgs = {
        "tpu_slice": NodeTypeConfig(
            name="tpu_slice",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=4,
            runtime_label_keys=(_SLICE, _WID),
            replica_config=WorkerReplicaConfig(
                workers_per_replica=2,
                domain_label_key=_SLICE,
                domain_exclusive=True,
                worker_index_label_key=_WID,
            ),
        ),
        "other_worker": NodeTypeConfig(
            name="other_worker",
            resources={"CPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=4,
            labels={"other": "yes"},
        ),
    }
    busy0 = _run_node(
        "tpu_slice",
        "b0",
        {"CPU": 4},
        {_SLICE: "slice-0", _WID: "0"},
        used={"CPU": 4},
        lid="L0",
    )
    dead1 = _terminated_teardown_row("tpu_slice", "dead-1", "L0")

    r_u = _mk_req({"CPU": 1}, constraints=[(_ANTI, "pg", "1")])
    r_1 = _mk_req({"CPU": 4}, label_selectors=[[(_WID, _IN, ["1"])]])
    r_3 = _mk_req({"CPU": 1}, label_selectors=[[("other", _IN, ["yes"])]])
    r_2 = _mk_req({"CPU": 1}, label_selectors=[[(_WID, _IN, ["0"])], []])

    sched = ResourceDemandScheduler(event_logger)
    now_t = [100.0]
    with patch(
        "ray.autoscaler.v2.scheduler.time.monotonic", side_effect=lambda: now_t[0]
    ):
        sched.schedule(
            SchedulingRequest(
                node_type_configs=cfgs,
                max_num_nodes=4,
                current_instances=[busy0, dead1],
                disable_launch_config_check=True,
                backfill_placeholder_ttl_s=10.0,
            )
        )
        now_t[0] = 120.0
        reply = sched.schedule(
            SchedulingRequest(
                node_type_configs=cfgs,
                max_num_nodes=4,
                current_instances=[busy0, dead1],
                resource_requests=ResourceRequestUtil.group_by_count(
                    [r_u, r_1, r_3, r_2]
                ),
                disable_launch_config_check=True,
                backfill_placeholder_ttl_s=10.0,
            )
        )
    to_launch, to_terminate = _launch_and_terminate(reply)
    assert to_launch == {"tpu_slice": 3}
    assert to_terminate == []
    assert reply.infeasible_resource_requests == [r_3]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
