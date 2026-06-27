import os
import sys
import time

import pytest

from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
    CloudResourceMonitor,
)
from ray.autoscaler.v2.scheduler import (
    ResourceDemandScheduler,
    ResourceRequestSource,
    SchedulingNode,
    SchedulingNodeStatus,
    SchedulingRequest,
)
from ray.core.generated.autoscaler_pb2 import ResourceRequest as PBResourceRequest
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceUpdateEvent,
    NodeKind,
)


def test_recovery_scoring():
    monitor = CloudResourceMonitor()
    node_type = "gpu-node"

    # Mock failure
    event = InstanceUpdateEvent(
        instance_type=node_type, new_instance_status=Instance.ALLOCATION_TIMEOUT
    )
    monitor.notify([event])

    # Immediately after failure, score should be 0.0
    scores = monitor.get_recoverable_resource_availabilities()
    assert scores[node_type] == 0.0

    # After safety floor (e.g., 11s, default safety floor is 10s)
    monitor._last_unavailable_timestamp[node_type] = time.time() - 11
    scores = monitor.get_recoverable_resource_availabilities()
    assert 0.0 < scores[node_type] < 0.1

    # Halfway through recovery window (600s / 2 = 300s)
    monitor._last_unavailable_timestamp[node_type] = time.time() - 300
    scores = monitor.get_recoverable_resource_availabilities()
    # 300 / 600 = 0.5
    assert pytest.approx(scores[node_type], 0.01) == 0.5

    # After recovery window
    monitor._last_unavailable_timestamp[node_type] = time.time() - 601
    scores = monitor.get_recoverable_resource_availabilities()
    assert scores[node_type] == 1.0


def test_scheduler_priority_tie_breaking():
    # Two node types with identical resources
    resources = {"CPU": 4}
    node_type_1 = "high-priority"
    node_type_2 = "low-priority"

    config_1 = NodeTypeConfig(
        name=node_type_1,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=10,
    )
    config_2 = NodeTypeConfig(
        name=node_type_2,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=0,
    )

    node_1 = SchedulingNode.from_node_config(
        config_1, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )
    node_2 = SchedulingNode.from_node_config(
        config_2, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )

    request = PBResourceRequest(resources_bundle={"CPU": 1})

    # Utilization and Availability are equal (all perfect)
    # Priority should break the tie
    best_node, infeasible, remaining_nodes = ResourceDemandScheduler._sched_best_node(
        [request], [node_2, node_1], ResourceRequestSource.PENDING_DEMAND, {}, {}
    )

    assert best_node.node_type == node_type_1


def test_schedule_context_propagation():
    resources = {"CPU": 4}
    node_type = "gpu-node"

    config = NodeTypeConfig(
        name=node_type,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=7,
    )

    # Mock cloud availabilities
    cloud_availabilities = {node_type: 0.1}  # failure recency
    recoverable_availabilities = {node_type: 0.5}

    req = SchedulingRequest(
        node_type_configs={node_type: config},
        cloud_resource_availabilities=cloud_availabilities,
        recoverable_resource_availabilities=recoverable_availabilities,
        disable_launch_config_check=True,
    )

    ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(req)

    # Check if a new node created from this context has the correct priority
    node_pools = [
        SchedulingNode.from_node_config(
            ctx.get_node_type_configs()[nt],
            status=SchedulingNodeStatus.TO_LAUNCH,
            node_kind=NodeKind.WORKER,
        )
        for nt, num_available in ctx.get_node_type_available().items()
    ]

    assert len(node_pools) == 1
    node = node_pools[0]
    assert node.priority == 7
    # Dynamic scores are in the context, not the node
    assert ctx.get_recoverable_resource_availabilities()[node_type] == 0.5
    assert ctx.get_cloud_resource_availabilities()[node_type] == 0.1


def test_scheduler_availability_over_priority():
    # High priority node is recovering (score 0.5)
    # Low priority node is available (score 1.0)
    resources = {"CPU": 4}
    node_type_1 = "high-priority"
    node_type_2 = "low-priority"

    config_1 = NodeTypeConfig(
        name=node_type_1,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=10,
    )
    config_2 = NodeTypeConfig(
        name=node_type_2,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=0,
    )

    node_1 = SchedulingNode.from_node_config(
        config_1, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )
    node_2 = SchedulingNode.from_node_config(
        config_2, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )

    request = PBResourceRequest(resources_bundle={"CPU": 1})

    # Recoverable Availability is higher for node_2, so it should be chosen despite lower priority
    best_node, infeasible, remaining_nodes = ResourceDemandScheduler._sched_best_node(
        [request],
        [node_1, node_2],
        ResourceRequestSource.PENDING_DEMAND,
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={node_type_1: 0.5, node_type_2: 1.0},
    )

    assert best_node.node_type == node_type_2


def test_scheduler_failure_recency_tie_breaking():
    # Same priority, same recoverable availability (1.0)
    # One has an older failure than the other.
    resources = {"CPU": 4}
    node_type_1 = "older-failure"
    node_type_2 = "newer-failure"

    config_1 = NodeTypeConfig(
        name=node_type_1,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=5,
    )
    config_2 = NodeTypeConfig(
        name=node_type_2,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=5,
    )

    node_1 = SchedulingNode.from_node_config(
        config_1, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )
    node_2 = SchedulingNode.from_node_config(
        config_2, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )

    request = PBResourceRequest(resources_bundle={"CPU": 1})

    best_node, infeasible, remaining_nodes = ResourceDemandScheduler._sched_best_node(
        [request],
        [node_1, node_2],
        ResourceRequestSource.PENDING_DEMAND,
        cloud_resource_availabilities={node_type_1: 0.9, node_type_2: 0.1},
        recoverable_resource_availabilities={node_type_1: 1.0, node_type_2: 1.0},
    )

    assert best_node.node_type == node_type_1


def test_recovery_integration():
    monitor = CloudResourceMonitor()
    node_type_1 = "high-priority"
    node_type_2 = "low-priority"

    # Mock failure for node_1 (high priority)
    event = InstanceUpdateEvent(
        instance_type=node_type_1, new_instance_status=Instance.ALLOCATION_TIMEOUT
    )
    monitor.notify([event])

    # Score should be 0.0 for node_1 immediately
    scores = monitor.get_recoverable_resource_availabilities()
    assert scores[node_type_1] == 0.0

    # Setup scheduler structures
    resources = {"CPU": 4}
    config_1 = NodeTypeConfig(
        name=node_type_1,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=10,
    )
    config_2 = NodeTypeConfig(
        name=node_type_2,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources,
        priority=0,
    )

    node_1 = SchedulingNode.from_node_config(
        config_1, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )
    node_2 = SchedulingNode.from_node_config(
        config_2, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )

    request = PBResourceRequest(resources_bundle={"CPU": 1})

    # Pass scores from monitor to scheduler
    best_node, infeasible, remaining_nodes = ResourceDemandScheduler._sched_best_node(
        [request],
        [node_1, node_2],
        ResourceRequestSource.PENDING_DEMAND,
        cloud_resource_availabilities={},
        recoverable_resource_availabilities=scores,
    )

    # Node 2 should be chosen because Node 1 is recovering, despite Node 1 having higher priority
    assert best_node.node_type == node_type_2


def test_scheduler_utilization_over_priority():
    # Node 1: 4 CPUs, priority 10
    # Node 2: 2 CPUs, priority 0
    # Request: 2 CPUs
    # Node 2 should be selected because it fits perfectly (utilization score is higher),
    # even though Node 1 has higher priority.
    resources_1 = {"CPU": 4}
    resources_2 = {"CPU": 2}
    node_type_1 = "large-node"
    node_type_2 = "small-node"

    config_1 = NodeTypeConfig(
        name=node_type_1,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources_1,
        priority=10,
    )
    config_2 = NodeTypeConfig(
        name=node_type_2,
        min_worker_nodes=0,
        max_worker_nodes=10,
        resources=resources_2,
        priority=0,
    )

    node_1 = SchedulingNode.from_node_config(
        config_1, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )
    node_2 = SchedulingNode.from_node_config(
        config_2, status=SchedulingNodeStatus.TO_LAUNCH, node_kind=NodeKind.WORKER
    )

    request = PBResourceRequest(resources_bundle={"CPU": 2})

    best_node, infeasible, remaining_nodes = ResourceDemandScheduler._sched_best_node(
        [request],
        [node_1, node_2],
        ResourceRequestSource.PENDING_DEMAND,
        cloud_resource_availabilities={},
        recoverable_resource_availabilities={},
    )

    assert best_node.node_type == node_type_2


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
