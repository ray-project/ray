import os
import sys

# coding: utf-8
from dataclasses import dataclass
from typing import Callable, List, Optional

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.autoscaler.v2.sdk import (
    _autoscaler_state_service_stub,
    request_cluster_resources,
)
from ray.autoscaler.v2.tests.util import get_cluster_resource_state
from ray.core.generated.experimental.autoscaler_pb2 import (
    ClusterResourceState,
    NodeStatus,
)
from ray.util.state.api import list_nodes


def assert_cluster_resource_constraints(
    state: ClusterResourceState, expected: List[dict]
):
    """
    Assert a GetClusterResourceStateReply has cluster_resource_constraints that
    matches with the expected resources.
    """
    # We only have 1 constraint for now.
    assert len(state.cluster_resource_constraints) == 1

    min_bundles = state.cluster_resource_constraints[0].min_bundles
    assert len(min_bundles) == len(expected)

    # Sort all the bundles by bundle's resource names
    min_bundles = sorted(
        min_bundles, key=lambda bundle: "".join(bundle.resources_bundle.keys())
    )
    expected = sorted(expected, key=lambda bundle: "".join(bundle.keys()))

    for actual_bundle, expected_bundle in zip(min_bundles, expected):
        assert dict(actual_bundle.resources_bundle) == expected_bundle


@dataclass
class NodeState:
    node_id: str
    node_status: NodeStatus
    idle_time_check_cb: Optional[Callable] = None


def assert_node_states(state: ClusterResourceState, expected_nodes: List[NodeState]):
    """
    Assert a GetClusterResourceStateReply has node states that
    matches with the expected nodes.
    """
    assert len(state.node_states) == len(expected_nodes)

    # Sort all the nodes by node's node_id
    node_states = sorted(state.node_states, key=lambda node: node.node_id)
    expected_nodes = sorted(expected_nodes, key=lambda node: node.node_id)

    for actual_node, expected_node in zip(node_states, expected_nodes):
        assert actual_node.status == expected_node.node_status
        if expected_node.idle_time_check_cb:
            assert expected_node.idle_time_check_cb(actual_node.idle_duration_ms)


def test_request_cluster_resources_basic(shutdown_only):
    ray.init(num_cpus=1)
    stub = _autoscaler_state_service_stub()

    # Request one
    request_cluster_resources([{"CPU": 1}])

    def verify():
        state = get_cluster_resource_state(stub)
        assert_cluster_resource_constraints(state, [{"CPU": 1}])
        return True

    wait_for_condition(verify)

    # Request another overrides the previous request
    request_cluster_resources([{"CPU": 2, "GPU": 1}, {"CPU": 1}])

    def verify():
        state = get_cluster_resource_state(stub)
        assert_cluster_resource_constraints(state, [{"CPU": 2, "GPU": 1}, {"CPU": 1}])
        return True

    wait_for_condition(verify)


def test_node_state_lifecycle_basic(ray_start_cluster):

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    node = cluster.add_node(num_cpus=1)

    stub = _autoscaler_state_service_stub()

    # We don't have node id from `add_node` unfortunately.
    def nodes_up():
        nodes = list_nodes()
        assert len(nodes) == 2
        return True

    wait_for_condition(nodes_up)

    def get_node_ids():
        head_node_id = None
        node_id = None
        nodes = list_nodes()
        for node in nodes:
            if node.is_head_node:
                head_node_id = node.node_id
            else:
                node_id = node.node_id
        return head_node_id, node_id

    head_node_id, node_id = get_node_ids()

    def verify_cluster_idle():
        state = get_cluster_resource_state(stub)
        assert_node_states(
            state,
            [
                NodeState(node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0),
                NodeState(head_node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0),
            ],
        )
        return True

    wait_for_condition(verify_cluster_idle)

    # Schedule a task running
    @ray.remote(num_cpus=0.1)
    def f():
        while True:
            pass

    t = f.remote()

    def verify_cluster_busy():
        state = get_cluster_resource_state(stub)
        assert_node_states(
            state,
            [
                NodeState(node_id, NodeStatus.RUNNING, lambda idle_ms: idle_ms == 0),
                NodeState(head_node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0),
            ],
        )
        return True

    wait_for_condition(verify_cluster_busy)

    # Kill the task
    ray.cancel(t, force=True)

    wait_for_condition(verify_cluster_idle)

    # Kill the node.
    cluster.remove_node(node)

    def verify_cluster_no_node():
        state = get_cluster_resource_state(stub)
        assert_node_states(
            state,
            [
                NodeState(node_id, NodeStatus.DEAD),
                NodeState(head_node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0),
            ],
        )
        return True

    wait_for_condition(verify_cluster_no_node)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
