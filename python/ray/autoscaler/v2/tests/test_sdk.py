import os
import sys
import time

# coding: utf-8
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import wait_for_condition
from ray.autoscaler.v2.sdk import request_cluster_resources
from ray.autoscaler.v2.tests.util import get_cluster_resource_state
from ray.core.generated.experimental import autoscaler_pb2_grpc
from ray.core.generated.experimental.autoscaler_pb2 import (
    ClusterResourceState,
    NodeStatus,
)
from ray.util.state.api import list_nodes


def _autoscaler_state_service_stub():
    """Get the grpc stub for the autoscaler state service"""
    gcs_address = ray.get_runtime_context().gcs_address
    gcs_channel = ray._private.utils.init_grpc_channel(
        gcs_address, ray_constants.GLOBAL_GRPC_OPTIONS
    )
    return autoscaler_pb2_grpc.AutoscalerStateServiceStub(gcs_channel)


def get_node_ids() -> Tuple[str, List[str]]:
    """Get the node ids of the head node and a worker node"""
    head_node_id = None
    nodes = list_nodes()
    worker_node_ids = []
    for node in nodes:
        if node.is_head_node:
            head_node_id = node.node_id
        else:
            worker_node_ids += [node.node_id]
    return head_node_id, worker_node_ids


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
    labels: Optional[dict] = None


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

        if expected_node.labels:
            assert sorted(actual_node.dynamic_labels) == sorted(expected_node.labels)


@dataclass
class GangResourceRequest:
    # Resource bundles.
    bundles: List[dict]
    # List of detail information about the request
    details: List[str]


def assert_gang_requests(
    state: ClusterResourceState, expected: List[GangResourceRequest]
):
    """
    Assert a GetClusterResourceStateReply has gang requests that
    matches with the expected requests.
    """
    assert len(state.pending_gang_resource_requests) == len(expected)

    # Sort all the requests by request's details
    requests = sorted(
        state.pending_gang_resource_requests, key=lambda request: request.details
    )
    expected = sorted(expected, key=lambda request: "".join(request.details))

    for actual_request, expected_request in zip(requests, expected):
        # Assert the detail contains the expected details
        for detail_str in expected_request.details:
            assert detail_str in actual_request.details


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


def test_node_info_basic(shutdown_only, monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("RAY_CLOUD_INSTANCE_ID", "instance-id")
        m.setenv("RAY_NODE_TYPE_NAME", "node-type-name")
        m.setenv("RAY_CLOUD_INSTANCE_TYPE_NAME", "instance-type-name")

        ctx = ray.init(num_cpus=1)
        ip = ctx.address_info["node_ip_address"]

        stub = _autoscaler_state_service_stub()

        def verify():
            state = get_cluster_resource_state(stub)

            assert len(state.node_states) == 1
            node = state.node_states[0]

            assert node.instance_id == "instance-id"
            assert node.ray_node_type_name == "node-type-name"
            assert node.node_ip_address == ip
            assert node.instance_type_name == "instance-type-name"

            return True

        wait_for_condition(verify)


def test_pg_pending_gang_requests_basic(ray_start_cluster):
    ray.init(num_cpus=1)

    # Create a pg that's pending.
    pg = ray.util.placement_group([{"CPU": 1}] * 3, strategy="STRICT_SPREAD")
    try:
        ray.get(pg.ready(), timeout=2)
    except TimeoutError:
        pass

    pg_id = pg.id.hex()

    stub = _autoscaler_state_service_stub()

    def verify():
        state = get_cluster_resource_state(stub)
        assert_gang_requests(
            state,
            [
                GangResourceRequest(
                    [{"CPU": 1}] * 3, details=[pg_id, "STRICT_SPREAD", "PENDING"]
                )
            ],
        )
        return True

    wait_for_condition(verify)


def test_pg_usage_labels(shutdown_only):

    ray.init(num_cpus=1)

    # Create a pg
    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(pg.ready())

    # Check the labels
    stub = _autoscaler_state_service_stub()
    head_node_id, _ = get_node_ids()

    pg_id = pg.id.hex()

    def verify():
        state = get_cluster_resource_state(stub)
        assert_node_states(
            state,
            [
                NodeState(
                    head_node_id, NodeStatus.RUNNING, labels={f"_PG_{pg_id}": ""}
                ),
            ],
        )
        return True

    wait_for_condition(verify)


def test_node_state_lifecycle_basic(ray_start_cluster):
    start_s = time.perf_counter()
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

    head_node_id, worker_node_ids = get_node_ids()
    node_id = worker_node_ids[0]

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

    # Sleep for a bit so head node should be idle longer than this.
    time.sleep(3)

    def verify_cluster_no_node():
        state = get_cluster_resource_state(stub)
        now_s = time.perf_counter()
        test_dur_ms = (now_s - start_s) * 1000
        assert_node_states(
            state,
            [
                NodeState(node_id, NodeStatus.DEAD),
                NodeState(
                    head_node_id,
                    NodeStatus.IDLE,
                    lambda idle_ms: idle_ms > 3 * 1000 and idle_ms < test_dur_ms,
                ),
            ],
        )
        return True

    wait_for_condition(verify_cluster_no_node)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
