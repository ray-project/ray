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
from ray.autoscaler.v2.schema import (
    ClusterStatus,
    LaunchRequest,
    NodeInfo,
    ResourceRequestByCount,
)
from ray.autoscaler.v2.sdk import get_cluster_status, request_cluster_resources
from ray.autoscaler.v2.tests.util import (
    get_available_resources,
    get_cluster_resource_state,
    get_total_resources,
    report_autoscaling_state,
)
from ray.core.generated import autoscaler_pb2, autoscaler_pb2_grpc
from ray.core.generated.autoscaler_pb2 import ClusterResourceState, NodeStatus
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
    state: ClusterResourceState, expected_bundles: List[dict], expected_count: List[int]
):
    """
    Assert a GetClusterResourceStateReply has cluster_resource_constraints that
    matches with the expected resources.
    """
    # We only have 1 constraint for now.
    assert len(state.cluster_resource_constraints) == 1

    min_bundles = state.cluster_resource_constraints[0].min_bundles
    assert len(min_bundles) == len(expected_bundles) == len(expected_count)

    # Sort all the bundles by bundle's resource names
    min_bundles = sorted(
        min_bundles,
        key=lambda bundle_by_count: "".join(
            bundle_by_count.request.resources_bundle.keys()
        ),
    )
    expected = zip(expected_bundles, expected_count)
    expected = sorted(
        expected, key=lambda bundle_count: "".join(bundle_count[0].keys())
    )

    for actual_bundle_count, expected_bundle_count in zip(min_bundles, expected):
        assert (
            dict(actual_bundle_count.request.resources_bundle)
            == expected_bundle_count[0]
        )
        assert actual_bundle_count.count == expected_bundle_count[1]


@dataclass
class ExpectedNodeState:
    node_id: str
    node_status: NodeStatus
    idle_time_check_cb: Optional[Callable] = None
    labels: Optional[dict] = None


def assert_node_states(
    state: ClusterResourceState, expected_nodes: List[ExpectedNodeState]
):
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
class ExpectedNodeInfo:
    node_id: Optional[str] = None
    node_status: Optional[str] = None
    idle_time_check_cb: Optional[Callable] = None
    instance_id: Optional[str] = None
    ray_node_type_name: Optional[str] = None
    instance_type_name: Optional[str] = None
    ip_address: Optional[str] = None
    details: Optional[str] = None

    # Check those resources are included in the actual node info.
    total_resources: Optional[dict] = None
    available_resources: Optional[dict] = None


def assert_nodes(actual_nodes: List[NodeInfo], expected_nodes: List[ExpectedNodeInfo]):

    assert len(actual_nodes) == len(expected_nodes)
    # Sort the nodes by id.
    actual_nodes = sorted(actual_nodes, key=lambda node: node.node_id)
    expected_nodes = sorted(expected_nodes, key=lambda node: node.node_id)

    for actual_node, expected_node in zip(actual_nodes, expected_nodes):
        if expected_node.node_id is not None:
            assert actual_node.node_id == expected_node.node_id
        if expected_node.node_status is not None:
            assert actual_node.node_status == expected_node.node_status
        if expected_node.instance_id is not None:
            assert actual_node.instance_id == expected_node.instance_id
        if expected_node.ray_node_type_name is not None:
            assert actual_node.ray_node_type_name == expected_node.ray_node_type_name
        if expected_node.instance_type_name is not None:
            assert actual_node.instance_type_name == expected_node.instance_type_name
        if expected_node.ip_address is not None:
            assert actual_node.ip_address == expected_node.ip_address
        if expected_node.details is not None:
            assert expected_node.details in actual_node.details

        if expected_node.idle_time_check_cb:
            assert expected_node.idle_time_check_cb(
                actual_node.resource_usage.idle_time_ms
            )

        if expected_node.total_resources:
            for resource_name, total in expected_node.total_resources.items():
                assert (
                    total
                    == get_total_resources(actual_node.resource_usage.usage)[
                        resource_name
                    ]
                )

        if expected_node.available_resources:
            for resource_name, available in expected_node.available_resources.items():
                assert (
                    available
                    == get_available_resources(actual_node.resource_usage.usage)[
                        resource_name
                    ]
                )


def assert_launches(
    cluster_status: ClusterStatus,
    expected_pending_launches: List[LaunchRequest],
    expected_failed_launches: List[LaunchRequest],
):
    def assert_launches(actuals, expects):
        for actual, expect in zip(actuals, expects):
            assert actual.instance_type_name == expect.instance_type_name
            assert actual.ray_node_type_name == expect.ray_node_type_name
            assert actual.count == expect.count
            assert actual.state == expect.state
            assert actual.request_ts_s == expect.request_ts_s

    assert len(cluster_status.pending_launches) == len(expected_pending_launches)
    assert len(cluster_status.failed_launches) == len(expected_failed_launches)

    actual_pending = sorted(
        cluster_status.pending_launches, key=lambda launch: launch.ray_node_type_name
    )
    expected_pending = sorted(
        expected_pending_launches, key=lambda launch: launch.ray_node_type_name
    )

    assert_launches(actual_pending, expected_pending)

    actual_failed = sorted(
        cluster_status.failed_launches, key=lambda launch: launch.ray_node_type_name
    )
    expected_failed = sorted(
        expected_failed_launches, key=lambda launch: launch.ray_node_type_name
    )

    assert_launches(actual_failed, expected_failed)


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
        assert_cluster_resource_constraints(state, [{"CPU": 1}], [1])
        return True

    wait_for_condition(verify)

    # Request another overrides the previous request
    request_cluster_resources([{"CPU": 2, "GPU": 1}, {"CPU": 1}])

    def verify():
        state = get_cluster_resource_state(stub)
        assert_cluster_resource_constraints(
            state, [{"CPU": 2, "GPU": 1}, {"CPU": 1}], [1, 1]
        )
        return True

    # Request multiple is aggregated by shape.
    request_cluster_resources([{"CPU": 1}] * 100)

    def verify():
        state = get_cluster_resource_state(stub)
        assert_cluster_resource_constraints(state, [{"CPU": 1}], [100])
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

            assert (
                state.cluster_session_name
                == ray._private.worker.global_worker.node.session_name
            )
            return True

        wait_for_condition(verify)


def test_pg_pending_gang_requests_basic(shutdown_only):
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
                ExpectedNodeState(
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
                ExpectedNodeState(
                    node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0
                ),
                ExpectedNodeState(
                    head_node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0
                ),
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
                ExpectedNodeState(
                    node_id, NodeStatus.RUNNING, lambda idle_ms: idle_ms == 0
                ),
                ExpectedNodeState(
                    head_node_id, NodeStatus.IDLE, lambda idle_ms: idle_ms > 0
                ),
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
                ExpectedNodeState(node_id, NodeStatus.DEAD),
                ExpectedNodeState(
                    head_node_id,
                    NodeStatus.IDLE,
                    lambda idle_ms: idle_ms > 3 * 1000 and idle_ms < test_dur_ms,
                ),
            ],
        )
        return True

    wait_for_condition(verify_cluster_no_node)


def test_get_cluster_status_resources(ray_start_cluster):
    cluster = ray_start_cluster
    # Head node
    cluster.add_node(num_cpus=1, _system_config={"enable_autoscaler_v2": True})
    ray.init(address=cluster.address)

    # Worker node
    cluster.add_node(num_cpus=2)

    @ray.remote(num_cpus=1)
    class Actor:
        def loop(self):
            while True:
                pass

    # Schedule tasks to use all resources.
    @ray.remote(num_cpus=1)
    def loop():
        while True:
            pass

    [loop.remote() for _ in range(2)]
    actor = Actor.remote()
    actor.loop.remote()

    def verify_cpu_resources_all_used():
        cluster_status = get_cluster_status()
        total_cluster_resources = get_total_resources(
            cluster_status.cluster_resource_usage
        )
        assert total_cluster_resources["CPU"] == 3.0

        available_cluster_resources = get_available_resources(
            cluster_status.cluster_resource_usage
        )
        assert available_cluster_resources["CPU"] == 0.0
        return True

    wait_for_condition(verify_cpu_resources_all_used)

    # Schedule more tasks should show up as task demands
    [loop.remote() for _ in range(2)]

    def verify_task_demands():
        resource_demands = get_cluster_status().resource_demands
        assert len(resource_demands.ray_task_actor_demand) == 1
        assert resource_demands.ray_task_actor_demand[0].bundles_by_count == [
            ResourceRequestByCount(
                bundle={"CPU": 1.0},
                count=2,
            )
        ]
        return True

    wait_for_condition(verify_task_demands)

    # Request resources through SDK
    request_cluster_resources(to_request=[{"GPU": 1, "CPU": 2}])

    def verify_cluster_constraint_demand():
        resource_demands = get_cluster_status().resource_demands
        assert len(resource_demands.cluster_constraint_demand) == 1
        assert resource_demands.cluster_constraint_demand[0].bundles_by_count == [
            ResourceRequestByCount(
                bundle={"GPU": 1.0, "CPU": 2.0},
                count=1,
            )
        ]
        return True

    wait_for_condition(verify_cluster_constraint_demand)

    # Try to schedule some PGs
    pg1 = ray.util.placement_group([{"CPU": 1}] * 3)

    def verify_pg_demands():
        resource_demands = get_cluster_status().resource_demands
        assert len(resource_demands.placement_group_demand) == 1
        assert resource_demands.placement_group_demand[0].bundles_by_count == [
            ResourceRequestByCount(
                bundle={"CPU": 1.0},
                count=3,
            )
        ]
        assert resource_demands.placement_group_demand[0].pg_id == pg1.id.hex()
        assert resource_demands.placement_group_demand[0].strategy == "PACK"
        assert resource_demands.placement_group_demand[0].state == "PENDING"
        return True

    wait_for_condition(verify_pg_demands)


def test_get_cluster_status(ray_start_cluster):
    # This test is to make sure the grpc stub is working.
    # TODO(rickyx): Add e2e tests for the autoscaler state service in a separate PR
    # to validate the data content.
    cluster = ray_start_cluster
    # Head node
    cluster.add_node(num_cpus=1, _system_config={"enable_autoscaler_v2": True})
    ray.init(address=cluster.address)
    # Worker node
    cluster.add_node(num_cpus=2)

    head_node_id, worker_node_ids = get_node_ids()

    def verify_nodes():
        cluster_status = get_cluster_status()
        assert_nodes(
            cluster_status.healthy_nodes,
            [
                ExpectedNodeInfo(
                    worker_node_ids[0],
                    "IDLE",
                    lambda idle_ms: idle_ms > 0,
                    total_resources={"CPU": 2.0},
                    available_resources={"CPU": 2.0},
                ),
                ExpectedNodeInfo(
                    head_node_id,
                    "IDLE",
                    lambda idle_ms: idle_ms > 0,
                    total_resources={"CPU": 1.0},
                    available_resources={"CPU": 1.0},
                ),
            ],
        )
        return True

    wait_for_condition(verify_nodes)

    # Schedule a task running
    @ray.remote(num_cpus=2)
    def f():
        while True:
            pass

    f.remote()

    def verify_nodes_busy():
        cluster_status = get_cluster_status()
        assert_nodes(
            cluster_status.healthy_nodes,
            [
                ExpectedNodeInfo(
                    worker_node_ids[0],
                    "RUNNING",
                    lambda idle_ms: idle_ms == 0,
                    total_resources={"CPU": 2.0},
                    available_resources={"CPU": 0.0},
                ),
                ExpectedNodeInfo(head_node_id, "IDLE", lambda idle_ms: idle_ms > 0),
            ],
        )
        return True

    wait_for_condition(verify_nodes_busy)

    stub = _autoscaler_state_service_stub()
    state = autoscaler_pb2.AutoscalingState(
        last_seen_cluster_resource_state_version=0,
        autoscaler_state_version=1,
        pending_instance_requests=[
            autoscaler_pb2.PendingInstanceRequest(
                instance_type_name="m5.large",
                ray_node_type_name="worker",
                count=2,
                request_ts=1000,
            )
        ],
        failed_instance_requests=[
            autoscaler_pb2.FailedInstanceRequest(
                instance_type_name="m5.large",
                ray_node_type_name="worker",
                count=2,
                start_ts=1000,
                failed_ts=2000,
                reason="insufficient quota",
            )
        ],
        pending_instances=[
            autoscaler_pb2.PendingInstance(
                instance_id="instance-id",
                instance_type_name="m5.large",
                ray_node_type_name="worker",
                ip_address="10.10.10.10",
                details="launching",
            )
        ],
    )
    report_autoscaling_state(stub, autoscaling_state=state)

    def verify_autoscaler_state():
        # TODO(rickyx): Add infeasible asserts.

        cluster_status = get_cluster_status()
        assert len(cluster_status.pending_launches) == 1
        assert_launches(
            cluster_status,
            expected_pending_launches=[
                LaunchRequest(
                    instance_type_name="m5.large",
                    ray_node_type_name="worker",
                    count=2,
                    state=LaunchRequest.Status.PENDING,
                    request_ts_s=1000,
                )
            ],
            expected_failed_launches=[
                LaunchRequest(
                    instance_type_name="m5.large",
                    ray_node_type_name="worker",
                    count=2,
                    state=LaunchRequest.Status.FAILED,
                    request_ts_s=1000,
                    failed_ts_s=2000,
                    details="insufficient quota",
                )
            ],
        )

        assert_nodes(
            cluster_status.pending_nodes,
            [
                ExpectedNodeInfo(
                    instance_id="instance-id",
                    ray_node_type_name="worker",
                    details="launching",
                    ip_address="10.10.10.10",
                )
            ],
        )
        return True

    wait_for_condition(verify_autoscaler_state)


@pytest.mark.parametrize(
    "env_val,enabled",
    [
        ("1", True),
        ("0", False),
        ("", False),
    ],
)
def test_is_autoscaler_v2_enabled(shutdown_only, monkeypatch, env_val, enabled):
    def reset_autoscaler_v2_enabled_cache():
        import ray.autoscaler.v2.utils as u

        u.cached_is_autoscaler_v2 = None

    reset_autoscaler_v2_enabled_cache()
    with monkeypatch.context() as m:
        m.setenv("RAY_enable_autoscaler_v2", env_val)
        ray.init()

        def verify():
            assert ray.autoscaler.v2.utils.is_autoscaler_v2() == enabled
            return True

        wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
