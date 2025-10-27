import sys

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.core.generated import autoscaler_pb2
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_request_worker_lease_idempotent(
    monkeypatch, shutdown_only, deterministic_failure, ray_start_cluster
):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.RequestWorkerLease=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    @ray.remote
    def simple_task_1():
        return 0

    @ray.remote
    def simple_task_2():
        return 1

    # Spin up a two-node cluster where we're targeting scheduling on the
    # remote node via NodeAffinitySchedulingStrategy to test remote RequestWorkerLease
    # calls.
    cluster = ray_start_cluster
    remote_node = cluster.add_node(num_cpus=1)

    result_ref1 = simple_task_1.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node.node_id, soft=False
        )
    ).remote()
    result_ref2 = simple_task_2.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node.node_id, soft=False
        )
    ).remote()

    assert ray.get([result_ref1, result_ref2]) == [0, 1]


def test_drain_node_idempotent(monkeypatch, shutdown_only, ray_start_cluster):
    # NOTE: not testing response failure since the node is already marked as draining and shuts down gracefully.
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.DrainRaylet=1:100:0",
    )

    cluster = ray_start_cluster
    worker_node = cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    worker_node_id = worker_node.node_id

    gcs_client = ray._raylet.GcsClient(address=cluster.address)

    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION,
        "Test drain",
        0,
    )
    assert is_accepted

    # After drain is accepted on an idle node since no tasks are running nor primary objects kept
    # on that raylet, it should be marked idle and gracefully shut down.
    def node_is_dead():
        nodes = ray.nodes()
        for node in nodes:
            if node["NodeID"] == worker_node_id:
                return not node["Alive"]
        return True

    wait_for_condition(node_is_dead, timeout=1)


# Bundles can be leaked if the gcs dies before the CancelResourceReserve RPCs are
# propagated to all the raylets. Since this is inherently racy, we block CancelResourceReserve RPCs
# from ever succeeding to make this test deterministic.
@pytest.fixture
def inject_rpc_failures(monkeypatch, request):
    deterministic_failure = request.param
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.ReleaseUnusedBundles=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100")
        + ",NodeManagerService.grpc_client.CancelResourceReserve=-1:100:0",
    )


@pytest.mark.parametrize("inject_rpc_failures", ["request", "response"], indirect=True)
@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [{"num_cpus": 1}],
    indirect=True,
)
def test_release_unused_bundles_idempotent(
    inject_rpc_failures,
    ray_start_cluster_head_with_external_redis,
):
    cluster = ray_start_cluster_head_with_external_redis

    @ray.remote(num_cpus=1)
    def task():
        return "success"

    pg = placement_group(name="test_pg", strategy="PACK", bundles=[{"CPU": 1}])

    result_ref = task.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=0,
        )
    ).remote()
    assert ray.get(result_ref) == "success"

    # Remove the placement group. This will trigger CancelResourceReserve RPCs which need to be blocked
    # for the placement group bundle to be leaked.
    remove_placement_group(pg)

    cluster.head_node.kill_gcs_server()
    # ReleaseUnusedBundles only triggers after GCS restart to clean up potentially leaked bundles.
    cluster.head_node.start_gcs_server()

    # If the leaked bundle wasn't cleaned up, this task will hang due to resource unavailability
    result = ray.get(task.remote())
    assert result == "success"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
