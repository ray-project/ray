import sys

import pytest

import ray
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


# Bundles can be leaked if the gcs dies before the CancelResourceReserve RPCs are
# propagated to all the raylets. Since this is inherently racy, we block CancelResourceReserve RPCs
# from ever succeeding to make this test deterministic.
@pytest.fixture
def inject_rpc_failures(monkeypatch):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.ReleaseUnusedBundles=1:100:0"
        + ",NodeManagerService.grpc_client.CancelResourceReserve=-1:100:0",
    )


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [{"num_cpus": 1}],
    indirect=True,
)
def test_release_unused_bundles_idempotent(
    inject_rpc_failures, ray_start_cluster_head_with_external_redis
):
    # NOTE: Not testing response failure because the leaked bundle is cleaned up anyway
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
    result = ray.get(task.remote(), timeout=30)
    assert result == "success"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
