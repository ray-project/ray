import sys

import pytest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


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

    # Spin up a two-node cluster where only the second node has a custom resource.
    # By requesting that resource on the task, the worker lease must target the
    # remote node's raylet (different from the driver's local node).
    cluster = ray_start_cluster
    # Remote node with a unique resource so scheduling targets this node.

    remote_node = cluster.add_node(num_cpus=1)

    remote_id = remote_node.node_id
    print(f"remote_id: {remote_id}")
    _ = simple_task_1.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_id, soft=False
        )
    ).remote()
    result_ref2 = simple_task_2.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_id, soft=False
        )
    ).remote()

    assert ray.get(result_ref2) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
