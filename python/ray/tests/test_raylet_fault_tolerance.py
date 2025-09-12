import sys

import numpy as np
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


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_testing_rpc_failure": "NodeManagerService.grpc_client.PinObjectIDs=1:100:0",
                # Need to reduce this from 1 second otherwise the object will be evicted before the retry is received and pins the object
                "RAY_grpc_client_check_connection_status_interval_milliseconds": "0",
            },
        }
    ],
    indirect=True,
)
def test_pin_object_ids_idempotent(shutdown_only, ray_start_cluster_head_with_env_vars):
    cluster = ray_start_cluster_head_with_env_vars
    remote_node_1 = cluster.add_node(
        num_cpus=1,
        object_store_memory=200 * 1024 * 1024,
    )
    remote_node_2 = cluster.add_node(
        num_cpus=1,
        object_store_memory=200 * 1024 * 1024,
    )

    # Max retries is 0 to prevent object reconstruction and force an ObjectLostError to occur when eviction happens
    @ray.remote(max_retries=0)
    def create_big_object():
        return np.zeros(150 * 1024 * 1024)

    @ray.remote(max_retries=0)
    def move_big_object_ref(big_object_ref_list):
        ray.get(big_object_ref_list[0])
        return "ok"

    big_object_ref = create_big_object.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node_1.node_id, soft=False
        )
    ).remote()
    result_ref = move_big_object_ref.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node_2.node_id, soft=False
        )
    ).remote([big_object_ref])
    assert ray.get(result_ref) == "ok"

    # Kill remote_node_1 so that the secondary copy on remote_node_2 is pinned
    cluster.remove_node(remote_node_1)
    # Create memory pressure on remote_node_2 so that the object is spilled if
    # pinned correctly or evicted if not.
    memory_pressure_ref = create_big_object.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node_2.node_id, soft=False
        )
    ).remote()
    ray.get(memory_pressure_ref)
    # If the object was not pinned, it would be evicted and we would get an ObjectLostError.
    # A successful get means that the object was spilled meaning it was pinned successfully.
    ray.get(big_object_ref)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
