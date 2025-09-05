import sys

import numpy as np
import pytest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_free_objects_idempotent(
    monkeypatch, shutdown_only, deterministic_failure, ray_start_cluster
):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "ObjectManagerService.grpc_client.FreeObjects=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    @ray.remote
    def simple_task(big_object_ref_list):
        ray.get(big_object_ref_list[0])
        return "ok"

    cluster = ray_start_cluster
    remote_node_1 = cluster.add_node(num_cpus=1)
    remote_node_2 = cluster.add_node(num_cpus=1)
    print("remote_id", remote_node_1.node_id)
    big_object_ref = ray.put(np.zeros(100 * 1024 * 1024))
    # Propogate the big object to the remote node's plasma store
    result_ref_1 = simple_task.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node_1.node_id, soft=False
        )
    ).remote([big_object_ref])
    result_ref_2 = simple_task.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_node_2.node_id, soft=False
        )
    ).remote([big_object_ref])
    assert ray.get(result_ref_1) == "ok"
    assert ray.get(result_ref_2) == "ok"
    del big_object_ref


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
