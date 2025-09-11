import sys

import numpy as np
import pytest

import ray
from ray._private.internal_api import node_stats
from ray._private.test_utils import wait_for_condition
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
    ray.init(address=cluster.address)

    def get_cluster_memory_usage():
        """Returns total memory usage across all alive nodes in bytes"""
        total_usage = 0
        nodes = ray.nodes()
        for node_info in nodes:
            if node_info["Alive"]:
                node_address = node_info["NodeManagerAddress"]
                node_port = node_info["NodeManagerPort"]
                stats = node_stats(node_address, node_port)
                total_usage += stats.store_stats.object_store_bytes_used
        return total_usage

    print("remote_id", remote_node_1.node_id)
    big_object_ref = ray.put(np.zeros(100 * 1024 * 1024))  # 100MB object

    # Propagate the big object to the remote nodes' plasma stores
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

    assert ray.get([result_ref_1, result_ref_2]) == ["ok", "ok"]

    del big_object_ref
    wait_for_condition(lambda: get_cluster_memory_usage() == 0, timeout=30)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
