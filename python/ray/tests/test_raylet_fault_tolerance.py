import sys
import time

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_request_worker_lease_idempotent(
    monkeypatch, shutdown_only, deterministic_failure
):
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.RequestWorkerLease=1:"
        + ("100:0" if deterministic_failure == "request" else "0:100"),
    )

    @ray.remote
    def simple_task():
        time.sleep(1)

    # Spin up a two-node cluster where only the second node has a custom resource.
    # By requesting that resource on the task, the worker lease must target the
    # remote node's raylet (different from the driver's local node).
    cluster = Cluster(initialize_head=True)
    # Remote node with a unique resource so scheduling targets this node.
    ray.init(address=cluster.address)

    remote_node = cluster.add_node(num_cpus=1, resources={"remote_node": 1})

    remote_id = remote_node.node_id

    _ = simple_task.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_id, soft=False
        ),
        resources={"remote_node": 1},
    ).remote()
    result_ref2 = simple_task.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=remote_id, soft=False
        ),
        resources={"remote_node": 1},
    ).remote()

    ray.get(result_ref2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
