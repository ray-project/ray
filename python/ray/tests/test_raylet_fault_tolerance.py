import sys

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import psutil


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


def test_kill_local_actor_rpc_retry_and_idempotency(monkeypatch, shutdown_only):
    """Test that KillLocalActor RPC retries work correctly and guarantee actor death.
    Not testing response since the actor is killed either way.
    """

    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "NodeManagerService.grpc_client.KillLocalActor=1:100:0",
    )

    ray.init()

    @ray.remote
    class SimpleActor:
        def ping(self):
            return "pong"

        def get_pid(self):
            import os

            return os.getpid()

    actor = SimpleActor.remote()

    result = ray.get(actor.ping.remote())
    assert result == "pong"

    worker_pid = ray.get(actor.get_pid.remote())

    assert psutil.pid_exists(worker_pid)

    ray.kill(actor)

    def verify_process_killed():
        return not psutil.pid_exists(worker_pid)

    wait_for_condition(verify_process_killed, timeout=30)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
