import sys
import pytest

import ray

from ray._private.test_utils import wait_for_condition
from ray.core.generated import common_pb2


def test_normal_termination(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()
    worker_node_id = worker_node.node_id

    @ray.remote
    class Actor:
        def ping(self):
            pass

    actor = Actor.options(num_cpus=0, resources={"worker": 1}).remote()
    ray.get(actor.ping.remote())

    # normal node termination
    cluster.remove_node(worker_node)

    worker_node_info = [
        node for node in ray.nodes() if node["NodeID"] == worker_node_id
    ][0]
    assert not worker_node_info["Alive"]
    assert worker_node_info["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
        "EXPECTED_TERMINATION"
    )
    assert worker_node_info["DeathReasonMessage"] == "received SIGTERM"

    try:
        ray.get(actor.ping.remote())
        raise
    except ray.exceptions.ActorDiedError as e:
        assert not e.preempted
        assert "The actor died because its node has died." in str(e)
        assert "the actor's node was terminated expectedly: received SIGTERM" in str(e)


def test_abnormal_termination(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    cluster = ray_start_cluster
    head_node = cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = head_node.node_id
    worker_node_id = worker_node.node_id

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id, worker_node_id}
    )

    @ray.remote
    class Actor:
        def ping(self):
            pass

    actor = Actor.options(num_cpus=0, resources={"worker": 1}).remote()
    ray.get(actor.ping.remote())

    # Simulate the worker node crashes.
    cluster.remove_node(worker_node, False)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id},
    )

    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
        "UNEXPECTED_TERMINATION"
    )
    assert (
        worker_node["DeathReasonMessage"]
        == "health check failed due to missing too many heartbeats"
    )

    try:
        ray.get(actor.ping.remote())
        raise
    except ray.exceptions.ActorDiedError as e:
        assert not e.preempted
        assert "The actor died because its node has died." in str(e)
        assert (
            "the actor's node was terminated unexpectedly: "
            "health check failed due to missing too many heartbeats" in str(e)
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
