import sys
import pytest

import ray

from ray._private.test_utils import wait_for_condition
from ray.core.generated import gcs_pb2


def test_normal_termination(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()
    worker_node_id = worker_node.node_id
    cluster.remove_node(worker_node)

    worker_node_info = [
        node for node in ray.nodes() if node["NodeID"] == worker_node_id
    ][0]
    assert not worker_node_info["Alive"]
    assert worker_node_info["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value(
        "EXPECTED_TERMINATION"
    )
    assert worker_node_info["DeathReasonMessage"] == "received SIGTERM"


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

    # Simulate the worker node crashes.
    cluster.remove_node(worker_node, False)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id},
    )

    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value(
        "UNEXPECTED_TERMINATION"
    )
    assert (
        worker_node["DeathReasonMessage"]
        == "health check failed due to missing too many heartbeats"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
