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
    assert worker_node_info["DeathReasonMessage"] == "Received SIGTERM"


def test_abnormal_termination(ray_start_cluster, fast_node_failure_detection):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

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
        == "Health check failed: missing too many heartbeats."
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
