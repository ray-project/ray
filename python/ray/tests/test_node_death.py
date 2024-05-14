import sys
import pytest

import ray

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
