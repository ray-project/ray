import ray

from ray.core.generated import gcs_pb2


def test_node_death(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()
    worker_node_id = worker_node.node_id
    cluster.remove_node(worker_node)

    worker_node_info = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node_info["Alive"] == False
    assert worker_node_info["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value("EXPECTED_TERMINATION")
    assert worker_node_info["DeathReasonMessage"] == "Received SIGTERM"

