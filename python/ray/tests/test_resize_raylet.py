import sys

import pytest

import ray
from ray._raylet import GcsClient


def test_resize_raylet_resource_instances(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    total_resources = gcs_client.resize_raylet_resource_instances(
        worker_node.node_id,
        {"CPU": 3.0},
    )

    assert total_resources["CPU"] == 3.0

    missing_node_id = ray.NodeID.from_random().hex()
    with pytest.raises(
        ValueError,
        match=f"Raylet {missing_node_id} is not alive.",
    ):
        gcs_client.resize_raylet_resource_instances(
            missing_node_id,
            {"CPU": 3.0},
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
