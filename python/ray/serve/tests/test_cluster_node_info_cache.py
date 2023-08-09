import pytest

import ray
from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache_factory import (
    create_cluster_node_info_cache,
)


def test_get_alive_nodes(ray_shutdown):
    ray.init()

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()
    assert cluster_node_info_cache.get_alive_nodes() == [
        (ray.get_runtime_context().get_node_id(), ray.nodes()[0]["NodeName"])
    ]
    assert cluster_node_info_cache.get_alive_node_ids() == {
        ray.get_runtime_context().get_node_id()
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
