import pytest

import ray
from ray.serve._private.default_impl import (
    create_cluster_node_info_cache,
)
from ray.tests.conftest import *  # noqa


def test_get_alive_nodes(ray_start_cluster):
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

    cluster_node_info_cache = create_cluster_node_info_cache()
    cluster_node_info_cache.update()
    assert set(cluster_node_info_cache.get_alive_nodes()) == {
        (head_node_id, ray.nodes()[0]["NodeName"]),
        (worker_node_id, ray.nodes()[0]["NodeName"]),
    }
    assert cluster_node_info_cache.get_alive_node_ids() == {
        head_node_id,
        worker_node_id,
    }
    assert (
        cluster_node_info_cache.get_alive_node_ids()
        == cluster_node_info_cache.get_active_node_ids()
    )

    cluster.remove_node(worker_node)
    cluster.wait_for_nodes()

    # The killed worker node shouldn't show up in the alive node list.
    cluster_node_info_cache.update()
    assert cluster_node_info_cache.get_alive_nodes() == [
        (head_node_id, ray.nodes()[0]["NodeName"])
    ]
    assert cluster_node_info_cache.get_alive_node_ids() == {head_node_id}
    assert (
        cluster_node_info_cache.get_alive_node_ids()
        == cluster_node_info_cache.get_active_node_ids()
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
