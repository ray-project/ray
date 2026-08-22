import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.test_utils import get_node_id
from ray.tests.conftest import *  # noqa


def test_get_alive_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()
    assert set(cluster_node_info_cache.get_alive_nodes()) == {
        (head_node_id, ray.nodes()[0]["NodeName"], ""),
        (worker_node_id, ray.nodes()[0]["NodeName"], ""),
    }
    assert cluster_node_info_cache.get_alive_node_ids() == {
        head_node_id,
        worker_node_id,
    }
    assert (
        cluster_node_info_cache.get_alive_node_ids()
        == cluster_node_info_cache.get_active_node_ids()
    )

    # Keep a lease on the worker so the raylet remains alive while draining.
    @ray.remote
    class LeaseHolder:
        def ping(self):
            return None

    lease_holder = LeaseHolder.options(num_cpus=0, resources={"worker": 0.01}).remote()
    ray.get(lease_holder.ping.remote())

    draining_deadline_ms = 2**63 - 2
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION,
        "test worker drain",
        draining_deadline_ms,
    )
    assert is_accepted

    def cache_has_draining_worker():
        cluster_node_info_cache.update()
        return cluster_node_info_cache.get_draining_nodes() == {
            worker_node_id: draining_deadline_ms
        }

    wait_for_condition(cache_has_draining_worker)
    assert cluster_node_info_cache.get_active_node_ids() == {head_node_id}

    cluster.remove_node(worker_node)
    cluster.wait_for_nodes()

    # The killed worker node shouldn't show up in the alive node list.
    cluster_node_info_cache.update()
    assert cluster_node_info_cache.get_alive_nodes() == [
        (head_node_id, ray.nodes()[0]["NodeName"], "")
    ]
    assert cluster_node_info_cache.get_alive_node_ids() == {head_node_id}
    assert (
        cluster_node_info_cache.get_alive_node_ids()
        == cluster_node_info_cache.get_active_node_ids()
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
