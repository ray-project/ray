import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.tests.conftest import *  # noqa


def test_get_draining_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    @ray.remote
    class Actor:
        def ready(self):
            pass

    # Make the worker node non-idle so the node won't be drained immediately.
    actor = Actor.options(resources={"worker": 1}).remote()
    ray.get(actor.ready.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
    )
    assert is_accepted

    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)

    def get_draining_nodes():
        cluster_node_info_cache.update()
        return cluster_node_info_cache.get_draining_node_ids()

    wait_for_condition(lambda: get_draining_nodes() == {worker_node_id})
    assert cluster_node_info_cache.get_active_node_ids() == {head_node_id}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
