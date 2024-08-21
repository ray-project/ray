import time

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.core.generated import autoscaler_pb2
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.tests.conftest import *  # noqa


@ray.remote
def get_node_id():
    return ray.get_runtime_context().get_node_id()


@ray.remote
class Actor:
    def ready(self):
        pass


def test_get_draining_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    # Make the worker node non-idle so the node won't be drained immediately.
    actor = Actor.options(resources={"worker": 1}).remote()
    ray.get(actor.ready.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 1,
    )
    assert is_accepted

    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)

    def get_draining_nodes():
        cluster_node_info_cache.update()
        return cluster_node_info_cache.get_draining_nodes()

    wait_for_condition(lambda: set(get_draining_nodes()) == {worker_node_id})
    for _ in range(10):
        cluster_node_info_cache.update()
        assert (
            cluster_node_info_cache.get_draining_nodes()[worker_node_id] == 2**63 - 1
        )

    assert cluster_node_info_cache.get_active_node_ids() == {head_node_id}


def test_get_draining_nodes_missing_deadline(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    # Make the worker node non-idle so the node won't be drained immediately.
    actor = Actor.options(resources={"worker": 1}).remote()
    ray.get(actor.ready.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    assert gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        0,
    )

    start = time.time()
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)

    def draining_node_cached():
        cluster_node_info_cache.update()
        draining_nodes = cluster_node_info_cache.get_draining_nodes()
        assert set(draining_nodes) == {worker_node_id}
        return True

    wait_for_condition(draining_node_cached)
    deadline = cluster_node_info_cache.get_draining_nodes()[worker_node_id]
    assert deadline >= (start + 300) * 1000

    # The deadline shouldn't change
    for _ in range(10):
        cluster_node_info_cache.update()
        assert cluster_node_info_cache.get_draining_nodes()[worker_node_id] == deadline

    assert cluster_node_info_cache.get_active_node_ids() == {head_node_id}


def test_get_node_az_basic(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"head": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-west-2a"},
    )
    cluster.add_node(
        resources={"worker1": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-west-2b"},
    )
    cluster.add_node(
        resources={"worker2": 1},
        labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "us-east-1"},
    )

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id_1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    worker_node_id_2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
    print("head_node_id", head_node_id)
    print("worker_node_id_1", worker_node_id_1)
    print("worker_node_id_2", worker_node_id_2)

    cluster_node_info_cache = create_cluster_node_info_cache(
        GcsClient(address=ray.get_runtime_context().gcs_address)
    )

    def get_node_az(node_id):
        cluster_node_info_cache.update()
        return cluster_node_info_cache.get_node_az(node_id)

    assert get_node_az(head_node_id) == "us-west-2a"
    assert get_node_az(worker_node_id_1) == "us-west-2b"
    assert get_node_az(worker_node_id_2) == "us-east-1"


def test_get_node_az_label_invalid_node_id(ray_start_cluster):
    """If get_node_az() is called with a non-existent node ID, it should return None."""

    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)

    cluster_node_info_cache = create_cluster_node_info_cache(
        GcsClient(address=ray.get_runtime_context().gcs_address)
    )

    def get_node_az(node_id):
        cluster_node_info_cache.update()
        return cluster_node_info_cache.get_node_az(node_id)

    assert get_node_az("random-node") is None


def test_get_node_az_label_missing(ray_start_cluster):
    """If get_node_az() is called on a valid node but the AZ label is
    missing, it should return None.
    """

    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())

    cluster_node_info_cache = create_cluster_node_info_cache(
        GcsClient(address=ray.get_runtime_context().gcs_address)
    )

    def get_node_az(node_id):
        cluster_node_info_cache.update()
        return cluster_node_info_cache.get_node_az(node_id)

    assert get_node_az(head_node_id) is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
