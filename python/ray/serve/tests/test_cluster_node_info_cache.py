import time
from typing import cast

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.ray_constants import RAY_GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_S
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2, gcs_pb2, gcs_service_pb2
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.test_utils import get_node_id
from ray.tests.conftest import *  # noqa


class FakeGcsClient:
    def __init__(self, nodes, draining_nodes):
        self.nodes = nodes
        self.draining_nodes = draining_nodes
        self.raise_on_get_draining_nodes = False

    def get_all_node_info(self, timeout):
        return self.nodes

    def get_all_resource_usage(self, timeout):
        return gcs_service_pb2.GetAllResourceUsageReply()

    def get_draining_nodes(self, timeout):
        if self.raise_on_get_draining_nodes:
            raise RuntimeError("failed to get draining nodes")
        return self.draining_nodes


def make_alive_node(node_id):
    return gcs_pb2.GcsNodeInfo(
        node_id=node_id.binary(),
        state=gcs_pb2.GcsNodeInfo.ALIVE,
    )


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


def test_get_draining_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    # Keep a lease on the worker so the raylet remains alive while draining.
    @ray.remote
    class LeaseHolder:
        def ping(self):
            return None

    lease_holder = LeaseHolder.options(num_cpus=0, resources={"worker": 0.01}).remote()
    ray.get(lease_holder.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION,
        "test worker drain",
        0,
    )
    assert is_accepted

    wait_for_condition(lambda: gcs_client.get_draining_nodes().get(worker_node_id) == 0)

    earliest_default_deadline_ms = int(
        (time.time() + RAY_GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_S) * 1000
    )

    def cache_has_draining_worker():
        cluster_node_info_cache.update()
        return worker_node_id in cluster_node_info_cache.get_draining_nodes()

    wait_for_condition(cache_has_draining_worker)
    draining_deadline_ms = cluster_node_info_cache.get_draining_nodes()[worker_node_id]
    latest_default_deadline_ms = int(
        (time.time() + RAY_GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_S) * 1000
    )
    assert (
        earliest_default_deadline_ms
        <= draining_deadline_ms
        <= latest_default_deadline_ms
    )
    assert cluster_node_info_cache.get_active_node_ids() == {head_node_id}

    # A deadline synthesized for a node without one must not slide forward on
    # every controller update.
    for _ in range(3):
        time.sleep(0.01)
        cluster_node_info_cache.update()
        assert cluster_node_info_cache.get_draining_nodes() == {
            worker_node_id: draining_deadline_ms
        }


def test_get_draining_nodes_filters_dead_nodes():
    alive_node_id = ray.NodeID.from_random()
    dead_node_id = ray.NodeID.from_random()
    gcs_client = FakeGcsClient(
        nodes={alive_node_id: make_alive_node(alive_node_id)},
        draining_nodes={alive_node_id.hex(): 123, dead_node_id.hex(): 456},
    )
    cluster_node_info_cache = create_cluster_node_info_cache(
        cast(GcsClient, gcs_client)
    )

    cluster_node_info_cache.update()

    assert cluster_node_info_cache.get_draining_nodes() == {alive_node_id.hex(): 123}


def test_get_draining_nodes_error_prunes_dead_nodes():
    alive_node_id = ray.NodeID.from_random()
    dead_node_id = ray.NodeID.from_random()
    gcs_client = FakeGcsClient(
        nodes={
            alive_node_id: make_alive_node(alive_node_id),
            dead_node_id: make_alive_node(dead_node_id),
        },
        draining_nodes={alive_node_id.hex(): 123, dead_node_id.hex(): 456},
    )
    cluster_node_info_cache = create_cluster_node_info_cache(
        cast(GcsClient, gcs_client)
    )
    cluster_node_info_cache.update()

    gcs_client.nodes = {alive_node_id: make_alive_node(alive_node_id)}
    gcs_client.raise_on_get_draining_nodes = True
    cluster_node_info_cache.update()

    assert cluster_node_info_cache.get_draining_nodes() == {alive_node_id.hex(): 123}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
