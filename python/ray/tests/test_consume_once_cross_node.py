"""Cross-node moves of consume_once objects.

The driver (the owner) runs on a head node without CPUs, so every object lives
on one of the two worker nodes.
"""

import sys
import time

import numpy as np
import pytest

import ray
from ray._private.test_utils import wait_for_condition

MiB = 1024 * 1024


@ray.remote
class ArrayProducer:
    def produce(self, n):
        return np.ones(n, dtype=np.uint8)


@ray.remote
class ArrayConsumer:
    def consume(self, *arrays):
        return sum(int(a.sum()) for a in arrays)


def _store_stats(node_id):
    for node in ray.nodes():
        if node["NodeID"] == node_id:
            return ray._private.internal_api.node_stats(
                node["NodeManagerAddress"], node["NodeManagerPort"], False
            ).store_stats
    raise AssertionError(f"node {node_id} not found")


def _num_primary_copies(node_id):
    """Objects pinned as a primary copy on the node, in plasma or spilled."""
    return _store_stats(node_id).num_object_store_primary_copies


def _num_spilled_objects(node_id):
    return _store_stats(node_id).spilled_objects_total


@pytest.fixture
def two_worker_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    node_a = cluster.add_node(
        num_cpus=2, resources={"node_a": 10}, object_store_memory=200 * MiB
    )
    node_b = cluster.add_node(
        num_cpus=2, resources={"node_b": 10}, object_store_memory=200 * MiB
    )
    cluster.wait_for_nodes()
    yield cluster, node_a, node_b


def test_cross_node_move_pins_consumer_copy(two_worker_nodes):
    _, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()

    ref = producer.produce.options(_consume_once=True).remote(MiB)
    ray.wait([ref], fetch_local=False)
    copies_a = _num_primary_copies(node_a.node_id)
    copies_b = _num_primary_copies(node_b.node_id)

    assert ray.get(consumer.consume.remote(ref)) == MiB
    # The pulled copy is pinned on the consumer's node as a primary copy. The
    # producer's copy stays until the owner frees it.
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    assert _num_primary_copies(node_a.node_id) == copies_a
    locations = ray.experimental.get_object_locations([ref])[ref]["node_ids"]
    assert {node_a.node_id, node_b.node_id} <= set(locations)
    # The owner can still read the object after the move.
    assert int(ray.get(ref).sum()) == MiB


def test_cross_node_move_pins_once_per_object(two_worker_nodes):
    _, _, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()

    repeated = producer.produce.options(_consume_once=True).remote(MiB)
    first = producer.produce.options(_consume_once=True).remote(MiB)
    second = producer.produce.options(_consume_once=True).remote(MiB)
    ray.wait([repeated, first, second], num_returns=3, fetch_local=False)
    copies_b = _num_primary_copies(node_b.node_id)

    assert ray.get(consumer.consume.remote(repeated, repeated)) == 2 * MiB
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    assert ray.get(consumer.consume.remote(first, second)) == 2 * MiB
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 3)


def test_same_node_consume_keeps_single_copy(two_worker_nodes):
    _, node_a, _ = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_a": 1}).remote()

    ref = producer.produce.options(_consume_once=True).remote(MiB)
    ray.wait([ref], fetch_local=False)
    copies_a = _num_primary_copies(node_a.node_id)

    assert ray.get(consumer.consume.remote(ref)) == MiB
    assert _num_primary_copies(node_a.node_id) == copies_a


def test_cross_node_move_survives_producer_node_death(two_worker_nodes):
    cluster, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()

    ref = producer.produce.options(_consume_once=True).remote(MiB)
    ray.wait([ref], fetch_local=False)
    copies_b = _num_primary_copies(node_b.node_id)
    assert ray.get(consumer.consume.remote(ref)) == MiB
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    # Let the primary-moved report reach the owner before the producer's node goes.
    time.sleep(1)

    # The owner now tracks the consumer's copy as the primary copy, so losing the
    # producer's node does not lose the object.
    cluster.remove_node(node_a, allow_graceful=False)
    assert int(ray.get(ref, timeout=60).sum()) == MiB


@pytest.mark.parametrize(
    "ray_start_cluster",
    [{"_system_config": {"object_spilling_threshold": 0.5}}],
    indirect=True,
)
def test_cross_node_move_of_spilled_object(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    node_a = cluster.add_node(
        num_cpus=2, resources={"node_a": 10}, object_store_memory=75 * MiB
    )
    node_b = cluster.add_node(
        num_cpus=2, resources={"node_b": 10}, object_store_memory=400 * MiB
    )
    cluster.wait_for_nodes()
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()

    refs = [
        producer.produce.options(_consume_once=True).remote(10 * MiB) for _ in range(10)
    ]
    ray.wait(refs, num_returns=len(refs), fetch_local=False)
    wait_for_condition(lambda: _num_spilled_objects(node_a.node_id) > 0)
    copies_b = _num_primary_copies(node_b.node_id)

    # A move served from the producer's spill file is pinned like any other.
    assert ray.get([consumer.consume.remote(ref) for ref in refs]) == [10 * MiB] * 10
    wait_for_condition(
        lambda: _num_primary_copies(node_b.node_id) == copies_b + len(refs)
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
