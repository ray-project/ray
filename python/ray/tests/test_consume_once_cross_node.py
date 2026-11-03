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
    def produce(self, n, as_bytes=False):
        return bytes(n) if as_bytes else np.ones(n, dtype=np.uint8)


def _total(arrays):
    return sum(len(a) if isinstance(a, bytes) else int(a.sum()) for a in arrays)


@ray.remote
class ArrayConsumer:
    def consume(self, *arrays):
        return _total(arrays)


@ray.remote
class Owner:
    """Owns consume_once objects: the only process allowed to read them without
    consuming them, so its reads leave plain copies on its own node."""

    def __init__(self, producer, consumer):
        self.producer = producer
        self.consumer = consumer
        self.refs = []

    def produce(self, n, count=1, as_bytes=False):
        self.refs = [
            self.producer.produce.options(_consume_once=True).remote(n, as_bytes)
            for _ in range(count)
        ]
        ray.wait(self.refs, num_returns=count, fetch_local=False)

    def read(self):
        return _total(ray.get(self.refs))

    def start_fetch(self):
        ray.wait(self.refs, num_returns=len(self.refs), timeout=0, fetch_local=True)

    def consume(self):
        return ray.get([self.consumer.consume.remote(ref) for ref in self.refs])

    def consume_repeated_and_pair(self):
        """`c.remote(r0, r0)` then `c.remote(r1, r2)`."""
        first = self.consumer.consume.remote(self.refs[0], self.refs[0])
        second = self.consumer.consume.remote(self.refs[1], self.refs[2])
        return ray.get([first, second])

    def locations(self):
        return [
            ray.experimental.get_object_locations([ref])[ref]["node_ids"]
            for ref in self.refs
        ]


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


def _num_restored_objects(node_id):
    return _store_stats(node_id).restored_objects_total


def _num_created_objects(node_id):
    """Objects ever created in the node's store, including received copies."""
    return _store_stats(node_id).cumulative_created_objects


@pytest.fixture
def two_worker_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    node_a = cluster.add_node(
        num_cpus=2, resources={"node_a": 10}, object_store_memory=200 * MiB
    )
    node_b = cluster.add_node(
        num_cpus=4, resources={"node_b": 10}, object_store_memory=200 * MiB
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


@pytest.mark.parametrize("as_bytes", [False, True])
def test_consume_of_local_copy_pins_it_without_transfer(two_worker_nodes, as_bytes):
    """A plain copy already on the consumer's node becomes the primary copy."""
    _, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()
    owner = Owner.options(resources={"node_b": 1}).remote(producer, consumer)

    ray.get(owner.produce.remote(MiB, as_bytes=as_bytes))
    assert ray.get(owner.read.remote()) == MiB
    copies_a = _num_primary_copies(node_a.node_id)
    copies_b = _num_primary_copies(node_b.node_id)
    created_b = _num_created_objects(node_b.node_id)

    assert ray.get(owner.consume.remote()) == [MiB]
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    assert _num_created_objects(node_b.node_id) == created_b
    assert _num_primary_copies(node_a.node_id) == copies_a
    assert {node_a.node_id, node_b.node_id} <= set(ray.get(owner.locations.remote())[0])


def test_consume_of_evicted_local_copy_pulls_it_as_move(two_worker_nodes):
    """A plain copy that the store evicted is pulled again and pinned."""
    _, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()
    owner = Owner.options(resources={"node_b": 1}).remote(producer, consumer)
    filler = ArrayProducer.options(resources={"node_b": 1}).remote()

    ray.get(owner.produce.remote(MiB))
    assert ray.get(owner.read.remote()) == MiB
    assert node_b.node_id in ray.get(owner.locations.remote())[0]
    # Pinned primaries fill the consumer's store until the plain copy is evicted.
    fillers = [filler.produce.remote(20 * MiB) for _ in range(10)]
    ray.wait(fillers, num_returns=len(fillers), fetch_local=False)
    wait_for_condition(
        lambda: node_b.node_id not in ray.get(owner.locations.remote())[0]
    )
    copies_a = _num_primary_copies(node_a.node_id)
    copies_b = _num_primary_copies(node_b.node_id)

    assert ray.get(owner.consume.remote()) == [MiB]
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    assert _num_primary_copies(node_a.node_id) == copies_a
    del fillers


def test_consume_during_owner_fetch_pulls_once(two_worker_nodes):
    """A consume while the owner's `ray.wait(fetch_local=True)` of the same
    objects is in flight shares its pull: each object is created once on the
    consumer's node and pinned."""
    _, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()
    owner = Owner.options(resources={"node_b": 1}).remote(producer, consumer)

    count = 4
    ray.get(owner.produce.remote(20 * MiB, count=count))
    copies_a = _num_primary_copies(node_a.node_id)
    copies_b = _num_primary_copies(node_b.node_id)
    created_b = _num_created_objects(node_b.node_id)

    ray.get(owner.start_fetch.remote())
    assert ray.get(owner.consume.remote()) == [20 * MiB] * count
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + count)
    assert _num_created_objects(node_b.node_id) == created_b + count
    assert _num_primary_copies(node_a.node_id) == copies_a


def test_consume_during_owner_get_pulls_once(two_worker_nodes):
    """Same as above with a blocking `ray.get` in flight on a threaded owner."""
    _, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()
    owner = Owner.options(resources={"node_b": 1}, max_concurrency=2).remote(
        producer, consumer
    )

    count = 4
    ray.get(owner.produce.remote(20 * MiB, count=count))
    copies_a = _num_primary_copies(node_a.node_id)
    copies_b = _num_primary_copies(node_b.node_id)
    created_b = _num_created_objects(node_b.node_id)

    read = owner.read.remote()
    consumed = owner.consume.remote()
    assert ray.get(read) == count * 20 * MiB
    assert ray.get(consumed) == [20 * MiB] * count
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + count)
    assert _num_created_objects(node_b.node_id) == created_b + count
    assert _num_primary_copies(node_a.node_id) == copies_a


def test_consumer_death_mid_move_keeps_object(two_worker_nodes):
    """Killing the consumer while its moves are in flight leaves the objects
    readable and the consumer's raylet healthy."""
    _, node_a, node_b = two_worker_nodes
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()

    count = 4
    refs = [
        producer.produce.options(_consume_once=True).remote(20 * MiB)
        for _ in range(count)
    ]
    ray.wait(refs, num_returns=count, fetch_local=False)
    copies_a = _num_primary_copies(node_a.node_id)

    results = [consumer.consume.remote(ref) for ref in refs]
    ray.kill(consumer, no_restart=True)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(results)
    assert [int(a.sum()) for a in ray.get(refs)] == [20 * MiB] * count
    assert _num_primary_copies(node_a.node_id) == copies_a

    # The node still serves moves after the sweep of the dead worker's records.
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()
    ref = producer.produce.options(_consume_once=True).remote(MiB)
    copies_b = _num_primary_copies(node_b.node_id)
    assert ray.get(consumer.consume.remote(ref)) == MiB
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)


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


# Owner placement for the topology tests: on the producer's node, on the
# consumer's node, or on a third node. The producer is always on node_a and the
# consumer on node_b.
OWNER_PLACEMENTS = ["with_producer", "with_consumer", "own_node"]


@pytest.fixture
def three_worker_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    nodes = {}
    for name in ("node_a", "node_b", "node_c"):
        nodes[name] = cluster.add_node(
            num_cpus=4, resources={name: 10}, object_store_memory=200 * MiB
        )
    cluster.wait_for_nodes()
    yield cluster, nodes


def _actors(nodes, owner_placement):
    owner_node = {
        "with_producer": "node_a",
        "with_consumer": "node_b",
        "own_node": "node_c",
    }[owner_placement]
    producer = ArrayProducer.options(resources={"node_a": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()
    owner = Owner.options(resources={owner_node: 1}, max_concurrency=2).remote(
        producer, consumer
    )
    return owner, nodes["node_a"], nodes["node_b"], nodes[owner_node]


def _primary_copies(*nodes):
    return [_num_primary_copies(n.node_id) for n in nodes]


@pytest.mark.parametrize("owner_placement", OWNER_PLACEMENTS)
def test_topology_move_pins_consumer_copy(three_worker_nodes, owner_placement):
    _, nodes = three_worker_nodes
    owner, node_a, node_b, owner_node = _actors(nodes, owner_placement)

    ray.get(owner.produce.remote(MiB))
    copies_a, copies_b, copies_owner = _primary_copies(node_a, node_b, owner_node)

    assert ray.get(owner.consume.remote()) == [MiB]
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    assert _num_primary_copies(node_a.node_id) == copies_a
    if owner_placement == "own_node":
        assert _num_primary_copies(owner_node.node_id) == copies_owner
    assert {node_a.node_id, node_b.node_id} <= set(ray.get(owner.locations.remote())[0])
    # The owner can still read the object after the move.
    assert ray.get(owner.read.remote()) == MiB


@pytest.mark.parametrize("owner_placement", OWNER_PLACEMENTS)
def test_topology_owner_read_then_consume(three_worker_nodes, owner_placement):
    """The owner reads the object before it is consumed. With the producer, it
    reads the primary in place; with the consumer, it leaves the copy that the
    consume then pins without a transfer; on its own node, it leaves a plain
    copy there that the move ignores."""
    _, nodes = three_worker_nodes
    owner, node_a, node_b, owner_node = _actors(nodes, owner_placement)

    ray.get(owner.produce.remote(MiB))
    assert ray.get(owner.read.remote()) == MiB
    copies_a, copies_b, copies_owner = _primary_copies(node_a, node_b, owner_node)
    created_b = _num_created_objects(node_b.node_id)

    assert ray.get(owner.consume.remote()) == [MiB]
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    assert _num_primary_copies(node_a.node_id) == copies_a
    transfers_to_b = 0 if owner_placement == "with_consumer" else 1
    assert _num_created_objects(node_b.node_id) == created_b + transfers_to_b
    if owner_placement == "own_node":
        assert _num_primary_copies(owner_node.node_id) == copies_owner
    assert ray.get(owner.read.remote()) == MiB


@pytest.mark.parametrize("owner_placement", OWNER_PLACEMENTS)
def test_topology_owner_fetch_in_flight_then_consume(
    three_worker_nodes, owner_placement
):
    """The owner's fetch of the objects is in flight when they are consumed."""
    _, nodes = three_worker_nodes
    owner, node_a, node_b, owner_node = _actors(nodes, owner_placement)

    count = 4
    ray.get(owner.produce.remote(20 * MiB, count=count))
    copies_a, copies_b, copies_owner = _primary_copies(node_a, node_b, owner_node)
    created_b = _num_created_objects(node_b.node_id)

    ray.get(owner.start_fetch.remote())
    read = owner.read.remote()
    assert ray.get(owner.consume.remote()) == [20 * MiB] * count
    assert ray.get(read) == count * 20 * MiB
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + count)
    # Each object is created once on the consumer's node, whoever's pull did it.
    assert _num_created_objects(node_b.node_id) == created_b + count
    assert _num_primary_copies(node_a.node_id) == copies_a
    if owner_placement == "own_node":
        assert _num_primary_copies(owner_node.node_id) == copies_owner


@pytest.mark.parametrize("owner_placement", OWNER_PLACEMENTS)
def test_topology_repeated_and_two_args(three_worker_nodes, owner_placement):
    _, nodes = three_worker_nodes
    owner, node_a, node_b, _ = _actors(nodes, owner_placement)

    ray.get(owner.produce.remote(MiB, count=3))
    copies_a, copies_b = _primary_copies(node_a, node_b)

    assert ray.get(owner.consume_repeated_and_pair.remote()) == [2 * MiB, 2 * MiB]
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 3)
    assert _num_primary_copies(node_a.node_id) == copies_a


@pytest.mark.parametrize("owner_placement", ["with_consumer", "own_node"])
def test_topology_move_survives_producer_node_death(
    three_worker_nodes, owner_placement
):
    cluster, nodes = three_worker_nodes
    owner, node_a, node_b, _ = _actors(nodes, owner_placement)

    ray.get(owner.produce.remote(MiB))
    copies_b = _num_primary_copies(node_b.node_id)
    assert ray.get(owner.consume.remote()) == [MiB]
    wait_for_condition(lambda: _num_primary_copies(node_b.node_id) == copies_b + 1)
    # Let the primary-moved report reach the owner before the producer's node goes.
    time.sleep(1)

    cluster.remove_node(node_a, allow_graceful=False)
    assert ray.get(owner.read.remote(), timeout=60) == MiB


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


@pytest.mark.parametrize(
    "ray_start_cluster",
    [{"_system_config": {"object_spilling_threshold": 0.5}}],
    indirect=True,
)
def test_same_node_consume_of_spilled_primary_is_not_a_move(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    node_b = cluster.add_node(
        num_cpus=2, resources={"node_b": 10}, object_store_memory=75 * MiB
    )
    cluster.wait_for_nodes()
    producer = ArrayProducer.options(resources={"node_b": 1}).remote()
    consumer = ArrayConsumer.options(resources={"node_b": 1}).remote()

    refs = [
        producer.produce.options(_consume_once=True).remote(10 * MiB) for _ in range(10)
    ]
    ray.wait(refs, num_returns=len(refs), fetch_local=False)
    wait_for_condition(lambda: _num_spilled_objects(node_b.node_id) > 0)
    copies_b = _num_primary_copies(node_b.node_id)

    # The node already holds every primary copy, spilled or not: the consume
    # restores what it needs and pins nothing new.
    assert ray.get([consumer.consume.remote(ref) for ref in refs]) == [10 * MiB] * 10
    assert _num_restored_objects(node_b.node_id) > 0
    assert _num_primary_copies(node_b.node_id) == copies_b


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
