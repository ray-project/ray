import sys
import time

import numpy as np
import pytest

import ray
from ray._private.test_utils import wait_for_condition

MiB = 1024 * 1024


@ray.remote
class Producer:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)

    def produce(self, size):
        return np.zeros(size, dtype=np.uint8)

    def put(self, size):
        # Wrapped so that the caller borrows the ref instead of getting the value.
        return [ray.put(np.zeros(size, dtype=np.uint8))]

    def get_freed(self):
        return self.freed


@ray.remote
class Consumer:
    def __init__(self):
        self.held = None

    def consume(self, array):
        return len(array)

    def hold(self, array):
        self.held = array

    def release(self):
        self.held = None


def assert_freed_once(producer, object_ids):
    """Wait for the hook to run for every object, then check it ran only once each."""
    expected = sorted(object_ids)
    wait_for_condition(lambda: sorted(ray.get(producer.get_freed.remote())) == expected)
    # Frees are batched for up to a second; wait past that for a second run.
    time.sleep(2)
    assert sorted(ray.get(producer.get_freed.remote())) == expected


def test_hook_runs_when_a_task_return_goes_out_of_scope(shutdown_only):
    ray.init(num_cpus=1)
    producer = Producer.remote()
    ref = producer.produce.remote(MiB)
    ray.wait([ref])
    object_id = ref.hex()
    del ref
    assert_freed_once(producer, [object_id])


def test_hook_runs_when_a_put_goes_out_of_scope(shutdown_only):
    ray.init(num_cpus=1)
    producer = Producer.remote()
    [ref] = ray.get(producer.put.remote(MiB))
    object_id = ref.hex()
    del ref
    assert_freed_once(producer, [object_id])


@pytest.mark.parametrize("read_after_spill", [False, True])
def test_hook_runs_once_for_spilled_objects(
    fs_only_object_spilling_config, shutdown_only, read_after_spill
):
    object_spilling_config, _ = fs_only_object_spilling_config
    ray.init(
        num_cpus=1,
        object_store_memory=75 * MiB,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )
    producer = Producer.remote()
    # 200 MiB in a 75 MiB store: most of them are spilled and evicted.
    refs = [producer.produce.remote(20 * MiB) for _ in range(10)]
    # Without fetch_local=False, the wait restores the evicted ones.
    ray.wait(refs, num_returns=len(refs), fetch_local=False)
    if read_after_spill:
        # Restores the evicted ones, so the node has a copy in memory and on disk.
        for ref in refs:
            ray.get(ref)
        del ref
    object_ids = [ref.hex() for ref in refs]
    del refs
    assert_freed_once(producer, object_ids)


def test_hook_runs_once_for_an_object_copied_to_another_node(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"producer": 1})
    producer = Producer.options(resources={"producer": 1}).remote()
    ref = producer.produce.remote(MiB)
    # Copies the object to the driver's node.
    ray.get(ref)
    object_id = ref.hex()
    del ref
    assert_freed_once(producer, [object_id])


def test_same_node_move_runs_the_hook_at_the_move(shutdown_only):
    ray.init(num_cpus=1)
    producer = Producer.remote()
    consumer = Consumer.remote()
    ref = producer.produce.options(_consume_once=True).remote(MiB)
    object_id = ref.hex()
    # The consumer keeps the object in memory, so only the move can run the hook.
    ray.get(consumer.hold.remote(ref))
    assert_freed_once(producer, [object_id])
    ray.get(consumer.release.remote())
    del ref
    assert_freed_once(producer, [object_id])


def test_same_node_move_of_spilled_objects_runs_the_hook_once(
    fs_only_object_spilling_config, shutdown_only
):
    object_spilling_config, _ = fs_only_object_spilling_config
    ray.init(
        num_cpus=1,
        object_store_memory=75 * MiB,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )
    producer = Producer.remote()
    consumer = Consumer.remote()
    refs = [
        producer.produce.options(_consume_once=True).remote(20 * MiB) for _ in range(10)
    ]
    ray.wait(refs, num_returns=len(refs), fetch_local=False)
    object_ids = [ref.hex() for ref in refs]
    # Most are spilled and evicted; each move restores one for the consumer.
    ray.get([consumer.consume.remote(ref) for ref in refs])
    assert_freed_once(producer, object_ids)
    del refs
    assert_freed_once(producer, object_ids)


@pytest.fixture
def driver_and_producer_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"driver_node": 1})
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"producer_node": 1})
    cluster.add_node(num_cpus=1, resources={"consumer_node": 1})
    cluster.wait_for_nodes()
    yield cluster


@pytest.mark.parametrize("read_before_move", [False, True])
def test_cross_node_move_runs_the_hook_once_on_the_producers_node(
    driver_and_producer_nodes, read_before_move
):
    producer = Producer.options(resources={"producer_node": 1}).remote()
    # With the read, the consumer runs on the driver's node, which already has a copy.
    consumer_node = "driver_node" if read_before_move else "consumer_node"
    consumer = Consumer.options(resources={consumer_node: 1}).remote()
    ref = producer.produce.options(_consume_once=True).remote(MiB)
    if read_before_move:
        ray.get(ref)
    else:
        ray.wait([ref], fetch_local=False)
    object_id = ref.hex()
    # The move frees the producer's copy, which runs the hook.
    ray.get(consumer.consume.remote(ref))
    assert_freed_once(producer, [object_id])
    del ref
    assert_freed_once(producer, [object_id])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
