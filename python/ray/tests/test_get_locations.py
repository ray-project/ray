import sys
import time

import numpy as np
import pandas as pd
import pytest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def test_uninitialized():
    with pytest.raises(RuntimeError):
        ray.experimental.get_object_locations([])


def test_get_locations_empty_list(ray_start_regular):
    locations = ray.experimental.get_object_locations([])
    assert len(locations) == 0


def test_get_locations(ray_start_regular):
    node_id = ray.get_runtime_context().get_node_id()
    sizes = [100, 1000]
    obj_refs = [ray.put(np.zeros(s, dtype=np.uint8)) for s in sizes]
    ray.wait(obj_refs)
    locations = ray.experimental.get_object_locations(obj_refs)
    assert len(locations) == 2
    for idx, obj_ref in enumerate(obj_refs):
        location = locations[obj_ref]
        assert location["object_size"] > sizes[idx]
        assert location["node_ids"] == [node_id]


def test_get_locations_inlined(ray_start_regular):
    node_id = ray.get_runtime_context().get_node_id()
    obj_refs = [ray.put("123")]
    ray.wait(obj_refs)
    locations = ray.experimental.get_object_locations(obj_refs)
    for idx, obj_ref in enumerate(obj_refs):
        location = locations[obj_ref]
        assert location["node_ids"] == [node_id]
        assert location["object_size"] > 0


def test_spilled_locations(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    ray.init(cluster.address)
    cluster.wait_for_nodes()

    node_id = ray.get_runtime_context().get_node_id()

    @ray.remote
    def task():
        arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
        refs = []
        refs.extend([ray.put(arr) for _ in range(2)])
        ray.get(ray.put(arr))
        ray.get(ray.put(arr))
        return refs

    object_refs = ray.get(task.remote())
    ray.wait(object_refs)
    locations = ray.experimental.get_object_locations(object_refs)
    for obj_ref in object_refs:
        location = locations[obj_ref]
        assert location["node_ids"] == [node_id]
        assert location["object_size"] > 0


def test_get_locations_multi_nodes(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    # head node
    cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    ray.init(cluster.address)
    # add 1 worker node
    cluster.add_node(
        num_cpus=0, resources={"custom": 1}, object_store_memory=75 * 1024 * 1024
    )
    cluster.wait_for_nodes()

    all_node_ids = list(map(lambda node: node["NodeID"], ray.nodes()))
    driver_node_id = ray.get_runtime_context().get_node_id()
    all_node_ids.remove(driver_node_id)
    worker_node_id = all_node_ids[0]

    @ray.remote(num_cpus=0, resources={"custom": 1})
    def create_object():
        return np.random.rand(1 * 1024 * 1024)

    @ray.remote
    def task():
        return [create_object.remote()]

    object_refs = ray.get(task.remote())
    ray.wait(object_refs)
    locations = ray.experimental.get_object_locations(object_refs)
    for obj_ref in object_refs:
        location = locations[obj_ref]
        assert set(location["node_ids"]) == {driver_node_id, worker_node_id}
        assert location["object_size"] > 0


def test_location_pending(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    ray.init(cluster.address)
    cluster.wait_for_nodes()

    @ray.remote
    def task():
        # sleep for 1 hour so the object will be pending
        time.sleep(3600)
        return 1

    object_ref = task.remote()
    locations = ray.experimental.get_object_locations([object_ref])
    assert len(locations) == 1
    location = locations[object_ref]
    assert location["node_ids"] == []
    assert location["object_size"] is None

    local_locations = ray.experimental.get_local_object_locations([object_ref])
    assert len(local_locations) == 1
    local_location = local_locations[object_ref]
    assert local_location["node_ids"] == []
    assert local_location["object_size"] is None


# Tests for `get_local_object_locations`. We use matrix test:
# - callee can be regular ray task, or streaming generator;
# - caller can be in the same node (single node cluster), or different node.
#
# ... so we have 4 tests.
#
# Each test has the caller to produce Object(s) that consumes big memory but has a small
# sys.getsizeof. The caller then asserts the object size from the API
# `ray.experimental.get_local_object_locations` is > the actual memory consumed.

BIG_OBJ_SIZE = 3 * 1024 * 1024  # 3 MiB


class BigObject:
    def __init__(self):
        self.data = np.zeros((BIG_OBJ_SIZE,), dtype=np.uint8)


@ray.remote
def gen_big_object(block_size):
    return pd.DataFrame([{"data": BigObject()} for _ in range(block_size)])


@ray.remote
def gen_big_objects(block_size, block_count):
    for _ in range(block_count):
        big_object = ray.get(gen_big_object.remote(block_size))
        yield big_object


def assert_object_size_gt(obj_ref: ray.ObjectRef, size: int):
    d = ray.experimental.get_local_object_locations([obj_ref])
    assert d is not None
    assert len(d) == 1
    assert d[obj_ref]["object_size"] > size


def test_get_local_locations(ray_start_regular):
    """
    caller and callee are in the same node.
    callee is a regular ray task.
    """
    obj_ref = gen_big_object.remote(3)
    ray.wait([obj_ref])
    # The dataframe consists of 3 MiB of NumPy NDArrays.
    assert_object_size_gt(obj_ref, BIG_OBJ_SIZE)


def test_get_local_locations_generator(ray_start_regular):
    """
    caller and callee are in the same node.
    callee is a streaming generator.
    """
    for obj_ref in gen_big_objects.remote(3, 10):
        # No need to ray.wait, the object ref must have been ready before it's yielded.
        # The dataframe consists of 3 MiB of NumPy NDArrays.
        assert_object_size_gt(obj_ref, BIG_OBJ_SIZE)


def test_get_local_locations_multi_nodes(ray_start_cluster):
    """
    caller and callee are in different nodes.
    callee is a regular ray task.
    """
    cluster = ray_start_cluster
    # head node
    head_node = cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    head_node_id = head_node.node_id
    ray.init(cluster.address)
    # add 1 worker node
    worker_node = cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    worker_node_id = worker_node.node_id
    cluster.wait_for_nodes()

    @ray.remote
    def caller():
        obj_ref = gen_big_object.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=worker_node_id, soft=False
            )
        ).remote(3)
        ray.wait([obj_ref])
        # The dataframe consists of 3 MiB of NumPy NDArrays.
        assert_object_size_gt(obj_ref, BIG_OBJ_SIZE)

    ray.get(
        caller.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote()
    )


def test_get_local_locations_generator_multi_nodes(ray_start_cluster):
    """
    caller and callee are in different nodes.
    callee is a streaming generator.
    """
    cluster = ray_start_cluster
    # head node
    head_node = cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    head_node_id = head_node.node_id
    ray.init(cluster.address)
    # add 1 worker node
    worker_node = cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    worker_node_id = worker_node.node_id
    cluster.wait_for_nodes()

    @ray.remote
    def caller():
        gen = gen_big_objects.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=worker_node_id, soft=False
            )
        ).remote(3, 10)
        for obj_ref in gen:
            # No need to ray.wait, the object ref must have been ready before it's
            # yielded.
            assert_object_size_gt(obj_ref, BIG_OBJ_SIZE)

    ray.get(
        caller.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote()
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
