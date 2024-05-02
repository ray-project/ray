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


def test_get_locations_timeout(ray_start_regular):
    sizes = [100, 1000]
    obj_refs = [ray.put(np.zeros(s, dtype=np.uint8)) for s in sizes]
    ray.wait(obj_refs)
    timeout_ms = 0
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.experimental.get_object_locations(obj_refs, timeout_ms)


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
    location = locations[object_ref]
    assert location["node_ids"] == []
    # TODO(chenshen): this is a result of converting int -1 to unsigned int;
    # should be fix by https://github.com/ray-project/ray/issues/16321
    assert location["object_size"] == 2**64 - 1


class BigObject:
    def __init__(self):
        # 100 MiB of memory used...
        self.data = np.zeros((100 * 1024 * 1024), dtype=np.uint8)


@ray.remote
def gen_big_objects(block_size, block_count):
    for _ in range(block_count):
        yield pd.DataFrame([{"data": BigObject()} for _ in range(block_size)])


def test_local_get_locations(ray_start_cluster):
    """
    Creates big memory consuming objects that appears to have a small `sys.getsizeof`.
    The streaming generator's consumer can get the size of it since it has a reference
    of the object.
    """
    for obj_ref in gen_big_objects.remote(3, 10):
        d = ray.experimental.get_local_object_locations(obj_ref)
        assert d is not None
        # The dataframe consists of 3 * 100MiB of NumPy NDArrays.
        assert d[obj_ref]["object_size"] > 3 * 100 * 1024 * 1024


def test_local_get_locations_multi_nodes(ray_start_cluster_enabled):
    """
    Same as test_local_get_locations, except that we assign the generator and the caller
    to different nodes and assert it still works, since the caller has references to the
    object refs.
    """
    cluster = ray_start_cluster_enabled
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
            d = ray.experimental.get_local_object_locations(obj_ref)
            assert d is not None
            # The dataframe consists of 3 * 100MiB of NumPy NDArrays.
            assert d[obj_ref]["object_size"] > 3 * 100 * 1024 * 1024

    ray.get(
        caller.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote()
    )


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
