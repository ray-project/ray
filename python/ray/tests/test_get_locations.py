import numpy as np
import pytest
import platform
import time

import ray


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
    node_id = ray.runtime_context.get_runtime_context().get()["node_id"]
    sizes = [100, 1000]
    obj_refs = [ray.put(np.zeros(s, dtype=np.uint8)) for s in sizes]
    ray.wait(obj_refs)
    locations = ray.experimental.get_object_locations(obj_refs)
    assert len(locations) == 2
    for idx, obj_ref in enumerate(obj_refs):
        location = locations[obj_ref]
        assert location["object_size"] > sizes[idx]
        assert location["node_ids"] == [node_id.hex()]


def test_get_locations_inlined(ray_start_regular):
    node_id = ray.runtime_context.get_runtime_context().get()["node_id"]
    obj_refs = [ray.put("123")]
    ray.wait(obj_refs)
    locations = ray.experimental.get_object_locations(obj_refs)
    for idx, obj_ref in enumerate(obj_refs):
        location = locations[obj_ref]
        assert location["node_ids"] == [node_id.hex()]
        assert location["object_size"] > 0


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_spilled_locations(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    ray.init(cluster.address)
    cluster.wait_for_nodes()

    node_id = ray.runtime_context.get_runtime_context().get()["node_id"]

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
        assert location["node_ids"] == [node_id.hex()]
        assert location["object_size"] > 0


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_get_locations_multi_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    # head node
    cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    ray.init(cluster.address)
    # add 1 worker node
    cluster.add_node(
        num_cpus=0, resources={"custom": 1}, object_store_memory=75 * 1024 * 1024
    )
    cluster.wait_for_nodes()

    all_node_ids = list(map(lambda node: node["NodeID"], ray.nodes()))
    driver_node_id = ray.runtime_context.get_runtime_context().get()["node_id"].hex()
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
    assert location["object_size"] == 2 ** 64 - 1
