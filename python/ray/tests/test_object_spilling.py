import json
import random
import time

import numpy as np
import pytest
import ray


def test_spill_objects_manually(shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _object_spilling_config={
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp"
            }
        },
        _system_config={
            "object_store_full_max_retries": 0,
            "max_io_workers": 4,
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []
    pinned_objects = set()
    spilled_objects = set()

    # Create objects of more than 200 MiB.
    for _ in range(25):
        ref = None
        while ref is None:
            try:
                ref = ray.put(arr)
                replay_buffer.append(ref)
                pinned_objects.add(ref)
            except ray.exceptions.ObjectStoreFullError:
                ref_to_spill = pinned_objects.pop()
                ray.experimental.force_spill_objects([ref_to_spill])
                spilled_objects.add(ref_to_spill)

    # Spill 2 more objects so we will always have enough space for
    # restoring objects back.
    refs_to_spill = (pinned_objects.pop(), pinned_objects.pop())
    ray.experimental.force_spill_objects(refs_to_spill)
    spilled_objects.update(refs_to_spill)

    # randomly sample objects
    for _ in range(100):
        ref = random.choice(replay_buffer)
        if ref in spilled_objects:
            ray.experimental.force_restore_spilled_objects([ref])
        sample = ray.get(ref)
        assert np.array_equal(sample, arr)


def test_spill_objects_manually_from_workers(shutdown_only):
    # Limit our object store to 100 MiB of memory.
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _object_spilling_config={
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp"
            }
        },
        _system_config={
            "object_store_full_max_retries": 0,
            "max_io_workers": 4,
        })

    @ray.remote
    def _worker():
        arr = np.random.rand(100 * 1024)
        ref = ray.put(arr)
        ray.experimental.force_spill_objects([ref])
        ray.experimental.force_restore_spilled_objects([ref])
        assert np.array_equal(ray.get(ref), arr)

    ray.get([_worker.remote() for _ in range(50)])


def test_spill_objects_manually_with_workers(shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _object_spilling_config={
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp"
            }
        },
        _system_config={
            "object_store_full_max_retries": 0,
            "max_io_workers": 4,
        })
    arrays = [np.random.rand(100 * 1024) for _ in range(50)]
    objects = [ray.put(arr) for arr in arrays]

    @ray.remote
    def _worker(object_refs):
        ray.experimental.force_spill_objects(object_refs)

    ray.get([_worker.remote([o]) for o in objects])

    for restored, arr in zip(ray.get(objects), arrays):
        assert np.array_equal(restored, arr)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 0,
        "object_store_memory": 75 * 1024 * 1024,
        "_object_spilling_config": {
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp"
            }
        },
        "_system_config": json.dumps({
            "object_store_full_max_retries": 0,
            "max_io_workers": 4,
        }),
    }],
    indirect=True)
def test_spill_remote_object(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(
        object_store_memory=75 * 1024 * 1024,
        _object_spilling_config={
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp"
            }
        })

    @ray.remote
    def put():
        return np.random.rand(5 * 1024 * 1024)  # 40 MB data

    # Create 2 objects. Only 1 should fit.
    ref = put.remote()
    ray.get(ref)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(put.remote())
    time.sleep(1)
    # Spill 1 object. The second should now fit.
    ray.experimental.force_spill_objects([ref])
    ray.get(put.remote())

    # TODO(swang): Restoring from the object directory is not yet supported.
    # ray.experimental.force_restore_spilled_objects([ref])
    # sample = ray.get(ref)
    # assert np.array_equal(sample, copy)


@pytest.mark.skip(reason="have not been fully implemented")
def test_spill_objects_automatically(shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config=json.dumps({
            "max_io_workers": 4,
            "object_store_full_max_retries": 2,
            "object_store_full_initial_delay_ms": 10,
            "auto_object_spilling": True,
        }))
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    # Wait raylet for starting an IO worker.
    time.sleep(1)

    # Create objects of more than 800 MiB.
    for _ in range(100):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)

    print("-----------------------------------")

    # randomly sample objects
    for _ in range(1000):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, arr)
