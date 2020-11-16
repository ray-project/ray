import copy
import json
import random
import platform
import sys
import time

import numpy as np
import pytest
import psutil
import ray

bucket_name = "object-spilling-test"
file_system_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": "/tmp"
    }
}
smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {
        "uri": f"s3://{bucket_name}/"
    }
}


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ])
def object_spilling_config(request, tmpdir):
    if request.param["type"] == "filesystem":
        request.param["params"]["directory_path"] = str(tmpdir)
    yield json.dumps(request.param)


@pytest.mark.skip("This test is for local benchmark.")
def test_sample_benchmark(object_spilling_config, shutdown_only):
    # --Config values--
    max_io_workers = 10
    object_store_limit = 500 * 1024 * 1024
    eight_mb = 1024 * 1024
    object_size = 12 * eight_mb
    spill_cnt = 50

    # Limit our object store to 200 MiB of memory.
    ray.init(
        object_store_memory=object_store_limit,
        _system_config={
            "object_store_full_max_retries": 0,
            "max_io_workers": max_io_workers,
            "object_spilling_config": object_spilling_config,
        })
    arr = np.random.rand(object_size)
    replay_buffer = []
    pinned_objects = set()

    # Create objects of more than 200 MiB.
    spill_start = time.perf_counter()
    for _ in range(spill_cnt):
        ref = None
        while ref is None:
            try:
                ref = ray.put(arr)
                replay_buffer.append(ref)
                pinned_objects.add(ref)
            except ray.exceptions.ObjectStoreFullError:
                ref_to_spill = pinned_objects.pop()
                ray.experimental.force_spill_objects([ref_to_spill])
    spill_end = time.perf_counter()

    # Make sure to remove unpinned objects.
    del pinned_objects
    restore_start = time.perf_counter()
    while replay_buffer:
        ref = replay_buffer.pop()
        sample = ray.get(ref)  # noqa
    restore_end = time.perf_counter()

    print(f"Object spilling benchmark for the config {object_spilling_config}")
    print(f"Spilling {spill_cnt} number of objects of size {object_size}B "
          f"takes {spill_end - spill_start} seconds with {max_io_workers} "
          "number of io workers.")
    print(f"Getting all objects takes {restore_end - restore_start} seconds.")


def test_invalid_config_raises_exception(shutdown_only):
    # Make sure ray.init raises an exception before
    # it starts processes when invalid object spilling
    # config is given.
    with pytest.raises(ValueError):
        ray.init(_system_config={
            "object_spilling_config": json.dumps({
                "type": "abc"
            }),
        })

    with pytest.raises(Exception):
        copied_config = copy.deepcopy(file_system_object_spilling_config)
        # Add invalid params to the config.
        copied_config["params"].update({"random_arg": "abc"})
        ray.init(_system_config={
            "object_spilling_config": json.dumps(copied_config),
        })


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_manually(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "object_store_full_max_retries": 0,
            "automatic_object_spilling_enabled": False,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []
    pinned_objects = set()

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

    def is_worker(cmdline):
        return cmdline and cmdline[0].startswith("ray::")

    # Make sure io workers are spawned with proper name.
    processes = [
        x.cmdline()[0] for x in psutil.process_iter(attrs=["cmdline"])
        if is_worker(x.info["cmdline"])
    ]
    assert (
        ray.ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE in processes)

    # Spill 2 more objects so we will always have enough space for
    # restoring objects back.
    refs_to_spill = (pinned_objects.pop(), pinned_objects.pop())
    ray.experimental.force_spill_objects(refs_to_spill)

    # randomly sample objects
    for _ in range(100):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref)
        assert np.array_equal(sample, arr)

    # Make sure io workers are spawned with proper name.
    processes = [
        x.cmdline()[0] for x in psutil.process_iter(attrs=["cmdline"])
        if is_worker(x.info["cmdline"])
    ]
    assert (
        ray.ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE in processes)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_manually_from_workers(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 100 MiB of memory.
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "object_store_full_max_retries": 0,
            "automatic_object_spilling_enabled": False,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
        })

    @ray.remote
    def _worker():
        arr = np.random.rand(1024 * 1024)  # 8 MB data
        ref = ray.put(arr)
        ray.experimental.force_spill_objects([ref])
        return ref

    # Create objects of more than 200 MiB.
    replay_buffer = [ray.get(_worker.remote()) for _ in range(25)]
    values = {ref: np.copy(ray.get(ref)) for ref in replay_buffer}
    # Randomly sample objects.
    for _ in range(100):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref)
        assert np.array_equal(sample, values[ref])


@pytest.mark.skip(reason="Not implemented yet.")
def test_spill_objects_manually_with_workers(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "object_store_full_max_retries": 0,
            "automatic_object_spilling_enabled": False,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
        })
    arrays = [np.random.rand(100 * 1024) for _ in range(50)]
    objects = [ray.put(arr) for arr in arrays]

    @ray.remote
    def _worker(object_refs):
        ray.experimental.force_spill_objects(object_refs)

    ray.get([_worker.remote([o]) for o in objects])

    for restored, arr in zip(ray.get(objects), arrays):
        assert np.array_equal(restored, arr)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 0,
        "object_store_memory": 75 * 1024 * 1024,
        "_system_config": {
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
            "max_io_workers": 4,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": "/tmp"
                }
            }),
        },
    }],
    indirect=True)
def test_spill_remote_object(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(object_store_memory=75 * 1024 * 1024)

    @ray.remote
    def put():
        return np.random.rand(5 * 1024 * 1024)  # 40 MB data

    @ray.remote
    def depends(arg):
        return

    ref = put.remote()
    copy = np.copy(ray.get(ref))
    # Evict local copy.
    ray.put(np.random.rand(5 * 1024 * 1024))  # 40 MB data
    # Remote copy should cause first remote object to get spilled.
    ray.get(put.remote())

    sample = ray.get(ref)
    assert np.array_equal(sample, copy)
    # Evict the spilled object.
    del sample
    ray.get(put.remote())
    ray.put(np.random.rand(5 * 1024 * 1024))  # 40 MB data

    # Test passing the spilled object as an arg to another task.
    ray.get(depends.remote(ref))


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_automatically(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
        })
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


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
@pytest.mark.skip(
    "Temporarily disabled until OutOfMemory retries can be moved "
    "into the plasma store")
def test_spill_during_get(object_spilling_config, shutdown_only):
    ray.init(
        num_cpus=4,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "max_io_workers": 2,
            "object_spilling_config": object_spilling_config,
        },
    )

    @ray.remote
    def f():
        return np.zeros(10 * 1024 * 1024)

    ids = []
    for i in range(10):
        x = f.remote()
        print(i, x)
        ids.append(x)

    # Concurrent gets, which require restoring from external storage, while
    # objects are being created.
    for x in ids:
        print(ray.get(x).shape)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_deadlock(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    # Wait raylet for starting an IO worker.
    time.sleep(1)

    # Create objects of more than 400 MiB.
    for _ in range(50):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)
        # This is doing random sampling with 50% prob.
        if random.randint(0, 9) < 5:
            for _ in range(5):
                ref = random.choice(replay_buffer)
                sample = ray.get(ref, timeout=0)
                assert np.array_equal(sample, arr)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
