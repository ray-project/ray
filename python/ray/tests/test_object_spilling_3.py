import json
import platform
import random
import re
import shutil
import sys
import time
import zlib
from collections import defaultdict

import numpy as np
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.tests.test_object_spilling import assert_no_thrashing, is_dir_empty


@pytest.mark.skipif(platform.system() in ["Windows"], reason="Failing on Windows.")
def test_multiple_directories(tmp_path, shutdown_only):
    num_dirs = 3
    temp_dirs = []
    for i in range(num_dirs):
        temp_folder = tmp_path / f"spill_{i}"
        temp_folder.mkdir()
        temp_dirs.append(temp_folder)

    # Limit our object store to 75 MiB of memory.
    min_spilling_size = 0
    object_spilling_config = json.dumps(
        {
            "type": "filesystem",
            "params": {"directory_path": [str(directory) for directory in temp_dirs]},
        }
    )
    address = ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 5,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": min_spilling_size,
        },
    )

    arr = np.ones(74 * 1024 * 1024, dtype=np.uint8)  # 74MB.
    object_refs = []
    # Now the storage is full.
    object_refs.append(ray.put(arr))

    num_object_spilled = 20
    for _ in range(num_object_spilled):
        object_refs.append(ray.put(arr))

    num_files = defaultdict(int)
    for temp_dir in temp_dirs:
        temp_folder = temp_dir / ray._private.ray_constants.DEFAULT_OBJECT_PREFIX
        for path in temp_folder.iterdir():
            num_files[str(temp_folder)] += 1

    for ref in object_refs:
        assert np.array_equal(ray.get(ref), arr)

    print("Check distribution...")
    min_count = 5
    is_distributed = [n_files >= min_count for n_files in num_files.values()]
    assert all(is_distributed)

    print("Check deletion...")
    # Empty object refs.
    object_refs = []
    # Add a new object so that the last entry is evicted.
    ref = ray.put(arr)
    for temp_dir in temp_dirs:
        temp_folder = temp_dir
        wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["address"])

    # Now kill ray and see all directories are deleted.
    print("Check directories are deleted...")
    ray.shutdown()
    for temp_dir in temp_dirs:
        wait_for_condition(lambda: is_dir_empty(temp_dir, append_path=""))


def _check_spilled(num_objects_spilled=0):
    def ok():
        s = ray._private.internal_api.memory_summary(stats_only=True)
        if num_objects_spilled == 0:
            return "Spilled " not in s

        m = re.search(r"Spilled (\d+) MiB, (\d+) objects", s)
        if m is not None:
            actual_num_objects = int(m.group(2))
            return actual_num_objects >= num_objects_spilled

        return False

    wait_for_condition(ok, timeout=90, retry_interval_ms=5000)


def _test_object_spilling_threshold(thres, num_objects, num_objects_spilled):
    try:
        ray.init(
            object_store_memory=2_200_000_000,
            _system_config={"object_spilling_threshold": thres} if thres else {},
        )
        objs = []
        for _ in range(num_objects):
            objs.append(ray.put(np.empty(200_000_000, dtype=np.uint8)))
        time.sleep(10)  # Wait for spilling to happen
        _check_spilled(num_objects_spilled)
    finally:
        ray.shutdown()


@pytest.mark.skipif(platform.system() != "Linux", reason="Failing on Windows/macOS.")
def test_object_spilling_threshold_default():
    _test_object_spilling_threshold(None, 10, 5)


@pytest.mark.skipif(platform.system() != "Linux", reason="Failing on Windows/macOS.")
def test_object_spilling_threshold_1_0():
    _test_object_spilling_threshold(1.0, 10, 0)


@pytest.mark.skipif(platform.system() != "Linux", reason="Failing on Windows/macOS.")
def test_object_spilling_threshold_0_1():
    _test_object_spilling_threshold(0.1, 10, 5)


def test_partial_retval_allocation(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(object_store_memory=100 * 1024 * 1024)
    ray.init(cluster.address)

    @ray.remote(num_returns=4)
    def f():
        return [np.zeros(50 * 1024 * 1024, dtype=np.uint8) for _ in range(4)]

    ret = f.remote()
    for obj in ret:
        obj = ray.get(obj)
        print(obj.size)


def test_pull_spilled_object(
    ray_start_cluster_enabled, multi_node_object_spilling_config, shutdown_only
):
    cluster = ray_start_cluster_enabled
    object_spilling_config, _ = multi_node_object_spilling_config

    # Head node.
    cluster.add_node(
        num_cpus=1,
        resources={"custom": 0},
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 2,
            "min_spilling_size": 1 * 1024 * 1024,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
        },
    )
    ray.init(cluster.address)

    # add 1 worker node
    cluster.add_node(
        num_cpus=1, resources={"custom": 1}, object_store_memory=75 * 1024 * 1024
    )
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, resources={"custom": 1})
    def create_objects():
        results = []
        for size in range(5):
            arr = np.random.rand(size * 1024 * 1024)
            hash_value = zlib.crc32(arr.tobytes())
            results.append([ray.put(arr), hash_value])
        # ensure the objects are spilled
        arr = np.random.rand(5 * 1024 * 1024)
        ray.get(ray.put(arr))
        ray.get(ray.put(arr))
        return results

    @ray.remote(num_cpus=1, resources={"custom": 0})
    def get_object(arr):
        return zlib.crc32(arr.tobytes())

    results = ray.get(create_objects.remote())
    for value_ref, hash_value in results:
        hash_value1 = ray.get(get_object.remote(value_ref))
        assert hash_value == hash_value1


# TODO(chenshen): fix error handling when spilled file
# missing/corrupted
@pytest.mark.skipif(True, reason="Currently hangs.")
def test_pull_spilled_object_failure(object_spilling_config, ray_start_cluster):
    object_spilling_config, temp_folder = object_spilling_config
    cluster = ray_start_cluster

    # Head node.
    cluster.add_node(
        num_cpus=1,
        resources={"custom": 0},
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 2,
            "min_spilling_size": 1 * 1024 * 1024,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
        },
    )
    ray.init(cluster.address)

    # add 1 worker node
    cluster.add_node(
        num_cpus=1, resources={"custom": 1}, object_store_memory=75 * 1024 * 1024
    )
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, resources={"custom": 1})
    def create_objects():
        arr = np.random.rand(5 * 1024 * 1024)
        hash_value = zlib.crc32(arr.tobytes())
        results = [ray.put(arr), hash_value]
        # ensure the objects are spilled
        arr = np.random.rand(5 * 1024 * 1024)
        ray.get(ray.put(arr))
        ray.get(ray.put(arr))
        return results

    @ray.remote(num_cpus=1, resources={"custom": 0})
    def get_object(arr):
        return zlib.crc32(arr.tobytes())

    [ref, hash_value] = ray.get(create_objects.remote())

    # remove spilled file
    shutil.rmtree(temp_folder)

    hash_value1 = ray.get(get_object.remote(ref))
    assert hash_value == hash_value1


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_spill_dir_cleanup_on_raylet_start(fs_only_object_spilling_config):
    object_spilling_config, temp_folder = fs_only_object_spilling_config
    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={"object_spilling_config": object_spilling_config},
    )
    ray.init(address=cluster.address)
    node2 = cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)

    # This task will run on node 2 because node 1 has no CPU resource
    @ray.remote(num_cpus=1)
    def run_workload():
        ids = []
        for _ in range(2):
            arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
            ids.append(ray.put(arr))
        return ids

    ids = ray.get(run_workload.remote())
    assert not is_dir_empty(temp_folder)

    # Kill node 2
    cluster.remove_node(node2)

    # Verify that the spill folder is not empty
    assert not is_dir_empty(temp_folder)

    # Start a new node
    cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)

    # Verify that the spill folder is now cleaned up
    assert is_dir_empty(temp_folder)

    # We hold the object refs to prevent them from being deleted
    del ids
    ray.shutdown()
    cluster.shutdown()


def test_spill_deadlock(object_spilling_config, shutdown_only):
    object_spilling_config, _ = object_spilling_config
    # Limit our object store to 75 MiB of memory.
    address = ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

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
    assert_no_thrashing(address["address"])


def test_spill_reconstruction_errors(ray_start_cluster, object_spilling_config):
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 100,
        "max_direct_call_object_size": 100,
        "task_retry_delay_ms": 100,
        "object_timeout_milliseconds": 200,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _system_config=config, object_store_memory=10**8)
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    @ray.remote
    def put():
        return np.zeros(10**5, dtype=np.uint8)

    @ray.remote
    def check(x):
        return

    ref = put.remote()
    for _ in range(4):
        ray.get(check.remote(ref))
        cluster.remove_node(node_to_kill, allow_graceful=False)
        node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    # All reconstruction attempts used up. The object's value should now be an
    # error in the local store.
    # Force object spilling and check that it can complete.
    xs = []
    for _ in range(20):
        xs.append(ray.put(np.zeros(10**7, dtype=np.uint8)))
    for x in xs:
        ray.get(x, timeout=10)

    with pytest.raises(ray.exceptions.ObjectLostError):
        ray.get(ref)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
