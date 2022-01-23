import json
import re
import platform
import sys
import time
from collections import defaultdict

import numpy as np
import pytest
import ray
from ray._private.test_utils import wait_for_condition
from ray.tests.test_object_spilling import is_dir_empty, assert_no_thrashing


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on "
    "Windows.")
def test_multiple_directories(tmp_path, shutdown_only):
    num_dirs = 3
    temp_dirs = []
    for i in range(num_dirs):
        temp_folder = tmp_path / f"spill_{i}"
        temp_folder.mkdir()
        temp_dirs.append(temp_folder)

    # Limit our object store to 75 MiB of memory.
    min_spilling_size = 0
    object_spilling_config = json.dumps({
        "type": "filesystem",
        "params": {
            "directory_path": [str(directory) for directory in temp_dirs]
        }
    })
    address = ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 5,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": min_spilling_size,
        })

    arr = np.ones(74 * 1024 * 1024, dtype=np.uint8)  # 74MB.
    object_refs = []
    # Now the storage is full.
    object_refs.append(ray.put(arr))

    num_object_spilled = 20
    for _ in range(num_object_spilled):
        object_refs.append(ray.put(arr))

    num_files = defaultdict(int)
    for temp_dir in temp_dirs:
        temp_folder = temp_dir / ray.ray_constants.DEFAULT_OBJECT_PREFIX
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
        s = ray.internal.internal_api.memory_summary(stats_only=True)
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
            _system_config={"object_spilling_threshold": thres}
            if thres else {})
        objs = []
        for _ in range(num_objects):
            objs.append(ray.put(np.empty(200_000_000, dtype=np.uint8)))
        time.sleep(10)  # Wait for spilling to happen
        _check_spilled(num_objects_spilled)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() != "Linux", reason="Failing on Windows/macOS.")
def test_object_spilling_threshold_default():
    _test_object_spilling_threshold(None, 10, 5)


@pytest.mark.skipif(
    platform.system() != "Linux", reason="Failing on Windows/macOS.")
def test_object_spilling_threshold_1_0():
    _test_object_spilling_threshold(1.0, 10, 0)


@pytest.mark.skipif(
    platform.system() != "Linux", reason="Failing on Windows/macOS.")
def test_object_spilling_threshold_0_1():
    _test_object_spilling_threshold(0.1, 10, 5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
