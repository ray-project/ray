import numpy as np
import json
import random
import os
import shutil
import platform
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.internal.internal_api import memory_summary

MB = 1024 * 1024


def _init_ray():
    return ray.init(num_cpus=2, object_store_memory=700e6)


def _check_spilled_mb(address, spilled=None, restored=None, fallback=None):
    def ok():
        s = memory_summary(address=address["redis_address"], stats_only=True)
        print(s)
        if restored:
            if "Restored {} MiB".format(restored) not in s:
                return False
        else:
            if "Restored" in s:
                return False
        if spilled:
            if "Spilled {} MiB".format(spilled) not in s:
                return False
        else:
            if "Spilled" in s:
                return False
        if fallback:
            if "Plasma filesystem mmap usage: {} MiB".format(
                    fallback) not in s:
                return False
        else:
            if "Plasma filesystem mmap usage:" in s:
                return False
        return True

    wait_for_condition(ok, timeout=3, retry_interval_ms=1000)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_fallback_when_spilling_impossible_on_put():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        x1p = ray.get(x1)
        # x2 will be fallback allocated on the filesystem.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        x2p = ray.get(x2)
        del x1p
        del x2p
        _check_spilled_mb(address, spilled=None, fallback=400)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_spilling_when_possible_on_put():
    try:
        address = _init_ray()
        results = []
        for _ in range(5):
            results.append(ray.put(np.zeros(400 * MB, dtype=np.uint8)))
        _check_spilled_mb(address, spilled=1600)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_fallback_when_spilling_impossible_on_get():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        # x1 will be spilled.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        _check_spilled_mb(address, spilled=400)
        # x1 will be restored, x2 will be spilled.
        x1p = ray.get(x1)
        _check_spilled_mb(address, spilled=800, restored=400)
        # x2 will be restored, triggering a fallback allocation.
        x2p = ray.get(x2)
        _check_spilled_mb(address, spilled=800, restored=800, fallback=400)
        del x1p
        del x2p
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_spilling_when_possible_on_get():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        # x1 will be spilled.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        _check_spilled_mb(address, spilled=400)
        # x1 will be restored, x2 will be spilled.
        ray.get(x1)
        _check_spilled_mb(address, spilled=800, restored=400)
        # x2 will be restored, spilling x1.
        ray.get(x2)
        _check_spilled_mb(address, spilled=800, restored=800)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_task_unlimited():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        refs = [x1]
        # x1 is spilled.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        x2p = ray.get(x2)
        sentinel = ray.put(np.zeros(100 * MB, dtype=np.uint8))
        _check_spilled_mb(address, spilled=400)

        @ray.remote
        def consume(refs):
            # triggers fallback allocation, spilling of the sentinel
            ray.get(refs[0])
            # triggers fallback allocation.
            return ray.put(np.zeros(400 * MB, dtype=np.uint8))

        # round 1
        ray.get(consume.remote(refs))
        _check_spilled_mb(address, spilled=500, restored=400, fallback=400)

        del x2p
        del sentinel
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_task_unlimited_multiget_args():
    try:
        address = _init_ray()
        # Too many refs to fit into memory.
        refs = []
        for _ in range(10):
            refs.append(ray.put(np.zeros(200 * MB, dtype=np.uint8)))
        x2 = ray.put(np.zeros(600 * MB, dtype=np.uint8))
        x2p = ray.get(x2)
        _check_spilled_mb(address, spilled=2000)

        @ray.remote
        def consume(refs):
            # Should work without thrashing.
            ray.get(refs)
            return os.getpid()

        ray.get([consume.remote(refs) for _ in range(1000)])
        _check_spilled_mb(address, spilled=2000, restored=2000, fallback=2000)
        del x2p
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows.")
def test_fd_reuse_no_memory_corruption(shutdown_only):
    @ray.remote
    class Actor:
        def produce(self, i):
            s = random.randrange(1, 200)
            z = np.ones(s * 1024 * 1024)
            z[0] = i
            return z

        def consume(self, x, i):
            print(x)
            assert x[0] == i, x

    ray.init(object_store_memory=100e6)
    a = Actor.remote()
    b = Actor.remote()
    for i in range(20):
        x_id = a.produce.remote(i)
        ray.get(b.consume.remote(x_id, i))


@pytest.mark.skipif(
    platform.system() != "Linux",
    reason="Only Linux handles fallback allocation disk full error.")
def test_fallback_allocation_failure(shutdown_only):
    file_system_config = {
        "type": "filesystem",
        "params": {
            "directory_path": "/tmp",
        }
    }
    ray.init(
        object_store_memory=100e6,
        _temp_dir="/dev/shm",
        _system_config={
            "object_spilling_config": json.dumps(file_system_config),
        })
    shm_size = shutil.disk_usage("/dev/shm").total
    object_size = max(100e6, shm_size // 5)
    num_exceptions = 0
    refs = []
    for i in range(8):
        print("Start put", i)
        try:
            refs.append(
                ray.get(ray.put(np.zeros(object_size, dtype=np.uint8))))
        except ray.exceptions.ObjectStoreFullError:
            num_exceptions = num_exceptions + 1
    assert num_exceptions > 0


# TODO(ekl) enable this test once we implement this behavior.
# @pytest.mark.skipif(
#    platform.system() == "Windows", reason="Need to fix up for Windows.")
# def test_task_unlimited_huge_args():
#     try:
#         address = _init_ray()
#
#         # PullManager should raise an error, since the set of task args is
#         # too huge to fit into memory.
#         @ray.remote
#         def consume(*refs):
#             return "ok"
#
#         # Too many refs to fit into memory.
#         refs = []
#         for _ in range(10):
#             refs.append(ray.put(np.zeros(200 * MB, dtype=np.uint8)))
#
#         with pytest.raises(Exception):
#             ray.get(consume.remote(*refs))
#     finally:
#         ray.shutdown()

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
