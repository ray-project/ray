import numpy as np
import json
import random
import os
import shutil
import platform
import pytest

import ray
from ray._private.test_utils import (
    check_spilled_mb,
    fetch_prometheus,
    wait_for_condition,
)

MB = 1024 * 1024


def _init_ray():
    return ray.init(num_cpus=2, object_store_memory=700e6)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
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
        check_spilled_mb(address, spilled=None, fallback=400)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_spilling_when_possible_on_put():
    try:
        address = _init_ray()
        results = []
        for _ in range(5):
            results.append(ray.put(np.zeros(400 * MB, dtype=np.uint8)))
        check_spilled_mb(address, spilled=1600)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_fallback_when_spilling_impossible_on_get():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        # x1 will be spilled.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        check_spilled_mb(address, spilled=400)
        # x1 will be restored, x2 will be spilled.
        x1p = ray.get(x1)
        check_spilled_mb(address, spilled=800, restored=400)
        # x2 will be restored, triggering a fallback allocation.
        x2p = ray.get(x2)
        check_spilled_mb(address, spilled=800, restored=800, fallback=400)
        del x1p
        del x2p
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_spilling_when_possible_on_get():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        # x1 will be spilled.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        check_spilled_mb(address, spilled=400)
        # x1 will be restored, x2 will be spilled.
        ray.get(x1)
        check_spilled_mb(address, spilled=800, restored=400)
        # x2 will be restored, spilling x1.
        ray.get(x2)
        check_spilled_mb(address, spilled=800, restored=800)
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_task_unlimited():
    try:
        address = _init_ray()
        x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        refs = [x1]
        # x1 is spilled.
        x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
        x2p = ray.get(x2)
        sentinel = ray.put(np.zeros(100 * MB, dtype=np.uint8))
        check_spilled_mb(address, spilled=400)

        @ray.remote
        def consume(refs):
            # triggers fallback allocation, spilling of the sentinel
            ray.get(refs[0])
            check_spilled_mb(address, spilled=500, restored=400, fallback=400)
            # triggers fallback allocation.
            return ray.put(np.zeros(400 * MB, dtype=np.uint8))

        # round 1
        _ = ray.get(ray.get(consume.remote(refs)))
        check_spilled_mb(address, spilled=500, restored=400, fallback=400)

        del x2p
        del sentinel
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_task_unlimited_multiget_args():
    try:
        address = _init_ray()
        # Too many refs to fit into memory.
        refs = []
        for _ in range(10):
            refs.append(ray.put(np.zeros(200 * MB, dtype=np.uint8)))
        x2 = ray.put(np.zeros(600 * MB, dtype=np.uint8))
        x2p = ray.get(x2)
        check_spilled_mb(address, spilled=2000)

        @ray.remote
        def consume(refs):
            # Should work without thrashing.
            ray.get(refs)
            return os.getpid()

        ray.get([consume.remote(refs) for _ in range(1000)])
        check_spilled_mb(address, spilled=2000, restored=2000, fallback=2000)
        del x2p
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
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
    reason="Only Linux handles fallback allocation disk full error.",
)
def test_fallback_allocation_failure(shutdown_only):
    file_system_config = {
        "type": "filesystem",
        "params": {
            "directory_path": "/tmp",
        },
    }
    ray.init(
        object_store_memory=100e6,
        _temp_dir="/dev/shm",
        _system_config={
            "object_spilling_config": json.dumps(file_system_config),
            # set local fs capacity to 100% so it never errors with out of disk.
            "local_fs_capacity_threshold": 1,
        },
    )
    shm_size = shutil.disk_usage("/dev/shm").total
    object_size = max(100e6, shm_size // 5)
    num_exceptions = 0
    refs = []
    for i in range(8):
        print("Start put", i)
        try:
            refs.append(ray.get(ray.put(np.zeros(object_size, dtype=np.uint8))))
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


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_plasma_allocate(shutdown_only):
    address = ray.init(
        object_store_memory=300 * 1024**2,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
        },
        _temp_dir="/tmp/for_test_plasma_allocate",
    )
    res = []
    data = np.random.randint(low=0, high=256, size=(90 * 1024**2,), dtype=np.uint8)
    for _ in range(3):
        res.append(ray.put(data))
    # keep reference for second and third object, force evict first object
    _ = ray.get(res[1:])  # noqa
    # keep reference for fourth object, avoid released by plasma GC.
    __ = ray.put(data)  # noqa

    # Check fourth object allocate in memory.
    check_spilled_mb(address, spilled=[90, 180])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Need to fix up for Windows."
)
def test_object_store_memory_metrics_reported_correctly(shutdown_only):
    """
    Verify when fallback allocation is used, prometheus stats report the correct
    used object store memory. https://github.com/ray-project/ray/issues/24624
    """
    obj_store_memory = 700e6
    address = ray.init(
        num_cpus=2,
        object_store_memory=obj_store_memory,
        _system_config={"metrics_report_interval_ms": 1000},
    )
    metrics_export_port = address["metrics_export_port"]
    addr = address["node_ip_address"]
    prom_addr = f"{addr}:{metrics_export_port}"

    x1 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
    # x1 will be spilled.
    x2 = ray.put(np.zeros(400 * MB, dtype=np.uint8))
    check_spilled_mb(address, spilled=400)
    # x1 will be restored, x2 will be spilled.
    x1p = ray.get(x1)
    check_spilled_mb(address, spilled=800, restored=400)
    # x2 will be restored, triggering a fallback allocation.
    x2p = ray.get(x2)
    check_spilled_mb(address, spilled=800, restored=800, fallback=400)

    def verify_used_object_store_memory(expected_mb):
        components_dict, metric_names, metric_samples = fetch_prometheus([prom_addr])

        def in_mb(bytes):
            return int(bytes / 1024 / 1024)

        total_memory = in_mb(obj_store_memory)
        available_memory_sample = None
        used_memory_sample = None
        fallback_memory_sample = None

        for sample in metric_samples:
            if sample.name == "ray_object_store_available_memory":
                available_memory_sample = sample
            if sample.name == "ray_object_store_used_memory":
                used_memory_sample = sample
            if sample.name == "ray_object_store_fallback_memory":
                fallback_memory_sample = sample

        if not (
            available_memory_sample and used_memory_sample and fallback_memory_sample
        ):
            return False

        avail_memory = in_mb(available_memory_sample.value)
        used_memory = in_mb(used_memory_sample.value)
        fallback_memory = in_mb(fallback_memory_sample.value)

        assert avail_memory == total_memory - used_memory
        assert used_memory == 400  # 400MB
        assert fallback_memory == 400
        return True

    wait_for_condition(lambda: verify_used_object_store_memory(expected_mb=30))
    del x1p
    del x2p


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
