import json
import sys
import shutil
import numpy as np

import platform
import pytest
import ray


def calculate_capacity_threshold(disk_capacity_in_bytes):
    usage = shutil.disk_usage("/tmp")
    threshold = min(1, 1.0 * (usage.used + disk_capacity_in_bytes) / usage.total)
    assert threshold > 0 and threshold < 1
    return threshold


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_put_out_of_disk(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(10 * 1024 * 1024)
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": local_fs_capacity_threshold,
        },
    )
    arr = np.random.rand(20 * 1024 * 1024)  # 160 MB data
    with pytest.raises(ray.exceptions.OutOfDiskError):
        ray.put(arr)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_task_returns(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(10 * 1024 * 1024)
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": local_fs_capacity_threshold,
        },
    )

    @ray.remote
    def foo():
        return np.random.rand(20 * 1024 * 1024)  # 160 MB data

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_task_put(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(10 * 1024 * 1024)
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": local_fs_capacity_threshold,
        },
    )

    @ray.remote
    def foo():
        ref = ray.put(np.random.rand(20 * 1024 * 1024))  # 160 MB data
        return ref

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_task_args(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(10 * 1024 * 1024)
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": local_fs_capacity_threshold,
        },
    )

    @ray.remote
    def foo():
        ref = ray.put(np.random.rand(20 * 1024 * 1024))  # 80 MB data
        return ref

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
