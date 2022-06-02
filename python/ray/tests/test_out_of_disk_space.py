import sys
import numpy as np

import pytest
import ray


def test_put_out_of_disk(shutdown_only):
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
    )
    arr = np.random.rand(20 * 1024 * 1024)  # 80 MB data
    with pytest.raises(ray.exceptions.RaySystemError):
        ray.put(arr)


def test_task_returns(shutdown_only):
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
    )

    @ray.remote
    def foo():
        return np.random.rand(20 * 1024 * 1024)  # 80 MB data

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())


def test_task_put(shutdown_only):
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
    )

    @ray.remote
    def foo():
        ref = ray.put(np.random.rand(20 * 1024 * 1024))  # 80 MB data
        return ref

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())

def test_task_args(shutdown_only):
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": 0,
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
