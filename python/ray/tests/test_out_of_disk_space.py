import sys
import numpy as np

import pytest
import ray


def test_put_out_of_disk(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
    )
    arr = np.random.rand(10 * 1024 * 1024)  # 80 MB data
    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        ray.put(arr)


def test_task_of_disk(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
    )

    @ray.remote
    def foo():
        return np.random.rand(10 * 1024 * 1024)  # 80 MB data

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())


def test_task_of_disk_1(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
    )

    @ray.remote
    def foo():
        ref = ray.put(np.random.rand(10 * 1024 * 1024))  # 80 MB data
        return ref

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(foo.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
