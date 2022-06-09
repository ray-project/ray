import sys
import shutil
import time
import numpy as np

import platform
import pytest
import ray

from ray.cluster_utils import Cluster


def calculate_capacity_threshold(disk_capacity_in_bytes):
    usage = shutil.disk_usage("/tmp")
    threshold = min(1, 1.0 - 1.0 * (usage.free - disk_capacity_in_bytes) / usage.total)
    return threshold


def get_current_usage():
    usage = shutil.disk_usage("/tmp")
    print(f"free: {usage.free} ")
    print(f"current usage: {1.0 - 1.0 * usage.free  / usage.total}")
    return 1.0 - 1.0 * usage.free / usage.total


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_put_out_of_disk(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(100 * 1024 * 1024)
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": local_fs_capacity_threshold,
            "local_fs_monitor_interval_ms": 10,
        },
    )
    print(get_current_usage())
    assert get_current_usage() < local_fs_capacity_threshold
    ref = ray.put(np.random.rand(20 * 1024 * 1024))
    ray.wait([ref])
    print(get_current_usage())
    assert get_current_usage() > local_fs_capacity_threshold
    time.sleep(1)
    with pytest.raises(ray.exceptions.OutOfDiskError):
        ray.put(np.random.rand(20 * 1024 * 1024))
    del ref
    assert get_current_usage() < local_fs_capacity_threshold
    time.sleep(1)
    ray.put(np.random.rand(20 * 1024 * 1024))


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_task_returns(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(1 * 1024 * 1024)
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
    local_fs_capacity_threshold = calculate_capacity_threshold(1 * 1024 * 1024)
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
    cluster = Cluster()
    cluster.add_node(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
        resources={"out_of_memory": 1},
    )
    cluster.add_node(
        num_cpus=1,
        object_store_memory=200 * 1024 * 1024,
        resources={"sufficient_memory": 1},
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote
    def foo():
        return np.random.rand(20 * 1024 * 1024)  # 160 MB data

    @ray.remote
    def bar(obj):
        print(obj)

    ref = foo.options(resources={"sufficient_memory": 1}).remote()
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(bar.options(resources={"out_of_memory": 1}).remote(ref))


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_actor(shutdown_only):
    cluster = Cluster()
    cluster.add_node(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": 0,
        },
        resources={"out_of_memory": 1},
    )
    cluster.add_node(
        num_cpus=1,
        object_store_memory=200 * 1024 * 1024,
        resources={"sufficient_memory": 1},
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote
    def foo():
        return np.random.rand(20 * 1024 * 1024)  # 160 MB data

    @ray.remote
    class Actor:
        def __init__(self, obj):
            self._obj = obj

        def foo(self):
            print(self._obj)

        def args_ood(self, obj):
            print(obj)

        def return_ood(self):
            return np.random.rand(20 * 1024 * 1024)

    ref = foo.options(resources={"sufficient_memory": 1}).remote()
    with pytest.raises(ray.exceptions.RayActorError):
        a = Actor.options(resources={"out_of_memory": 0.001}).remote(ref)
        ray.get(a.foo.remote())

    a = Actor.options(resources={"out_of_memory": 1}).remote(1)
    ray.get(a.foo.remote())
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(a.args_ood.remote(ref))

    ray.get(a.foo.remote())
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(a.return_ood.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
