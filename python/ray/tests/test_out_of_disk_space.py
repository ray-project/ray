import sys
import shutil
import numpy as np

import platform
import pytest
import ray

from ray.cluster_utils import Cluster


def calculate_capacity_threshold(disk_capacity_in_bytes):
    usage = shutil.disk_usage("/tmp")
    threshold = min(1, 1.0 * (usage.used + disk_capacity_in_bytes) / usage.total)
    assert threshold > 0 and threshold < 1
    return threshold


@pytest.mark.skipif(platform.system() == "Windows", reason="Not targeting Windows")
def test_put_fits_in_memory(shutdown_only):
    local_fs_capacity_threshold = calculate_capacity_threshold(10 * 1024 * 1024)
    ray.init(
        num_cpus=1,
        object_store_memory=80 * 1024 * 1024,
        _system_config={
            "local_fs_capacity_threshold": local_fs_capacity_threshold,
        },
    )
    arr = np.random.rand(9 * 1024 * 1024)  # 160 MB data
    ray.put(arr)


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
