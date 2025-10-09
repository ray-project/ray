"""Tests for ray.util.multiprocessing that require a standalone Ray cluster per test.

Tests that can run on a shared Ray cluster fixture should go in test_multiprocessing.py
"""
import math
import multiprocessing as mp
import os
import sys

import pytest

import ray
from ray._private.test_utils import external_redis_test_enabled
from ray.util.multiprocessing import Pool


@pytest.fixture(scope="module")
def ray_init_4_cpu_shared():
    yield ray.init(num_cpus=4)


@pytest.fixture
def pool_4_processes(ray_init_4_cpu_shared):
    pool = Pool(processes=4)
    yield pool
    pool.terminate()
    pool.join()


@pytest.fixture
def pool_4_processes_python_multiprocessing_lib():
    pool = mp.Pool(processes=4)
    yield pool
    pool.terminate()
    pool.join()


@pytest.mark.skipif(
    external_redis_test_enabled(),
    reason="Starts multiple Ray instances in parallel with the same namespace.",
)
def test_ray_init(shutdown_only):
    def getpid(i: int):
        return os.getpid()

    def check_pool_size(pool, size: int):
        assert len(set(pool.map(getpid, range(size)))) == size

    # Check that starting a pool starts ray if not initialized.
    assert not ray.is_initialized()
    with Pool(processes=4) as pool:
        assert ray.is_initialized()
        check_pool_size(pool, 4)
        assert int(ray.cluster_resources()["CPU"]) == 4
    pool.join()

    # Check that starting a pool doesn't affect ray if there is a local
    # ray cluster running.
    assert ray.is_initialized()
    assert int(ray.cluster_resources()["CPU"]) == 4
    with Pool(processes=2) as pool:
        assert ray.is_initialized()
        check_pool_size(pool, 2)
        assert int(ray.cluster_resources()["CPU"]) == 4
    pool.join()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    assert ray.is_initialized()
    assert int(ray.cluster_resources()["CPU"]) == 4
    with pytest.raises(ValueError):
        Pool(processes=8)
        assert int(ray.cluster_resources()["CPU"]) == 4


@pytest.mark.skipif(
    external_redis_test_enabled(),
    reason="Starts multiple Ray instances in parallel with the same namespace.",
)
@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 1,
            "num_nodes": 1,
            "do_init": False,
        }
    ],
    indirect=True,
)
def test_connect_to_ray(monkeypatch, ray_start_cluster):
    def getpid(args):
        return os.getpid()

    def check_pool_size(pool, size):
        args = [tuple() for _ in range(size)]
        assert len(set(pool.map(getpid, args))) == size

    # Use different numbers of CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.
    ray.init(address=ray_start_cluster.address)
    existing_cluster_cpus = int(ray.cluster_resources()["CPU"])
    local_cluster_cpus = existing_cluster_cpus + 1
    ray.shutdown()

    # Check that starting a pool connects to the running ray cluster by default.
    assert not ray.is_initialized()
    with Pool() as pool:
        assert ray.is_initialized()
        check_pool_size(pool, existing_cluster_cpus)
        assert int(ray.cluster_resources()["CPU"]) == existing_cluster_cpus
    pool.join()
    ray.shutdown()

    # Check that starting a pool connects to a running ray cluster if
    # ray_address is set to the cluster address.
    assert not ray.is_initialized()
    with Pool(ray_address=ray_start_cluster.address) as pool:
        check_pool_size(pool, existing_cluster_cpus)
        assert int(ray.cluster_resources()["CPU"]) == existing_cluster_cpus
    pool.join()
    ray.shutdown()

    # Check that starting a pool connects to a running ray cluster if
    # RAY_ADDRESS is set to the cluster address.
    assert not ray.is_initialized()
    monkeypatch.setenv("RAY_ADDRESS", ray_start_cluster.address)
    with Pool() as pool:
        check_pool_size(pool, existing_cluster_cpus)
        assert int(ray.cluster_resources()["CPU"]) == existing_cluster_cpus
    pool.join()
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    assert not ray.is_initialized()
    with pytest.raises(Exception):
        Pool(processes=existing_cluster_cpus + 1)
    assert int(ray.cluster_resources()["CPU"]) == existing_cluster_cpus
    ray.shutdown()

    # Check that starting a pool starts a local ray cluster if ray_address="local".
    assert not ray.is_initialized()
    with Pool(processes=local_cluster_cpus, ray_address="local") as pool:
        check_pool_size(pool, local_cluster_cpus)
        assert int(ray.cluster_resources()["CPU"]) == local_cluster_cpus
    pool.join()
    ray.shutdown()

    # Check that starting a pool starts a local ray cluster if RAY_ADDRESS="local".
    assert not ray.is_initialized()
    monkeypatch.setenv("RAY_ADDRESS", "local")
    with Pool(processes=local_cluster_cpus) as pool:
        check_pool_size(pool, local_cluster_cpus)
        assert int(ray.cluster_resources()["CPU"]) == local_cluster_cpus
    pool.join()
    ray.shutdown()


def test_maxtasksperchild(shutdown_only):
    with Pool(processes=5, maxtasksperchild=1) as pool:
        assert len(set(pool.map(lambda _: os.getpid(), range(20)))) == 20
    pool.join()


def test_deadlock_avoidance_in_recursive_tasks(shutdown_only):
    ray.init(num_cpus=1)

    def poolit_a(_):
        with Pool() as pool:
            return list(pool.map(math.sqrt, range(0, 2, 1)))

    def poolit_b():
        with Pool() as pool:
            return list(pool.map(poolit_a, range(2, 4, 1)))

    result = poolit_b()
    assert result == [[0.0, 1.0], [0.0, 1.0]]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
