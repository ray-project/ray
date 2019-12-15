from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import tempfile
import subprocess

import ray
from ray.experimental.multiprocessing import Pool, TimeoutError


@pytest.fixture
def pool():
    pool = Pool(processes=1)
    yield pool
    pool.terminate()
    ray.shutdown()


@pytest.fixture
def pool_4_processes():
    pool = Pool(processes=4)
    yield pool
    pool.terminate()
    ray.shutdown()


def test_initialize_ray(shutdown_only):
    def getpid(args):
        import os
        return os.getpid()

    def check_pool_size(pool, size):
        args = [tuple() for _ in range(size)]
        assert len(set(pool.map(getpid, args))) == size

    # Check that starting a pool starts ray if not initialized.
    pool = Pool(processes=2)
    assert ray.is_initialized()
    assert int(ray.state.cluster_resources()["CPU"]) == 2
    check_pool_size(pool, 2)
    ray.shutdown()

    # Check that starting a pool doesn't affect ray if there is a local
    # ray cluster running.
    ray.init(num_cpus=3)
    assert ray.is_initialized()
    pool = Pool(processes=2)
    assert int(ray.state.cluster_resources()["CPU"]) == 3
    check_pool_size(pool, 2)
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    ray.init(num_cpus=1)
    assert ray.is_initialized()
    with pytest.raises(ValueError):
        Pool(processes=2)
    assert int(ray.state.cluster_resources()["CPU"]) == 1
    ray.shutdown()

    # Use different numbers of CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.
    init_cpus = 2
    start_cpus = 3

    # Start a ray cluster in the background.
    subprocess.check_output(
        ["ray", "start", "--head", "--num-cpus={}".format(start_cpus)])

    # Check that starting a pool still starts ray if RAY_ADDRESS not set.
    pool = Pool()
    assert ray.is_initialized()
    assert int(ray.state.cluster_resources()["CPU"]) == init_cpus
    check_pool_size(pool, init_cpus)
    ray.shutdown()

    # Set RAY_ADDRESS, so pools should connect to the running ray cluster.
    os.environ["RAY_ADDRESS"] = "auto"

    # Check that starting a pool connects to a running ray cluster if
    # RAY_ADDRESS is set.
    pool = Pool()
    assert ray.is_initialized()
    assert int(ray.state.cluster_resources()["CPU"]) == start_cpus
    check_pool_size(pool, start_cpus)
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    with pytest.raises(Exception):
        Pool(processes=start_cpus + 1)
    assert int(ray.state.cluster_resources()["CPU"]) == start_cpus
    ray.shutdown()

    # Clean up the background ray cluster.
    subprocess.check_output(["ray", "stop"])


def test_initializer(shutdown_only):
    test_path = os.path.join(tempfile.gettempdir(), "initializer_test")

    def init(test_path, index):
        with open(test_path, "wb") as f:
            print(os.urandom(64), file=f)

    num_processes = 4
    pool = Pool(
        processes=num_processes, initializer=init, initargs=(test_path, ))

    def get(test_path):
        with open(test_path, "rb") as f:
            return f.readline()

    # Check that the initializer ran once on each process in the pool.
    results = set()
    for result in pool.map(get, []):
        results.add(result)

    assert len(results) == num_processes
    pool.terminate()


@pytest.mark.skip(reason="Modifying globals in initializer not working.")
def test_initializer_globals(shutdown_only):
    def init(arg1, arg2):
        global x
        x = arg1 + arg2

    pool = Pool(processes=4, initializer=init, initargs=(1, 2))

    def get(i):
        return x

    for result in pool.map(get, range(100)):
        assert result == 3

    pool.terminate()
    ray.shutdown()


def test_apply(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply(f, (1, 2), {"kwarg2": 3}) == 1
    with pytest.raises(AssertionError):
        pool.apply(f, (
            2,
            2,
        ), {"kwarg2": 3})
    with pytest.raises(Exception):
        pool.apply(f, (1, ))
    with pytest.raises(Exception):
        pool.apply(f, (1, 2), {"kwarg1": 3})


def test_apply_async(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply_async(f, (1, 2), {"kwarg2": 3}).get() == 1
    with pytest.raises(AssertionError):
        pool.apply_async(f, (
            2,
            2,
        ), {
            "kwarg2": 3
        }).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, )).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, 2), {"kwarg1": 3}).get()

    # Won't return until the input ObjectID is fulfilled.
    def ten_over(input):
        return 10 / ray.get(input[0])

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    result = pool.apply_async(ten_over, ([object_id], ))
    result.wait(timeout=0.01)
    assert not result.ready()
    with pytest.raises(TimeoutError):
        result.get(timeout=0.1)

    # Fulfill the ObjectID.
    ray.worker.global_worker.put_object(10, object_id=object_id)
    result.wait(timeout=10)
    assert result.ready()
    assert result.successful()
    assert result.get() == 1

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    result = pool.apply_async(ten_over, ([object_id], ))
    with pytest.raises(ValueError, match="not ready"):
        result.successful()

    # Fulfill the ObjectID with 0, causing the task to fail (divide by zero).
    ray.worker.global_worker.put_object(0, object_id=object_id)
    result.wait(timeout=10)
    assert result.ready()
    assert not result.successful()
    with pytest.raises(ZeroDivisionError):
        result.get()


def test_map(pool_4_processes):
    def f(args):
        index = args[0]
        return index, os.getpid()
