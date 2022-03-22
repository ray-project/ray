import os
import sys
import pytest
import tempfile

import ray
from ray._private.test_utils import SignalActor
from ray.util.multiprocessing import Pool, TimeoutError


def teardown_function(function):
    # Delete environment variable if set.
    if "RAY_ADDRESS" in os.environ:
        del os.environ["RAY_ADDRESS"]


@pytest.fixture
def pool():
    pool = Pool(processes=1)
    yield pool
    pool.terminate()
    pool.join()
    ray.shutdown()


@pytest.fixture
def pool_4_processes():
    pool = Pool(processes=4)
    yield pool
    pool.terminate()
    pool.join()
    ray.shutdown()


@pytest.fixture
def pool_4_processes_python_multiprocessing_lib():
    import multiprocessing as mp

    pool = mp.Pool(processes=4)
    yield pool
    pool.terminate()
    pool.join()


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=1)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_ray_init(shutdown_only):
    def getpid(args):
        return os.getpid()

    def check_pool_size(pool, size):
        args = [tuple() for _ in range(size)]
        assert len(set(pool.map(getpid, args))) == size

    # Check that starting a pool starts ray if not initialized.
    pool = Pool(processes=2)
    assert ray.is_initialized()
    assert int(ray.cluster_resources()["CPU"]) == 2
    check_pool_size(pool, 2)
    pool.terminate()
    pool.join()
    ray.shutdown()

    # Check that starting a pool doesn't affect ray if there is a local
    # ray cluster running.
    ray.init(num_cpus=3)
    assert ray.is_initialized()
    pool = Pool(processes=2)
    assert int(ray.cluster_resources()["CPU"]) == 3
    check_pool_size(pool, 2)
    pool.terminate()
    pool.join()
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    ray.init(num_cpus=1)
    assert ray.is_initialized()
    with pytest.raises(ValueError):
        Pool(processes=2)
    assert int(ray.cluster_resources()["CPU"]) == 1
    ray.shutdown()


@pytest.mark.parametrize(
    "ray_start_cluster_enabled",
    [
        {
            "num_cpus": 1,
            "num_nodes": 1,
            "do_init": False,
        }
    ],
    indirect=True,
)
def test_connect_to_ray(ray_start_cluster_enabled):
    def getpid(args):
        return os.getpid()

    def check_pool_size(pool, size):
        args = [tuple() for _ in range(size)]
        assert len(set(pool.map(getpid, args))) == size

    address = ray_start_cluster_enabled.address
    # Use different numbers of CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.
    start_cpus = 1  # Set in fixture.
    init_cpus = 2

    # Check that starting a pool still starts ray if RAY_ADDRESS not set.
    pool = Pool(processes=init_cpus)
    assert ray.is_initialized()
    assert int(ray.cluster_resources()["CPU"]) == init_cpus
    check_pool_size(pool, init_cpus)
    pool.terminate()
    pool.join()
    ray.shutdown()

    # Check that starting a pool connects to a running ray cluster if
    # ray_address is passed in.
    pool = Pool(ray_address=address)
    assert ray.is_initialized()
    assert int(ray.cluster_resources()["CPU"]) == start_cpus
    check_pool_size(pool, start_cpus)
    pool.terminate()
    pool.join()
    ray.shutdown()

    # Set RAY_ADDRESS, so pools should connect to the running ray cluster.
    os.environ["RAY_ADDRESS"] = address

    # Check that starting a pool connects to a running ray cluster if
    # RAY_ADDRESS is set.
    pool = Pool()
    assert ray.is_initialized()
    assert int(ray.cluster_resources()["CPU"]) == start_cpus
    check_pool_size(pool, start_cpus)
    pool.terminate()
    pool.join()
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    with pytest.raises(Exception):
        Pool(processes=start_cpus + 1)
    assert int(ray.cluster_resources()["CPU"]) == start_cpus
    ray.shutdown()


def test_initializer(shutdown_only):
    def init(dirname):
        with open(os.path.join(dirname, str(os.getpid())), "w") as f:
            print("hello", file=f)

    with tempfile.TemporaryDirectory() as dirname:
        num_processes = 4
        pool = Pool(processes=num_processes, initializer=init, initargs=(dirname,))

        assert len(os.listdir(dirname)) == 4
        pool.terminate()
        pool.join()


def test_close(pool_4_processes):
    def f(signal):
        ray.get(signal.wait.remote())
        return "hello"

    signal = SignalActor.remote()
    result = pool_4_processes.map_async(f, [signal for _ in range(4)])
    assert not result.ready()
    pool_4_processes.close()
    assert not result.ready()

    # Signal the head of line tasks to finish.
    ray.get(signal.send.remote())
    pool_4_processes.join()

    # close() shouldn't interrupt pending tasks, so check that they succeeded.
    result.wait(timeout=10)
    assert result.ready()
    assert result.successful()
    assert result.get() == ["hello"] * 4


def test_terminate(pool_4_processes):
    def f(signal):
        return ray.get(signal.wait.remote())

    signal = SignalActor.remote()
    result = pool_4_processes.map_async(f, [signal for _ in range(4)])
    assert not result.ready()
    pool_4_processes.terminate()

    # terminate() should interrupt pending tasks, so check that join() returns
    # even though the tasks should be blocked forever.
    pool_4_processes.join()
    result.wait(timeout=10)
    assert result.ready()
    assert not result.successful()
    with pytest.raises(ray.exceptions.RayError):
        result.get()


def test_apply(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply(f, (1, 2), {"kwarg2": 3}) == 1
    with pytest.raises(AssertionError):
        pool.apply(
            f,
            (
                2,
                2,
            ),
            {"kwarg2": 3},
        )
    with pytest.raises(Exception):
        pool.apply(f, (1,))
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
        pool.apply_async(
            f,
            (
                2,
                2,
            ),
            {"kwarg2": 3},
        ).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1,)).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, 2), {"kwarg1": 3}).get()

    # Won't return until the input ObjectRef is fulfilled.
    def ten_over(args):
        signal, val = args
        ray.get(signal.wait.remote())
        return 10 / val

    signal = SignalActor.remote()
    result = pool.apply_async(ten_over, ([signal, 10],))
    result.wait(timeout=0.01)
    assert not result.ready()
    with pytest.raises(TimeoutError):
        result.get(timeout=0.01)

    # Fulfill the ObjectRef.
    ray.get(signal.send.remote())
    result.wait(timeout=10)
    assert result.ready()
    assert result.successful()
    assert result.get() == 1

    signal = SignalActor.remote()
    result = pool.apply_async(ten_over, ([signal, 0],))
    with pytest.raises(ValueError, match="not ready"):
        result.successful()

    # Fulfill the ObjectRef with 0, causing the task to fail (divide by zero).
    ray.get(signal.send.remote())
    result.wait(timeout=10)
    assert result.ready()
    assert not result.successful()
    with pytest.raises(ZeroDivisionError):
        result.get()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
