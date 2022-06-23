import os
import sys
import platform
import pytest
import tempfile
import time
import random
from collections import defaultdict
import queue
import math

import ray
from ray._private.test_utils import SignalActor
from ray.util.multiprocessing import Pool, TimeoutError, JoinableQueue

from ray.util.joblib import register_ray

from joblib import parallel_backend, Parallel, delayed
from ray._private.test_utils import test_external_redis


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


@pytest.fixture
def ray_start_4_cpu():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.skipif(
    test_external_redis(), reason="The same Redis is used within the test."
)
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
@pytest.mark.skipif(
    test_external_redis(), reason="The same Redis is used within the test."
)
def test_connect_to_ray(ray_start_cluster):
    def getpid(args):
        return os.getpid()

    def check_pool_size(pool, size):
        args = [tuple() for _ in range(size)]
        assert len(set(pool.map(getpid, args))) == size

    address = ray_start_cluster.address
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


def test_map(pool_4_processes):
    def f(index):
        return index, os.getpid()

    results = pool_4_processes.map(f, range(1000))
    assert len(results) == 1000

    pid_counts = defaultdict(int)
    for i, (index, pid) in enumerate(results):
        assert i == index
        pid_counts[pid] += 1

    # Check that the functions are spread somewhat evenly.
    for count in pid_counts.values():
        assert count > 100

    def bad_func(args):
        raise Exception("test_map failure")

    with pytest.raises(Exception, match="test_map failure"):
        pool_4_processes.map(bad_func, range(100))


def test_map_async(pool_4_processes):
    def f(args):
        index, signal = args
        ray.get(signal.wait.remote())
        return index, os.getpid()

    signal = SignalActor.remote()
    async_result = pool_4_processes.map_async(f, [(i, signal) for i in range(1000)])
    assert not async_result.ready()
    with pytest.raises(TimeoutError):
        async_result.get(timeout=0.01)
    async_result.wait(timeout=0.01)

    # Send the signal to finish the tasks.
    ray.get(signal.send.remote())
    async_result.wait(timeout=10)
    assert async_result.ready()
    assert async_result.successful()

    results = async_result.get()
    assert len(results) == 1000

    pid_counts = defaultdict(int)
    for i, (index, pid) in enumerate(results):
        assert i == index
        pid_counts[pid] += 1

    # Check that the functions are spread somewhat evenly.
    for count in pid_counts.values():
        assert count > 100

    def bad_func(index):
        if index == 50:
            raise Exception("test_map_async failure")

    async_result = pool_4_processes.map_async(bad_func, range(100))
    async_result.wait(10)
    assert async_result.ready()
    assert not async_result.successful()

    with pytest.raises(Exception, match="test_map_async failure"):
        async_result.get()


def test_starmap(pool):
    def f(*args):
        return args

    args = [tuple(range(i)) for i in range(100)]
    assert pool.starmap(f, args) == args
    assert pool.starmap(lambda x, y: x + y, zip([1, 2], [3, 4])) == [4, 6]


def test_callbacks(pool_4_processes, pool_4_processes_python_multiprocessing_lib):
    Queue = JoinableQueue if platform.system() == "Windows" else queue.Queue
    callback_queue = Queue()

    def callback(result):
        callback_queue.put(result)

    def error_callback(error):
        callback_queue.put(error)

    # Will not error, check that callback is called.
    result = pool_4_processes.apply_async(
        callback_test_helper, ((0, [1]),), callback=callback
    )
    assert callback_queue.get() == 0
    result.get()

    # Will error, check that error_callback is called.
    result = pool_4_processes.apply_async(
        callback_test_helper, ((0, [0]),), error_callback=error_callback
    )
    assert isinstance(callback_queue.get(), Exception)
    with pytest.raises(Exception, match="intentional failure"):
        result.get()

    # Ensure Ray's map_async behavior matches Multiprocessing's map_async
    process_pools = [pool_4_processes, pool_4_processes_python_multiprocessing_lib]

    for process_pool in process_pools:
        # Test error callbacks for map_async.
        test_callback_types = ["regular callback", "error callback"]

        for callback_type in test_callback_types:
            # Reinitialize queue to track number of callback calls made by
            # the current process_pool and callback_type in map_async
            callback_queue = Queue()

            indices, error_indices = list(range(100)), []
            if callback_type == "error callback":
                error_indices = [2, 50, 98]
            result = process_pool.map_async(
                callback_test_helper,
                [(index, error_indices) for index in indices],
                callback=callback,
                error_callback=error_callback,
            )
            callback_results = None
            result.wait()

            callback_results = callback_queue.get()
            callback_queue.task_done()

            # Ensure that callback or error_callback was called only once
            assert callback_queue.qsize() == 0

            if callback_type == "regular callback":
                assert result.successful()
            else:
                assert not result.successful()

            if callback_type == "regular callback":
                # Check that regular callback returned a list of all indices
                for index in callback_results:
                    assert index in indices
                    indices.remove(index)
                assert len(indices) == 0
            else:
                # Check that error callback returned a single exception
                assert isinstance(callback_results, Exception)


def callback_test_helper(args):
    """
    This is a helper function for the test_callbacks test. It must be placed
    outside the test because Python's Multiprocessing library uses Pickle to
    serialize functions, but Pickle cannot serialize local functions.
    """
    time.sleep(0.1 * random.random())
    index = args[0]
    err_indices = args[1]
    if index in err_indices:
        raise Exception("intentional failure")
    return index


@pytest.mark.parametrize("use_iter", [True, False])
def test_imap(pool_4_processes, use_iter):
    def f(args):
        time.sleep(0.1 * random.random())
        index = args[0]
        err_indices = args[1]
        if index in err_indices:
            raise Exception("intentional failure")
        return index

    error_indices = [2, 50, 98]
    if use_iter:
        imap_iterable = iter([(index, error_indices) for index in range(100)])
    else:
        imap_iterable = [(index, error_indices) for index in range(100)]
    result_iter = pool_4_processes.imap(f, imap_iterable, chunksize=11)
    for i in range(100):
        result = result_iter.next()
        if i in error_indices:
            assert isinstance(result, Exception)
        else:
            assert result == i

    with pytest.raises(StopIteration):
        result_iter.next()


@pytest.mark.parametrize("use_iter", [True, False])
def test_imap_unordered(pool_4_processes, use_iter):
    def f(args):
        time.sleep(0.1 * random.random())
        index = args[0]
        err_indices = args[1]
        if index in err_indices:
            raise Exception("intentional failure")
        return index

    error_indices = [2, 50, 98]
    in_order = []
    num_errors = 0
    if use_iter:
        imap_iterable = iter([(index, error_indices) for index in range(100)])
    else:
        imap_iterable = [(index, error_indices) for index in range(100)]
    result_iter = pool_4_processes.imap_unordered(f, imap_iterable, chunksize=11)
    for i in range(100):
        result = result_iter.next()
        if isinstance(result, Exception):
            in_order.append(True)
            num_errors += 1
        else:
            in_order.append(result == i)

    # Check that the results didn't come back all in order.
    # NOTE: this could be flaky if the calls happened to finish in order due
    # to the random sleeps, but it's very unlikely.
    assert not all(in_order)
    assert num_errors == len(error_indices)

    with pytest.raises(StopIteration):
        result_iter.next()


@pytest.mark.parametrize("use_iter", [True, False])
def test_imap_timeout(pool_4_processes, use_iter):
    def f(args):
        index, wait_index, signal = args
        time.sleep(0.1 * random.random())
        if index == wait_index:
            ray.get(signal.wait.remote())
        return index

    wait_index = 23
    signal = SignalActor.remote()
    if use_iter:
        imap_iterable = iter([(index, wait_index, signal) for index in range(100)])
    else:
        imap_iterable = [(index, wait_index, signal) for index in range(100)]
    result_iter = pool_4_processes.imap(f, imap_iterable)
    for i in range(100):
        if i == wait_index:
            with pytest.raises(TimeoutError):
                result = result_iter.next(timeout=0.1)
            ray.get(signal.send.remote())

        result = result_iter.next()
        assert result == i

    with pytest.raises(StopIteration):
        result_iter.next()

    wait_index = 23
    signal = SignalActor.remote()
    if use_iter:
        imap_iterable = iter([(index, wait_index, signal) for index in range(100)])
    else:
        imap_iterable = [(index, wait_index, signal) for index in range(100)]
    result_iter = pool_4_processes.imap_unordered(f, imap_iterable, chunksize=11)
    in_order = []
    for i in range(100):
        try:
            result = result_iter.next(timeout=1)
        except TimeoutError:
            ray.get(signal.send.remote())
            result = result_iter.next()

        in_order.append(result == i)

    # Check that the results didn't come back all in order.
    # NOTE: this could be flaky if the calls happened to finish in order due
    # to the random sleeps, but it's very unlikely.
    assert not all(in_order)

    with pytest.raises(StopIteration):
        result_iter.next()


def test_maxtasksperchild(shutdown_only):
    def f(args):
        return os.getpid()

    pool = Pool(5, maxtasksperchild=1)
    assert len(set(pool.map(f, range(20)))) == 20
    pool.terminate()
    pool.join()


def test_deadlock_avoidance_in_recursive_tasks(ray_start_1_cpu):
    def poolit_a(_):
        with Pool(ray_address="auto") as pool:
            return list(pool.map(math.sqrt, range(0, 2, 1)))

    def poolit_b():
        with Pool(ray_address="auto") as pool:
            return list(pool.map(poolit_a, range(2, 4, 1)))

    result = poolit_b()
    assert result == [[0.0, 1.0], [0.0, 1.0]]


def test_task_to_actor_assignment(ray_start_4_cpu):

    register_ray()

    pause_time = 5

    def worker_func(worker_id):
        launch_time = time.time()
        time.sleep(pause_time)
        return worker_id, launch_time

    num_workers = 4
    output = []
    with parallel_backend("ray", n_jobs=-1):
        output = Parallel()(
            delayed(worker_func)(worker_id) for worker_id in range(num_workers)
        )

    worker_ids = set()
    launch_times = []
    for worker_id, launch_time in output:
        worker_ids.add(worker_id)
        launch_times.append(launch_time)

    assert len(worker_ids) == num_workers

    for i in range(num_workers):
        for j in range(i + 1, num_workers):
            assert abs(launch_times[i] - launch_times[j]) < 1


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
