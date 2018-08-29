from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import pytest
import time

import ray
import ray.ray_constants as ray_constants


@pytest.fixture
def ray_start_regular():
    # Start the Ray processes.
    ray.init(num_cpus=10)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(params=[(1, 4), (4, 4)])
def ray_start_combination(request):
    num_local_schedulers = request.param[0]
    num_workers_per_scheduler = request.param[1]
    # Start the Ray processes.
    ray.worker._init(
        start_ray_local=True,
        num_workers=num_workers_per_scheduler,
        num_local_schedulers=num_local_schedulers,
        num_cpus=10)
    yield num_local_schedulers, num_workers_per_scheduler
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_submitting_tasks(ray_start_combination):
    @ray.remote
    def f(x):
        return x

    for _ in range(1):
        ray.get([f.remote(1) for _ in range(1000)])

    for _ in range(10):
        ray.get([f.remote(1) for _ in range(100)])

    for _ in range(100):
        ray.get([f.remote(1) for _ in range(10)])

    for _ in range(1000):
        ray.get([f.remote(1) for _ in range(1)])

    assert ray.services.all_processes_alive()


def test_dependencies(ray_start_combination):
    @ray.remote
    def f(x):
        return x

    x = 1
    for _ in range(1000):
        x = f.remote(x)
    ray.get(x)

    @ray.remote
    def g(*xs):
        return 1

    xs = [g.remote(1)]
    for _ in range(100):
        xs.append(g.remote(*xs))
        xs.append(g.remote(1))
    ray.get(xs)

    assert ray.services.all_processes_alive()


def test_submitting_many_tasks(ray_start_regular):
    @ray.remote
    def f(x):
        return 1

    def g(n):
        x = 1
        for i in range(n):
            x = f.remote(x)
        return x

    ray.get([g(1000) for _ in range(100)])
    assert ray.services.all_processes_alive()


def test_getting_and_putting(ray_start_regular):
    for n in range(8):
        x = np.zeros(10**n)

        for _ in range(100):
            ray.put(x)

        x_id = ray.put(x)
        for _ in range(1000):
            ray.get(x_id)

    assert ray.services.all_processes_alive()


def test_getting_many_objects(ray_start_regular):
    @ray.remote
    def f():
        return 1

    n = 10**4  # TODO(pcm): replace by 10 ** 5 once this is faster.
    lst = ray.get([f.remote() for _ in range(n)])
    assert lst == n * [1]

    assert ray.services.all_processes_alive()


def test_wait(ray_start_combination):
    num_local_schedulers, num_workers_per_scheduler = ray_start_combination
    num_workers = num_local_schedulers * num_workers_per_scheduler

    @ray.remote
    def f(x):
        return x

    x_ids = [f.remote(i) for i in range(100)]
    for i in range(len(x_ids)):
        ray.wait([x_ids[i]])
    for i in range(len(x_ids) - 1):
        ray.wait(x_ids[i:])

    @ray.remote
    def g(x):
        time.sleep(x)

    for i in range(1, 5):
        x_ids = [
            g.remote(np.random.uniform(0, i)) for _ in range(2 * num_workers)
        ]
        ray.wait(x_ids, num_returns=len(x_ids))

    assert ray.services.all_processes_alive()


@pytest.fixture(params=[1, 4])
def ray_start_reconstruction(request):
    num_local_schedulers = request.param

    # Start the Redis global state store.
    node_ip_address = "127.0.0.1"
    use_raylet = os.environ.get("RAY_USE_XRAY") == "1"
    redis_address, redis_shards = ray.services.start_redis(
        node_ip_address, use_raylet=use_raylet)
    redis_ip_address = ray.services.get_ip_address(redis_address)
    redis_port = ray.services.get_port(redis_address)
    time.sleep(0.1)

    # Start the Plasma store instances with a total of 1GB memory.
    plasma_store_memory = 10**9
    plasma_addresses = []
    objstore_memory = plasma_store_memory // num_local_schedulers
    for i in range(num_local_schedulers):
        store_stdout_file, store_stderr_file = ray.services.new_log_files(
            "plasma_store_{}".format(i), True)
        manager_stdout_file, manager_stderr_file = (ray.services.new_log_files(
            "plasma_manager_{}".format(i), True))
        plasma_addresses.append(
            ray.services.start_objstore(
                node_ip_address,
                redis_address,
                objstore_memory=objstore_memory,
                store_stdout_file=store_stdout_file,
                store_stderr_file=store_stderr_file,
                manager_stdout_file=manager_stdout_file,
                manager_stderr_file=manager_stderr_file))

    # Start the rest of the services in the Ray cluster.
    address_info = {
        "redis_address": redis_address,
        "redis_shards": redis_shards,
        "object_store_addresses": plasma_addresses
    }
    ray.worker._init(
        address_info=address_info,
        start_ray_local=True,
        num_workers=1,
        num_local_schedulers=num_local_schedulers,
        num_cpus=[1] * num_local_schedulers,
        redirect_output=True)

    yield (redis_ip_address, redis_port, plasma_store_memory,
           num_local_schedulers)

    # The code after the yield will run as teardown code.
    assert ray.services.all_processes_alive()

    # Determine the IDs of all local schedulers that had a task scheduled
    # or submitted.
    state = ray.experimental.state.GlobalState()
    state._initialize_global_state(redis_ip_address, redis_port)
    if os.environ.get("RAY_USE_NEW_GCS") == "on":
        tasks = state.task_table()
        local_scheduler_ids = {
            task["LocalSchedulerID"]
            for task in tasks.values()
        }

    # Make sure that all nodes in the cluster were used by checking that
    # the set of local scheduler IDs that had a task scheduled or submitted
    # is equal to the total number of local schedulers started. We add one
    # to the total number of local schedulers to account for
    # NIL_LOCAL_SCHEDULER_ID. This is the local scheduler ID associated
    # with the driver task, since it is not scheduled by a particular local
    # scheduler.
    if os.environ.get("RAY_USE_NEW_GCS") == "on":
        assert len(local_scheduler_ids) == num_local_schedulers + 1

    # Clean up the Ray cluster.
    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Failing with new GCS API on Linux.")
def test_simple(ray_start_reconstruction):
    _, _, plasma_store_memory, num_local_schedulers = ray_start_reconstruction
    # Define the size of one task's return argument so that the combined
    # sum of all objects' sizes is at least twice the plasma stores'
    # combined allotted memory.
    num_objects = 100
    size = int(plasma_store_memory * 1.5 / (num_objects * 8))

    # Define a remote task with no dependencies, which returns a numpy
    # array of the given size.
    @ray.remote
    def foo(i, size):
        array = np.zeros(size)
        array[0] = i
        return array

    # Launch num_objects instances of the remote task.
    args = []
    for i in range(num_objects):
        args.append(foo.remote(i, size))

    # Get each value to force each task to finish. After some number of
    # gets, old values should be evicted.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get each value again to force reconstruction.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get values sequentially, in chunks.
    num_chunks = 4 * num_local_schedulers
    chunk = num_objects // num_chunks
    for i in range(num_chunks):
        values = ray.get(args[i * chunk:(i + 1) * chunk])
        del values


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Failing with new GCS API on Linux.")
def test_recursive(ray_start_reconstruction):
    _, _, plasma_store_memory, num_local_schedulers = ray_start_reconstruction
    # Define the size of one task's return argument so that the combined
    # sum of all objects' sizes is at least twice the plasma stores'
    # combined allotted memory.
    num_objects = 100
    size = int(plasma_store_memory * 1.5 / (num_objects * 8))

    # Define a root task with no dependencies, which returns a numpy array
    # of the given size.
    @ray.remote
    def no_dependency_task(size):
        array = np.zeros(size)
        return array

    # Define a task with a single dependency, which returns its one
    # argument.
    @ray.remote
    def single_dependency(i, arg):
        arg = np.copy(arg)
        arg[0] = i
        return arg

    # Launch num_objects instances of the remote task, each dependent on
    # the one before it.
    arg = no_dependency_task.remote(size)
    args = []
    for i in range(num_objects):
        arg = single_dependency.remote(i, arg)
        args.append(arg)

    # Get each value to force each task to finish. After some number of
    # gets, old values should be evicted.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get each value again to force reconstruction.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get 10 values randomly.
    for _ in range(10):
        i = np.random.randint(num_objects)
        value = ray.get(args[i])
        assert value[0] == i
    # Get values sequentially, in chunks.
    num_chunks = 4 * num_local_schedulers
    chunk = num_objects // num_chunks
    for i in range(num_chunks):
        values = ray.get(args[i * chunk:(i + 1) * chunk])
        del values


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Failing with new GCS API on Linux.")
def test_multiple_recursive(ray_start_reconstruction):
    _, _, plasma_store_memory, _ = ray_start_reconstruction
    # Define the size of one task's return argument so that the combined
    # sum of all objects' sizes is at least twice the plasma stores'
    # combined allotted memory.
    num_objects = 100
    size = plasma_store_memory * 2 // (num_objects * 8)

    # Define a root task with no dependencies, which returns a numpy array
    # of the given size.
    @ray.remote
    def no_dependency_task(size):
        array = np.zeros(size)
        return array

    # Define a task with multiple dependencies, which returns its first
    # argument.
    @ray.remote
    def multiple_dependency(i, arg1, arg2, arg3):
        arg1 = np.copy(arg1)
        arg1[0] = i
        return arg1

    # Launch num_args instances of the root task. Then launch num_objects
    # instances of the multi-dependency remote task, each dependent on the
    # num_args tasks before it.
    num_args = 3
    args = []
    for i in range(num_args):
        arg = no_dependency_task.remote(size)
        args.append(arg)
    for i in range(num_objects):
        args.append(multiple_dependency.remote(i, *args[i:i + num_args]))

    # Get each value to force each task to finish. After some number of
    # gets, old values should be evicted.
    args = args[num_args:]
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get each value again to force reconstruction.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get 10 values randomly.
    for _ in range(10):
        i = np.random.randint(num_objects)
        value = ray.get(args[i])
        assert value[0] == i


def wait_for_errors(error_check):
    # Wait for errors from all the nondeterministic tasks.
    errors = []
    time_left = 100
    while time_left > 0:
        errors = ray.error_info()
        if error_check(errors):
            break
        time_left -= 1
        time.sleep(1)

        # Make sure that enough errors came through.
    assert error_check(errors)
    return errors


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") == "1",
    reason="This test does not work with xray yet.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Failing with new GCS API on Linux.")
def test_nondeterministic_task(ray_start_reconstruction):
    _, _, plasma_store_memory, num_local_schedulers = ray_start_reconstruction
    # Define the size of one task's return argument so that the combined
    # sum of all objects' sizes is at least twice the plasma stores'
    # combined allotted memory.
    num_objects = 1000
    size = plasma_store_memory * 2 // (num_objects * 8)

    # Define a nondeterministic remote task with no dependencies, which
    # returns a random numpy array of the given size. This task should
    # produce an error on the driver if it is ever reexecuted.
    @ray.remote
    def foo(i, size):
        array = np.random.rand(size)
        array[0] = i
        return array

    # Define a deterministic remote task with no dependencies, which
    # returns a numpy array of zeros of the given size.
    @ray.remote
    def bar(i, size):
        array = np.zeros(size)
        array[0] = i
        return array

    # Launch num_objects instances, half deterministic and half
    # nondeterministic.
    args = []
    for i in range(num_objects):
        if i % 2 == 0:
            args.append(foo.remote(i, size))
        else:
            args.append(bar.remote(i, size))

    # Get each value to force each task to finish. After some number of
    # gets, old values should be evicted.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i
    # Get each value again to force reconstruction.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i

    def error_check(errors):
        if num_local_schedulers == 1:
            # In a single-node setting, each object is evicted and
            # reconstructed exactly once, so exactly half the objects will
            # produce an error during reconstruction.
            min_errors = num_objects // 2
        else:
            # In a multinode setting, each object is evicted zero or one
            # times, so some of the nondeterministic tasks may not be
            # reexecuted.
            min_errors = 1
        return len(errors) >= min_errors

    errors = wait_for_errors(error_check)
    # Make sure all the errors have the correct type.
    assert all(error["type"] == ray_constants.HASH_MISMATCH_PUSH_ERROR
               for error in errors)


@pytest.fixture
def ray_start_driver_put_errors():
    plasma_store_memory = 10**9
    # Start the Ray processes.
    ray.init(num_cpus=1, object_store_memory=plasma_store_memory)
    yield plasma_store_memory
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Failing with new GCS API on Linux.")
def test_driver_put_errors(ray_start_driver_put_errors):
    plasma_store_memory = ray_start_driver_put_errors
    # Define the size of one task's return argument so that the combined
    # sum of all objects' sizes is at least twice the plasma stores'
    # combined allotted memory.
    num_objects = 100
    size = plasma_store_memory * 2 // (num_objects * 8)

    # Define a task with a single dependency, a numpy array, that returns
    # another array.
    @ray.remote
    def single_dependency(i, arg):
        arg = np.copy(arg)
        arg[0] = i
        return arg

    # Launch num_objects instances of the remote task, each dependent on
    # the one before it. The first instance of the task takes a numpy array
    # as an argument, which is put into the object store.
    args = []
    arg = single_dependency.remote(0, np.zeros(size))
    for i in range(num_objects):
        arg = single_dependency.remote(i, arg)
        args.append(arg)
    # Get each value to force each task to finish. After some number of
    # gets, old values should be evicted.
    for i in range(num_objects):
        value = ray.get(args[i])
        assert value[0] == i

    # Get each value starting from the beginning to force reconstruction.
    # Currently, since we're not able to reconstruct `ray.put` objects that
    # were evicted and whose originating tasks are still running, this
    # for-loop should hang on its first iteration and push an error to the
    # driver.
    ray.worker.global_worker.local_scheduler_client.reconstruct_objects(
        [args[0]], False)

    def error_check(errors):
        return len(errors) > 1

    errors = wait_for_errors(error_check)
    assert all(error["type"] == ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR
               for error in errors)


# NOTE(swang): This test tries to launch 1000 workers and breaks.
# TODO(rkn): This test needs to be updated to use pytest.
# class WorkerPoolTests(unittest.TestCase):
#
#   def tearDown(self):
#     ray.shutdown()
#
#   def testBlockingTasks(self):
#     @ray.remote
#     def f(i, j):
#       return (i, j)
#
#     @ray.remote
#     def g(i):
#       # Each instance of g submits and blocks on the result of another remote
#       # task.
#       object_ids = [f.remote(i, j) for j in range(10)]
#       return ray.get(object_ids)
#
#     ray.init(num_workers=1)
#     ray.get([g.remote(i) for i in range(1000)])
#     ray.shutdown()
