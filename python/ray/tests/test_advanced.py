# coding: utf-8
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import random
import six
import sys
import threading
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.test_utils

from ray.test_utils import RayTestTimeoutException

logger = logging.getLogger(__name__)


# issue https://github.com/ray-project/ray/issues/7105
def test_internal_free(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Sampler:
        def sample(self):
            return [1, 2, 3, 4, 5]

        def sample_big(self):
            return np.zeros(1024 * 1024)

    sampler = Sampler.remote()

    # Free does not delete from in-memory store.
    obj_id = sampler.sample.remote()
    ray.get(obj_id)
    ray.internal.free(obj_id)
    assert ray.get(obj_id) == [1, 2, 3, 4, 5]

    # Free deletes big objects from plasma store.
    big_id = sampler.sample_big.remote()
    ray.get(big_id)
    ray.internal.free(big_id)
    time.sleep(1)  # wait for delete RPC to propagate
    with pytest.raises(Exception):
        ray.get(big_id)


def test_wait_iterables(ray_start_regular):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    objectids = (f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5))
    ready_ids, remaining_ids = ray.experimental.wait(objectids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3

    objectids = np.array(
        [f.remote(1.0),
         f.remote(0.5),
         f.remote(0.5),
         f.remote(0.5)])
    ready_ids, remaining_ids = ray.experimental.wait(objectids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    @ray.remote
    def g(l):
        # The argument l should be a list containing one object ID.
        ray.wait([l[0]])

    @ray.remote
    def h(l):
        # The argument l should be a list containing one object ID.
        ray.get(l[0])

    # Make sure that multiple wait requests involving the same object ID
    # all return.
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ID all
    # return.
    x = f.remote(1)
    ray.get([h.remote([x]), h.remote([x])])


def test_caching_functions_to_run(shutdown_only):
    # Test that we export functions to run on all workers before the driver
    # is connected.
    def f(worker_info):
        sys.path.append(1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def f(worker_info):
        sys.path.append(2)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def g(worker_info):
        sys.path.append(3)

    ray.worker.global_worker.run_function_on_all_workers(g)

    def f(worker_info):
        sys.path.append(4)

    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.init(num_cpus=1)

    @ray.remote
    def get_state():
        time.sleep(1)
        return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

    res1 = get_state.remote()
    res2 = get_state.remote()
    assert ray.get(res1) == (1, 2, 3, 4)
    assert ray.get(res2) == (1, 2, 3, 4)

    # Clean up the path on the workers.
    def f(worker_info):
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()

    ray.worker.global_worker.run_function_on_all_workers(f)


def test_running_function_on_all_workers(ray_start_regular):
    def f(worker_info):
        sys.path.append("fake_directory")

    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path1():
        return sys.path

    assert "fake_directory" == ray.get(get_path1.remote())[-1]

    def f(worker_info):
        sys.path.pop(-1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    # Create a second remote function to guarantee that when we call
    # get_path2.remote(), the second function to run will have been run on
    # the worker.
    @ray.remote
    def get_path2():
        return sys.path

    assert "fake_directory" not in ray.get(get_path2.remote())


def test_profiling_api(ray_start_2_cpus):
    @ray.remote
    def f():
        with ray.profile("custom_event", extra_data={"name": "custom name"}):
            pass

    ray.put(1)
    object_id = f.remote()
    ray.wait([object_id])
    ray.get(object_id)

    # Wait until all of the profiling information appears in the profile
    # table.
    timeout_seconds = 20
    start_time = time.time()
    while True:
        profile_data = ray.timeline()
        event_types = {event["cat"] for event in profile_data}
        expected_types = [
            "task",
            "task:deserialize_arguments",
            "task:execute",
            "task:store_outputs",
            "wait_for_function",
            "ray.get",
            "ray.put",
            "ray.wait",
            "submit_task",
            "fetch_and_run_function",
            "register_remote_function",
            "custom_event",  # This is the custom one from ray.profile.
        ]

        if all(expected_type in event_types
               for expected_type in expected_types):
            break

        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for information in "
                "profile table. Missing events: {}.".format(
                    set(expected_types) - set(event_types)))

        # The profiling information only flushes once every second.
        time.sleep(1.1)


def test_wait_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"RemoteResource": 1})
    def f():
        return

    # Make sure we have enough workers on the remote nodes to execute some
    # tasks.
    tasks = [f.remote() for _ in range(10)]
    start = time.time()
    ray.get(tasks)
    end = time.time()

    # Submit some more tasks that can only be executed on the remote nodes.
    tasks = [f.remote() for _ in range(10)]
    # Sleep for a bit to let the tasks finish.
    time.sleep((end - start) * 2)
    _, unready = ray.wait(tasks, num_returns=len(tasks), timeout=0)
    # All remote tasks should have finished.
    assert len(unready) == 0


@pytest.mark.skip(reason="TODO(ekl)")
def test_object_transfer_dump(ray_start_cluster):
    cluster = ray_start_cluster

    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10**9)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return

    # These objects will live on different nodes.
    object_ids = [
        f._remote(args=[1], resources={str(i): 1}) for i in range(num_nodes)
    ]

    # Broadcast each object from each machine to each other machine.
    for object_id in object_ids:
        ray.get([
            f._remote(args=[object_id], resources={str(i): 1})
            for i in range(num_nodes)
        ])

    # The profiling information only flushes once every second.
    time.sleep(1.1)

    transfer_dump = ray.object_transfer_timeline()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes**2
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_receive"
    }) == num_nodes
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_send"
    }) == num_nodes


def test_identical_function_names(ray_start_regular):
    # Define a bunch of remote functions and make sure that we don't
    # accidentally call an older version.

    num_calls = 200

    @ray.remote
    def f():
        return 1

    results1 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 2

    results2 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 3

    results3 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 4

    results4 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 5

    results5 = [f.remote() for _ in range(num_calls)]

    assert ray.get(results1) == num_calls * [1]
    assert ray.get(results2) == num_calls * [2]
    assert ray.get(results3) == num_calls * [3]
    assert ray.get(results4) == num_calls * [4]
    assert ray.get(results5) == num_calls * [5]

    @ray.remote
    def g():
        return 1

    @ray.remote  # noqa: F811
    def g():
        return 2

    @ray.remote  # noqa: F811
    def g():
        return 3

    @ray.remote  # noqa: F811
    def g():
        return 4

    @ray.remote  # noqa: F811
    def g():
        return 5

    result_values = ray.get([g.remote() for _ in range(num_calls)])
    assert result_values == num_calls * [5]


def test_illegal_api_calls(ray_start_regular):

    # Verify that we cannot call put on an ObjectID.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.put(x)
    # Verify that we cannot call get on a regular value.
    with pytest.raises(Exception):
        ray.get(3)


# TODO(hchen): This test currently doesn't work in Python 2. This is likely
# because plasma client isn't thread-safe. This needs to be fixed from the
# Arrow side. See #4107 for relevant discussions.
@pytest.mark.skipif(six.PY2, reason="Doesn't work in Python 2.")
def test_multithreading(ray_start_2_cpus):
    # This test requires at least 2 CPUs to finish since the worker does not
    # release resources when joining the threads.

    def run_test_in_multi_threads(test_case, num_threads=10, num_repeats=25):
        """A helper function that runs test cases in multiple threads."""

        def wrapper():
            for _ in range(num_repeats):
                test_case()
                time.sleep(random.randint(0, 10) / 1000.0)
            return "ok"

        executor = ThreadPoolExecutor(max_workers=num_threads)
        futures = [executor.submit(wrapper) for _ in range(num_threads)]
        for future in futures:
            assert future.result() == "ok"

    @ray.remote
    def echo(value, delay_ms=0):
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        return value

    def test_api_in_multi_threads():
        """Test using Ray api in multiple threads."""

        @ray.remote
        class Echo:
            def echo(self, value):
                return value

        # Test calling remote functions in multiple threads.
        def test_remote_call():
            value = random.randint(0, 1000000)
            result = ray.get(echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_remote_call)

        # Test multiple threads calling one actor.
        actor = Echo.remote()

        def test_call_actor():
            value = random.randint(0, 1000000)
            result = ray.get(actor.echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_call_actor)

        # Test put and get.
        def test_put_and_get():
            value = random.randint(0, 1000000)
            result = ray.get(ray.put(value))
            assert value == result

        run_test_in_multi_threads(test_put_and_get)

        # Test multiple threads waiting for objects.
        num_wait_objects = 10
        objects = [
            echo.remote(i, delay_ms=10) for i in range(num_wait_objects)
        ]

        def test_wait():
            ready, _ = ray.wait(
                objects,
                num_returns=len(objects),
                timeout=1000.0,
            )
            assert len(ready) == num_wait_objects
            assert ray.get(ready) == list(range(num_wait_objects))

        run_test_in_multi_threads(test_wait, num_repeats=1)

    # Run tests in a driver.
    test_api_in_multi_threads()

    # Run tests in a worker.
    @ray.remote
    def run_tests_in_worker():
        test_api_in_multi_threads()
        return "ok"

    assert ray.get(run_tests_in_worker.remote()) == "ok"

    # Test actor that runs background threads.
    @ray.remote
    class MultithreadedActor:
        def __init__(self):
            self.lock = threading.Lock()
            self.thread_results = []

        def background_thread(self, wait_objects):
            try:
                # Test wait
                ready, _ = ray.wait(
                    wait_objects,
                    num_returns=len(wait_objects),
                    timeout=1000.0,
                )
                assert len(ready) == len(wait_objects)
                for _ in range(20):
                    num = 10
                    # Test remote call
                    results = [echo.remote(i) for i in range(num)]
                    assert ray.get(results) == list(range(num))
                    # Test put and get
                    objects = [ray.put(i) for i in range(num)]
                    assert ray.get(objects) == list(range(num))
                    time.sleep(random.randint(0, 10) / 1000.0)
            except Exception as e:
                with self.lock:
                    self.thread_results.append(e)
            else:
                with self.lock:
                    self.thread_results.append("ok")

        def spawn(self):
            wait_objects = [echo.remote(i, delay_ms=10) for i in range(10)]
            self.threads = [
                threading.Thread(
                    target=self.background_thread, args=(wait_objects, ))
                for _ in range(20)
            ]
            [thread.start() for thread in self.threads]

        def join(self):
            [thread.join() for thread in self.threads]
            assert self.thread_results == ["ok"] * len(self.threads)
            return "ok"

    actor = MultithreadedActor.remote()
    actor.spawn.remote()
    ray.get(actor.join.remote()) == "ok"


def test_local_mode(shutdown_only):
    @ray.remote
    def local_mode_f():
        return np.array([0, 0])

    @ray.remote
    def local_mode_g(x):
        x[0] = 1
        return x

    ray.init(local_mode=True)

    @ray.remote
    def f():
        return np.ones([3, 4, 5])

    xref = f.remote()
    # Remote functions should return ObjectIDs.
    assert isinstance(xref, ray.ObjectID)
    assert np.alltrue(ray.get(xref) == np.ones([3, 4, 5]))
    y = np.random.normal(size=[11, 12])
    # Check that ray.get(ray.put) is the identity.
    assert np.alltrue(y == ray.get(ray.put(y)))

    # Make sure objects are immutable, this example is why we need to copy
    # arguments before passing them into remote functions in python mode
    aref = local_mode_f.remote()
    assert np.alltrue(ray.get(aref) == np.array([0, 0]))
    bref = local_mode_g.remote(ray.get(aref))
    # Make sure local_mode_g does not mutate aref.
    assert np.alltrue(ray.get(aref) == np.array([0, 0]))
    assert np.alltrue(ray.get(bref) == np.array([1, 0]))

    # wait should return the first num_returns values passed in as the
    # first list and the remaining values as the second list
    num_returns = 5
    object_ids = [ray.put(i) for i in range(20)]
    ready, remaining = ray.wait(
        object_ids, num_returns=num_returns, timeout=None)
    assert ready == object_ids[:num_returns]
    assert remaining == object_ids[num_returns:]

    # Check that ray.put() and ray.internal.free() work in local mode.

    v1 = np.ones(10)
    v2 = np.zeros(10)

    k1 = ray.put(v1)
    assert np.alltrue(v1 == ray.get(k1))
    k2 = ray.put(v2)
    assert np.alltrue(v2 == ray.get(k2))

    ray.internal.free([k1, k2])
    with pytest.raises(Exception):
        ray.get(k1)
    with pytest.raises(Exception):
        ray.get(k2)

    # Should fail silently.
    ray.internal.free([k1, k2])

    # Test actors in LOCAL_MODE.

    @ray.remote
    class LocalModeTestClass:
        def __init__(self, array):
            self.array = array

        def set_array(self, array):
            self.array = array

        def get_array(self):
            return self.array

        def modify_and_set_array(self, array):
            array[0] = -1
            self.array = array

        @ray.method(num_return_vals=3)
        def returns_multiple(self):
            return 1, 2, 3

    test_actor = LocalModeTestClass.remote(np.arange(10))
    obj = test_actor.get_array.remote()
    assert isinstance(obj, ray.ObjectID)
    assert np.alltrue(ray.get(obj) == np.arange(10))

    test_array = np.arange(10)
    # Remote actor functions should not mutate arguments
    test_actor.modify_and_set_array.remote(test_array)
    assert np.alltrue(test_array == np.arange(10))
    # Remote actor functions should keep state
    test_array[0] = -1
    assert np.alltrue(test_array == ray.get(test_actor.get_array.remote()))

    # Check that actor handles work in local mode.

    @ray.remote
    def use_actor_handle(handle):
        array = np.ones(10)
        handle.set_array.remote(array)
        assert np.alltrue(array == ray.get(handle.get_array.remote()))

    ray.get(use_actor_handle.remote(test_actor))

    # Check that exceptions are deferred until ray.get().

    exception_str = "test_advanced remote task exception"

    @ray.remote
    def throws():
        raise Exception(exception_str)

    obj = throws.remote()
    with pytest.raises(Exception, match=exception_str):
        ray.get(obj)

    # Check that multiple return values are handled properly.

    @ray.remote(num_return_vals=3)
    def returns_multiple():
        return 1, 2, 3

    obj1, obj2, obj3 = returns_multiple.remote()
    assert ray.get(obj1) == 1
    assert ray.get(obj2) == 2
    assert ray.get(obj3) == 3
    assert ray.get([obj1, obj2, obj3]) == [1, 2, 3]

    obj1, obj2, obj3 = test_actor.returns_multiple.remote()
    assert ray.get(obj1) == 1
    assert ray.get(obj2) == 2
    assert ray.get(obj3) == 3
    assert ray.get([obj1, obj2, obj3]) == [1, 2, 3]

    @ray.remote(num_return_vals=2)
    def returns_multiple_throws():
        raise Exception(exception_str)

    obj1, obj2 = returns_multiple_throws.remote()
    with pytest.raises(Exception, match=exception_str):
        ray.get(obj)
        ray.get(obj1)
    with pytest.raises(Exception, match=exception_str):
        ray.get(obj2)

    # Check that Actors are not overwritten by remote calls from different
    # classes.
    @ray.remote
    class RemoteActor1:
        def __init__(self):
            pass

        def function1(self):
            return 0

    @ray.remote
    class RemoteActor2:
        def __init__(self):
            pass

        def function2(self):
            return 1

    actor1 = RemoteActor1.remote()
    _ = RemoteActor2.remote()
    assert ray.get(actor1.function1.remote()) == 0

    # Test passing ObjectIDs.
    @ray.remote
    def direct_dep(input):
        return input

    @ray.remote
    def indirect_dep(input):
        return ray.get(direct_dep.remote(input[0]))

    assert ray.get(indirect_dep.remote(["hello"])) == "hello"


def test_wait_makes_object_local(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    @ray.remote
    class Foo:
        def method(self):
            return np.zeros(1024 * 1024)

    a = Foo.remote()

    # Test get makes the object local.
    x_id = a.method.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ray.get(x_id)
    assert ray.worker.global_worker.core_worker.object_exists(x_id)

    # Test wait makes the object local.
    x_id = a.method.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ok, _ = ray.wait([x_id])
    assert len(ok) == 1
    assert ray.worker.global_worker.core_worker.object_exists(x_id)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
