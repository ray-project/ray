# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
import glob
import json
import logging
import os
import random
import setproctitle
import shutil
import six
import sys
import socket
import subprocess
import tempfile
import threading
import time

import numpy as np
import pickle
import pytest

import ray
from ray import signature
import ray.ray_constants as ray_constants
import ray.cluster_utils
import ray.test_utils

from ray.test_utils import RayTestTimeoutException

logger = logging.getLogger(__name__)


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
        class Echo(object):
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
    class MultithreadedActor(object):
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


def test_free_objects_multi_node(ray_start_cluster):
    # This test will do following:
    # 1. Create 3 raylets that each hold an actor.
    # 2. Each actor creates an object which is the deletion target.
    # 3. Wait 0.1 second for the objects to be deleted.
    # 4. Check that the deletion targets have been deleted.
    # Caution: if remote functions are used instead of actor methods,
    # one raylet may create more than one worker to execute the
    # tasks, so the flushing operations may be executed in different
    # workers and the plasma client holding the deletion target
    # may not be flushed.
    cluster = ray_start_cluster
    config = json.dumps({"object_manager_repeated_push_delay_ms": 1000})
    for i in range(3):
        cluster.add_node(
            num_cpus=1,
            resources={"Custom{}".format(i): 1},
            _internal_config=config)
    ray.init(address=cluster.address)

    class RawActor(object):
        def get(self):
            return ray.worker.global_worker.node.unique_id

    ActorOnNode0 = ray.remote(resources={"Custom0": 1})(RawActor)
    ActorOnNode1 = ray.remote(resources={"Custom1": 1})(RawActor)
    ActorOnNode2 = ray.remote(resources={"Custom2": 1})(RawActor)

    def create(actors):
        a = actors[0].get.remote()
        b = actors[1].get.remote()
        c = actors[2].get.remote()
        (l1, l2) = ray.wait([a, b, c], num_returns=3)
        assert len(l1) == 3
        assert len(l2) == 0
        return (a, b, c)

    def run_one_test(actors, local_only, delete_creating_tasks):
        (a, b, c) = create(actors)
        # The three objects should be generated on different object stores.
        assert ray.get(a) != ray.get(b)
        assert ray.get(a) != ray.get(c)
        assert ray.get(c) != ray.get(b)
        ray.internal.free(
            [a, b, c],
            local_only=local_only,
            delete_creating_tasks=delete_creating_tasks)
        # Wait for the objects to be deleted.
        time.sleep(0.1)
        return (a, b, c)

    actors = [
        ActorOnNode0.remote(),
        ActorOnNode1.remote(),
        ActorOnNode2.remote()
    ]
    # Case 1: run this local_only=False. All 3 objects will be deleted.
    (a, b, c) = run_one_test(actors, False, False)
    (l1, l2) = ray.wait([a, b, c], timeout=0.01, num_returns=1)
    # All the objects are deleted.
    assert len(l1) == 0
    assert len(l2) == 3
    # Case 2: run this local_only=True. Only 1 object will be deleted.
    (a, b, c) = run_one_test(actors, True, False)
    (l1, l2) = ray.wait([a, b, c], timeout=0.01, num_returns=3)
    # One object is deleted and 2 objects are not.
    assert len(l1) == 2
    assert len(l2) == 1
    # The deleted object will have the same store with the driver.
    local_return = ray.worker.global_worker.node.unique_id
    for object_id in l1:
        assert ray.get(object_id) != local_return

    # Case3: These cases test the deleting creating tasks for the object.
    (a, b, c) = run_one_test(actors, False, False)
    task_table = ray.tasks()
    for obj in [a, b, c]:
        assert ray._raylet.compute_task_id(obj).hex() in task_table

    (a, b, c) = run_one_test(actors, False, True)
    task_table = ray.tasks()
    for obj in [a, b, c]:
        assert ray._raylet.compute_task_id(obj).hex() not in task_table


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
    class LocalModeTestClass(object):
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
    class RemoteActor1(object):
        def __init__(self):
            pass

        def function1(self):
            return 0

    @ray.remote
    class RemoteActor2(object):
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


def test_resource_constraints(shutdown_only):
    num_workers = 20
    ray.init(num_cpus=10, num_gpus=2)

    @ray.remote(num_cpus=0)
    def get_worker_id():
        time.sleep(0.1)
        return os.getpid()

    # Attempt to wait for all of the workers to start up.
    while True:
        if len(
                set(
                    ray.get([
                        get_worker_id.remote() for _ in range(num_workers)
                    ]))) == num_workers:
            break

    time_buffer = 2

    # At most 10 copies of this can run at once.
    @ray.remote(num_cpus=1)
    def f(n):
        time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(10)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(11)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    @ray.remote(num_cpus=3)
    def f(n):
        time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(3)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(4)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    @ray.remote(num_gpus=1)
    def f(n):
        time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(2)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(3)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(4)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1


def test_multi_resource_constraints(shutdown_only):
    num_workers = 20
    ray.init(num_cpus=10, num_gpus=10)

    @ray.remote(num_cpus=0)
    def get_worker_id():
        time.sleep(0.1)
        return os.getpid()

    # Attempt to wait for all of the workers to start up.
    while True:
        if len(
                set(
                    ray.get([
                        get_worker_id.remote() for _ in range(num_workers)
                    ]))) == num_workers:
            break

    @ray.remote(num_cpus=1, num_gpus=9)
    def f(n):
        time.sleep(n)

    @ray.remote(num_cpus=9, num_gpus=1)
    def g(n):
        time.sleep(n)

    time_buffer = 2

    start_time = time.time()
    ray.get([f.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5), f.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    start_time = time.time()
    ray.get([g.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    start_time = time.time()
    ray.get([f.remote(0.5), f.remote(0.5), g.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1


def test_gpu_ids(shutdown_only):
    num_gpus = 10
    ray.init(num_cpus=10, num_gpus=num_gpus)

    def get_gpu_ids(num_gpus_per_worker):
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == num_gpus_per_worker
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    f0 = ray.remote(num_gpus=0)(lambda: get_gpu_ids(0))
    f1 = ray.remote(num_gpus=1)(lambda: get_gpu_ids(1))
    f2 = ray.remote(num_gpus=2)(lambda: get_gpu_ids(2))
    f4 = ray.remote(num_gpus=4)(lambda: get_gpu_ids(4))
    f5 = ray.remote(num_gpus=5)(lambda: get_gpu_ids(5))

    # Wait for all workers to start up.
    @ray.remote
    def f():
        time.sleep(0.1)
        return os.getpid()

    start_time = time.time()
    while True:
        if len(set(ray.get([f.remote() for _ in range(10)]))) == 10:
            break
        if time.time() > start_time + 10:
            raise RayTestTimeoutException(
                "Timed out while waiting for workers to start "
                "up.")

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    assert list_of_ids == 10 * [[]]

    list_of_ids = ray.get([f1.remote() for _ in range(10)])
    set_of_ids = {tuple(gpu_ids) for gpu_ids in list_of_ids}
    assert set_of_ids == {(i, ) for i in range(10)}

    list_of_ids = ray.get([f2.remote(), f4.remote(), f4.remote()])
    all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
    assert set(all_ids) == set(range(10))

    # There are only 10 GPUs, and each task uses 5 GPUs, so there should only
    # be 2 tasks scheduled at a given time.
    t1 = time.time()
    ray.get([f5.remote() for _ in range(20)])
    assert time.time() - t1 >= 10 * 0.1

    # Test that actors have CUDA_VISIBLE_DEVICES set properly.

    @ray.remote
    class Actor0(object):
        def __init__(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 0
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            # Set self.x to make sure that we got here.
            self.x = 1

        def test(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 0
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            return self.x

    @ray.remote(num_gpus=1)
    class Actor1(object):
        def __init__(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            # Set self.x to make sure that we got here.
            self.x = 1

        def test(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            return self.x

    a0 = Actor0.remote()
    ray.get(a0.test.remote())

    a1 = Actor1.remote()
    ray.get(a1.test.remote())


def test_zero_cpus(shutdown_only):
    ray.init(num_cpus=0)

    # We should be able to execute a task that requires 0 CPU resources.
    @ray.remote(num_cpus=0)
    def f():
        return 1

    ray.get(f.remote())

    # We should be able to create an actor that requires 0 CPU resources.
    @ray.remote(num_cpus=0)
    class Actor(object):
        def method(self):
            pass

    a = Actor.remote()
    x = a.method.remote()
    ray.get(x)


def test_zero_cpus_actor(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    node_id = ray.worker.global_worker.node.unique_id

    @ray.remote
    class Foo(object):
        def method(self):
            return ray.worker.global_worker.node.unique_id

    # Make sure tasks and actors run on the remote raylet.
    a = Foo.remote()
    assert ray.get(a.method.remote()) != node_id


def test_fractional_resources(shutdown_only):
    ray.init(num_cpus=6, num_gpus=3, resources={"Custom": 1})

    @ray.remote(num_gpus=0.5)
    class Foo1(object):
        def method(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            return gpu_ids[0]

    foos = [Foo1.remote() for _ in range(6)]
    gpu_ids = ray.get([f.method.remote() for f in foos])
    for i in range(3):
        assert gpu_ids.count(i) == 2
    del foos

    @ray.remote
    class Foo2(object):
        def method(self):
            pass

    # Create an actor that requires 0.7 of the custom resource.
    f1 = Foo2._remote([], {}, resources={"Custom": 0.7})
    ray.get(f1.method.remote())
    # Make sure that we cannot create an actor that requires 0.7 of the
    # custom resource. TODO(rkn): Re-enable this once ray.wait is
    # implemented.
    f2 = Foo2._remote([], {}, resources={"Custom": 0.7})
    ready, _ = ray.wait([f2.method.remote()], timeout=0.5)
    assert len(ready) == 0
    # Make sure we can start an actor that requries only 0.3 of the custom
    # resource.
    f3 = Foo2._remote([], {}, resources={"Custom": 0.3})
    ray.get(f3.method.remote())

    del f1, f3

    # Make sure that we get exceptions if we submit tasks that require a
    # fractional number of resources greater than 1.

    @ray.remote(num_cpus=1.5)
    def test():
        pass

    with pytest.raises(ValueError):
        test.remote()

    with pytest.raises(ValueError):
        Foo2._remote([], {}, resources={"Custom": 1.5})


def test_multiple_raylets(ray_start_cluster):
    # This test will define a bunch of tasks that can only be assigned to
    # specific raylets, and we will check that they are assigned
    # to the correct raylets.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=11, num_gpus=0)
    cluster.add_node(num_cpus=5, num_gpus=5)
    cluster.add_node(num_cpus=10, num_gpus=1)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    # Define a bunch of remote functions that all return the socket name of
    # the plasma store. Since there is a one-to-one correspondence between
    # plasma stores and raylets (at least right now), this can be
    # used to identify which raylet the task was assigned to.

    # This must be run on the zeroth raylet.
    @ray.remote(num_cpus=11)
    def run_on_0():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the first raylet.
    @ray.remote(num_gpus=2)
    def run_on_1():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the second raylet.
    @ray.remote(num_cpus=6, num_gpus=1)
    def run_on_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This can be run anywhere.
    @ray.remote(num_cpus=0, num_gpus=0)
    def run_on_0_1_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the first or second raylet.
    @ray.remote(num_gpus=1)
    def run_on_1_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the zeroth or second raylet.
    @ray.remote(num_cpus=8)
    def run_on_0_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    def run_lots_of_tasks():
        names = []
        results = []
        for i in range(100):
            index = np.random.randint(6)
            if index == 0:
                names.append("run_on_0")
                results.append(run_on_0.remote())
            elif index == 1:
                names.append("run_on_1")
                results.append(run_on_1.remote())
            elif index == 2:
                names.append("run_on_2")
                results.append(run_on_2.remote())
            elif index == 3:
                names.append("run_on_0_1_2")
                results.append(run_on_0_1_2.remote())
            elif index == 4:
                names.append("run_on_1_2")
                results.append(run_on_1_2.remote())
            elif index == 5:
                names.append("run_on_0_2")
                results.append(run_on_0_2.remote())
        return names, results

    client_table = ray.nodes()
    store_names = []
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"].get("GPU", 0) == 0
    ]
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"].get("GPU", 0) == 5
    ]
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"].get("GPU", 0) == 1
    ]
    assert len(store_names) == 3

    def validate_names_and_results(names, results):
        for name, result in zip(names, ray.get(results)):
            if name == "run_on_0":
                assert result in [store_names[0]]
            elif name == "run_on_1":
                assert result in [store_names[1]]
            elif name == "run_on_2":
                assert result in [store_names[2]]
            elif name == "run_on_0_1_2":
                assert (result in [
                    store_names[0], store_names[1], store_names[2]
                ])
            elif name == "run_on_1_2":
                assert result in [store_names[1], store_names[2]]
            elif name == "run_on_0_2":
                assert result in [store_names[0], store_names[2]]
            else:
                raise Exception("This should be unreachable.")
            assert set(ray.get(results)) == set(store_names)

    names, results = run_lots_of_tasks()
    validate_names_and_results(names, results)

    # Make sure the same thing works when this is nested inside of a task.

    @ray.remote
    def run_nested1():
        names, results = run_lots_of_tasks()
        return names, results

    @ray.remote
    def run_nested2():
        names, results = ray.get(run_nested1.remote())
        return names, results

    names, results = ray.get(run_nested2.remote())
    validate_names_and_results(names, results)


def test_custom_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3, resources={"CustomResource": 0})
    cluster.add_node(num_cpus=3, resources={"CustomResource": 1})
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource": 1})
    def g():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource": 1})
    def h():
        ray.get([f.remote() for _ in range(5)])
        return ray.worker.global_worker.node.unique_id

    # The f tasks should be scheduled on both raylets.
    assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2

    node_id = ray.worker.global_worker.node.unique_id

    # The g tasks should be scheduled only on the second raylet.
    raylet_ids = set(ray.get([g.remote() for _ in range(50)]))
    assert len(raylet_ids) == 1
    assert list(raylet_ids)[0] != node_id

    # Make sure that resource bookkeeping works when a task that uses a
    # custom resources gets blocked.
    ray.get([h.remote() for _ in range(5)])


def test_node_id_resource(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3)
    cluster.add_node(num_cpus=3)
    ray.init(address=cluster.address)

    local_node = ray.state.current_node_id()

    # Note that these will have the same IP in the test cluster
    assert len(ray.state.node_ids()) == 2
    assert local_node in ray.state.node_ids()

    @ray.remote(resources={local_node: 1})
    def f():
        return ray.state.current_node_id()

    # Check the node id resource is automatically usable for scheduling.
    assert ray.get(f.remote()) == ray.state.current_node_id()


def test_two_custom_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 1,
            "CustomResource2": 2
        })
    cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 3,
            "CustomResource2": 4
        })
    ray.init(address=cluster.address)

    @ray.remote(resources={"CustomResource1": 1})
    def f():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource2": 1})
    def g():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource1": 1, "CustomResource2": 3})
    def h():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource1": 4})
    def j():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource3": 1})
    def k():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    # The f and g tasks should be scheduled on both raylets.
    assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2
    assert len(set(ray.get([g.remote() for _ in range(50)]))) == 2

    node_id = ray.worker.global_worker.node.unique_id

    # The h tasks should be scheduled only on the second raylet.
    raylet_ids = set(ray.get([h.remote() for _ in range(50)]))
    assert len(raylet_ids) == 1
    assert list(raylet_ids)[0] != node_id

    # Make sure that tasks with unsatisfied custom resource requirements do
    # not get scheduled.
    ready_ids, remaining_ids = ray.wait([j.remote(), k.remote()], timeout=0.5)
    assert ready_ids == []


def test_many_custom_resources(shutdown_only):
    num_custom_resources = 10000
    total_resources = {
        str(i): np.random.randint(1, 7)
        for i in range(num_custom_resources)
    }
    ray.init(num_cpus=5, resources=total_resources)

    def f():
        return 1

    remote_functions = []
    for _ in range(20):
        num_resources = np.random.randint(0, num_custom_resources + 1)
        permuted_resources = np.random.permutation(
            num_custom_resources)[:num_resources]
        random_resources = {
            str(i): total_resources[str(i)]
            for i in permuted_resources
        }
        remote_function = ray.remote(resources=random_resources)(f)
        remote_functions.append(remote_function)

    remote_functions.append(ray.remote(f))
    remote_functions.append(ray.remote(resources=total_resources)(f))

    results = []
    for remote_function in remote_functions:
        results.append(remote_function.remote())
        results.append(remote_function.remote())
        results.append(remote_function.remote())

    ray.get(results)


# TODO: 5 retry attempts may be too little for Travis and we may need to
# increase it if this test begins to be flaky on Travis.
def test_zero_capacity_deletion_semantics(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1, resources={"test_resource": 1})

    def test():
        resources = ray.available_resources()
        MAX_RETRY_ATTEMPTS = 5
        retry_count = 0

        del resources["memory"]
        del resources["object_store_memory"]
        for key in list(resources.keys()):
            if key.startswith("node:"):
                del resources[key]

        while resources and retry_count < MAX_RETRY_ATTEMPTS:
            time.sleep(0.1)
            resources = ray.available_resources()
            retry_count += 1

        if retry_count >= MAX_RETRY_ATTEMPTS:
            raise RuntimeError(
                "Resources were available even after five retries.", resources)

        return resources

    function = ray.remote(
        num_cpus=2, num_gpus=1, resources={"test_resource": 1})(test)
    cluster_resources = ray.get(function.remote())

    # All cluster resources should be utilized and
    # cluster_resources must be empty
    assert cluster_resources == {}


@pytest.fixture
def save_gpu_ids_shutdown_only():
    # Record the curent value of this environment variable so that we can
    # reset it after the test.
    original_gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    yield None

    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Reset the environment variable.
    if original_gpu_ids is not None:
        os.environ["CUDA_VISIBLE_DEVICES"] = original_gpu_ids
    else:
        del os.environ["CUDA_VISIBLE_DEVICES"]


def test_specific_gpus(save_gpu_ids_shutdown_only):
    allowed_gpu_ids = [4, 5, 6]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
        [str(i) for i in allowed_gpu_ids])
    ray.init(num_gpus=3)

    @ray.remote(num_gpus=1)
    def f():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        assert gpu_ids[0] in allowed_gpu_ids

    @ray.remote(num_gpus=2)
    def g():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert gpu_ids[0] in allowed_gpu_ids
        assert gpu_ids[1] in allowed_gpu_ids

    ray.get([f.remote() for _ in range(100)])
    ray.get([g.remote() for _ in range(100)])


def test_blocking_tasks(ray_start_regular):
    @ray.remote
    def f(i, j):
        return (i, j)

    @ray.remote
    def g(i):
        # Each instance of g submits and blocks on the result of another
        # remote task.
        object_ids = [f.remote(i, j) for j in range(2)]
        return ray.get(object_ids)

    @ray.remote
    def h(i):
        # Each instance of g submits and blocks on the result of another
        # remote task using ray.wait.
        object_ids = [f.remote(i, j) for j in range(2)]
        return ray.wait(object_ids, num_returns=len(object_ids))

    ray.get([h.remote(i) for i in range(4)])

    @ray.remote
    def _sleep(i):
        time.sleep(0.01)
        return (i)

    @ray.remote
    def sleep():
        # Each instance of sleep submits and blocks on the result of
        # another remote task, which takes some time to execute.
        ray.get([_sleep.remote(i) for i in range(10)])

    ray.get(sleep.remote())


def test_max_call_tasks(ray_start_regular):
    @ray.remote(max_calls=1)
    def f():
        return os.getpid()

    pid = ray.get(f.remote())
    ray.test_utils.wait_for_pid_to_exit(pid)

    @ray.remote(max_calls=2)
    def f():
        return os.getpid()

    pid1 = ray.get(f.remote())
    pid2 = ray.get(f.remote())
    assert pid1 == pid2
    ray.test_utils.wait_for_pid_to_exit(pid1)


def attempt_to_load_balance(remote_function,
                            args,
                            total_tasks,
                            num_nodes,
                            minimum_count,
                            num_attempts=100):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get(
            [remote_function.remote(*args) for _ in range(total_tasks)])
        names = set(locations)
        counts = [locations.count(name) for name in names]
        logger.info("Counts are {}.".format(counts))
        if (len(names) == num_nodes
                and all(count >= minimum_count for count in counts)):
            break
        attempts += 1
    assert attempts < num_attempts


def test_load_balancing(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets
    # in a roughly equal manner.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.node.unique_id

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


def test_load_balancing_with_dependencies(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        time.sleep(0.010)
        return ray.worker.global_worker.node.unique_id

    # This object will be local to one of the raylets. Make sure
    # this doesn't prevent tasks from being scheduled on other raylets.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 25)


def wait_for_num_tasks(num_tasks, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.tasks()) >= num_tasks:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.objects()) >= num_objects:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_global_state_api(shutdown_only):

    error_message = ("The ray global state API cannot be used "
                     "before ray.init has been called.")

    with pytest.raises(Exception, match=error_message):
        ray.objects()

    with pytest.raises(Exception, match=error_message):
        ray.tasks()

    with pytest.raises(Exception, match=error_message):
        ray.nodes()

    with pytest.raises(Exception, match=error_message):
        ray.jobs()

    ray.init(num_cpus=5, num_gpus=3, resources={"CustomResource": 1})

    assert ray.cluster_resources()["CPU"] == 5
    assert ray.cluster_resources()["GPU"] == 3
    assert ray.cluster_resources()["CustomResource"] == 1

    assert ray.objects() == {}

    job_id = ray.utils.compute_job_id_from_driver(
        ray.WorkerID(ray.worker.global_worker.worker_id))
    driver_task_id = ray.worker.global_worker.current_task_id.hex()

    # One task is put in the task table which corresponds to this driver.
    wait_for_num_tasks(1)
    task_table = ray.tasks()
    assert len(task_table) == 1
    assert driver_task_id == list(task_table.keys())[0]
    task_spec = task_table[driver_task_id]["TaskSpec"]
    nil_unique_id_hex = ray.UniqueID.nil().hex()
    nil_actor_id_hex = ray.ActorID.nil().hex()

    assert task_spec["TaskID"] == driver_task_id
    assert task_spec["ActorID"] == nil_actor_id_hex
    assert task_spec["Args"] == []
    assert task_spec["JobID"] == job_id.hex()
    assert task_spec["FunctionID"] == nil_unique_id_hex
    assert task_spec["ReturnObjectIDs"] == []

    client_table = ray.nodes()
    node_ip_address = ray.worker.global_worker.node_ip_address

    assert len(client_table) == 1
    assert client_table[0]["NodeManagerAddress"] == node_ip_address

    @ray.remote
    def f(*xs):
        return 1

    x_id = ray.put(1)
    result_id = f.remote(1, "hi", x_id)

    # Wait for one additional task to complete.
    wait_for_num_tasks(1 + 1)
    task_table = ray.tasks()
    assert len(task_table) == 1 + 1
    task_id_set = set(task_table.keys())
    task_id_set.remove(driver_task_id)
    task_id = list(task_id_set)[0]

    task_spec = task_table[task_id]["TaskSpec"]
    assert task_spec["ActorID"] == nil_actor_id_hex
    assert task_spec["Args"] == [
        signature.DUMMY_TYPE, 1, signature.DUMMY_TYPE, "hi",
        signature.DUMMY_TYPE, x_id
    ]
    assert task_spec["JobID"] == job_id.hex()
    assert task_spec["ReturnObjectIDs"] == [result_id]

    assert task_table[task_id] == ray.tasks(task_id)

    # Wait for two objects, one for the x_id and one for result_id.
    wait_for_num_objects(2)

    def wait_for_object_table():
        timeout = 10
        start_time = time.time()
        while time.time() - start_time < timeout:
            object_table = ray.objects()
            tables_ready = (object_table[x_id]["ManagerIDs"] is not None and
                            object_table[result_id]["ManagerIDs"] is not None)
            if tables_ready:
                return
            time.sleep(0.1)
        raise RayTestTimeoutException(
            "Timed out while waiting for object table to "
            "update.")

    object_table = ray.objects()
    assert len(object_table) == 2

    assert object_table[x_id] == ray.objects(x_id)
    object_table_entry = ray.objects(result_id)
    assert object_table[result_id] == object_table_entry

    job_table = ray.jobs()

    assert len(job_table) == 1
    assert job_table[0]["JobID"] == job_id.hex()
    assert job_table[0]["NodeManagerAddress"] == node_ip_address


# TODO(rkn): Pytest actually has tools for capturing stdout and stderr, so we
# should use those, but they seem to conflict with Ray's use of faulthandler.
class CaptureOutputAndError(object):
    """Capture stdout and stderr of some span.

    This can be used as follows.

        captured = {}
        with CaptureOutputAndError(captured):
            # Do stuff.
        # Access captured["out"] and captured["err"].
    """

    def __init__(self, captured_output_and_error):
        if sys.version_info >= (3, 0):
            import io
            self.output_buffer = io.StringIO()
            self.error_buffer = io.StringIO()
        else:
            import cStringIO
            self.output_buffer = cStringIO.StringIO()
            self.error_buffer = cStringIO.StringIO()
        self.captured_output_and_error = captured_output_and_error

    def __enter__(self):
        sys.stdout.flush()
        sys.stderr.flush()
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.output_buffer
        sys.stderr = self.error_buffer

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        self.captured_output_and_error["out"] = self.output_buffer.getvalue()
        self.captured_output_and_error["err"] = self.error_buffer.getvalue()


def test_logging_to_driver(shutdown_only):
    ray.init(num_cpus=1, log_to_driver=True)

    @ray.remote
    def f():
        # It's important to make sure that these print statements occur even
        # without calling sys.stdout.flush() and sys.stderr.flush().
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    output_lines = captured["out"]
    for i in range(200):
        assert str(i) in output_lines

    # TODO(rkn): Check that no additional logs appear beyond what we expect
    # and that there are no duplicate logs. Once we address the issue
    # described in https://github.com/ray-project/ray/pull/5462, we should
    # also check that nothing is logged to stderr.


def test_not_logging_to_driver(shutdown_only):
    ray.init(num_cpus=1, log_to_driver=False)

    @ray.remote
    def f():
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    output_lines = captured["out"]
    assert len(output_lines) == 0

    # TODO(rkn): Check that no additional logs appear beyond what we expect
    # and that there are no duplicate logs. Once we address the issue
    # described in https://github.com/ray-project/ray/pull/5462, we should
    # also check that nothing is logged to stderr.


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_workers(shutdown_only):
    num_workers = 3
    ray.init(num_cpus=num_workers)

    @ray.remote
    def f():
        return id(ray.worker.global_worker), os.getpid()

    # Wait until all of the workers have started.
    worker_ids = set()
    while len(worker_ids) != num_workers:
        worker_ids = set(ray.get([f.remote() for _ in range(10)]))


def test_specific_job_id():
    dummy_driver_id = ray.JobID.from_int(1)
    ray.init(num_cpus=1, job_id=dummy_driver_id)

    # in driver
    assert dummy_driver_id == ray._get_runtime_context().current_driver_id

    # in worker
    @ray.remote
    def f():
        return ray._get_runtime_context().current_driver_id

    assert dummy_driver_id == ray.get(f.remote())

    ray.shutdown()


def test_object_id_properties():
    id_bytes = b"00112233445566778899"
    object_id = ray.ObjectID(id_bytes)
    assert object_id.binary() == id_bytes
    object_id = ray.ObjectID.nil()
    assert object_id.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(b"0123456789")
    object_id = ray.ObjectID.from_random()
    assert not object_id.is_nil()
    assert object_id.binary() != id_bytes
    id_dumps = pickle.dumps(object_id)
    id_from_dumps = pickle.loads(id_dumps)
    assert id_from_dumps == object_id


@pytest.fixture
def shutdown_only_with_initialization_check():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    assert not ray.is_initialized()


def test_initialized(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0)
    assert ray.is_initialized()


def test_initialized_local_mode(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0, local_mode=True)
    assert ray.is_initialized()


def test_wait_reconstruction(shutdown_only):
    ray.init(num_cpus=1, object_store_memory=int(10**8))

    @ray.remote
    def f():
        return np.zeros(6 * 10**7, dtype=np.uint8)

    x_id = f.remote()
    ray.wait([x_id])
    ray.wait([f.remote()])
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ready_ids, _ = ray.wait([x_id])
    assert len(ready_ids) == 1


def test_ray_setproctitle(ray_start_2_cpus):
    @ray.remote
    class UniqueName(object):
        def __init__(self):
            assert setproctitle.getproctitle() == "ray::UniqueName.__init__()"

        def f(self):
            assert setproctitle.getproctitle() == "ray::UniqueName.f()"

    @ray.remote
    def unique_1():
        assert "unique_1" in setproctitle.getproctitle()

    actor = UniqueName.remote()
    ray.get(actor.f.remote())
    ray.get(unique_1.remote())


def test_duplicate_error_messages(shutdown_only):
    ray.init(num_cpus=0)

    driver_id = ray.WorkerID.nil()
    error_data = ray.gcs_utils.construct_error_message(driver_id, "test",
                                                       "message", 0)

    # Push the same message to the GCS twice (they are the same because we
    # do not include a timestamp).

    r = ray.worker.global_worker.redis_client

    r.execute_command("RAY.TABLE_APPEND",
                      ray.gcs_utils.TablePrefix.Value("ERROR_INFO"),
                      ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB"),
                      driver_id.binary(), error_data)

    # Before https://github.com/ray-project/ray/pull/3316 this would
    # give an error
    r.execute_command("RAY.TABLE_APPEND",
                      ray.gcs_utils.TablePrefix.Value("ERROR_INFO"),
                      ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB"),
                      driver_id.binary(), error_data)


@pytest.mark.skipif(
    os.getenv("TRAVIS") is None,
    reason="This test should only be run on Travis.")
def test_ray_stack(ray_start_2_cpus):
    def unique_name_1():
        time.sleep(1000)

    @ray.remote
    def unique_name_2():
        time.sleep(1000)

    @ray.remote
    def unique_name_3():
        unique_name_1()

    unique_name_2.remote()
    unique_name_3.remote()

    success = False
    start_time = time.time()
    while time.time() - start_time < 30:
        # Attempt to parse the "ray stack" call.
        output = ray.utils.decode(subprocess.check_output(["ray", "stack"]))
        if ("unique_name_1" in output and "unique_name_2" in output
                and "unique_name_3" in output):
            success = True
            break

    if not success:
        raise Exception("Failed to find necessary information with "
                        "'ray stack'")


def test_pandas_parquet_serialization():
    # Only test this if pandas is installed
    pytest.importorskip("pandas")

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    tempdir = tempfile.mkdtemp()
    filename = os.path.join(tempdir, "parquet-test")
    pd.DataFrame({"col1": [0, 1], "col2": [0, 1]}).to_parquet(filename)
    with open(os.path.join(tempdir, "parquet-compression"), "wb") as f:
        table = pa.Table.from_arrays([pa.array([1, 2, 3])], ["hello"])
        pq.write_table(table, f, compression="lz4")
    # Clean up
    shutil.rmtree(tempdir)


def test_socket_dir_not_existing(shutdown_only):
    random_name = ray.ObjectID.from_random().hex()
    temp_raylet_socket_dir = "/tmp/ray/tests/{}".format(random_name)
    temp_raylet_socket_name = os.path.join(temp_raylet_socket_dir,
                                           "raylet_socket")
    ray.init(num_cpus=1, raylet_socket_name=temp_raylet_socket_name)


def test_raylet_is_robust_to_random_messages(ray_start_regular):
    node_manager_address = None
    node_manager_port = None
    for client in ray.nodes():
        if "NodeManagerAddress" in client:
            node_manager_address = client["NodeManagerAddress"]
            node_manager_port = client["NodeManagerPort"]
    assert node_manager_address
    assert node_manager_port
    # Try to bring down the node manager:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((node_manager_address, node_manager_port))
    s.send(1000 * b"asdf")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


def test_non_ascii_comment(ray_start_regular):
    @ray.remote
    def f():
        # 日本語 Japanese comment
        return 1

    assert ray.get(f.remote()) == 1


def test_shutdown_disconnect_global_state():
    ray.init(num_cpus=0)
    ray.shutdown()

    with pytest.raises(Exception) as e:
        ray.objects()
    assert str(e.value).endswith("ray.init has been called.")


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True)
def test_put_pins_object(ray_start_object_store_memory):
    x_id = ray.put("HI")
    x_copy = ray.ObjectID(x_id.binary())
    assert ray.get(x_copy) == "HI"

    # x cannot be evicted since x_id pins it
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert ray.get(x_id) == "HI"
    assert ray.get(x_copy) == "HI"

    # now it can be evicted since x_id pins it but x_copy does not
    del x_id
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(x_copy)

    # weakref put
    y_id = ray.put("HI", weakref=True)
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(y_id)

    @ray.remote
    def check_no_buffer_ref(x):
        assert x[0].get_buffer_ref() is None

    z_id = ray.put("HI")
    assert z_id.get_buffer_ref() is not None
    ray.get(check_no_buffer_ref.remote([z_id]))


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True)
def test_redis_lru_with_set(ray_start_object_store_memory):
    x = np.zeros(8 * 10**7, dtype=np.uint8)
    x_id = ray.put(x, weakref=True)

    # Remove the object from the object table to simulate Redis LRU eviction.
    removed = False
    start_time = time.time()
    while time.time() < start_time + 10:
        if ray.state.state.redis_clients[0].delete(b"OBJECT" +
                                                   x_id.binary()) == 1:
            removed = True
            break
    assert removed

    # Now evict the object from the object store.
    ray.put(x)  # This should not crash.


def test_decorated_function(ray_start_regular):
    def function_invocation_decorator(f):
        def new_f(args, kwargs):
            # Reverse the arguments.
            return f(args[::-1], {"d": 5}), kwargs

        return new_f

    def f(a, b, c, d=None):
        return a, b, c, d

    f.__ray_invocation_decorator__ = function_invocation_decorator
    f = ray.remote(f)

    result_id, kwargs = f.remote(1, 2, 3, d=4)
    assert kwargs == {"d": 4}
    assert ray.get(result_id) == (3, 2, 1, 5)


def test_get_postprocess(ray_start_regular):
    def get_postprocessor(object_ids, values):
        return [value for value in values if value > 0]

    ray.worker.global_worker._post_get_hooks.append(get_postprocessor)

    assert ray.get(
        [ray.put(i) for i in [0, 1, 3, 5, -1, -3, 4]]) == [1, 3, 5, 4]


def test_export_after_shutdown(ray_start_regular):
    # This test checks that we can use actor and remote function definitions
    # across multiple Ray sessions.

    @ray.remote
    def f():
        pass

    @ray.remote
    class Actor(object):
        def method(self):
            pass

    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray and use the remote function and actor again.
    ray.init(num_cpus=1)
    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray again and make sure that these definitions can be exported from
    # workers.
    ray.init(num_cpus=2)

    @ray.remote
    def export_definitions_from_worker(remote_function, actor_class):
        ray.get(remote_function.remote())
        actor_handle = actor_class.remote()
        ray.get(actor_handle.method.remote())

    ray.get(export_definitions_from_worker.remote(f, Actor))


def test_invalid_unicode_in_worker_log(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    # Wait till first worker log file is created.
    while True:
        log_file_paths = glob.glob("{}/worker*.out".format(logs_dir))
        if len(log_file_paths) == 0:
            time.sleep(0.2)
        else:
            break

    with open(log_file_paths[0], "wb") as f:
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.flush()

    # Wait till the log monitor reads the file.
    time.sleep(1.0)

    # Make sure that nothing has died.
    assert ray.services.remaining_processes_alive()


@pytest.mark.skip(reason="This test is too expensive to run.")
def test_move_log_files_to_old(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    @ray.remote
    class Actor(object):
        def f(self):
            print("function f finished")

    # First create a temporary actor.
    actors = [
        Actor.remote() for i in range(ray_constants.LOG_MONITOR_MAX_OPEN_FILES)
    ]
    ray.get([a.f.remote() for a in actors])

    # Make sure no log files are in the "old" directory before the actors
    # are killed.
    assert len(glob.glob("{}/old/worker*.out".format(logs_dir))) == 0

    # Now kill the actors so the files get moved to logs/old/.
    [a.__ray_terminate__.remote() for a in actors]

    while True:
        log_file_paths = glob.glob("{}/old/worker*.out".format(logs_dir))
        if len(log_file_paths) > 0:
            with open(log_file_paths[0], "r") as f:
                assert "function f finished\n" in f.readlines()
            break

    # Make sure that nothing has died.
    assert ray.services.remaining_processes_alive()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
