# coding: utf-8
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import random
import sys
import threading
import time

import os
import numpy as np
import pytest

import ray.cluster_utils

import ray._private.profiling as profiling
from ray._private.test_utils import (
    client_test_enabled,
    RayTestTimeoutException,
    SignalActor,
)
from ray.exceptions import ReferenceCountingAssertionError

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


# issue https://github.com/ray-project/ray/issues/7105
@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_internal_free(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Sampler:
        def sample(self):
            return [1, 2, 3, 4, 5]

        def sample_big(self):
            return np.zeros(1024 * 1024)

    sampler = Sampler.remote()

    # Free deletes from in-memory store.
    obj_ref = sampler.sample.remote()
    ray.get(obj_ref)
    ray.internal.free(obj_ref)
    with pytest.raises(ReferenceCountingAssertionError):
        ray.get(obj_ref)

    # Free deletes big objects from plasma store.
    big_id = sampler.sample_big.remote()
    ray.get(big_id)
    ray.internal.free(big_id)
    time.sleep(1)  # wait for delete RPC to propagate
    with pytest.raises(ReferenceCountingAssertionError):
        ray.get(big_id)


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    @ray.remote
    def g(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.wait([input_list[0]])

    @ray.remote
    def h(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.get(input_list[0])

    # Make sure that multiple wait requests involving the same object ref
    # all return.
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ref all
    # return.
    x = f.remote(1)
    ray.get([h.remote([x]), h.remote([x])])


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
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


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_running_function_on_all_workers(ray_start_regular):
    def f(worker_info):
        sys.path.append("fake_directory")

    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path1():
        return sys.path

    assert "fake_directory" == ray.get(get_path1.remote())[-1]

    # the function should only run on the current driver once.
    assert sys.path[-1] == "fake_directory"
    if len(sys.path) > 1:
        assert sys.path[-2] != "fake_directory"

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


@pytest.mark.skipif(
    "RAY_PROFILING" not in os.environ, reason="Only tested in client/profiling build."
)
@pytest.mark.skipif(
    client_test_enabled(),
    reason=(
        "wait_for_function will miss in this mode. To be fixed after using"
        " gcs to bootstrap all component."
    ),
)
def test_profiling_api(ray_start_2_cpus):
    @ray.remote
    def f(delay):
        with profiling.profile("custom_event", extra_data={"name": "custom name"}):
            time.sleep(delay)
            pass

    @ray.remote
    def g(input_list):
        # The argument input_list should be a list containing one object ref.
        ray.wait([input_list[0]])

    ray.put(1)
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

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
            # TODO (Alex) :https://github.com/ray-project/ray/pull/9346
            # "register_remote_function",
            "custom_event",  # This is the custom one from ray.profile.
        ]

        if all(expected_type in event_types for expected_type in expected_types):
            break

        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for information in "
                "profile table. Missing events: {}.".format(
                    set(expected_types) - set(event_types)
                )
            )

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
        cluster.add_node(resources={str(i): 1}, object_store_memory=10 ** 9)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return

    # These objects will live on different nodes.
    object_refs = [f._remote(args=[1], resources={str(i): 1}) for i in range(num_nodes)]

    # Broadcast each object from each machine to each other machine.
    for object_ref in object_refs:
        ray.get(
            [
                f._remote(args=[object_ref], resources={str(i): 1})
                for i in range(num_nodes)
            ]
        )

    # The profiling information only flushes once every second.
    time.sleep(1.1)

    transfer_dump = ray.state.object_transfer_timeline()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes ** 2
    assert (
        len(
            {
                event["pid"]
                for event in transfer_dump
                if event["name"] == "transfer_receive"
            }
        )
        == num_nodes
    )
    assert (
        len(
            {
                event["pid"]
                for event in transfer_dump
                if event["name"] == "transfer_send"
            }
        )
        == num_nodes
    )


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
    def g():  # noqa: F811
        return 2

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 3

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 4

    @ray.remote  # noqa: F811
    def g():  # noqa: F811
        return 5

    result_values = ray.get([g.remote() for _ in range(num_calls)])
    assert result_values == num_calls * [5]


def test_illegal_api_calls(ray_start_regular):

    # Verify that we cannot call put on an ObjectRef.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.put(x)
    # Verify that we cannot call get on a regular value.
    with pytest.raises(Exception):
        ray.get(3)


@pytest.mark.skipif(
    client_test_enabled(), reason="grpc interaction with releasing resources"
)
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
        objects = [echo.remote(i, delay_ms=10) for i in range(num_wait_objects)]

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
                threading.Thread(target=self.background_thread, args=(wait_objects,))
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


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
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


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_future_resolution_skip_plasma(ray_start_cluster):
    cluster = ray_start_cluster
    # Disable worker caching so worker leases are not reused; set object
    # inlining size threshold so the borrowed ref is inlined.
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 100 * 1024,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin_head": 1})
    def f(x):
        return x + 1

    @ray.remote(resources={"pin_worker": 1})
    def g(x):
        borrowed_ref = x[0]
        f_ref = f.remote(borrowed_ref)
        f_result = ray.get(f_ref)
        # borrowed_ref should be inlined on future resolution and shouldn't be
        # in Plasma.
        assert ray.worker.global_worker.core_worker.object_exists(
            borrowed_ref, memory_store_only=True
        )
        return f_result * 2

    one = f.remote(0)
    g_ref = g.remote([one])
    assert ray.get(g_ref) == 4


def test_task_output_inline_bytes_limit(ray_start_cluster):
    cluster = ray_start_cluster
    # Disable worker caching so worker leases are not reused; set object
    # inlining size threshold and enable storing of small objects in in-memory
    # object store so the borrowed ref is inlined.
    # set task_rpc_inlined_bytes_limit which only allows inline 20 bytes.
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 100 * 1024,
            "task_rpc_inlined_bytes_limit": 20,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(num_returns=5, resources={"pin_head": 1})
    def f():
        return list(range(5))

    @ray.remote(resources={"pin_worker": 1})
    def sum():
        numbers = f.remote()
        result = 0
        for i, ref in enumerate(numbers):
            result += ray.get(ref)
            inlined = ray.worker.global_worker.core_worker.object_exists(
                ref, memory_store_only=True
            )
            if i < 2:
                assert inlined
            else:
                assert not inlined
        return result

    assert ray.get(sum.remote()) == 10


def test_task_arguments_inline_bytes_limit(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "max_direct_call_object_size": 100 * 1024,
            # if task_rpc_inlined_bytes_limit is greater than
            # max_grpc_message_size, this test fails.
            "task_rpc_inlined_bytes_limit": 18 * 1024,
            "max_grpc_message_size": 20 * 1024,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin_worker": 1})
    def foo(ref1, ref2, ref3):
        return ref1 == ref2 + ref3

    @ray.remote(resources={"pin_head": 1})
    def bar():
        # if the refs are inlined, the test fails.
        # refs = [ray.put(np.random.rand(1024) for _ in range(3))]
        # return ray.get(
        #     foo.remote(refs[0], refs[1], refs[2]))

        return ray.get(
            foo.remote(
                np.random.rand(1024),  # 8k
                np.random.rand(1024),  # 8k
                np.random.rand(1024),
            )
        )  # 8k

    ray.get(bar.remote())


# This case tests whether gcs-based actor scheduler works properly with
# a normal task co-existed.
def test_schedule_actor_and_normal_task(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        memory=1024 ** 3, _system_config={"gcs_actor_scheduling_enabled": True}
    )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=600 * 1024 ** 2, num_cpus=0.01)
    class Foo:
        def method(self):
            return 2

    @ray.remote(memory=600 * 1024 ** 2, num_cpus=0.01)
    def fun(singal1, signal_actor2):
        signal_actor2.send.remote()
        ray.get(singal1.wait.remote())
        return 1

    singal1 = SignalActor.remote()
    signal2 = SignalActor.remote()

    o1 = fun.remote(singal1, signal2)
    # Make sure the normal task is executing.
    ray.get(signal2.wait.remote())

    # The normal task is blocked now.
    # Try to create actor and make sure this actor is not created for the time
    # being.
    foo = Foo.remote()
    o2 = foo.method.remote()
    ready_list, remaining_list = ray.wait([o2], timeout=2)
    assert len(ready_list) == 0 and len(remaining_list) == 1

    # Send a signal to unblock the normal task execution.
    ray.get(singal1.send.remote())

    # Check the result of normal task.
    assert ray.get(o1) == 1

    # Make sure the actor is created.
    assert ray.get(o2) == 2


# This case tests whether gcs-based actor scheduler works properly
# in a large scale.
def test_schedule_many_actors_and_normal_tasks(ray_start_cluster):
    cluster = ray_start_cluster

    node_count = 10
    actor_count = 50
    each_actor_task_count = 50
    normal_task_count = 1000
    node_memory = 2 * 1024 ** 3

    for i in range(node_count):
        cluster.add_node(
            memory=node_memory,
            _system_config={"gcs_actor_scheduling_enabled": True} if i == 0 else {},
        )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=100 * 1024 ** 2, num_cpus=0.01)
    class Foo:
        def method(self):
            return 2

    @ray.remote(memory=100 * 1024 ** 2, num_cpus=0.01)
    def fun():
        return 1

    normal_task_object_list = [fun.remote() for _ in range(normal_task_count)]
    actor_list = [Foo.remote() for _ in range(actor_count)]
    actor_object_list = [
        actor.method.remote()
        for _ in range(each_actor_task_count)
        for actor in actor_list
    ]
    for object in ray.get(actor_object_list):
        assert object == 2

    for object in ray.get(normal_task_object_list):
        assert object == 1


# This case tests whether gcs-based actor scheduler distributes actors
# in a balanced way. By default, it uses the `SPREAD` strategy of
# gcs resource scheduler.
@pytest.mark.parametrize("args", [[5, 20], [5, 3]])
def test_actor_distribution_balance(ray_start_cluster, args):
    cluster = ray_start_cluster

    node_count = args[0]
    actor_count = args[1]

    for i in range(node_count):
        cluster.add_node(
            memory=1024 ** 3,
            _system_config={"gcs_actor_scheduling_enabled": True} if i == 0 else {},
        )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=100 * 1024 ** 2, num_cpus=0.01)
    class Foo:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    actor_distribution = {}
    actor_list = [Foo.remote() for _ in range(actor_count)]
    for actor in actor_list:
        node_id = ray.get(actor.method.remote())
        if node_id not in actor_distribution.keys():
            actor_distribution[node_id] = []
        actor_distribution[node_id].append(actor)

    if node_count >= actor_count:
        assert len(actor_distribution) == actor_count
        for node_id, actors in actor_distribution.items():
            assert len(actors) == 1
    else:
        assert len(actor_distribution) == node_count
        for node_id, actors in actor_distribution.items():
            assert len(actors) <= int(actor_count / node_count)


# This case tests whether RequestWorkerLeaseReply carries normal task resources
# when the request is rejected (due to resource preemption by normal tasks).
def test_worker_lease_reply_with_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        memory=2000 * 1024 ** 2,
        _system_config={
            "gcs_resource_report_poll_period_ms": 1000000,
            "gcs_actor_scheduling_enabled": True,
        },
    )
    node2 = cluster.add_node(memory=1000 * 1024 ** 2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=1500 * 1024 ** 2)
    def fun(signal):
        signal.send.remote()
        time.sleep(30)
        return 0

    signal = SignalActor.remote()
    fun.remote(signal)
    # Make sure that the `fun` is running.
    ray.get(signal.wait.remote())

    @ray.remote(memory=800 * 1024 ** 2)
    class Foo:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    foo1 = Foo.remote()
    o1 = foo1.method.remote()
    ready_list, remaining_list = ray.wait([o1], timeout=10)
    # If RequestWorkerLeaseReply carries normal task resources,
    # GCS will then schedule foo1 to node2. Otherwise,
    # GCS would keep trying to schedule foo1 to
    # node1 and getting rejected.
    assert len(ready_list) == 1 and len(remaining_list) == 0
    assert ray.get(o1) == node2.unique_id


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
