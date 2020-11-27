# coding: utf-8
import logging
import os
import sys
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.test_utils

from ray.test_utils import (
    RayTestTimeoutException,
    wait_for_condition,
)

logger = logging.getLogger(__name__)


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
    num_gpus = 3
    ray.init(num_cpus=num_gpus, num_gpus=num_gpus)

    def get_gpu_ids(num_gpus_per_worker):
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

    # Wait for all workers to start up.
    @ray.remote
    def f():
        time.sleep(0.2)
        return os.getpid()

    start_time = time.time()
    while True:
        num_workers_started = len(
            set(ray.get([f.remote() for _ in range(num_gpus)])))
        if num_workers_started == num_gpus:
            break
        if time.time() > start_time + 10:
            raise RayTestTimeoutException(
                "Timed out while waiting for workers to start "
                "up.")

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    assert list_of_ids == 10 * [[]]
    ray.get([f1.remote() for _ in range(10)])
    ray.get([f2.remote() for _ in range(10)])

    # Test that actors have CUDA_VISIBLE_DEVICES set properly.

    @ray.remote
    class Actor0:
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
    class Actor1:
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
    class Actor:
        def method(self):
            pass

    a = Actor.remote()
    x = a.method.remote()
    ray.get(x)


def test_zero_cpus_actor(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    valid_node = cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    @ray.remote
    class Foo:
        def method(self):
            return ray.worker.global_worker.node.unique_id

    # Make sure tasks and actors run on the remote raylet.
    a = Foo.remote()
    assert valid_node.unique_id == ray.get(a.method.remote())


def test_fractional_resources(shutdown_only):
    ray.init(num_cpus=6, num_gpus=3, resources={"Custom": 1})

    @ray.remote(num_gpus=0.5)
    class Foo1:
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
    class Foo2:
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
    cluster.add_node(num_cpus=1, resources={"CustomResource": 0})
    custom_resource_node = cluster.add_node(
        num_cpus=1, resources={"CustomResource": 1})
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource": 1})
    def g():
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource": 1})
    def h():
        ray.get([f.remote() for _ in range(5)])
        return ray.worker.global_worker.node.unique_id

    # The g tasks should be scheduled only on the second raylet.
    raylet_ids = set(ray.get([g.remote() for _ in range(50)]))
    assert len(raylet_ids) == 1
    assert list(raylet_ids)[0] == custom_resource_node.unique_id

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
    custom_resource_node = cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 3,
            "CustomResource2": 4
        })
    ray.init(address=cluster.address)

    @ray.remote
    def foo():
        # Sleep a while to emulate a slow operation. This is needed to make
        # sure tasks are scheduled to different nodes.
        time.sleep(0.1)
        return ray.worker.global_worker.node.unique_id

    # Make sure each node has at least one idle worker.
    wait_for_condition(
        lambda: len(set(ray.get([foo.remote() for _ in range(6)]))) == 2)

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
    assert len(set(ray.get([f.remote() for _ in range(500)]))) == 2
    assert len(set(ray.get([g.remote() for _ in range(500)]))) == 2

    # The h tasks should be scheduled only on the second raylet.
    raylet_ids = set(ray.get([h.remote() for _ in range(50)]))
    assert len(raylet_ids) == 1
    assert list(raylet_ids)[0] == custom_resource_node.unique_id

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

    def delete_miscellaneous_item(resources):
        del resources["memory"]
        del resources["object_store_memory"]
        for key in list(resources.keys()):
            if key.startswith("node:"):
                del resources[key]

    def test():
        resources = ray.available_resources()
        MAX_RETRY_ATTEMPTS = 5
        retry_count = 0

        delete_miscellaneous_item(resources)

        while resources and retry_count < MAX_RETRY_ATTEMPTS:
            time.sleep(0.1)
            resources = ray.available_resources()
            delete_miscellaneous_item(resources)
            retry_count += 1

        if retry_count >= MAX_RETRY_ATTEMPTS:
            raise RuntimeError(
                "Resources were available even after {} retries.".format(
                    MAX_RETRY_ATTEMPTS), resources)

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
        assert int(gpu_ids[0]) in allowed_gpu_ids

    @ray.remote(num_gpus=2)
    def g():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert int(gpu_ids[0]) in allowed_gpu_ids
        assert int(gpu_ids[1]) in allowed_gpu_ids

    ray.get([f.remote() for _ in range(100)])
    ray.get([g.remote() for _ in range(100)])


def test_local_mode_gpus(save_gpu_ids_shutdown_only):
    allowed_gpu_ids = [4, 5, 6, 7, 8]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
        [str(i) for i in allowed_gpu_ids])

    from importlib import reload
    reload(ray.worker)

    ray.init(num_gpus=3, local_mode=True)

    @ray.remote
    def f():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 3
        for gpu in gpu_ids:
            assert int(gpu) in allowed_gpu_ids

    ray.get([f.remote() for _ in range(100)])


def test_blocking_tasks(ray_start_regular):
    @ray.remote
    def f(i, j):
        return (i, j)

    @ray.remote
    def g(i):
        # Each instance of g submits and blocks on the result of another
        # remote task.
        object_refs = [f.remote(i, j) for j in range(2)]
        return ray.get(object_refs)

    @ray.remote
    def h(i):
        # Each instance of g submits and blocks on the result of another
        # remote task using ray.wait.
        object_refs = [f.remote(i, j) for j in range(2)]
        return ray.wait(object_refs, num_returns=len(object_refs))

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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
