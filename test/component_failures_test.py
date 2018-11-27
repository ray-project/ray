from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import json
import signal
import sys
import time

import numpy as np
import pytest

import ray
from ray.test.cluster_utils import Cluster
from ray.test.test_utils import run_string_as_driver_nonblocking


@pytest.fixture
def ray_start_workers_separate():
    # Start the Ray processes.
    ray.worker._init(
        num_cpus=1,
        start_workers_from_local_scheduler=False,
        start_ray_local=True,
        redirect_output=True)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_cluster():
    node_args = {
        "resources": dict(CPU=8),
        "_internal_config": json.dumps({
            "initial_reconstruction_timeout_milliseconds": 1000,
            "num_heartbeats_timeout": 10
        })
    }
    # Start with 4 worker nodes and 8 cores each.
    g = Cluster(initialize_head=True, connect=True, head_node_args=node_args)
    workers = []
    for _ in range(4):
        workers.append(g.add_node(**node_args))
    g.wait_for_nodes()
    yield g
    ray.shutdown()
    g.shutdown()


# This test checks that when a worker dies in the middle of a get, the plasma
# store and raylet will not die.
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Not working with new GCS API.")
def test_dying_worker_get(shutdown_only):
    # Start the Ray processes.
    ray.init(num_cpus=2)

    @ray.remote
    def sleep_forever():
        time.sleep(10**6)

    @ray.remote
    def get_worker_pid():
        return os.getpid()

    x_id = sleep_forever.remote()
    time.sleep(0.01)  # Try to wait for the sleep task to get scheduled.
    # Get the PID of the other worker.
    worker_pid = ray.get(get_worker_pid.remote())

    @ray.remote
    def f(id_in_a_list):
        ray.get(id_in_a_list[0])

    # Have the worker wait in a get call.
    result_id = f.remote([x_id])
    time.sleep(1)

    # Make sure the task hasn't finished.
    ready_ids, _ = ray.wait([result_id], timeout=0)
    assert len(ready_ids) == 0

    # Kill the worker.
    os.kill(worker_pid, signal.SIGKILL)
    time.sleep(0.1)

    # Make sure the sleep task hasn't finished.
    ready_ids, _ = ray.wait([x_id], timeout=0)
    assert len(ready_ids) == 0
    # Seal the object so the store attempts to notify the worker that the
    # get has been fulfilled.
    ray.worker.global_worker.put_object(x_id, 1)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray.services.all_processes_alive()


# This test checks that when a driver dies in the middle of a get, the plasma
# store and raylet will not die.
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Not working with new GCS API.")
def test_dying_driver_get(shutdown_only):
    # Start the Ray processes.
    address_info = ray.init(num_cpus=1)

    @ray.remote
    def sleep_forever():
        time.sleep(10**6)

    x_id = sleep_forever.remote()

    driver = """
import ray
ray.init("{}")
ray.get(ray.ObjectID(ray.utils.hex_to_binary("{}")))
""".format(address_info["redis_address"], x_id.hex())

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    assert p.poll() is None

    # Kill the driver process.
    p.kill()
    p.wait()
    time.sleep(0.1)

    # Make sure the original task hasn't finished.
    ready_ids, _ = ray.wait([x_id], timeout=0)
    assert len(ready_ids) == 0
    # Seal the object so the store attempts to notify the worker that the
    # get has been fulfilled.
    ray.worker.global_worker.put_object(x_id, 1)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray.services.all_processes_alive()


# This test checks that when a worker dies in the middle of a wait, the plasma
# store and raylet will not die.
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Not working with new GCS API.")
def test_dying_worker_wait(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def sleep_forever():
        time.sleep(10**6)

    @ray.remote
    def get_pid():
        return os.getpid()

    x_id = sleep_forever.remote()
    # Get the PID of the worker that block_in_wait will run on (sleep a little
    # to make sure that sleep_forever has already started).
    time.sleep(0.1)
    worker_pid = ray.get(get_pid.remote())

    @ray.remote
    def block_in_wait(object_id_in_list):
        ray.wait(object_id_in_list)

    # Have the worker wait in a wait call.
    block_in_wait.remote([x_id])
    time.sleep(0.1)

    # Kill the worker.
    os.kill(worker_pid, signal.SIGKILL)
    time.sleep(0.1)

    # Create the object.
    ray.worker.global_worker.put_object(x_id, 1)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray.services.all_processes_alive()


# This test checks that when a driver dies in the middle of a wait, the plasma
# store and raylet will not die.
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Not working with new GCS API.")
def test_dying_driver_wait(shutdown_only):
    # Start the Ray processes.
    address_info = ray.init(num_cpus=1)

    @ray.remote
    def sleep_forever():
        time.sleep(10**6)

    x_id = sleep_forever.remote()

    driver = """
import ray
ray.init("{}")
ray.wait([ray.ObjectID(ray.utils.hex_to_binary("{}"))])
""".format(address_info["redis_address"], x_id.hex())

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    assert p.poll() is None

    # Kill the driver process.
    p.kill()
    p.wait()
    time.sleep(0.1)

    # Make sure the original task hasn't finished.
    ready_ids, _ = ray.wait([x_id], timeout=0)
    assert len(ready_ids) == 0
    # Seal the object so the store attempts to notify the worker that the
    # wait can return.
    ray.worker.global_worker.put_object(x_id, 1)
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray.services.all_processes_alive()


@pytest.fixture(params=[(1, 4), (4, 4)])
def ray_start_workers_separate_multinode(request):
    num_local_schedulers = request.param[0]
    num_initial_workers = request.param[1]
    # Start the Ray processes.
    ray.worker._init(
        num_local_schedulers=num_local_schedulers,
        start_workers_from_local_scheduler=False,
        start_ray_local=True,
        num_cpus=[num_initial_workers] * num_local_schedulers,
        redirect_output=True)
    yield num_local_schedulers, num_initial_workers
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_worker_failed(ray_start_workers_separate_multinode):
    num_local_schedulers, num_initial_workers = (
        ray_start_workers_separate_multinode)

    @ray.remote
    def f(x):
        time.sleep(0.5)
        return x

    # Submit more tasks than there are workers so that all workers and
    # cores are utilized.
    object_ids = [
        f.remote(i) for i in range(num_initial_workers * num_local_schedulers)
    ]
    object_ids += [f.remote(object_id) for object_id in object_ids]
    # Allow the tasks some time to begin executing.
    time.sleep(0.1)
    # Kill the workers as the tasks execute.
    for worker in (
            ray.services.all_processes[ray.services.PROCESS_TYPE_WORKER]):
        worker.terminate()
        time.sleep(0.1)
    # Make sure that we can still get the objects after the executing tasks
    # died.
    ray.get(object_ids)


def _test_component_failed(component_type):
    """Kill a component on all worker nodes and check workload succeeds."""
    # Start with 4 workers and 4 cores.
    num_local_schedulers = 4
    num_workers_per_scheduler = 8
    ray.worker._init(
        num_local_schedulers=num_local_schedulers,
        start_ray_local=True,
        num_cpus=[num_workers_per_scheduler] * num_local_schedulers,
        redirect_output=True,
        _internal_config=json.dumps({
            "initial_reconstruction_timeout_milliseconds": 1000,
            "num_heartbeats_timeout": 10,
        }))

    # Submit many tasks with many dependencies.
    @ray.remote
    def f(x):
        return x

    @ray.remote
    def g(*xs):
        return 1

    # Kill the component on all nodes except the head node as the tasks
    # execute. Do this in a loop while submitting tasks between each
    # component failure.
    time.sleep(0.1)
    components = ray.services.all_processes[component_type]
    for process in components[1:]:
        # Submit a round of tasks with many dependencies.
        x = 1
        for _ in range(1000):
            x = f.remote(x)

        xs = [g.remote(1)]
        for _ in range(100):
            xs.append(g.remote(*xs))
            xs.append(g.remote(1))

        # Kill a component on one of the nodes.
        process.terminate()
        time.sleep(1)
        process.kill()
        process.wait()
        assert not process.poll() is None

        # Make sure that we can still get the objects after the
        # executing tasks died.
        ray.get(x)
        ray.get(xs)


def check_components_alive(component_type, check_component_alive):
    """Check that a given component type is alive on all worker nodes.
    """
    components = ray.services.all_processes[component_type][1:]
    for component in components:
        if check_component_alive:
            assert component.poll() is None
        else:
            print("waiting for " + component_type + " with PID " +
                  str(component.pid) + "to terminate")
            component.wait()
            print("done waiting for " + component_type + " with PID " +
                  str(component.pid) + "to terminate")
            assert not component.poll() is None


def test_raylet_failed():
    # Kill all local schedulers on worker nodes.
    _test_component_failed(ray.services.PROCESS_TYPE_RAYLET)

    # The plasma stores should still be alive on the worker nodes.
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE, True)

    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_plasma_store_failed():
    # Kill all plasma stores on worker nodes.
    _test_component_failed(ray.services.PROCESS_TYPE_PLASMA_STORE)

    # No processes should be left alive on the worker nodes.
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE, False)
    check_components_alive(ray.services.PROCESS_TYPE_RAYLET, False)

    ray.shutdown()


def test_actor_creation_node_failure(ray_start_cluster):
    # TODO(swang): Refactor test_raylet_failed, etc to reuse the below code.
    cluster = ray_start_cluster

    @ray.remote
    class Child(object):
        def __init__(self, death_probability):
            self.death_probability = death_probability

        def ping(self):
            # Exit process with some probability.
            exit_chance = np.random.rand()
            if exit_chance < self.death_probability:
                sys.exit(-1)

    num_children = 100
    # Children actors will die about half the time.
    death_probability = 0.5

    children = [Child.remote(death_probability) for _ in range(num_children)]
    while len(cluster.list_all_nodes()) > 1:
        for j in range(3):
            # Submit some tasks on the actors. About half of the actors will
            # fail.
            children_out = [child.ping.remote() for child in children]
            # Wait for all the tasks to complete. This should trigger
            # reconstruction for any actor creation tasks that were forwarded
            # to nodes that then failed.
            ready, _ = ray.wait(
                children_out, num_returns=len(children_out), timeout=30000)
            assert len(ready) == len(children_out)

            # Replace any actors that died.
            for i, out in enumerate(children_out):
                try:
                    ray.get(out)
                except ray.worker.RayGetError:
                    children[i] = Child.remote(death_probability)
        # Remove a node. Any actor creation tasks that were forwarded to this
        # node must be reconstructed.
        cluster.remove_node(cluster.list_all_nodes()[-1])


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_driver_lives_sequential():
    ray.worker.init()
    all_processes = ray.services.all_processes
    processes = (all_processes[ray.services.PROCESS_TYPE_PLASMA_STORE] +
                 all_processes[ray.services.PROCESS_TYPE_RAYLET])

    # Kill all the components sequentially.
    for process in processes:
        process.terminate()
        time.sleep(0.1)
        process.kill()
        process.wait()

    ray.shutdown()
    # If the driver can reach the tearDown method, then it is still alive.


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_driver_lives_parallel():
    ray.worker.init()
    all_processes = ray.services.all_processes
    processes = (all_processes[ray.services.PROCESS_TYPE_PLASMA_STORE] +
                 all_processes[ray.services.PROCESS_TYPE_RAYLET])

    # Kill all the components in parallel.
    for process in processes:
        process.terminate()

    time.sleep(0.1)
    for process in processes:
        process.kill()

    for process in processes:
        process.wait()

    # If the driver can reach the tearDown method, then it is still alive.
    ray.shutdown()
