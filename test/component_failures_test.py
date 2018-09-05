from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import os
import ray
import time

import pyarrow as pa


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


# This test checks that when a worker dies in the middle of a get, the
# plasma store and manager will not die.
@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY", False),
    reason="This test does not work with xray yet.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Not working with new GCS API.")
def test_dying_worker_get(ray_start_workers_separate):
    obj_id = 20 * b"a"

    @ray.remote
    def f():
        ray.worker.global_worker.plasma_client.get(ray.ObjectID(obj_id))

    # Have the worker wait in a get call.
    f.remote()

    # Kill the worker.
    time.sleep(1)
    (ray.services.all_processes[ray.services.PROCESS_TYPE_WORKER][0]
     .terminate())
    time.sleep(0.1)

    # Seal the object so the store attempts to notify the worker that the
    # get has been fulfilled.
    ray.worker.global_worker.plasma_client.create(
        pa.plasma.ObjectID(obj_id), 100)
    ray.worker.global_worker.plasma_client.seal(pa.plasma.ObjectID(obj_id))
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray.services.all_processes_alive(
        exclude=[ray.services.PROCESS_TYPE_WORKER])


# This test checks that when a worker dies in the middle of a wait, the
# plasma store and manager will not die.
@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY", False),
    reason="This test does not work with xray yet.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Not working with new GCS API.")
def test_dying_worker_wait(ray_start_workers_separate):
    obj_id = 20 * b"a"

    @ray.remote
    def f():
        ray.worker.global_worker.plasma_client.wait([ray.ObjectID(obj_id)])

    # Have the worker wait in a get call.
    f.remote()

    # Kill the worker.
    time.sleep(1)
    (ray.services.all_processes[ray.services.PROCESS_TYPE_WORKER][0]
     .terminate())
    time.sleep(0.1)

    # Seal the object so the store attempts to notify the worker that the
    # get has been fulfilled.
    ray.worker.global_worker.plasma_client.create(
        pa.plasma.ObjectID(obj_id), 100)
    ray.worker.global_worker.plasma_client.seal(pa.plasma.ObjectID(obj_id))
    time.sleep(0.1)

    # Make sure that nothing has died.
    assert ray.services.all_processes_alive(
        exclude=[ray.services.PROCESS_TYPE_WORKER])


@pytest.fixture(params=[(1, 4), (4, 4)])
def ray_start_workers_separate_multinode(request):
    num_local_schedulers = request.param[0]
    num_initial_workers = request.param[1]
    # Start the Ray processes.
    ray.worker._init(
        num_workers=(num_initial_workers * num_local_schedulers),
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
    # Raylet is able to pass a harder failure test than legacy ray.
    use_raylet = os.environ.get("RAY_USE_XRAY") == "1"

    # Start with 4 workers and 4 cores.
    num_local_schedulers = 4
    num_workers_per_scheduler = 8
    ray.worker._init(
        num_workers=num_workers_per_scheduler,
        num_local_schedulers=num_local_schedulers,
        start_ray_local=True,
        num_cpus=[num_workers_per_scheduler] * num_local_schedulers,
        redirect_output=True)

    if use_raylet:
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
        # NOTE(swang): Legacy ray hangs on this test if the plasma manager
        # is killed.
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
    else:

        @ray.remote
        def f(x, j):
            time.sleep(0.2)
            return x

        # Submit more tasks than there are workers so that all workers and
        # cores are utilized.
        object_ids = [
            f.remote(i, 0)
            for i in range(num_workers_per_scheduler * num_local_schedulers)
        ]
        object_ids += [f.remote(object_id, 1) for object_id in object_ids]
        object_ids += [f.remote(object_id, 2) for object_id in object_ids]

        # Kill the component on all nodes except the head node as the tasks
        # execute.
        time.sleep(0.1)
        components = ray.services.all_processes[component_type]
        for process in components[1:]:
            process.terminate()
            time.sleep(1)

        for process in components[1:]:
            process.kill()
            process.wait()
            assert not process.poll() is None

        # Make sure that we can still get the objects after the executing
        # tasks died.
        results = ray.get(object_ids)
        expected_results = 4 * list(
            range(num_workers_per_scheduler * num_local_schedulers))
        assert results == expected_results


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


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only makes sense with xray.")
def test_raylet_failed():
    # Kill all local schedulers on worker nodes.
    _test_component_failed(ray.services.PROCESS_TYPE_RAYLET)

    # The plasma stores and plasma managers should still be alive on the
    # worker nodes.
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE, True)

    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") == "1",
    reason="This test does not make sense with xray.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_local_scheduler_failed():
    # Kill all local schedulers on worker nodes.
    _test_component_failed(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER)

    # The plasma stores and plasma managers should still be alive on the
    # worker nodes.
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE, True)
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_MANAGER, True)
    check_components_alive(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER, False)

    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") == "1",
    reason="This test does not make sense with xray.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_plasma_manager_failed():
    # Kill all plasma managers on worker nodes.
    _test_component_failed(ray.services.PROCESS_TYPE_PLASMA_MANAGER)

    # The plasma stores should still be alive (but unreachable) on the
    # worker nodes.
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE, True)
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_MANAGER, False)
    check_components_alive(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER, False)

    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_plasma_store_failed():
    # Kill all plasma stores on worker nodes.
    _test_component_failed(ray.services.PROCESS_TYPE_PLASMA_STORE)

    # No processes should be left alive on the worker nodes.
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE, False)
    check_components_alive(ray.services.PROCESS_TYPE_PLASMA_MANAGER, False)
    check_components_alive(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER, False)
    check_components_alive(ray.services.PROCESS_TYPE_RAYLET, False)

    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_driver_lives_sequential():
    ray.worker.init()
    all_processes = ray.services.all_processes
    processes = (all_processes[ray.services.PROCESS_TYPE_PLASMA_STORE] +
                 all_processes[ray.services.PROCESS_TYPE_PLASMA_MANAGER] +
                 all_processes[ray.services.PROCESS_TYPE_LOCAL_SCHEDULER] +
                 all_processes[ray.services.PROCESS_TYPE_GLOBAL_SCHEDULER] +
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
                 all_processes[ray.services.PROCESS_TYPE_PLASMA_MANAGER] +
                 all_processes[ray.services.PROCESS_TYPE_LOCAL_SCHEDULER] +
                 all_processes[ray.services.PROCESS_TYPE_GLOBAL_SCHEDULER] +
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
