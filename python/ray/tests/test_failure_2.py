import json
import logging
import os
import signal
import sys
import threading
import time

import numpy as np
import pytest
import redis

import ray
from ray.experimental.internal_kv import _internal_kv_get
from ray.autoscaler._private.util import DEBUG_AUTOSCALING_ERROR
import ray._private.utils
from ray.util.placement_group import placement_group
import ray.ray_constants as ray_constants
from ray.cluster_utils import Cluster
from ray.test_utils import (init_error_pubsub, get_error_message, Semaphore,
                            wait_for_condition)


def test_warning_for_infeasible_tasks(ray_start_regular, error_pubsub):
    p = error_pubsub
    # Check that we get warning messages for infeasible tasks.

    @ray.remote(num_gpus=1)
    def f():
        pass

    @ray.remote(resources={"Custom": 1})
    class Foo:
        pass

    # This task is infeasible.
    f.remote()
    errors = get_error_message(p, 1, ray_constants.INFEASIBLE_TASK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.INFEASIBLE_TASK_ERROR

    # This actor placement task is infeasible.
    Foo.remote()
    errors = get_error_message(p, 1, ray_constants.INFEASIBLE_TASK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.INFEASIBLE_TASK_ERROR

    # Placement group cannot be made, but no warnings should occur.
    pg = placement_group([{"GPU": 1}], strategy="STRICT_PACK")
    pg.ready()
    f.options(placement_group=pg).remote()

    errors = get_error_message(
        p, 1, ray_constants.INFEASIBLE_TASK_ERROR, timeout=5)
    assert len(errors) == 0, errors


def test_warning_for_infeasible_zero_cpu_actor(shutdown_only):
    # Check that we cannot place an actor on a 0 CPU machine and that we get an
    # infeasibility warning (even though the actor creation task itself
    # requires no CPUs).

    ray.init(num_cpus=0)
    p = init_error_pubsub()

    @ray.remote
    class Foo:
        pass

    # The actor creation should be infeasible.
    Foo.remote()
    errors = get_error_message(p, 1, ray_constants.INFEASIBLE_TASK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.INFEASIBLE_TASK_ERROR
    p.close()


def test_warning_for_too_many_actors(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)

    p = init_error_pubsub()

    @ray.remote
    class Foo:
        def __init__(self):
            time.sleep(1000)

    # NOTE: We should save actor, otherwise it will be out of scope.
    actor_group1 = [Foo.remote() for _ in range(num_cpus * 3)]
    assert len(actor_group1) == num_cpus * 3
    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR

    actor_group2 = [Foo.remote() for _ in range(num_cpus)]
    assert len(actor_group2) == num_cpus
    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR
    p.close()


def test_warning_for_too_many_nested_tasks(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)
    p = init_error_pubsub()

    remote_wait = Semaphore.remote(value=0)
    nested_wait = Semaphore.remote(value=0)

    ray.get([
        remote_wait.locked.remote(),
        nested_wait.locked.remote(),
    ])

    @ray.remote
    def f():
        time.sleep(1000)
        return 1

    @ray.remote
    def h(nested_waits):
        nested_wait.release.remote()
        ray.get(nested_waits)
        ray.get(f.remote())

    @ray.remote
    def g(remote_waits, nested_waits):
        # Sleep so that the f tasks all get submitted to the scheduler after
        # the g tasks.
        remote_wait.release.remote()
        # wait until every lock is released.
        ray.get(remote_waits)
        ray.get(h.remote(nested_waits))

    num_root_tasks = num_cpus * 4
    # Lock remote task until everything is scheduled.
    remote_waits = []
    nested_waits = []
    for _ in range(num_root_tasks):
        remote_waits.append(remote_wait.acquire.remote())
        nested_waits.append(nested_wait.acquire.remote())

    [g.remote(remote_waits, nested_waits) for _ in range(num_root_tasks)]

    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR
    p.close()


def test_warning_for_many_duplicate_remote_functions_and_actors(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def create_remote_function():
        @ray.remote
        def g():
            return 1

        return ray.get(g.remote())

    for _ in range(ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD - 1):
        ray.get(create_remote_function.remote())

    import io
    log_capture_string = io.StringIO()
    ch = logging.StreamHandler(log_capture_string)

    # TODO(rkn): It's terrible to have to rely on this implementation detail,
    # the fact that the warning comes from ray._private.import_thread.logger.
    # However, I didn't find a good way to capture the output for all loggers
    # simultaneously.
    ray._private.import_thread.logger.addHandler(ch)

    ray.get(create_remote_function.remote())

    start_time = time.time()
    while time.time() < start_time + 10:
        log_contents = log_capture_string.getvalue()
        if len(log_contents) > 0:
            break

    ray._private.import_thread.logger.removeHandler(ch)

    assert "remote function" in log_contents
    assert "has been exported {} times.".format(
        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD) in log_contents

    # Now test the same thing but for actors.

    @ray.remote
    def create_actor_class():
        # Require a GPU so that the actor is never actually created and we
        # don't spawn an unreasonable number of processes.
        @ray.remote(num_gpus=1)
        class Foo:
            pass

        Foo.remote()

    for _ in range(ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD - 1):
        ray.get(create_actor_class.remote())

    log_capture_string = io.StringIO()
    ch = logging.StreamHandler(log_capture_string)

    # TODO(rkn): As mentioned above, it's terrible to have to rely on this
    # implementation detail.
    ray._private.import_thread.logger.addHandler(ch)

    ray.get(create_actor_class.remote())

    start_time = time.time()
    while time.time() < start_time + 10:
        log_contents = log_capture_string.getvalue()
        if len(log_contents) > 0:
            break

    ray._private.import_thread.logger.removeHandler(ch)

    assert "actor" in log_contents
    assert "has been exported {} times.".format(
        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD) in log_contents


def test_redis_module_failure(ray_start_regular):
    address_info = ray_start_regular
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    def run_failure_test(expecting_message, *command):
        with pytest.raises(
                Exception, match=".*{}.*".format(expecting_message)):
            client = redis.StrictRedis(
                host=address[0],
                port=int(address[1]),
                password=ray_constants.REDIS_DEFAULT_PASSWORD)
            client.execute_command(*command)

    def run_one_command(*command):
        client = redis.StrictRedis(
            host=address[0],
            port=int(address[1]),
            password=ray_constants.REDIS_DEFAULT_PASSWORD)
        client.execute_command(*command)

    run_failure_test("wrong number of arguments", "RAY.TABLE_ADD", 13)
    run_failure_test("Prefix must be in the TablePrefix range",
                     "RAY.TABLE_ADD", 100000, 1, 1, 1)
    run_failure_test("Prefix must be in the TablePrefix range",
                     "RAY.TABLE_REQUEST_NOTIFICATIONS", 100000, 1, 1, 1)
    run_failure_test("Prefix must be a valid TablePrefix integer",
                     "RAY.TABLE_ADD", b"a", 1, 1, 1)
    run_failure_test("Pubsub channel must be in the TablePubsub range",
                     "RAY.TABLE_ADD", 1, 10000, 1, 1)
    run_failure_test("Pubsub channel must be a valid integer", "RAY.TABLE_ADD",
                     1, b"a", 1, 1)
    # Change the key from 1 to 2, since the previous command should have
    # succeeded at writing the key, but not publishing it.
    run_failure_test("Index is less than 0.", "RAY.TABLE_APPEND", 1, 1, 2, 1,
                     -1)
    run_failure_test("Index is not a number.", "RAY.TABLE_APPEND", 1, 1, 2, 1,
                     b"a")
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1)
    # It's okay to add duplicate entries.
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1)
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1, 0)
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1, 1)
    run_one_command("RAY.SET_ADD", 1, 1, 3, 1)
    # It's okey to add duplicate entries.
    run_one_command("RAY.SET_ADD", 1, 1, 3, 1)
    run_one_command("RAY.SET_REMOVE", 1, 1, 3, 1)
    # It's okey to remove duplicate entries.
    run_one_command("RAY.SET_REMOVE", 1, 1, 3, 1)


# Note that this test will take at least 10 seconds because it must wait for
# the monitor to detect enough missed heartbeats.
def test_warning_for_dead_node(ray_start_cluster_2_nodes, error_pubsub):
    cluster = ray_start_cluster_2_nodes
    cluster.wait_for_nodes()
    p = error_pubsub

    node_ids = {item["NodeID"] for item in ray.nodes()}

    # Try to make sure that the monitor has received at least one heartbeat
    # from the node.
    time.sleep(0.5)

    # Kill both raylets.
    cluster.list_all_nodes()[1].kill_raylet()
    cluster.list_all_nodes()[0].kill_raylet()

    # Check that we get warning messages for both raylets.
    errors = get_error_message(p, 2, ray_constants.REMOVED_NODE_ERROR, 40)

    # Extract the client IDs from the error messages. This will need to be
    # changed if the error message changes.
    warning_node_ids = {error.error_message.split(" ")[5] for error in errors}

    assert node_ids == warning_node_ids


def test_warning_for_dead_autoscaler(ray_start_regular, error_pubsub):
    # Terminate the autoscaler process.
    from ray.worker import _global_node
    autoscaler_process = _global_node.all_processes[
        ray_constants.PROCESS_TYPE_MONITOR][0].process
    autoscaler_process.terminate()

    # Confirm that we receive an autoscaler failure error.
    errors = get_error_message(
        error_pubsub, 1, ray_constants.MONITOR_DIED_ERROR, timeout=5)
    assert len(errors) == 1

    # Confirm that the autoscaler failure error is stored.
    error = _internal_kv_get(DEBUG_AUTOSCALING_ERROR)
    assert error is not None


def test_raylet_crash_when_get(ray_start_regular):
    def sleep_to_kill_raylet():
        # Don't kill raylet before default workers get connected.
        time.sleep(2)
        ray.worker._global_node.kill_raylet()

    object_ref = ray.put(np.zeros(200 * 1024, dtype=np.uint8))
    ray.internal.free(object_ref)

    thread = threading.Thread(target=sleep_to_kill_raylet)
    thread.start()
    with pytest.raises(ray.exceptions.ObjectLostError):
        ray.get(object_ref)
    thread.join()


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_nodes": 1,
        "num_cpus": 2,
    }, {
        "num_nodes": 2,
        "num_cpus": 1,
    }],
    indirect=True)
def test_eviction(ray_start_cluster):
    @ray.remote
    def large_object():
        return np.zeros(10 * 1024 * 1024)

    obj = large_object.remote()
    assert (isinstance(ray.get(obj), np.ndarray))
    # Evict the object.
    ray.internal.free([obj])
    # ray.get throws an exception.
    with pytest.raises(ray.exceptions.ObjectLostError):
        ray.get(obj)

    @ray.remote
    def dependent_task(x):
        return

    # If the object is passed by reference, the task throws an
    # exception.
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(dependent_task.remote(obj))


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_nodes": 2,
        "num_cpus": 1,
    }, {
        "num_nodes": 1,
        "num_cpus": 2,
    }],
    indirect=True)
def test_serialized_id(ray_start_cluster):
    @ray.remote
    def small_object():
        # Sleep a bit before creating the object to force a timeout
        # at the getter.
        time.sleep(1)
        return 1

    @ray.remote
    def dependent_task(x):
        return x

    @ray.remote
    def get(obj_refs, test_dependent_task):
        print("get", obj_refs)
        obj_ref = obj_refs[0]
        if test_dependent_task:
            assert ray.get(dependent_task.remote(obj_ref)) == 1
        else:
            assert ray.get(obj_ref) == 1

    obj = small_object.remote()
    ray.get(get.remote([obj], False))

    obj = small_object.remote()
    ray.get(get.remote([obj], True))

    obj = ray.put(1)
    ray.get(get.remote([obj], False))

    obj = ray.put(1)
    ray.get(get.remote([obj], True))


@pytest.mark.parametrize("use_actors,node_failure",
                         [(False, False), (False, True), (True, False),
                          (True, True)])
def test_fate_sharing(ray_start_cluster, use_actors, node_failure):
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 100,
    }
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _system_config=config)
    ray.init(address=cluster.address)
    # Node to place the parent actor.
    node_to_kill = cluster.add_node(num_cpus=1, resources={"parent": 1})
    # Node to place the child actor.
    cluster.add_node(num_cpus=1, resources={"child": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def sleep():
        time.sleep(1000)

    @ray.remote(resources={"child": 1})
    def probe():
        return

    # TODO(swang): This test does not pass if max_restarts > 0 for the
    # raylet codepath. Add this parameter once the GCS actor service is enabled
    # by default.
    @ray.remote
    class Actor(object):
        def __init__(self):
            return

        def start_child(self, use_actors):
            if use_actors:
                child = Actor.options(resources={"child": 1}).remote()
                ray.get(child.sleep.remote())
            else:
                ray.get(sleep.options(resources={"child": 1}).remote())

        def sleep(self):
            time.sleep(1000)

        def get_pid(self):
            return os.getpid()

    # Returns whether the "child" resource is available.
    def child_resource_available():
        p = probe.remote()
        ready, _ = ray.wait([p], timeout=1)
        return len(ready) > 0

    # Test fate sharing if the parent process dies.
    def test_process_failure(use_actors):
        a = Actor.options(resources={"parent": 1}).remote()
        pid = ray.get(a.get_pid.remote())
        a.start_child.remote(use_actors=use_actors)
        # Wait for the child to be scheduled.
        wait_for_condition(lambda: not child_resource_available())
        # Kill the parent process.
        os.kill(pid, 9)
        wait_for_condition(child_resource_available)

    # Test fate sharing if the parent node dies.
    def test_node_failure(node_to_kill, use_actors):
        a = Actor.options(resources={"parent": 1}).remote()
        a.start_child.remote(use_actors=use_actors)
        # Wait for the child to be scheduled.
        wait_for_condition(lambda: not child_resource_available())
        # Kill the parent process.
        cluster.remove_node(node_to_kill, allow_graceful=False)
        node_to_kill = cluster.add_node(num_cpus=1, resources={"parent": 1})
        wait_for_condition(child_resource_available)
        return node_to_kill

    if node_failure:
        test_node_failure(node_to_kill, use_actors)
    else:
        test_process_failure(use_actors)


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "ping_gcs_rpc_server_max_retries": 100
        }
    }],
    indirect=True)
def test_gcs_server_failiure_report(ray_start_regular, log_pubsub):
    p = log_pubsub
    # Get gcs server pid to send a signal.
    all_processes = ray.worker._global_node.all_processes
    gcs_server_process = all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    os.kill(gcs_server_pid, signal.SIGBUS)
    msg = None
    cnt = 0
    # wait for max 30 seconds.
    while cnt < 3000 and not msg:
        msg = p.get_message()
        if msg is None:
            time.sleep(0.01)
            cnt += 1
            continue
        data = json.loads(ray._private.utils.decode(msg["data"]))
        assert data["pid"] == "gcs_server"


def test_raylet_node_manager_server_failure(ray_start_cluster_head,
                                            log_pubsub):
    cluster = ray_start_cluster_head
    redis_port = int(cluster.address.split(":")[1])
    # Reuse redis port to make node manager grpc server fail to start.
    cluster.add_node(wait=False, node_manager_port=redis_port)
    p = log_pubsub
    cnt = 0
    # wait for max 10 seconds.
    found = False
    while cnt < 1000 and not found:
        msg = p.get_message()
        if msg is None:
            time.sleep(0.01)
            cnt += 1
            continue
        data = json.loads(ray._private.utils.decode(msg["data"]))
        if data["pid"] == "raylet":
            found = any("Failed to start the grpc server." in line
                        for line in data["lines"])
    assert found


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
