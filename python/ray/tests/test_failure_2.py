import os
import signal
import sys
import threading
import time

import numpy as np
import pytest

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
from ray._private.ray_constants import DEBUG_AUTOSCALING_ERROR
from ray._private.test_utils import (
    Semaphore,
    get_error_message,
    get_log_batch,
    init_error_pubsub,
    run_string_as_driver_nonblocking,
    wait_for_condition,
)
from ray.cluster_utils import cluster_not_supported
from ray.experimental.internal_kv import _internal_kv_get


def test_warning_for_too_many_actors(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)

    p = init_error_pubsub()

    @ray.remote(num_cpus=0)
    class Foo:
        def __init__(self):
            time.sleep(1000)

    # NOTE: We should save actor, otherwise it will be out of scope.
    actor_group1 = [Foo.remote() for _ in range(num_cpus * 10)]
    assert len(actor_group1) == num_cpus * 10
    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR

    actor_group2 = [Foo.remote() for _ in range(num_cpus * 3)]
    assert len(actor_group2) == num_cpus * 3
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

    ray.get(
        [
            remote_wait.locked.remote(),
            nested_wait.locked.remote(),
        ]
    )

    @ray.remote(num_cpus=0.25)
    def f():
        time.sleep(1000)
        return 1

    @ray.remote(num_cpus=0.25)
    def h(nested_waits):
        nested_wait.release.remote()
        ray.get(nested_waits)
        ray.get(f.remote())

    @ray.remote(num_cpus=0.25)
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


@pytest.mark.skipif(
    sys.platform == "win32", reason="Killing process on Windows does not raise a signal"
)
def test_warning_for_dead_autoscaler(ray_start_regular, error_pubsub):
    # Terminate the autoscaler process.
    from ray._private.worker import _global_node

    autoscaler_process = _global_node.all_processes[ray_constants.PROCESS_TYPE_MONITOR][
        0
    ].process
    autoscaler_process.terminate()

    # Confirm that we receive an autoscaler failure error.
    errors = get_error_message(
        error_pubsub, 1, ray_constants.MONITOR_DIED_ERROR, timeout=5
    )
    assert len(errors) == 1

    # Confirm that the autoscaler failure error is stored.
    error = _internal_kv_get(DEBUG_AUTOSCALING_ERROR)
    assert error is not None


def test_raylet_crash_when_get(ray_start_regular):
    def sleep_to_kill_raylet():
        # Don't kill raylet before default workers get connected.
        time.sleep(2)
        ray._private.worker._global_node.kill_raylet()

    object_ref = ray.put(np.zeros(200 * 1024, dtype=np.uint8))
    ray._private.internal_api.free(object_ref)

    thread = threading.Thread(target=sleep_to_kill_raylet)
    thread.start()
    with pytest.raises(ray.exceptions.ObjectFreedError):
        ray.get(object_ref)
    thread.join()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_nodes": 1,
            "num_cpus": 2,
        },
        {
            "num_nodes": 2,
            "num_cpus": 1,
        },
    ],
    indirect=True,
)
def test_eviction(ray_start_cluster):
    @ray.remote
    def large_object():
        return np.zeros(10 * 1024 * 1024)

    obj = large_object.remote()
    assert isinstance(ray.get(obj), np.ndarray)
    # Evict the object.
    ray._private.internal_api.free([obj])
    # ray.get throws an exception.
    with pytest.raises(ray.exceptions.ObjectFreedError):
        ray.get(obj)

    @ray.remote
    def dependent_task(x):
        return

    # If the object is passed by reference, the task throws an
    # exception.
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(dependent_task.remote(obj))


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_nodes": 2,
            "num_cpus": 1,
        },
        {
            "num_nodes": 1,
            "num_cpus": 2,
        },
    ],
    indirect=True,
)
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


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
@pytest.mark.parametrize(
    "use_actors,node_failure",
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_fate_sharing(ray_start_cluster, use_actors, node_failure):
    config = {
        "health_check_initial_delay_ms": 0,
        "health_check_period_ms": 100,
        "health_check_failure_threshold": 10,
    }
    cluster = ray_start_cluster
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


def test_list_named_actors_timeout(monkeypatch, shutdown_only):
    with monkeypatch.context() as m:
        # defer for 3s
        m.setenv(
            "RAY_testing_asio_delay_us",
            "ActorInfoGcsService.grpc_server.ListNamedActors=3000000:3000000",
        )
        ray.init(_system_config={"gcs_server_request_timeout_seconds": 1})

        @ray.remote
        class A:
            pass

        a = A.options(name="hi").remote()
        print(a)
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.util.list_named_actors()


def test_raylet_node_manager_server_failure(ray_start_cluster_head, log_pubsub):
    cluster = ray_start_cluster_head
    redis_port = int(cluster.address.split(":")[1])
    # Reuse redis port to make node manager grpc server fail to start.
    with pytest.raises(Exception):
        cluster.add_node(wait=False, node_manager_port=redis_port)

    # wait for max 10 seconds.
    def matcher(log_batch):
        return log_batch["pid"] == "raylet" and any(
            "Failed to start the grpc server." in line for line in log_batch["lines"]
        )

    match = get_log_batch(log_pubsub, 1, timeout=10, matcher=matcher)
    assert len(match) > 0


def test_gcs_server_crash_cluster(ray_start_cluster):
    # Test the GCS server failures will crash the driver.
    cluster = ray_start_cluster
    GCS_RECONNECTION_TIMEOUT = 5
    node = cluster.add_node(
        num_cpus=0,
        _system_config={"gcs_rpc_server_reconnect_timeout_s": GCS_RECONNECTION_TIMEOUT},
    )

    script = """
import ray
import time

ray.init(address="auto")
time.sleep(60)
    """

    # Get gcs server pid to send a signal.
    all_processes = node.all_processes
    gcs_server_process = all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    proc = run_string_as_driver_nonblocking(script)
    # Wait long enough to start the driver.
    time.sleep(5)
    start = time.time()
    print(gcs_server_pid)
    os.kill(gcs_server_pid, signal.SIGKILL)
    wait_for_condition(lambda: proc.poll() is None, timeout=10)
    # Make sure the driver was exited within the timeout instead of hanging.
    # * 2 for avoiding flakiness.
    assert time.time() - start < GCS_RECONNECTION_TIMEOUT * 2
    # Make sure all processes are cleaned up after GCS is crashed.
    # Currently, not every process is fate shared with GCS.
    # It seems like log monitor, ray client server, and Redis
    # are not fate shared.
    # TODO(sang): Fix it.
    # wait_for_condition(lambda: not node.any_processes_alive())


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
