import sys
import time

import ray

import pytest
import grpc
from grpc._channel import _InactiveRpcError
import numpy as np
import psutil
import subprocess

import ray.ray_constants as ray_constants

from ray.cluster_utils import Cluster, cluster_not_supported
from ray import NodeID
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray._private.test_utils import (
    init_error_pubsub,
    get_error_message,
    run_string_as_driver,
    wait_for_condition,
)
from ray.exceptions import LocalRayletDiedError
import ray.experimental.internal_kv as internal_kv


def search_raylet(cluster):
    """Return the number of running processes."""
    raylets = []
    for node in cluster.list_all_nodes():
        procs = node.all_processes
        raylet_proc_info = procs.get(ray_constants.PROCESS_TYPE_RAYLET)
        if raylet_proc_info:
            assert len(raylet_proc_info) == 1
            raylet = psutil.Process(raylet_proc_info[0].process.pid)
            if raylet.status() == "running":
                raylets.append(psutil.Process(raylet_proc_info[0].process.pid))
    return raylets


def test_retry_system_level_error(ray_start_regular):
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    @ray.remote(max_retries=1)
    def func(counter):
        count = counter.increment.remote()
        if ray.get(count) == 1:
            import os

            os._exit(0)
        else:
            return 1

    counter1 = Counter.remote()
    r1 = func.remote(counter1)
    assert ray.get(r1) == 1

    counter2 = Counter.remote()
    r2 = func.options(max_retries=0).remote(counter2)
    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(r2)


def test_retry_application_level_error(ray_start_regular):
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    @ray.remote(max_retries=1, retry_exceptions=True)
    def func(counter):
        count = counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError()
        else:
            return 2

    counter1 = Counter.remote()
    r1 = func.remote(counter1)
    assert ray.get(r1) == 2

    counter2 = Counter.remote()
    r2 = func.options(max_retries=0).remote(counter2)
    with pytest.raises(ValueError):
        ray.get(r2)

    counter3 = Counter.remote()
    r3 = func.options(retry_exceptions=False).remote(counter3)
    with pytest.raises(ValueError):
        ray.get(r3)


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
def test_connect_with_disconnected_node(shutdown_only):
    config = {
        "num_heartbeats_timeout": 50,
        "raylet_heartbeat_period_milliseconds": 10,
    }
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _system_config=config)
    ray.init(address=cluster.address)
    p = init_error_pubsub()
    errors = get_error_message(p, 1, timeout=5)
    assert len(errors) == 0
    # This node will be killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR)
    assert len(errors) == 1
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR)
    assert len(errors) == 1
    # This node is killed by SIGTERM, ray_monitor will not mark it again.
    removing_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(removing_node, allow_graceful=True)
    errors = get_error_message(p, 1, timeout=2)
    assert len(errors) == 0
    # There is no connection error to a dead node.
    errors = get_error_message(p, 1, timeout=2)
    assert len(errors) == 0
    p.close()


def test_detached_actor_ref(call_ray_start):
    address = call_ray_start

    driver_script = """
import ray
import time


@ray.remote
def foo(x):
    return ray.put(42)


@ray.remote
class Actor:
    def __init__(self):
        self.ref = None

    def invoke(self):
        self.ref = foo.remote(0)
        # Wait for the task to finish before exiting the driver.
        ray.get(self.ref)

    def get(self):
        print("get", self.ref)
        return self.ref


if __name__ == "__main__":
    ray.init(address="{}", namespace="default")
    a = Actor.options(name="holder", lifetime="detached").remote()
    # Wait for the task to finish before exiting the driver.
    ray.get(a.invoke.remote())
    print("success")
""".format(
        address
    )

    out = run_string_as_driver(driver_script)
    assert "success" in out

    import time

    time.sleep(5)

    # connect to the cluster
    ray.init(address=address, namespace="default")
    actor = ray.get_actor("holder")
    x = actor.get.remote()
    while isinstance(x, ray.ObjectRef):
        x = ray.get(x)
    assert x == 42


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("debug_enabled", [False, True])
def test_object_lost_error(ray_start_cluster, debug_enabled):
    cluster = ray_start_cluster
    system_config = {
        "num_heartbeats_timeout": 3,
    }
    if debug_enabled:
        system_config["record_ref_creation_sites"] = True
    cluster.add_node(num_cpus=0, _system_config=system_config)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            return

        def foo(self):
            return "x" * 1000_000

        def done(self):
            return

    @ray.remote
    def borrower(ref):
        ray.get(ref[0])

    @ray.remote
    def task_arg(ref):
        return

    a = Actor.remote()
    x = a.foo.remote()
    ray.get(a.done.remote())
    cluster.remove_node(worker_node, allow_graceful=False)
    cluster.add_node(num_cpus=1)

    y = borrower.remote([x])

    try:
        ray.get(x)
        assert False
    except ray.exceptions.ObjectLostError as e:
        error = str(e)
        print(error)
        assert ("actor call" in error) == debug_enabled
        assert ("test_object_lost_error" in error) == debug_enabled

    try:
        ray.get(y)
        assert False
    except ray.exceptions.RayTaskError as e:
        error = str(e)
        print(error)
        assert ("actor call" in error) == debug_enabled
        assert ("test_object_lost_error" in error) == debug_enabled

    try:
        ray.get(task_arg.remote(x))
    except ray.exceptions.RayTaskError as e:
        error = str(e)
        print(error)
        assert ("actor call" in error) == debug_enabled
        assert ("test_object_lost_error" in error) == debug_enabled


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 0,
            "_system_config": {
                "num_heartbeats_timeout": 10,
                "raylet_heartbeat_period_milliseconds": 100,
            },
        }
    ],
    indirect=True,
)
def test_raylet_graceful_shutdown_through_rpc(ray_start_cluster_head, error_pubsub):
    """
    Prepare the cluster.
    """
    cluster = ray_start_cluster_head
    head_node_port = None
    for n in ray.nodes():
        head_node_port = int(n["NodeManagerPort"])
    worker = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    worker_node_port = None
    for n in ray.nodes():
        port = int(n["NodeManagerPort"])
        if port != head_node_port and n["alive"]:
            worker_node_port = port
    """
    warm up the cluster
    """

    @ray.remote
    def f():
        pass

    ray.get(f.remote())

    # Kill a raylet gracefully.
    def kill_raylet(ip, port, graceful=True):
        raylet_address = f"{ip}:{port}"
        channel = grpc.insecure_channel(raylet_address)
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        print(f"Sending a shutdown request to {ip}:{port}")
        try:
            stub.ShutdownRaylet(
                node_manager_pb2.ShutdownRayletRequest(graceful=graceful)
            )
        except _InactiveRpcError:
            assert not graceful

    """
    Kill the first worker non-gracefully.
    """
    ip = worker.node_ip_address
    kill_raylet(ip, worker_node_port, graceful=False)
    p = error_pubsub
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR, timeout=10)
    # Should print the heartbeat messages.
    assert "has missed too many heartbeats from it" in errors[0].error_message
    # NOTE the killed raylet is a zombie since the
    # parent process (the pytest script) hasn't called wait syscall.
    # For normal scenarios where raylet is created by
    # ray start, this issue won't exist.
    try:
        wait_for_condition(lambda: len(search_raylet(cluster)) == 1)
    except Exception:
        print("More than one raylets are detected.")
        print(search_raylet(cluster))
    """
    Kill the second worker gracefully.
    """
    worker = cluster.add_node(num_cpus=0)
    worker_node_port = None
    for n in ray.nodes():
        port = int(n["NodeManagerPort"])
        if port != head_node_port and n["alive"]:
            worker_node_port = port
    # Kill the second worker gracefully.
    ip = worker.node_ip_address
    kill_raylet(ip, worker_node_port, graceful=True)
    p = error_pubsub
    # Error shouldn't be printed to the driver.
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR, timeout=5)
    # Error messages shouldn't be published.
    assert len(errors) == 0
    try:
        wait_for_condition(lambda: len(search_raylet(cluster)) == 1)
    except Exception:
        print("More than one raylets are detected.")
        print(search_raylet(cluster))
    """
    Make sure head node is not dead.
    """
    ray.get(f.options(num_cpus=0).remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 0,
            "_system_config": {
                "num_heartbeats_timeout": 10,
                "raylet_heartbeat_period_milliseconds": 100,
            },
        }
    ],
    indirect=True,
)
def test_gcs_drain(ray_start_cluster_head, error_pubsub):
    """
    Prepare the cluster.
    """
    cluster = ray_start_cluster_head
    head_node_id = ray.nodes()[0]["NodeID"]
    NUM_NODES = 2
    for _ in range(NUM_NODES):
        cluster.add_node(num_cpus=1)
    worker_node_ids = []
    for n in ray.nodes():
        if n["NodeID"] != head_node_id:
            worker_node_ids.append(n["NodeID"])
    """
    Warm up the cluster.
    """

    @ray.remote(num_cpus=1)
    class A:
        def ready(self):
            pass

    actors = [A.remote() for _ in range(NUM_NODES)]
    ray.get([actor.ready.remote() for actor in actors])
    """
    Test batch drain.
    """
    # Prepare requests.
    gcs_server_addr = cluster.gcs_address
    options = (("grpc.enable_http_proxy", 0),)
    channel = grpc.insecure_channel(gcs_server_addr, options)
    stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(channel)
    r = gcs_service_pb2.DrainNodeRequest()
    for worker_id in worker_node_ids:
        data = r.drain_node_data.add()
        data.node_id = NodeID.from_hex(worker_id).binary()
    stub.DrainNode(r)

    p = error_pubsub
    # Error shouldn't be printed to the driver.
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR, timeout=5)
    assert len(errors) == 0
    # There should be only a head node since we drained worker nodes.
    # NOTE: In the current implementation we kill nodes when draining them.
    # This check should be removed once we implement
    # the proper drain behavior.
    try:
        wait_for_condition(lambda: len(search_raylet(cluster)) == 1)
    except Exception:
        print("More than one raylets are detected.")
        print(search_raylet(cluster))
    """
    Make sure the API is idempotent.
    """
    for _ in range(10):
        stub.DrainNode(r)
    p = error_pubsub
    # Error shouldn't be printed to the driver.
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR, timeout=5)
    assert len(errors) == 0
    """
    Make sure the GCS states are updated properly.
    """
    for n in ray.nodes():
        node_id = n["NodeID"]
        is_alive = n["Alive"]
        if node_id == head_node_id:
            assert is_alive
        if node_id in worker_node_ids:
            assert not is_alive
    """
    Make sure head node is not dead and functional.
    """
    a = A.options(num_cpus=0).remote()
    ray.get(a.ready.remote())


def test_worker_start_timeout(monkeypatch, ray_start_cluster):
    # This test is to make sure
    #   1. when worker failed to register, raylet will print useful log
    #   2. raylet will kill hanging worker
    with monkeypatch.context() as m:
        # this delay will make worker start slow
        m.setenv(
            "RAY_testing_asio_delay_us",
            "InternalKVGcsService.grpc_server.InternalKVGet=2000000:2000000",
        )
        m.setenv("RAY_worker_register_timeout_seconds", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=4, object_store_memory=1e9)
        script = """
import ray
ray.init(address='auto')

@ray.remote
def task():
    return None

ray.get(task.remote(), timeout=3)
"""
        with pytest.raises(subprocess.CalledProcessError) as e:
            run_string_as_driver(script)

        # make sure log is correct
        assert (
            "The process is still alive, probably it's hanging during start"
        ) in e.value.output.decode()
        # worker will be killed so it won't try to register to raylet
        assert (
            "Received a register request from an unknown worker shim process"
        ) not in e.value.output.decode()


def test_task_failure_when_driver_local_raylet_dies(ray_start_cluster):
    cluster = ray_start_cluster
    head = cluster.add_node(num_cpus=4, resources={"foo": 1})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote(resources={"foo": 1})
    def func():
        internal_kv._internal_kv_put("test_func", "func")
        while True:
            time.sleep(1)

    func.remote()
    while not internal_kv._internal_kv_exists("test_func"):
        time.sleep(0.1)

    # The lease request should wait inside raylet
    # since there is no available resources.
    ret = func.remote()
    # Waiting for the lease request to reach raylet.
    time.sleep(1)
    head.kill_raylet()
    with pytest.raises(LocalRayletDiedError):
        ray.get(ret)


def test_locality_aware_scheduling_for_dead_nodes(shutdown_only):
    """Test that locality-ware scheduling can handle dead nodes."""
    # Create a cluster with 4 nodes.
    config = {
        "num_heartbeats_timeout": 5,
        "raylet_heartbeat_period_milliseconds": 50,
    }
    cluster = Cluster()
    cluster.add_node(num_cpus=4, resources={"node1": 1}, _system_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    node2 = cluster.add_node(num_cpus=4, resources={"node2": 1})
    node3 = cluster.add_node(num_cpus=4, resources={"node3": 1})
    node4 = cluster.add_node(num_cpus=4, resources={"node4": 1})
    cluster.wait_for_nodes()

    # Create 2 objects on node 2.
    @ray.remote(resources={"node2": 0.1})
    def create_object():
        return np.zeros(10 * 1024 * 1024, dtype=np.uint8)

    obj1 = create_object.remote()
    obj2 = create_object.remote()

    # Push these 2 objects to other nodes.
    # node2 will have obj1 and obj2.
    # node3 will have obj1.
    # node4 will have obj2.
    @ray.remote
    class MyActor:
        def __init__(self, obj_refs):
            # Note, we need to keep obj_refs to prevent the objects from
            # being garbage collected.
            self.obj_refs = obj_refs
            self.obj = ray.get(obj_refs)

        def ready(self):
            return True

    actors = [
        MyActor.options(resources={"node2": 0.1}).remote([obj1, obj2]),
        MyActor.options(resources={"node3": 0.1}).remote([obj1]),
        MyActor.options(resources={"node4": 0.1}).remote([obj2]),
    ]

    assert all(ray.get(actor.ready.remote()) is True for actor in actors)

    # This function requires obj1 and obj2.
    @ray.remote
    def func(obj1, obj2):
        return ray.worker.global_worker.node.unique_id

    # This function should be scheduled to node2. As node2 has both objects.
    assert ray.get(func.remote(obj1, obj2)) == node2.unique_id

    # Kill node2, and re-schedule the function.
    # It should be scheduled to either node3 or node4.
    node2.kill_raylet()
    # Waits for the driver to receive the NodeRemoved notification.
    time.sleep(1)
    target_node = ray.get(func.remote(obj1, obj2))
    assert target_node == node3.unique_id or target_node == node4.unique_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
