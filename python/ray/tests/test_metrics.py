import os
import json
import grpc
import pytest
import requests
import time

import ray
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.test_utils import (RayTestTimeoutException,
                            wait_until_succeeded_without_exception,
                            wait_until_server_available)

import psutil  # We must import psutil after ray because we bundle it with ray.


def test_worker_stats(shutdown_only):
    addresses = ray.init(num_cpus=1, include_webui=True)
    raylet = ray.nodes()[0]
    num_cpus = raylet["Resources"]["CPU"]
    raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                    ray.nodes()[0]["NodeManagerPort"])

    channel = grpc.insecure_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)

    def try_get_node_stats(num_retry=5, timeout=2):
        reply = None
        for _ in range(num_retry):
            try:
                reply = stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(), timeout=timeout)
                break
            except grpc.RpcError:
                continue
        assert reply is not None
        return reply

    reply = try_get_node_stats()
    # Check that there is one connected driver.
    drivers = [worker for worker in reply.workers_stats if worker.is_driver]
    assert len(drivers) == 1
    assert os.getpid() == drivers[0].pid

    @ray.remote
    def f():
        ray.show_in_webui("test")
        return os.getpid()

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray.show_in_webui("test")
            return os.getpid()

    # Test show_in_webui for remote functions.
    worker_pid = ray.get(f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for worker in reply.workers_stats:
        stats = worker.core_worker_stats
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
            assert worker.pid == worker_pid
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    # Test show_in_webui for remote actors.
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for worker in reply.workers_stats:
        stats = worker.core_worker_stats
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
            assert worker.pid == worker_pid
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    timeout_seconds = 20
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for worker processes")

        # Wait for the workers to start.
        if len(reply.workers_stats) < num_cpus + 1:
            time.sleep(1)
            reply = try_get_node_stats()
            continue

        # Check that the rest of the processes are workers, 1 for each CPU.
        assert len(reply.workers_stats) == num_cpus + 1
        views = [view.view_name for view in reply.view_data]
        assert "redis_latency" in views
        assert "local_available_resource" in views
        # Check that all processes are Python.
        pids = [worker.pid for worker in reply.workers_stats]
        processes = [
            p.info["name"] for p in psutil.process_iter(attrs=["pid", "name"])
            if p.info["pid"] in pids
        ]
        for process in processes:
            # TODO(ekl) why does travis/mi end up in the process list
            assert ("python" in process or "ray" in process
                    or "travis" in process)
        break

    # Test kill_actor.
    def actor_killed(PID):
        """Check For the existence of a unix pid."""
        try:
            os.kill(PID, 0)
        except OSError:
            return True
        else:
            return False

    assert (wait_until_server_available(addresses["webui_url"]) is True)

    webui_url = addresses["webui_url"]
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")
    for worker in reply.workers_stats:
        if worker.is_driver:
            continue
        requests.get(
            webui_url + "/api/kill_actor",
            params={
                "actor_id": ray.utils.binary_to_hex(
                    worker.core_worker_stats.actor_id),
                "ip_address": worker.core_worker_stats.ip_address,
                "port": worker.core_worker_stats.port
            })
    timeout_seconds = 20
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException("Timed out while killing actors")
        if all(
                actor_killed(worker.pid) for worker in reply.workers_stats
                if not worker.is_driver):
            break


def test_raylet_info_endpoint(shutdown_only):
    addresses = ray.init(include_webui=True, num_cpus=6)

    @ray.remote
    def f():
        return "test"

    @ray.remote(num_cpus=1)
    class ActorA:
        def __init__(self):
            pass

    @ray.remote(resources={"CustomResource": 1})
    class ActorB:
        def __init__(self):
            pass

    @ray.remote(num_cpus=2)
    class ActorC:
        def __init__(self):
            self.children = [ActorA.remote(), ActorB.remote()]

        def local_store(self):
            self.local_storage = [f.remote() for _ in range(10)]

        def remote_store(self):
            self.remote_storage = ray.put("test")

        def getpid(self):
            return os.getpid()

    c = ActorC.remote()
    actor_pid = ray.get(c.getpid.remote())
    c.local_store.remote()
    c.remote_store.remote()

    assert (wait_until_server_available(addresses["webui_url"]) is True)

    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            webui_url = addresses["webui_url"]
            webui_url = webui_url.replace("localhost", "http://127.0.0.1")
            response = requests.get(webui_url + "/api/raylet_info")
            response.raise_for_status()
            try:
                raylet_info = response.json()
            except Exception as ex:
                print("failed response: {}".format(response.text))
                raise ex
            actor_info = raylet_info["result"]["actors"]
            try:
                assert len(actor_info) == 1
                _, parent_actor_info = actor_info.popitem()
                assert parent_actor_info["numObjectIdsInScope"] == 13
                assert parent_actor_info["numLocalObjects"] == 10
                children = parent_actor_info["children"]
                assert len(children) == 2
                break
            except AssertionError:
                if time.time() > start_time + 30:
                    raise Exception("Timed out while waiting for actor info \
                        or object store info update.")
        except requests.exceptions.ConnectionError:
            if time.time() > start_time + 30:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")

    assert parent_actor_info["usedResources"]["CPU"] == 2
    assert parent_actor_info["numExecutedTasks"] == 4
    for _, child_actor_info in children.items():
        if child_actor_info["state"] == -1:
            assert child_actor_info["requiredResources"]["CustomResource"] == 1
        else:
            assert child_actor_info["state"] == 1
            assert len(child_actor_info["children"]) == 0
            assert child_actor_info["usedResources"]["CPU"] == 1

    profiling_id = requests.get(
        webui_url + "/api/launch_profiling",
        params={
            "node_id": ray.nodes()[0]["NodeID"],
            "pid": actor_pid,
            "duration": 5
        }).json()["result"]
    start_time = time.time()
    while True:
        # Sometimes some startup time is required
        if time.time() - start_time > 30:
            raise RayTestTimeoutException(
                "Timed out while collecting profiling stats.")
        profiling_info = requests.get(
            webui_url + "/api/check_profiling_status",
            params={
                "profiling_id": profiling_id,
            }).json()
        status = profiling_info["result"]["status"]
        assert status in ("finished", "pending", "error")
        if status in ("finished", "error"):
            break
        time.sleep(1)


def test_raylet_infeasible_tasks(shutdown_only):
    """
    This test creates an actor that requires 5 GPUs
    but a ray cluster only has 3 GPUs. As a result,
    the new actor should be an infeasible actor.
    """
    addresses = ray.init(num_gpus=3)

    @ray.remote(num_gpus=5)
    class ActorRequiringGPU:
        def __init__(self):
            pass

    ActorRequiringGPU.remote()

    def test_infeasible_actor(ray_addresses):
        assert (wait_until_server_available(addresses["webui_url"]) is True)
        webui_url = ray_addresses["webui_url"].replace("localhost",
                                                       "http://127.0.0.1")
        raylet_info = requests.get(webui_url + "/api/raylet_info").json()
        actor_info = raylet_info["result"]["actors"]
        assert len(actor_info) == 1

        _, infeasible_actor_info = actor_info.popitem()
        assert infeasible_actor_info["state"] == -1
        assert infeasible_actor_info["invalidStateType"] == "infeasibleActor"

    assert (wait_until_succeeded_without_exception(
        test_infeasible_actor,
        (AssertionError, requests.exceptions.ConnectionError),
        addresses,
        timeout_ms=30000,
        retry_interval_ms=1000) is True)


def test_raylet_pending_tasks(shutdown_only):
    # Make sure to specify num_cpus. Otherwise, the test can be broken
    # when the number of cores is less than the number of spawned actors.
    addresses = ray.init(num_gpus=3, num_cpus=4)

    @ray.remote(num_gpus=1)
    class ActorRequiringGPU:
        def __init__(self):
            pass

    @ray.remote
    class ParentActor:
        def __init__(self):
            self.a = [ActorRequiringGPU.remote() for i in range(4)]

    # If we do not get ParentActor actor handler, reference counter will
    # terminate ParentActor.
    parent_actor = ParentActor.remote()
    assert parent_actor is not None

    def test_pending_actor(ray_addresses):
        assert (wait_until_server_available(addresses["webui_url"]) is True)
        webui_url = ray_addresses["webui_url"].replace("localhost",
                                                       "http://127.0.0.1")
        raylet_info = requests.get(webui_url + "/api/raylet_info").json()
        actor_info = raylet_info["result"]["actors"]
        assert len(actor_info) == 1
        _, infeasible_actor_info = actor_info.popitem()

        # Verify there are 4 spawned actors.
        children = infeasible_actor_info["children"]
        assert len(children) == 4

        pending_actor_detected = 0
        for child_id, child in children.items():
            if ("invalidStateType" in child
                    and child["invalidStateType"] == "pendingActor"):
                pending_actor_detected += 1
        # 4 GPUActors are spawned although there are only 3 GPUs.
        # One actor should be in the pending state.
        assert pending_actor_detected == 1

    assert (wait_until_succeeded_without_exception(
        test_pending_actor,
        (AssertionError, requests.exceptions.ConnectionError),
        addresses,
        timeout_ms=30000,
        retry_interval_ms=1000) is True)


@pytest.mark.skipif(
    os.environ.get("TRAVIS") is None,
    reason="This test requires password-less sudo due to py-spy requirement.")
def test_profiling_info_endpoint(shutdown_only):
    ray.init(num_cpus=1)

    redis_client = ray.worker.global_worker.redis_client

    node_ip = ray.nodes()[0]["NodeManagerAddress"]

    while True:
        reporter_port = redis_client.get("REPORTER_PORT:{}".format(node_ip))
        if reporter_port:
            break

    reporter_channel = grpc.insecure_channel("{}:{}".format(
        node_ip, int(reporter_port)))
    reporter_stub = reporter_pb2_grpc.ReporterServiceStub(reporter_channel)

    @ray.remote(num_cpus=1)
    class ActorA:
        def __init__(self):
            pass

        def getpid(self):
            return os.getpid()

    a = ActorA.remote()
    actor_pid = ray.get(a.getpid.remote())

    reply = reporter_stub.GetProfilingStats(
        reporter_pb2.GetProfilingStatsRequest(pid=actor_pid, duration=10))
    profiling_stats = json.loads(reply.profiling_stats)
    assert profiling_stats is not None


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
