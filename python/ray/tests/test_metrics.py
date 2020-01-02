from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import grpc
import psutil
import requests
import time

import ray
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.test_utils import RayTestTimeoutException


def test_worker_stats(shutdown_only):
    ray.init(num_cpus=1, include_webui=False)
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
    class Actor(object):
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
        if stats.webui_display == "test":
            target_worker_present = True
            assert worker.pid == worker_pid
        else:
            assert stats.webui_display == ""
    assert target_worker_present

    # Test show_in_webui for remote actors.
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for worker in reply.workers_stats:
        stats = worker.core_worker_stats
        if stats.webui_display == "test":
            target_worker_present = True
            assert worker.pid == worker_pid
        else:
            assert stats.webui_display == ""
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


def test_raylet_info_endpoint(shutdown_only):
    addresses = ray.init(include_webui=True, num_cpus=6)

    @ray.remote(num_cpus=1)
    class A(object):
        def __init__(self):
            pass

        def f(self):
            return os.getpid()

    @ray.remote(num_cpus=2)
    class B(object):
        def __init__(self):
            self.children = [A.remote(), A.remote()]

        def f(self):
            return os.getpid(), ray.get(
                [child.f.remote() for child in self.children])

    # TODO: Currently there is a race condition of Dashboard subscription
    # and actor initialization. This will be fixed after #6629 is merged.
    time.sleep(10)

    b = B.remote()
    pids = ray.get(b.f.remote())
    assert len(pids) == 2 and len(pids[1]) == 2

    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            webui_url = addresses["webui_url"]
            webui_url = webui_url.replace("localhost", "http://127.0.0.1")
            raylet_info = requests.get(webui_url + "/api/raylet_info").json()
            actor_info = raylet_info["result"]["actorInfo"]
            try:
                assert len(actor_info) == 1
                print("actor_info", actor_info)
                _, parent_actor_info = actor_info.popitem()
                children = parent_actor_info["children"]
                assert len(children) == 2
                break
            except AssertionError:
                if time.time() > start_time + 30:
                    raise Exception(
                        "Timed out while waiting for actorInfo to show up.")
        except requests.exceptions.ConnectionError:
            if time.time() > start_time + 30:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")

    assert parent_actor_info["usedResources"]["CPU"] == 2
    for _, child_actor_info in children.items():
        assert len(child_actor_info["children"]) == 0
        assert child_actor_info["usedResources"]["CPU"] == 1


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
