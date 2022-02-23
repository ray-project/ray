import os
import grpc
import requests
import time

import ray
from ray.core.generated import common_pb2
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray._private.test_utils import (
    RayTestTimeoutException,
    wait_until_succeeded_without_exception,
)
from ray._private.utils import init_grpc_channel

import psutil  # We must import psutil after ray because we bundle it with ray.


def test_worker_stats(shutdown_only):
    ray.init(num_cpus=1, include_dashboard=True)
    raylet = ray.nodes()[0]
    num_cpus = raylet["Resources"]["CPU"]
    raylet_address = "{}:{}".format(
        raylet["NodeManagerAddress"], ray.nodes()[0]["NodeManagerPort"]
    )

    channel = init_grpc_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)

    def try_get_node_stats(num_retry=5, timeout=2):
        reply = None
        for _ in range(num_retry):
            try:
                reply = stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(), timeout=timeout
                )
                break
            except grpc.RpcError:
                continue
        assert reply is not None
        return reply

    reply = try_get_node_stats()
    # Check that there is one connected driver.
    drivers = [
        worker
        for worker in reply.core_workers_stats
        if worker.worker_type == common_pb2.DRIVER
    ]
    assert len(drivers) == 1
    assert os.getpid() == drivers[0].pid

    @ray.remote
    def f():
        ray.worker.show_in_dashboard("test")
        return os.getpid()

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray.worker.show_in_dashboard("test")
            return os.getpid()

    # Test show_in_dashboard for remote functions.
    worker_pid = ray.get(f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for stats in reply.core_workers_stats:
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
            assert stats.pid == worker_pid
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    # Test show_in_dashboard for remote actors.
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for stats in reply.core_workers_stats:
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
            assert stats.pid == worker_pid
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    timeout_seconds = 20
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for worker processes"
            )

        # Wait for the workers to start.
        if len(reply.core_workers_stats) < num_cpus + 1:
            time.sleep(1)
            reply = try_get_node_stats()
            continue

        # Check that the rest of the processes are workers, 1 for each CPU.
        assert len(reply.core_workers_stats) == num_cpus + 1
        # Check that all processes are Python.
        pids = [worker.pid for worker in reply.core_workers_stats]
        processes = [
            p.info["name"]
            for p in psutil.process_iter(attrs=["pid", "name"])
            if p.info["pid"] in pids
        ]
        for process in processes:
            # TODO(ekl) why does travis/mi end up in the process list
            assert (
                "python" in process
                or "mini" in process
                or "conda" in process
                or "travis" in process
                or "runner" in process
                or "ray" in process
            )
        break


def test_multi_node_metrics_export_port_discovery(ray_start_cluster):
    NUM_NODES = 3
    cluster = ray_start_cluster
    nodes = [cluster.add_node() for _ in range(NUM_NODES)]
    nodes = {
        node.address_info["metrics_export_port"]: node.address_info for node in nodes
    }
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    node_info_list = ray.nodes()

    for node_info in node_info_list:
        metrics_export_port = node_info["MetricsExportPort"]
        address_info = nodes[metrics_export_port]
        assert address_info["raylet_socket_name"] == node_info["RayletSocketName"]

        # Make sure we can ping Prometheus endpoints.
        def test_prometheus_endpoint():
            response = requests.get(
                "http://localhost:{}".format(metrics_export_port),
                # Fail the request early on if connection timeout
                timeout=0.01,
            )
            return response.status_code == 200

        assert wait_until_succeeded_without_exception(
            test_prometheus_endpoint,
            (requests.exceptions.ConnectionError,),
            # The dashboard takes more than 2s to startup.
            timeout_ms=5000,
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
