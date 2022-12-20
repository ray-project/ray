import os
import platform

import psutil  # We must import psutil after ray because we bundle it with ray.
import pytest
import requests

import ray
from ray._private.test_utils import (
    wait_for_condition,
    wait_until_succeeded_without_exception,
    get_node_stats,
)
from ray.core.generated import common_pb2

_WIN32 = os.name == "nt"


@pytest.mark.skipif(platform.system() == "Windows", reason="Hangs on Windows.")
def test_worker_stats(shutdown_only):
    ray.init(num_cpus=2, include_dashboard=True)
    raylet = ray.nodes()[0]
    reply = get_node_stats(raylet)
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
        ray._private.worker.show_in_dashboard("test")
        return os.getpid()

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray._private.worker.show_in_dashboard("test")
            return os.getpid()

    # Test show_in_dashboard for remote functions.
    worker_pid = ray.get(f.remote())
    reply = get_node_stats(raylet)
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
    reply = get_node_stats(raylet)
    target_worker_present = False
    for stats in reply.core_workers_stats:
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    # 1 actor + 1 worker for task + 1 driver
    num_workers = 3

    def verify():
        reply = get_node_stats(raylet)
        # Check that the rest of the processes are workers, 1 for each CPU.

        assert len(reply.core_workers_stats) == num_workers
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
                or "pytest" in process
                or "ray" in process
            ), process

        return True

    wait_for_condition(verify)


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
                timeout=1.0,
            )
            return response.status_code == 200

        assert wait_until_succeeded_without_exception(
            test_prometheus_endpoint,
            (requests.exceptions.ConnectionError,),
            # The dashboard takes more than 2s to startup.
            timeout_ms=10 * 1000,
        )


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
