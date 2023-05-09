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

def get_owner_info(node_ids):
    node_addrs = {n["NodeID"] : (n['NodeManagerAddress'], n['NodeManagerPort']) for n in ray.nodes()}
    import gc
    gc.collect()
    import time
    time.sleep(1)
    owner_stats = {n : 0 for n in node_ids}
    primary_copy_stats = {n : 0 for n in node_ids}

    for node_id in node_ids:
        node_stats = ray._private.internal_api.node_stats(node_addrs[node_id][0], node_addrs[node_id][1], False, True)
        for owner_stat in node_stats.store_stats.owner_stats:
            owner_id = ray.NodeID(owner_stat.owner_id).hex()
            owner_stats[owner_id] += owner_stat.num_object_ids
        print(node_id, node_stats.core_workers_stats)
        primary_copy_stats[node_id] = node_stats.store_stats.object_store_primary_copies_total

    print(owner_stats)
    print(node_ids)
    owner_stats = [owner_stats.get(node_id, 0) for node_id in node_ids]
    primary_copy_stats = [primary_copy_stats.get(node_id, 0) for node_id in node_ids]
    print("owner_stats", owner_stats)
    print("primary_copy_stats", primary_copy_stats)

    return owner_stats, primary_copy_stats   

def test_node_object_metrics(ray_start_cluster, monkeypatch):
    monkeypatch.setenv("RAY_lineage_pinning_enabled", "0")
    NUM_NODES = 3
    cluster = ray_start_cluster
    for i in range(NUM_NODES):
        cluster.add_node(True, resources={f"node_{i}": 1})
        if i == 0:
            ray.init(address=cluster.address)
    node_ids = []

    for i in range(NUM_NODES):
        @ray.remote(resources={f"node_{i}": 1})
        def get_node_id():
            return ray.get_runtime_context().get_node_id()
        node_ids.append(ray.get(get_node_id.remote()))    
    
    
    # Object store stats
    # x is owned by node_0
    # x is stored at node_0
    x = ray.put([1] * 1024 * 1024 * 2)
    
    @ray.remote(resources={"node_1":1})
    def big_obj():
        return ray.put([1] * 1024 * 1024 * 10)
    # Object store stats
    # big_obj is owned by node_1
    # big_obj is stored at node_1
    big_obj_ref = big_obj.remote()
    ray.get(big_obj_ref)

    @ray.remote(resources={"node_2":1})
    class A:
        def gen(self, s):
            return [1] * s
        def owned_by_a(self, s):
            return ray.put([1] * s)
    a = A.remote()
    # Object store stats
    # a_obj is owned by node_0
    # a_obj is stored at node_2
    a_obj = a.gen.remote(1024 * 1024 * 10)
    ray.get(a_obj)

    # b is owned by node_2
    # b is stored at node_2
    b = a.owned_by_a.remote(1024 * 1024 * 10)
    import time

    ray.get(b)
    @ray.remote(resources={"node_0":1})
    def sleep():
        time.sleep(100000)
    # s is not available
    s = sleep.remote()
    print(">>>SLEEP: ", s)
    @ray.remote(resources={"node_1":1})
    def pass_obj(x):
        return ray.get(x)
    p = pass_obj.remote([s])
    time.sleep(1)

    def check():
        owner_stats, primary_copy_stats = get_owner_info(node_ids)
        assert owner_stats == [2, 1, 1]
        assert primary_copy_stats == [1, 1, 2]
        return True
    wait_for_condition(check)


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


def test_opentelemetry_conflict(shutdown_only):
    ray.init()
    # If opencensus protobuf doesn't conflict, this shouldn't raise an exception.
    # Otherwise, it raises an error saying
    # opencensus/proto/resource/v1/resource.proto:
    # A file with this name is already in the pool.
    from opentelemetry.exporter.opencensus.trace_exporter import (  # noqa
        OpenCensusSpanExporter,
    )

    # Make sure the similar resource protobuf also doesn't raise an exception.
    from opentelemetry.proto.resource.v1 import resource_pb2  # noqa


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
