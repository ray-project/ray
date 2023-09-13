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

    @ray.remote(num_cpus=1)
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
    node_addrs = {
        n["NodeID"]: (n["NodeManagerAddress"], n["NodeManagerPort"])
        for n in ray.nodes()
    }
    # Force a global gc to clean up the object store.
    ray._private.internal_api.global_gc()
    owner_stats = {n: 0 for n in node_ids}
    primary_copy_stats = {n: 0 for n in node_ids}

    for node_id in node_ids:
        node_stats = ray._private.internal_api.node_stats(
            node_addrs[node_id][0], node_addrs[node_id][1], False
        )
        num_owned_objects = sum(
            [stats.num_owned_objects for stats in node_stats.core_workers_stats]
        )
        num_owned_actors = sum(
            [stats.num_owned_actors for stats in node_stats.core_workers_stats]
        )
        owner_stats[node_id] = (num_owned_objects, num_owned_actors)
        primary_copy_stats[
            node_id
        ] = node_stats.store_stats.num_object_store_primary_copies

    print(owner_stats)
    print(node_ids)
    owner_stats = [owner_stats.get(node_id, 0) for node_id in node_ids]
    primary_copy_stats = [primary_copy_stats.get(node_id, 0) for node_id in node_ids]
    print("owner_stats", owner_stats)
    print("primary_copy_stats", primary_copy_stats)

    return owner_stats, primary_copy_stats


def test_node_object_metrics(ray_start_cluster):
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
    x = ray.put([1])  # noqa: F841
    wait_for_condition(
        lambda: get_owner_info(node_ids) == ([(1, 0), (0, 0), (0, 0)], [1, 0, 0])
    )

    # Test nested with put
    @ray.remote(resources={"node_1": 1})
    def big_obj():
        # b is owned by node_1
        # b is stored at node_1
        b = ray.put([1] * 1024 * 1024 * 10)
        return b

    # Object store stats
    # big_obj is owned by node_0
    # big_obj is stored in memory (no primary copy)
    big_obj_ref = big_obj.remote()  # noqa: F841
    wait_for_condition(
        lambda: get_owner_info(node_ids) == ([(2, 0), (1, 0), (0, 0)], [1, 1, 0])
    )

    # Test nested with task (small output)
    @ray.remote(resources={"node_1": 1})
    def nest_task(s):
        @ray.remote(resources={"node_2": 1})
        def task():
            return [1] * s

        # t is owned by node_1
        # if s is small,
        # then it's is stored in memory of node_1 (no primary copy)
        # else it's stored in object store of node_1
        t = task.remote()
        return t

    # nest_ref is owned by node_0
    # nest_ref is stored in memory (no primary copy)
    nest_ref = nest_task.remote(1)  # noqa: F841
    wait_for_condition(
        lambda: get_owner_info(node_ids) == ([(3, 0), (2, 0), (0, 0)], [1, 1, 0])
    )

    big_nest = nest_task.remote(1024 * 1024 * 10)  # noqa: F841

    wait_for_condition(
        lambda: get_owner_info(node_ids) == ([(4, 0), (3, 0), (0, 0)], [1, 1, 1])
    )

    # Test with assigned owned
    @ray.remote(resources={"node_2": 0.5}, num_cpus=0)
    class A:
        def ready(self):
            return

        def gen(self):
            return ray.put(10)

    # actor is owned by node_0
    # actor is not an object, so no object store copies
    actor = A.remote()  # noqa: F841
    ray.get(actor.ready.remote())
    # o is owned by actor (node_2)
    # o is stored in object store of node_0
    o = ray.put(1, _owner=actor)  # noqa: F841
    wait_for_condition(
        lambda: get_owner_info(node_ids) == ([(4, 1), (3, 0), (1, 0)], [2, 1, 1])
    )

    # Test with detached owned
    # detached actor is owned by GCS. So it's not counted in the owner stats
    detached_actor = A.options(lifetime="detached", name="A").remote()
    ray.get(detached_actor.ready.remote())
    for i in range(3):
        assert get_owner_info(node_ids) == ([(4, 1), (3, 0), (1, 0)], [2, 1, 1])
        import time

        time.sleep(1)
    # gen_obj is owned by node_0
    # the inner object is owned by A (node_2)
    # the inner object is stored in object store of node_2
    gen_obj = detached_actor.gen.remote()  # noqa: F841
    wait_for_condition(
        lambda: get_owner_info(node_ids) == ([(5, 1), (3, 0), (2, 0)], [2, 1, 2])
    )


def test_running_tasks(ray_start_cluster):
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

    @ray.remote
    def f(t):
        import time

        time.sleep(t)

    tasks = [
        f.options(resources={"node_0": 1}).remote(0),
        f.options(resources={"node_1": 1}).remote(100000),
        f.options(resources={"node_2": 1}).remote(100000),
    ]

    ready, pending = ray.wait(tasks)
    assert len(ready) == 1
    assert len(pending) == 2

    node_addrs = {
        n["NodeID"]: (n["NodeManagerAddress"], n["NodeManagerPort"])
        for n in ray.nodes()
    }

    def check():
        for i in range(NUM_NODES):
            node_stats = ray._private.internal_api.node_stats(
                node_addrs[node_ids[i]][0], node_addrs[node_ids[i]][1], False
            )

            if i == 0:
                assert (
                    sum(
                        [
                            stats.num_running_tasks
                            for stats in node_stats.core_workers_stats
                        ]
                    )
                    == 0
                )
            else:
                assert (
                    sum(
                        [
                            stats.num_running_tasks
                            for stats in node_stats.core_workers_stats
                        ]
                    )
                    == 1
                )
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
