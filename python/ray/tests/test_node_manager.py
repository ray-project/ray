import ray
from ray.util.state import list_workers
from ray._private.test_utils import (
    get_load_metrics_report,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
    get_resource_usage,
)
import pytest
import os
from ray.util.state import list_objects
import subprocess
from ray._private.utils import get_num_cpus
import time
import sys


# This tests the queue transitions for infeasible tasks. This has been an issue
# in the past, e.g., https://github.com/ray-project/ray/issues/3275.
def test_infeasible_tasks(ray_start_cluster):
    cluster = ray_start_cluster

    @ray.remote
    def f():
        return

    cluster.add_node(resources={str(0): 100})
    ray.init(address=cluster.address)

    # Submit an infeasible task.
    x_id = f._remote(args=[], kwargs={}, resources={str(1): 1})

    # Add a node that makes the task feasible and make sure we can get the
    # result.
    cluster.add_node(resources={str(1): 100})
    ray.get(x_id)

    # Start a driver that submits an infeasible task and then let it exit.
    driver_script = """
import ray

ray.init(address="{}")

@ray.remote(resources={})
def f():
{}pass  # This is a weird hack to insert some blank space.

f.remote()
""".format(
        cluster.address, "{str(2): 1}", "    "
    )

    run_string_as_driver(driver_script)

    # Now add a new node that makes the task feasible.
    cluster.add_node(resources={str(2): 100})

    # Make sure we can still run tasks on all nodes.
    ray.get([f._remote(args=[], kwargs={}, resources={str(i): 1}) for i in range(3)])


@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
def test_kill_driver_clears_backlog(call_ray_start):
    driver = """
import ray

@ray.remote
def f():
    import time
    time.sleep(300)

refs = [f.remote() for _ in range(10000)]

ray.get(refs)
  """
    proc = run_string_as_driver_nonblocking(driver)
    ctx = ray.init(address=call_ray_start)

    def get_backlog_and_pending():
        resources_batch = get_resource_usage(
            gcs_address=ctx.address_info["gcs_address"]
        )
        backlog = (
            resources_batch.resource_load_by_shape.resource_demands[0].backlog_size
            if resources_batch.resource_load_by_shape.resource_demands
            else 0
        )

        pending = 0
        demands = get_load_metrics_report(webui_url=ctx.address_info["webui_url"])[
            "resourceDemand"
        ]
        for demand in demands:
            resource_dict, amount = demand
            if "CPU" in resource_dict:
                pending = amount

        return pending, backlog

    def check_backlog(expect_backlog) -> bool:
        pending, backlog = get_backlog_and_pending()
        if expect_backlog:
            return pending > 0 and backlog > 0
        else:
            return pending == 0 and backlog == 0

    wait_for_condition(
        check_backlog, timeout=10, retry_interval_ms=1000, expect_backlog=True
    )

    os.kill(proc.pid, 9)

    wait_for_condition(
        check_backlog, timeout=10, retry_interval_ms=1000, expect_backlog=False
    )


def get_infeasible_queued(ray_ctx):
    resources_batch = get_resource_usage(
        gcs_address=ray_ctx.address_info["gcs_address"]
    )

    infeasible_queued = (
        resources_batch.resource_load_by_shape.resource_demands[
            0
        ].num_infeasible_requests_queued
        if len(resources_batch.resource_load_by_shape.resource_demands) > 0
        and hasattr(
            resources_batch.resource_load_by_shape.resource_demands[0],
            "num_infeasible_requests_queued",
        )
        else 0
    )

    return infeasible_queued


def check_infeasible(expect_infeasible, ray_ctx) -> bool:
    infeasible_queued = get_infeasible_queued(ray_ctx)
    if expect_infeasible:
        return infeasible_queued > 0
    else:
        return infeasible_queued == 0


@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
def test_kill_driver_clears_infeasible(call_ray_start):
    driver = """
import ray

@ray.remote
def f():
    pass

ray.get(f.options(num_cpus=99999999).remote())
  """
    proc = run_string_as_driver_nonblocking(driver)
    ctx = ray.init(address=call_ray_start)

    wait_for_condition(
        check_infeasible,
        timeout=10,
        retry_interval_ms=1000,
        expect_infeasible=True,
        ray_ctx=ctx,
    )

    os.kill(proc.pid, 9)

    wait_for_condition(
        check_infeasible,
        timeout=10,
        retry_interval_ms=1000,
        expect_infeasible=False,
        ray_ctx=ctx,
    )


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port=25555"],
    indirect=True,
)
def test_exiting_driver_clears_infeasible(call_ray_start):
    # Test that there is no leaking infeasible demands
    # from an exited driver.
    # See https://github.com/ray-project/ray/issues/43687
    # for a bug where it happened.

    driver = """
import ray

ray.init()

@ray.remote
def f():
    pass

f.options(num_cpus=99999999).remote()
  """
    proc = run_string_as_driver_nonblocking(driver)
    proc.wait()

    client_driver = """
import ray

ray.init("ray://127.0.0.1:25555")

@ray.remote
def f():
    pass

f.options(num_cpus=99999999).remote()
  """
    proc = run_string_as_driver_nonblocking(client_driver)
    proc.wait()

    ctx = ray.init(address=call_ray_start)

    # Give gcs some time to update the load
    time.sleep(1)

    wait_for_condition(
        check_infeasible,
        timeout=10,
        retry_interval_ms=1000,
        expect_infeasible=False,
        ray_ctx=ctx,
    )


def test_kill_driver_keep_infeasible_detached_actor(ray_start_cluster):
    cluster = ray_start_cluster
    address = cluster.address

    cluster.add_node(num_cpus=1)

    driver_script = """
import ray

@ray.remote
class A:
    def fn(self):
        pass

ray.init(address="{}", namespace="test_det")

ray.get(A.options(num_cpus=123, name="det", lifetime="detached").remote())
""".format(
        cluster.address
    )

    proc = run_string_as_driver_nonblocking(driver_script)

    ctx = ray.init(address=address, namespace="test_det")

    wait_for_condition(
        check_infeasible,
        timeout=10,
        retry_interval_ms=1000,
        expect_infeasible=True,
        ray_ctx=ctx,
    )

    os.kill(proc.pid, 9)

    cluster.add_node(num_cpus=200)

    det_actor = ray.get_actor("det")

    ray.get(det_actor.fn.remote())


@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
def test_reference_global_import_does_not_leak_worker_upon_driver_exit(call_ray_start):
    driver = """
import ray
import numpy as np
import tensorflow

@ray.remote(max_retries=0)
def leak_repro(obj):
    tensorflow
    return []

refs = []
for i in range(100_000):
    refs.append(leak_repro.remote(i))

ray.get(refs)
  """
    try:
        run_string_as_driver(driver)
    except subprocess.CalledProcessError:
        pass

    ray.init(address=call_ray_start)

    def no_object_leaks():
        objects = list_objects(_explain=True, timeout=3)
        return len(objects) == 0

    wait_for_condition(no_object_leaks, timeout=10, retry_interval_ms=1000)


@pytest.mark.skipif(
    sys.platform == "win32", reason="subprocess command only works for unix"
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head --system-config={"enable_worker_prestart":true}"""],
    indirect=True,
)
def test_worker_prestart_on_node_manager_start(call_ray_start, shutdown_only):
    def num_idle_workers(count):
        result = subprocess.check_output(
            "ps aux | grep ray::IDLE | grep -v grep",
            shell=True,
        )
        return len(result.splitlines()) == count

    wait_for_condition(num_idle_workers, count=get_num_cpus())

    with ray.init():
        for _ in range(5):
            workers = list_workers(filters=[("worker_type", "=", "WORKER")])
            assert len(workers) == get_num_cpus(), workers
            time.sleep(1)


@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
def test_jobs_prestart_worker_once(call_ray_start, shutdown_only):
    with ray.init():
        workers = list_workers(filters=[("worker_type", "=", "WORKER")])
        assert len(workers) == get_num_cpus(), workers
    with ray.init():
        for _ in range(5):
            workers = list_workers(filters=[("worker_type", "=", "WORKER")])
            assert len(workers) == get_num_cpus(), workers
            time.sleep(1)


def test_can_use_prestart_idle_workers(ray_start_cluster):
    """Test that actors and GPU tasks can use prestarted workers."""
    cluster = ray_start_cluster
    NUM_CPUS = 4
    NUM_GPUS = 4
    cluster.add_node(num_cpus=NUM_CPUS, num_gpus=NUM_GPUS)
    ray.init(address=cluster.address)

    wait_for_condition(
        lambda: len(list_workers(filters=[("worker_type", "=", "WORKER")])) == NUM_CPUS
    )

    # These workers don't have job_id or is_actor_worker.
    workers = list_workers(filters=[("worker_type", "=", "WORKER")], detail=True)
    worker_pids = {worker.pid for worker in workers}
    assert len(worker_pids) == NUM_CPUS

    @ray.remote
    class A:
        def getpid(self):
            return os.getpid()

    @ray.remote
    def f():
        return os.getpid()

    used_worker_pids = set()
    cpu_actor = A.options(num_cpus=1).remote()
    used_worker_pids.add(ray.get(cpu_actor.getpid.remote()))

    gpu_actor = A.options(num_gpus=1).remote()
    used_worker_pids.add(ray.get(gpu_actor.getpid.remote()))

    used_worker_pids.add(ray.get(f.options(num_cpus=1).remote()))
    used_worker_pids.add(ray.get(f.options(num_gpus=1).remote()))

    assert used_worker_pids == worker_pids


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
