import collections
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import List, Optional

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import (
    get_load_metrics_report,
    get_resource_usage,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
)
from ray._private.utils import get_num_cpus
from ray.util.state import list_objects, list_workers


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


MyPlugin = "HangOnSecondWorkerPlugin"
MY_PLUGIN_CLASS_PATH = "ray.tests.test_node_manager.HangOnSecondWorkerPlugin"
PLUGIN_TIMEOUT = 10


class HangOnSecondWorkerPlugin(RuntimeEnvPlugin):
    """
    The first worker will start up normally, but all subsequent workers will hang at
    start up indefinitely. How it works: Ray RuntimeEnvAgent caches the modified context
    so we can't do it in modify_context. Instead, we use a bash command to read a file
    and hang forever. We don't have a good file lock mechanism in bash (flock is not
    installed by default in macos), so we also serialize the worker startup.
    """

    name = MyPlugin

    def __init__(self):
        # Each URI has a temp dir, a counter file, and a hang.sh script.
        self.uris = collections.defaultdict(dict)

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        return [runtime_env[self.name]]

    async def create(
        self,
        uri: Optional[str],
        runtime_env,
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        d = self.uris[uri]
        d["temp_dir"] = tempfile.mkdtemp()
        logger.info(f"caching temp dir {d['temp_dir']} for uri {uri}")
        d["counter_file"] = os.path.join(d["temp_dir"], "script_run_count")
        with open(d["counter_file"], "w+") as f:
            f.write("0")
        d["hang_sh"] = os.path.join(d["temp_dir"], "hang.sh")
        with open(d["hang_sh"], "w+") as f:
            f.write(
                f"""#!/bin/bash

counter_file="{d['counter_file']}"

count=$(cat "$counter_file")

if [ "$count" -eq "0" ]; then
  echo "1" > "$counter_file"
  echo "first time run"
  exit 0
elif [ "$count" -eq "1" ]; then
  echo "2" > "$counter_file"
  echo "second time run, sleeping..."
  sleep 1000
fi
"""
            )
        os.chmod(d["hang_sh"], 0o755)
        return 0.1

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        logger.info(f"Starting worker: {uris}, {runtime_env}")
        if self.name not in runtime_env:
            return
        assert len(uris) == 1
        uri = uris[0]
        hang_sh = self.uris[uri]["hang_sh"]
        ctx.command_prefix += ["bash", hang_sh, "&&"]

    def delete_uri(self, uri: str, logger: logging.Logger) -> float:
        temp_dir = self.uris[uri]["temp_dir"]
        shutil.rmtree(temp_dir)
        del self.uris[uri]
        logger.info(f"temp_dir removed: {temp_dir}")


@pytest.fixture
def serialize_worker_startup(monkeypatch):
    """Only one worker starts up each time, since our bash script is not process-safe"""
    monkeypatch.setenv("RAY_worker_maximum_startup_concurrency", "1")
    yield


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + MY_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_can_reuse_released_workers(
    serialize_worker_startup, set_runtime_env_plugins, ray_start_cluster
):
    """
    Uses a runtime env plugin to make sure only 1 worker can start and all subsequent
    workers will hang in runtime start up forever. We issue 10 tasks and test that
    all the following tasks can still be scheduled on the first worker released from the
    first task, i.e. tasks are not binded to the workers that they requested to start.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    @ray.remote(runtime_env={"env_vars": {"HELLO": "WORLD"}, MyPlugin: "key"})
    def f():
        # Sleep for a while to make sure other tasks also request workers.
        time.sleep(1)
        print(f"pid={os.getpid()}, env HELLO={os.environ.get('HELLO')}")
        return os.getpid()

    objs = [f.remote() for i in range(10)]

    pids = ray.get(objs)
    for pid in pids:
        assert pid == pids[0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
