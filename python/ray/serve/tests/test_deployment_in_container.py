import asyncio
import os
import re
import sys
from pathlib import Path

import numpy as np
import pytest

import ray
from ray import serve
from ray._private.state_api_test_utils import verify_failed_task
from ray._private.test_utils import wait_for_condition
from ray.serve.handle import DeploymentHandle
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.state import list_tasks, list_workers


@pytest.mark.skip
def test_basic(ray_start_stop):
    @serve.deployment(
        ray_actor_options={
            "runtime_env": {
                "container": {
                    "image": "zcin/runtime-env-prototype:nested",
                    "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
                }
            }
        }
    )
    class Model:
        def __call__(self):
            with open("file.txt") as f:
                return f.read()

    def check_application(app_handle: DeploymentHandle, expected: str):
        ref = app_handle.remote()
        assert ref.result() == expected
        return True

    h = serve.run(Model.bind())
    wait_for_condition(
        check_application,
        app_handle=h,
        expected="Hi I'm Cindy, this is version 1\n",
        timeout=300,
    )


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_put_get(shutdown_only):
    @ray.remote(
        runtime_env={
            "container": {
                "image": "zcin/ray:nightly-py3816-cpu",
                "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
            }
        }
    )
    # @ray.remote
    def create_ref():
        ref = ray.put(np.zeros(100_000_000))
        return ref

    ray.init()
    wrapped_ref = create_ref.remote()
    ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_shared_memory(shutdown_only):
    @ray.remote(
        runtime_env={
            "container": {
                "image": "zcin/ray:nightly-py3816-cpu",
                "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
            }
        }
    )
    # @ray.remote
    def f():
        array = np.random.rand(5000, 5000)
        return ray.put(array)

    ray.init()
    ref = ray.get(f.remote())
    val = ray.get(ref)
    assert sys.getsizeof(val) < 4 * 5000 * 5000
    assert val.shape == (5000, 5000)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_log_file_exists(shutdown_only):
    """Verify worker log file exists"""
    ray.init(num_cpus=1)

    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    def task_created():
        tasks = list_tasks()
        assert len(tasks) > 0
        return True

    # Run a basic workload.
    @ray.remote(
        runtime_env={
            "container": {
                "image": "zcin/ray:nightly-py3816-cpu",
                "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
            }
        }
    )
    # @ray.remote
    def f():
        for i in range(10):
            print(f"test {i}")

    f.remote()
    wait_for_condition(task_created)

    task_state = list_tasks()[0]
    worker_id = task_state.worker_id
    worker_pid = task_state.worker_pid
    print(f"Worker ID: {worker_id}")
    print(f"Worker PID: {worker_pid}")

    paths = [path.name for path in log_dir_path.iterdir()]
    assert f"python-core-worker-{worker_id}_{worker_pid}.log" in paths
    assert any(
        re.search(f"^worker-{worker_id}-.*-{worker_pid}.err$", path) for path in paths
    )
    assert any(
        re.search(f"^worker-{worker_id}-.*-{worker_pid}.out$", path) for path in paths
    )


@pytest.mark.skip
def test_worker_exit_intended_system_exit_and_user_error(shutdown_only):
    """
    INTENDED_SYSTEM_EXIT
    - (not tested, hard to test) Unused resource removed
    - (tested) Pg removed
    - (tested) Idle
    USER_ERROR
    - (tested) Actor init failed
    """

    ray.init(num_cpus=1)

    def get_worker_by_pid(pid, detail=True):
        for w in list_workers(detail=detail):
            if w["pid"] == pid:
                return w
        assert False

    @ray.remote
    def f():
        return ray.get(g.remote())

    @ray.remote
    def g():
        return os.getpid()

    # Start a task that has a blocking call ray.get with g.remote.
    # g.remote will borrow the CPU and start a new worker.
    # The worker started for g.remote will exit by IDLE timeout.
    pid = ray.get(f.remote())

    def verify_exit_by_idle_timeout():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        return type == "INTENDED_SYSTEM_EXIT" and "it was idle" in detail

    wait_for_condition(verify_exit_by_idle_timeout)

    ray.shutdown()

    @ray.remote(num_cpus=1)
    class A:
        def __init__(self):
            self.sleeping = False

        async def getpid(self):
            while not self.sleeping:
                await asyncio.sleep(0.1)
            return os.getpid()

        async def sleep(self):
            self.sleeping = True
            await asyncio.sleep(9999)

    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    a = A.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()
    a.sleep.options(name="sleep").remote()
    pid = ray.get(a.getpid.remote())
    ray.util.remove_placement_group(pg)

    def verify_exit_by_pg_removed():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert verify_failed_task(
            name="sleep",
            error_type="ACTOR_DIED",
            error_message=["INTENDED_SYSTEM_EXIT", "placement group was removed"],
        )
        return (
            type == "INTENDED_SYSTEM_EXIT" and "placement group was removed" in detail
        )

    wait_for_condition(verify_exit_by_pg_removed)

    @ray.remote
    class PidDB:
        def __init__(self):
            self.pid = None

        def record_pid(self, pid):
            self.pid = pid

        def get_pid(self):
            return self.pid

    p = PidDB.remote()

    @ray.remote
    class FaultyActor:
        def __init__(self):
            p.record_pid.remote(os.getpid())
            raise Exception("exception in the initialization method")

        def ready(self):
            pass

    a = FaultyActor.remote()
    wait_for_condition(lambda: ray.get(p.get_pid.remote()) is not None)
    pid = ray.get(p.get_pid.remote())

    def verify_exit_by_actor_init_failure():
        worker = get_worker_by_pid(pid)
        type = worker["exit_type"]
        detail = worker["exit_detail"]
        assert (
            type == "USER_ERROR" and "exception in the initialization method" in detail
        )
        return verify_failed_task(
            name="FaultyActor.__init__",
            error_type="TASK_EXECUTION_EXCEPTION",
            error_message="exception in the initialization method",
        )

    wait_for_condition(verify_exit_by_actor_init_failure)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
