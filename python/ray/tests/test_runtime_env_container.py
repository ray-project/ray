import sys

import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import run_in_container, NESTED_IMAGE_NAME

# Runtime env that points to an image that
# - is a ray image built from the changes in the current commit
# - contains a custom file that Ray actors can read from when executing requests
# See `docker/runtime_env_container/Dockerfile` and the `podman_docker_cluster` fixture
CONTAINER_SPEC = {
    "image": NESTED_IMAGE_NAME,
    "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
}
CONTAINER_RUNTIME_ENV = {"container": CONTAINER_SPEC}


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_put_get(podman_docker_cluster):
    """Test ray.put and ray.get."""

    container_id = podman_docker_cluster
    put_get_script = """
import ray
import numpy as np

@ray.remote(runtime_env={
    "container": {
        "image": "rayproject/ray:runtime_env_container_nested",
        "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py", # noqa
    }
})
def create_ref():
    with open("file.txt") as f:
        assert f.read().strip() == "helloworldalice"

    ref = ray.put(np.zeros(100_000_000))
    return ref

wrapped_ref = create_ref.remote()
ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)
""".strip()

    run_in_container(["python", "-c", put_get_script], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_shared_memory(podman_docker_cluster):
    """Test shared memory."""

    container_id = podman_docker_cluster
    put_get_script = """
import ray
import numpy as np
import sys

@ray.remote(runtime_env={
    "container": {
        "image": "rayproject/ray:runtime_env_container_nested",
        "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py", # noqa
    }
})
def f():
    array = np.random.rand(5000, 5000)
    return ray.put(array)

ray.init()
ref = ray.get(f.remote())
val = ray.get(ref)
size = sys.getsizeof(val)
assert size < sys.getsizeof(np.random.rand(5000, 5000))
print(f"Size of result fetched from ray.put: {size}")
assert val.shape == (5000, 5000)
""".strip()

    run_in_container(["python", "-c", put_get_script], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_log_file_exists(podman_docker_cluster):
    """Verify worker log file exists"""

    container_id = podman_docker_cluster
    put_get_script = f"""
import ray
from pathlib import Path
import re
from ray.util.state import list_tasks
from ray._private.test_utils import wait_for_condition

ray.init(num_cpus=1)

session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
session_path = Path(session_dir)
log_dir_path = session_path / "logs"

def task_finished():
    tasks = list_tasks()
    assert len(tasks) > 0
    assert tasks[0].worker_id
    assert tasks[0].worker_pid
    assert tasks[0].state == "FINISHED"
    return True

# Run a basic workload.
@ray.remote(runtime_env={CONTAINER_SPEC})
def f():
    for i in range(10):
        print(f"test {{i}}")

f.remote()
wait_for_condition(task_finished)

task_state = list_tasks()[0]
worker_id = task_state.worker_id
worker_pid = task_state.worker_pid
print(f"Worker ID: {{worker_id}}")
print(f"Worker PID: {{worker_pid}}")

paths = [path.name for path in log_dir_path.iterdir()]
assert f"python-core-worker-{{worker_id}}_{{worker_pid}}.log" in paths
assert any(re.search(f"^worker-{{worker_id}}-.*-{{worker_pid}}.err$", p) for p in paths)
assert any(re.search(f"^worker-{{worker_id}}-.*-{{worker_pid}}.out$", p) for p in paths)
""".strip()

    run_in_container(["python", "-c", put_get_script], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_worker_exit_intended_system_exit_and_user_error(podman_docker_cluster):
    """
    INTENDED_SYSTEM_EXIT
    - (not tested, hard to test) Unused resource removed
    - (tested) Pg removed
    - (tested) Idle
    USER_ERROR
    - (tested) Actor init failed
    """

    container_id = podman_docker_cluster
    put_get_script = f"""
import asyncio
import os

import ray
from ray._private.state_api_test_utils import verify_failed_task
from ray.util.state import list_workers
from ray._private.test_utils import wait_for_condition
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

ray.init(num_cpus=1)

def get_worker_by_pid(pid, detail=True):
    for w in list_workers(detail=detail):
        if w["pid"] == pid:
            return w
    assert False

@ray.remote(runtime_env={CONTAINER_SPEC})
def f():
    return ray.get(g.remote())

@ray.remote(runtime_env={CONTAINER_SPEC})
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

@ray.remote(num_cpus=1, runtime_env={CONTAINER_SPEC})
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

pg = ray.util.placement_group(bundles=[{{"CPU": 1}}])
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

@ray.remote(runtime_env={CONTAINER_SPEC})
class PidDB:
    def __init__(self):
        self.pid = None

    def record_pid(self, pid):
        self.pid = pid

    def get_pid(self):
        return self.pid

p = PidDB.remote()

@ray.remote(runtime_env={CONTAINER_SPEC})
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
""".strip()

    run_in_container(["python", "-c", put_get_script], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_serve_basic(podman_docker_cluster):
    """Test Serve deployment."""

    container_id = podman_docker_cluster
    put_get_script = f"""
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.handle import DeploymentHandle

@serve.deployment(ray_actor_options={{"runtime_env": {CONTAINER_SPEC}}})
class Model:
    def __call__(self):
        with open("file.txt") as f:
            return f.read().strip()

def check_application(app_handle: DeploymentHandle, expected: str):
    ref = app_handle.remote()
    assert ref.result() == expected
    return True

h = serve.run(Model.bind())
wait_for_condition(
    check_application,
    app_handle=h,
    expected="helloworldalice",
    timeout=300,
)
""".strip()

    run_in_container(["python", "-c", put_get_script], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.skip
def test_serve_telemetry(podman_docker_cluster):
    """Test Serve deployment telemetry."""

    container_id = podman_docker_cluster
    put_get_script = f"""
import os
import subprocess

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.tests.common.utils import (
    TELEMETRY_ROUTE_PREFIX,
    start_telemetry_app,
    check_ray_started,
)

os.environ["RAY_USAGE_STATS_ENABLED"] = "1"
os.environ["RAY_USAGE_STATS_REPORT_URL"] = (
    f"http://127.0.0.1:8000{{TELEMETRY_ROUTE_PREFIX}}"
)
os.environ["RAY_USAGE_STATS_REPORT_INTERVAL_S"] = "1"

subprocess.check_output(["ray", "start", "--head"])
wait_for_condition(check_ray_started, timeout=5)

storage_handle = start_telemetry_app()
wait_for_condition(
    lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
)
report = ray.get(storage_handle.get_report.remote())
assert ServeUsageTag.CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report) is None

@serve.deployment(ray_actor_options={{"runtime_env": {CONTAINER_SPEC}}})
class Model:
    def __call__(self):
        with open("file.txt") as f:
            return f.read().strip()
h = serve.run(Model.bind())

assert h.remote().result() == "helloworldalice"

def check_telemetry():
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert ServeUsageTag.CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report) == "1"
    return True

wait_for_condition(check_telemetry)
print("Telemetry check passed!")
""".strip()

    run_in_container(["python", "-c", put_get_script], container_id)


class TestValidation:
    def test_container_with_env_vars(self):
        with pytest.raises(ValueError):

            @ray.remote(
                runtime_env={
                    "container": CONTAINER_SPEC,
                    "env_vars": {"HELLO": "WORLD"},
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_pip(self):
        with pytest.raises(ValueError):

            @ray.remote(
                runtime_env={
                    "container": CONTAINER_SPEC,
                    "pip": ["requests"],
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_conda(self):
        with pytest.raises(ValueError):

            @ray.remote(
                runtime_env={
                    "container": CONTAINER_SPEC,
                    "conda": ["requests"],
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_py_modules(self):
        with pytest.raises(ValueError):

            @ray.remote(
                runtime_env={
                    "container": CONTAINER_SPEC,
                    "py_modules": ["requests"],
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_working_dir(self):
        with pytest.raises(ValueError):

            @ray.remote(
                runtime_env={
                    "container": CONTAINER_SPEC,
                    "working_dir": ".",
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_env_vars_and_working_dir(self):
        with pytest.raises(ValueError):

            @ray.remote(
                runtime_env={
                    "container": CONTAINER_SPEC,
                    "env_vars": {"HELLO": "WORLD"},
                    "working_dir": ".",
                }
            )
            def f():
                return ray.put((1, 10))


# @pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
# def test_b(podman_docker_cluster):
#     container_id = podman_docker_cluster

#     put_get_script = """
# import ray
# import numpy as np

# @ray.remote
# def create_ref():
#     ref = ray.put(np.zeros(100_000_000))
#     return ref

# wrapped_ref = create_ref.remote()
# print(wrapped_ref)
# assert (ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)).all()
# """
#     put_get_script = put_get_script.strip()
#     ray_script = ["docker", "exec", container_id, "python", "-c", put_get_script]
#     print("Executing", ray_script)
#     print(subprocess.check_output(ray_script))


# @pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
# def test_c(podman_docker_cluster):
#     container_id = podman_docker_cluster
#     put_get_script = f"""
# import ray
# import numpy as np

# @ray.remote(runtime_env={CONTAINER_SPEC})
# def create_ref():
#     print("yoo")
#     return "hii"

# output = ray.get(create_ref.remote())
# print(output)
# """.strip()

#     run_in_container(["python", "-c", put_get_script], container_id)


# @pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
# def test_c2(podman_docker_cluster):
#     container_id = podman_docker_cluster

#     put_get_script = """
# import ray
# import numpy as np

# @ray.remote(runtime_env={
#     "container": {
#         "image": "rayproject/ray:runtime_env_container_nested",
#         "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py", # noqa
#     }
# })
# def create_ref():
#     with open("/home/ray/file.txt") as f:
#         print(f.read())
#     print("yoo")
#     return "hii"

# output = ray.get(create_ref.remote())
# print(output)
# """
#     put_get_script = put_get_script.strip()
#     # with open("/home/ray/file.txt") as f:
#     #     assert f.read().strip() == "helloworldalice"
#     run_in_container(["python", "-c", put_get_script], container_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
