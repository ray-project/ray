import os
import pytest
import sys
import time
import requests
from pathlib import Path

import ray
from ray.exceptions import RuntimeEnvSetupError
from ray._private.test_utils import wait_for_condition, get_error_message
from ray._private.utils import (get_wheel_filename, get_master_wheel_url,
                                get_release_wheel_url)


def test_get_wheel_filename():
    ray_version = "2.0.0.dev0"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            filename = get_wheel_filename(sys_platform, ray_version,
                                          py_version)
            prefix = "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/"
            url = f"{prefix}{filename}"
            assert requests.head(url).status_code == 200, url


def test_get_master_wheel_url():
    ray_version = "2.0.0.dev0"
    test_commit = "58a73821fbfefbf53a19b6c7ffd71e70ccf258c7"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            url = get_master_wheel_url(test_commit, sys_platform, ray_version,
                                       py_version)
            assert requests.head(url).status_code == 200, url


def test_get_release_wheel_url():
    test_commits = {"1.6.0": "5052fe67d99f1d4bfc81b2a8694dbf2aa807bbdc"}
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            for version, commit in test_commits.items():
                url = get_release_wheel_url(commit, sys_platform, version,
                                            py_version)
                assert requests.head(url).status_code == 200, url


def test_decorator_task(start_cluster):
    cluster, address = start_cluster
    ray.init(address)

    @ray.remote(runtime_env={"env_vars": {"foo": "bar"}})
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "bar"


def test_decorator_actor(start_cluster):
    cluster, address = start_cluster
    ray.init(address)

    @ray.remote(runtime_env={"env_vars": {"foo": "bar"}})
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "bar"


def test_decorator_complex(start_cluster):
    cluster, address = start_cluster
    ray.init(address, runtime_env={"env_vars": {"foo": "job"}})

    @ray.remote
    def env_from_job():
        return os.environ.get("foo")

    assert ray.get(env_from_job.remote()) == "job"

    @ray.remote(runtime_env={"env_vars": {"foo": "task"}})
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "task"

    @ray.remote(runtime_env={"env_vars": {"foo": "actor"}})
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "actor"

    # Test that runtime_env can be overridden by specifying .options().

    assert ray.get(
        f.options(runtime_env={
            "env_vars": {
                "foo": "new"
            }
        }).remote()) == "new"

    a = A.options(runtime_env={"env_vars": {"foo": "new2"}}).remote()
    assert ray.get(a.g.remote()) == "new2"


def test_container_option_serialize():
    runtime_env = {
        "container": {
            "image": "ray:latest",
            "run_options": ["--name=test"]
        }
    }
    job_config = ray.job_config.JobConfig(runtime_env=runtime_env)
    job_config_serialized = job_config.serialize()
    # job_config_serialized is JobConfig protobuf serialized string,
    # job_config.runtime_env_info.serialized_runtime_env
    # has container_option info
    assert job_config_serialized.count(b"ray:latest") == 1
    assert job_config_serialized.count(b"--name=test") == 1


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="conda in runtime_env unsupported on Windows.")
def test_invalid_conda_env(shutdown_only):
    ray.init()

    @ray.remote
    def f():
        pass

    @ray.remote
    class A:
        def f(self):
            pass

    start = time.time()
    bad_env = {"conda": {"dependencies": ["this_doesnt_exist"]}}
    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.options(runtime_env=bad_env).remote())
    first_time = time.time() - start

    # Check that another valid task can run.
    ray.get(f.remote())

    a = A.options(runtime_env=bad_env).remote()
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(a.f.remote())

    # The second time this runs it should be faster as the error is cached.
    start = time.time()
    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.options(runtime_env=bad_env).remote())

    assert (time.time() - start) < (first_time / 2.0)


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
def test_no_spurious_worker_startup(shutdown_only):
    """Test that no extra workers start up during a long env installation."""

    # Causes agent to sleep for 15 seconds to simulate creating a runtime env.
    os.environ["RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S"] = "15"
    ray.init(num_cpus=1)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def get(self):
            return self.value

    # Set a nonempty runtime env so that the runtime env setup hook is called.
    runtime_env = {"env_vars": {"a": "b"}}

    # Instantiate an actor that requires the long runtime env installation.
    a = Counter.options(runtime_env=runtime_env).remote()
    assert ray.get(a.get.remote()) == 0

    # Check "debug_state.txt" to ensure no extra workers were started.
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    debug_state_path = session_path / "logs" / "debug_state.txt"

    def get_num_workers():
        with open(debug_state_path) as f:
            for line in f.readlines():
                num_workers_prefix = "- num PYTHON workers: "
                if num_workers_prefix in line:
                    return int(line[len(num_workers_prefix):])
        return None

    # Wait for "debug_state.txt" to be updated to reflect the started worker.
    start = time.time()
    wait_for_condition(
        lambda: get_num_workers() is not None and get_num_workers() > 0)
    time_waited = time.time() - start
    print(f"Waited {time_waited} for debug_state.txt to be updated")

    # If any workers were unnecessarily started during the initial env
    # installation, they will bypass the runtime env setup hook (because the
    # created env will have been cached) and should be added to num_workers
    # within a few seconds.  Adjusting the default update period for
    # debut_state.txt via this cluster_utils pytest fixture seems to be broken,
    # so just check it for the next 10 seconds (the default period).
    start = time.time()
    got_num_workers = False
    while time.time() - start < 10:
        # Check that no more workers were started.
        num_workers = get_num_workers()
        if num_workers is not None:
            got_num_workers = True
            assert num_workers <= 1
        time.sleep(0.1)
    assert got_num_workers, "failed to read num workers for 10 seconds"


@pytest.fixture
def runtime_env_local_dev_env_var():
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"
    yield
    del os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"]


@pytest.mark.skipif(sys.platform == "win32", reason="very slow on Windows.")
def test_runtime_env_no_spurious_resource_deadlock_msg(
        runtime_env_local_dev_env_var, ray_start_regular, error_pubsub):
    p = error_pubsub

    @ray.remote(runtime_env={"pip": ["tensorflow", "torch"]})
    def f():
        pass

    # Check no warning printed.
    ray.get(f.remote())
    errors = get_error_message(p, 5, ray.ray_constants.RESOURCE_DEADLOCK_ERROR)
    assert len(errors) == 0


@pytest.fixture
def set_agent_failure_env_var():
    os.environ["_RAY_AGENT_FAILING"] = "1"
    yield
    del os.environ["_RAY_AGENT_FAILING"]


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "_system_config": {
            "agent_restart_interval_ms": 10,
            "agent_max_restart_count": 5
        }
    }],
    indirect=True)
def test_runtime_env_broken(set_agent_failure_env_var, ray_start_cluster_head):
    @ray.remote
    class A:
        def ready(self):
            pass

    @ray.remote
    def f():
        pass

    runtime_env = {"env_vars": {"TF_WARNINGS": "none"}}
    """
    Test task raises an exception.
    """
    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.options(runtime_env=runtime_env).remote())
    """
    Test actor task raises an exception.
    """
    a = A.options(runtime_env=runtime_env).remote()
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(a.ready.remote())


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
