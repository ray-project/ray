import logging
import os
import sys
import time
from pathlib import Path
from typing import List

import pytest

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import (
    get_error_message,
    get_log_sources,
    wait_for_condition,
)
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_decorator_task(start_cluster, runtime_env_class):
    cluster, address = start_cluster
    ray.init(address)

    runtime_env = runtime_env_class(env_vars={"foo": "bar"})

    @ray.remote(runtime_env=runtime_env)
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "bar"


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_decorator_actor(start_cluster, runtime_env_class):
    cluster, address = start_cluster
    ray.init(address)

    runtime_env = runtime_env_class(env_vars={"foo": "bar"})

    @ray.remote(runtime_env=runtime_env)
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "bar"


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_decorator_complex(start_cluster, runtime_env_class):
    cluster, address = start_cluster
    runtime_env_for_init = runtime_env_class(env_vars={"foo": "job"})
    ray.init(address, runtime_env=runtime_env_for_init)

    @ray.remote
    def env_from_job():
        return os.environ.get("foo")

    assert ray.get(env_from_job.remote()) == "job"

    runtime_env_for_f = runtime_env_class(env_vars={"foo": "task"})

    @ray.remote(runtime_env=runtime_env_for_f)
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "task"

    runtime_env_for_A = runtime_env_class(env_vars={"foo": "actor"})

    @ray.remote(runtime_env=runtime_env_for_A)
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "actor"

    # Test that runtime_env can be overridden by specifying .options().
    runtime_env_for_f_new = runtime_env_class(env_vars={"foo": "new"})
    assert ray.get(f.options(runtime_env=runtime_env_for_f_new).remote()) == "new"

    runtime_env_for_A_new = runtime_env_class(env_vars={"foo": "new2"})
    a = A.options(runtime_env=runtime_env_for_A_new).remote()
    assert ray.get(a.g.remote()) == "new2"


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_no_spurious_worker_startup(shutdown_only, runtime_env_class, monkeypatch):
    """Test that no extra workers start up during a long env installation."""

    # Causes agent to sleep for 15 seconds to simulate creating a runtime env.
    monkeypatch.setenv("RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S", "15")
    ray.init(num_cpus=1)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def get(self):
            return self.value

    # Set a nonempty runtime env so that the runtime env setup hook is called.
    runtime_env = runtime_env_class(env_vars={"a": "b"})

    # Instantiate an actor that requires the long runtime env installation.
    a = Counter.options(runtime_env=runtime_env).remote()
    assert ray.get(a.get.remote()) == 0

    # Check "debug_state.txt" to ensure no extra workers were started.
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    debug_state_path = session_path / "logs" / "debug_state.txt"

    def get_num_workers():
        with open(debug_state_path) as f:
            for line in f.readlines():
                num_workers_prefix = "- num PYTHON workers: "
                if num_workers_prefix in line:
                    return int(line[len(num_workers_prefix) :])
        return None

    # Wait for "debug_state.txt" to be updated to reflect the started worker.
    start = time.time()
    wait_for_condition(lambda: get_num_workers() is not None and get_num_workers() > 0)
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
        # Check that no more than one extra worker is started. We add one
        # because Ray will prestart an idle worker for the one available CPU.
        num_workers = get_num_workers()
        if num_workers is not None:
            got_num_workers = True
            assert num_workers <= 2
        time.sleep(0.1)
    assert got_num_workers, "failed to read num workers for 10 seconds"


@pytest.fixture
def runtime_env_local_dev_env_var(monkeypatch):
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", "1")
    yield


@pytest.mark.skipif(sys.platform == "win32", reason="very slow on Windows.")
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_runtime_env_no_spurious_resource_deadlock_msg(
    runtime_env_local_dev_env_var, ray_start_regular, error_pubsub, runtime_env_class
):
    p = error_pubsub
    runtime_env = runtime_env_class(pip=["tensorflow", "torch"])

    @ray.remote(runtime_env=runtime_env)
    def f():
        pass

    # Check no warning printed.
    ray.get(f.remote())
    errors = get_error_message(p, 5, ray._private.ray_constants.RESOURCE_DEADLOCK_ERROR)
    assert len(errors) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Hangs on windows.")
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_failed_job_env_no_hang(shutdown_only, runtime_env_class):
    """Test that after a failed job-level env, tasks can still be run."""
    runtime_env_for_init = runtime_env_class(pip=["ray-doesnotexist-123"])
    ray.init(runtime_env=runtime_env_for_init)

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401

        return True

    runtime_env_for_f = runtime_env_class(pip=["pip-install-test==0.5"])
    assert ray.get(f.options(runtime_env=runtime_env_for_f).remote())

    # Task with no runtime env should inherit the bad job env.
    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.remote())


RT_ENV_AGENT_SLOW_STARTUP_PLUGIN_CLASS_PATH = (
    "ray.tests.test_runtime_env.RtEnvAgentSlowStartupPlugin"  # noqa
)
RT_ENV_AGENT_SLOW_STARTUP_PLUGIN_NAME = "RtEnvAgentSlowStartupPlugin"
RT_ENV_AGENT_SLOW_STARTUP_PLUGIN_CLASS_PATH = (
    "ray.tests.test_runtime_env.RtEnvAgentSlowStartupPlugin"
)


class RtEnvAgentSlowStartupPlugin(RuntimeEnvPlugin):

    name = RT_ENV_AGENT_SLOW_STARTUP_PLUGIN_NAME

    def __init__(self):
        # This happens in Runtime Env Agent start up process. Make it slow.
        import time

        time.sleep(5)
        print("starting...")


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + RT_ENV_AGENT_SLOW_STARTUP_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_slow_runtime_env_agent_startup_on_task_pressure(
    shutdown_only, set_runtime_env_plugins
):
    """
    Starts nodes with runtime env agent and a slow plugin. Then when the runtime env
    agent is still starting up, we submit a lot of tasks to the cluster. The tasks
    should wait for the runtime env agent to start up and then run.
    https://github.com/ray-project/ray/issues/45353
    """
    ray.init()

    @ray.remote(num_cpus=0.1)
    def get_foo():
        return os.environ.get("foo")

    print("Submitting 20 tasks...")

    # Each task has a different runtime env to ensure the agent is invoked for each.
    vals = ray.get(
        [
            get_foo.options(runtime_env={"env_vars": {"foo": f"bar{i}"}}).remote()
            for i in range(20)
        ]
    )
    print("20 tasks done.")
    assert vals == [f"bar{i}" for i in range(20)]


@pytest.fixture
def enable_dev_mode(local_env_var_enabled, monkeypatch):
    enabled = "1" if local_env_var_enabled else "0"
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED", enabled)
    yield


@pytest.mark.skipif(
    sys.platform == "win32", reason="conda in runtime_env unsupported on Windows."
)
@pytest.mark.skipif(
    sys.version_info >= (3, 10, 0),
    reason=("Currently not passing for Python 3.10"),
)
@pytest.mark.parametrize("local_env_var_enabled", [False, True])
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_runtime_env_log_msg(
    local_env_var_enabled,
    enable_dev_mode,
    ray_start_cluster_head,
    log_pubsub,
    runtime_env_class,
):
    p = log_pubsub

    @ray.remote
    def f():
        pass

    good_env = runtime_env_class(pip=["requests"])
    ray.get(f.options(runtime_env=good_env).remote())
    sources = get_log_sources(p, 5)
    if local_env_var_enabled:
        assert "runtime_env" in sources
    else:
        assert "runtime_env" not in sources


def test_to_make_ensure_runtime_env_api(start_cluster):
    # make sure RuntimeEnv can be used in an be used interchangeably with
    # an unstructured dictionary in the relevant API calls.
    ENV_KEY = "TEST_RUNTIME_ENV"

    @ray.remote(runtime_env=RuntimeEnv(env_vars={ENV_KEY: "f1"}))
    def f1():
        assert os.environ.get(ENV_KEY) == "f1"

    ray.get(f1.remote())

    @ray.remote
    def f2():
        assert os.environ.get(ENV_KEY) == "f2"

    ray.get(f2.options(runtime_env=RuntimeEnv(env_vars={ENV_KEY: "f2"})).remote())

    @ray.remote(runtime_env=RuntimeEnv(env_vars={ENV_KEY: "a1"}))
    class A1:
        def f(self):
            assert os.environ.get(ENV_KEY) == "a1"

    a1 = A1.remote()
    ray.get(a1.f.remote())

    @ray.remote
    class A2:
        def f(self):
            assert os.environ.get(ENV_KEY) == "a2"

    a2 = A2.options(runtime_env=RuntimeEnv(env_vars={ENV_KEY: "a2"})).remote()
    ray.get(a2.f.remote())


MY_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env.MyPlugin"
MY_PLUGIN_NAME = "MyPlugin"
success_retry_number = 3
runtime_env_retry_times = 0


# This plugin can make runtime env creation failed before the retry times
# increased to `success_retry_number`.
class MyPlugin(RuntimeEnvPlugin):

    name = MY_PLUGIN_NAME

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return runtime_env_dict[MY_PLUGIN_NAME]

    @staticmethod
    def modify_context(
        uris: List[str],
        runtime_env: dict,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        global runtime_env_retry_times
        runtime_env_retry_times += 1
        if runtime_env_retry_times != success_retry_number:
            raise ValueError(f"Fault injection {runtime_env_retry_times}")
        pass


@pytest.mark.parametrize(
    "set_runtime_env_retry_times",
    [
        str(success_retry_number - 1),
        str(success_retry_number),
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + MY_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_runtime_env_retry(
    set_runtime_env_retry_times, set_runtime_env_plugins, ray_start_regular
):
    @ray.remote
    def f():
        return "ok"

    runtime_env_retry_times = int(set_runtime_env_retry_times)
    if runtime_env_retry_times >= success_retry_number:
        # Enough retry times
        output = ray.get(
            f.options(runtime_env={MY_PLUGIN_NAME: {"key": "value"}}).remote()
        )
        assert output == "ok"
    else:
        # No enough retry times
        with pytest.raises(
            RuntimeEnvSetupError, match=f"Fault injection {runtime_env_retry_times}"
        ):
            ray.get(f.options(runtime_env={MY_PLUGIN_NAME: {"key": "value"}}).remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
