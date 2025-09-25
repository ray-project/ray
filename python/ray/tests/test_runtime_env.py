"""All tests in this file use a module-scoped fixture to reduce runtime.

If you need a customized Ray instance (e.g., to change system config or env vars),
put the test in `test_runtime_env_standalone.py`.
"""
import os
import re
import sys

import pytest

import ray
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_decorator_task(start_cluster_shared, runtime_env_class):
    cluster, address = start_cluster_shared
    ray.init(address)

    runtime_env = runtime_env_class(env_vars={"foo": "bar"})

    @ray.remote(runtime_env=runtime_env)
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "bar"


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_decorator_actor(start_cluster_shared, runtime_env_class):
    cluster, address = start_cluster_shared
    ray.init(address)

    runtime_env = runtime_env_class(env_vars={"foo": "bar"})

    @ray.remote(runtime_env=runtime_env)
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "bar"


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_decorator_complex(start_cluster_shared, runtime_env_class):
    cluster, address = start_cluster_shared
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


def test_to_make_ensure_runtime_env_api(start_cluster_shared):
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


def test_runtime_env_config(start_cluster_shared):
    _, address = start_cluster_shared
    bad_configs = []
    bad_configs.append({"setup_timeout_seconds": 10.0})
    bad_configs.append({"setup_timeout_seconds": 0})
    bad_configs.append({"setup_timeout_seconds": "10"})

    good_configs = []
    good_configs.append({"setup_timeout_seconds": 10})
    good_configs.append({"setup_timeout_seconds": -1})

    @ray.remote
    def f():
        return True

    def raise_exception_run(fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception:
            pass
        else:
            assert False

    for bad_config in bad_configs:

        def run(runtime_env):
            raise_exception_run(ray.init, address, runtime_env=runtime_env)
            raise_exception_run(f.options, runtime_env=runtime_env)

        runtime_env = {"config": bad_config}
        run(runtime_env)

        raise_exception_run(RuntimeEnvConfig, **bad_config)
        raise_exception_run(RuntimeEnv, config=bad_config)

    for good_config in good_configs:

        def run(runtime_env):
            ray.shutdown()
            ray.init(address, runtime_env=runtime_env)
            assert ray.get(f.options(runtime_env=runtime_env).remote())

        runtime_env = {"config": good_config}
        run(runtime_env)
        runtime_env = {"config": RuntimeEnvConfig(**good_config)}
        run(runtime_env)
        runtime_env = RuntimeEnv(config=good_config)
        run(runtime_env)
        runtime_env = RuntimeEnv(config=RuntimeEnvConfig(**good_config))
        run(runtime_env)


def test_runtime_env_error_includes_node_ip(start_cluster_shared):
    """Test that RuntimeEnv errors include node IP information for debugging."""
    _, address = start_cluster_shared
    ray.init(address=address)

    # Test with invalid pip package to trigger RuntimeEnvSetupError.
    @ray.remote(
        runtime_env={
            "pip": ["nonexistent-package"],
            "config": {"setup_timeout_seconds": 1},
        }
    )
    def f():
        return "should not reach here"

    # Test pip package error
    with pytest.raises(RuntimeEnvSetupError) as exception_info:
        ray.get(f.remote())
    error_message = str(exception_info.value)
    print(f"Pip error message: {error_message}")
    # Check that error message contains node IP information
    # The format should be like "[Node 192.168.1.100] ..." or "[Node unknown] ..."
    assert re.search(
        r"\[Node ((\d{1,3}\.){3}\d{1,3}|unknown)\] ", error_message
    ), f"Error message should contain node IP or 'unknown' in proper format: {error_message}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
