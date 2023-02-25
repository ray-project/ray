from dataclasses import dataclass
import dataclasses
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest
from ray.runtime_env.runtime_env import RuntimeEnvConfig
import requests

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.uri_cache import URICache
from ray._private.runtime_env.utils import (
    SubprocessCalledProcessError,
    check_output_cmd,
)
from ray._private.test_utils import (
    chdir,
    get_error_message,
    get_log_sources,
    wait_for_condition,
)
from ray._private.utils import (
    get_master_wheel_url,
    get_release_wheel_url,
    get_wheel_filename,
)
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv

import ray._private.ray_constants as ray_constants


def test_get_wheel_filename():
    """Test the code that generates the filenames of the `latest` wheels."""
    # NOTE: These should not be changed for releases.
    ray_version = "3.0.0.dev0"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            if sys_platform == "win32" and py_version == (3, 6):
                # Windows wheels are not built for py3.6 anymore
                continue
            filename = get_wheel_filename(sys_platform, ray_version, py_version)
            prefix = "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/"
            url = f"{prefix}{filename}"
            assert requests.head(url).status_code == 200, url


def test_get_master_wheel_url():
    """Test the code that generates the filenames of `master` commit wheels."""
    # NOTE: These should not be changed for releases.
    ray_version = "3.0.0.dev0"
    # This should be a commit for which wheels have already been built for
    # all platforms and python versions at
    # `s3://ray-wheels/master/<test_commit>/`.
    test_commit = "6fd684bbdb186a73732f6113a83a12b63200f170"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            if sys_platform == "win32" and py_version == (3, 6):
                # Windows wheels are not built for py3.6 anymore
                continue
            url = get_master_wheel_url(
                test_commit, sys_platform, ray_version, py_version
            )
            assert requests.head(url).status_code == 200, url


def test_get_release_wheel_url():
    """Test the code that generates the filenames of the `release` branch wheels."""
    # This should be a commit for which wheels have already been built for
    # all platforms and python versions at
    # `s3://ray-wheels/releases/2.2.0/<commit>/`.
    test_commits = {"2.2.0": "b6af0887ee5f2e460202133791ad941a41f15beb"}
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            for version, commit in test_commits.items():
                if sys_platform == "win32" and py_version == (3, 6):
                    # Windows wheels are not built for py3.6 anymore
                    continue
                url = get_release_wheel_url(commit, sys_platform, version, py_version)
                assert requests.head(url).status_code == 200, url


def test_current_py_version_supported():
    """Test that the running python version is supported.

    This is run as a check in the Ray `runtime_env` `conda` code
    before downloading the Ray wheel into the conda environment.
    If Ray wheels are not available for this python version, then
    the `conda` environment installation will fail.

    When a new python version is added to the Ray wheels, please update
    `ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS`.  In a subsequent commit,
    once wheels have been built for the new python version, please update
    the tests test_get_wheel_filename, test_get_master_wheel_url, and
    (after the first Ray release with the new python version)
    test_get_release_wheel_url.
    """
    py_version = sys.version_info[:2]
    assert py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS


def test_compatible_with_dataclasses():
    """Test that the output of RuntimeEnv.to_dict() can be used as a dataclass field."""
    config = RuntimeEnvConfig(setup_timeout_seconds=1)
    runtime_env = RuntimeEnv(
        pip={
            "packages": ["tensorflow", "requests"],
            "pip_check": False,
            "pip_version": "==22.0.2;python_version=='3.8.11'",
        },
        env_vars={"FOO": "BAR"},
        config=config,
    )

    @dataclass
    class RuntimeEnvDataClass:
        runtime_env: Dict[str, Any]

    dataclasses.asdict(RuntimeEnvDataClass(runtime_env.to_dict()))

    @dataclass
    class RuntimeEnvConfigDataClass:
        config: Dict[str, Any]

    dataclasses.asdict(RuntimeEnvConfigDataClass(config.to_dict()))


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


@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_container_option_serialize(runtime_env_class):
    runtime_env = runtime_env_class(
        container={"image": "ray:latest", "run_options": ["--name=test"]}
    )
    job_config = ray.job_config.JobConfig(runtime_env=runtime_env)
    job_config_serialized = job_config.serialize()
    # job_config_serialized is JobConfig protobuf serialized string,
    # job_config.runtime_env_info.serialized_runtime_env
    # has container_option info
    assert job_config_serialized.count(b"ray:latest") == 1
    assert job_config_serialized.count(b"--name=test") == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_no_spurious_worker_startup(shutdown_only, runtime_env_class):
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


class TestURICache:
    def test_zero_cache_size(self):
        uris_to_sizes = {"5": 5, "3": 3}

        def delete_fn(uri, logger):
            return uris_to_sizes[uri]

        cache = URICache(delete_fn, max_total_size_bytes=0, debug_mode=True)
        cache.add("5", 5)
        assert cache.get_total_size_bytes() == 5
        cache.mark_unused("5")
        assert cache.get_total_size_bytes() == 0
        cache.add("3", 3)
        cache.add("5", 5)
        assert cache.get_total_size_bytes() == 8
        cache.mark_unused("3")
        cache.mark_unused("5")
        assert cache.get_total_size_bytes() == 0

    def test_nonzero_cache_size(self):
        uris_to_sizes = {"a": 4, "b": 4, "c": 4}

        def delete_fn(uri, logger):
            return uris_to_sizes[uri]

        cache = URICache(delete_fn, max_total_size_bytes=10, debug_mode=True)
        cache.add("a", 4)
        cache.add("b", 4)
        cache.mark_unused("a")
        assert "a" in cache
        cache.add("c", 4)
        # Now we have total size 12, which exceeds the max size 10.
        assert cache.get_total_size_bytes() == 8
        # "a" was the only unused URI, so it must have been deleted.
        assert "b" and "c" in cache and "a" not in cache

    def test_mark_used_nonadded_uri_error(self):
        cache = URICache(debug_mode=True)
        with pytest.raises(ValueError):
            cache.mark_used("nonadded_uri")

    def test_mark_used(self):
        uris_to_sizes = {"a": 3, "b": 3, "big": 300}

        def delete_fn(uri, logger):
            return uris_to_sizes[uri]

        cache = URICache(delete_fn, max_total_size_bytes=10, debug_mode=True)
        cache.add("a", 3)
        cache.add("b", 3)
        cache.mark_unused("a")
        cache.mark_unused("b")
        assert "a" in cache and "b" in cache
        assert cache.get_total_size_bytes() == 6

        cache.mark_used("a")
        cache.add("big", 300)
        # We are over capacity and the only unused URI is "b", so we delete it
        assert "a" in cache and "big" in cache and "b" not in cache
        assert cache.get_total_size_bytes() == 303

        cache.mark_unused("big")
        assert "big" not in cache
        assert cache.get_total_size_bytes() == 3

    def test_many_URIs(self):
        uris_to_sizes = {str(i): i for i in range(1000)}

        def delete_fn(uri, logger):
            return uris_to_sizes[uri]

        cache = URICache(delete_fn, debug_mode=True)
        for i in range(1000):
            cache.add(str(i), i)
        for i in range(1000):
            cache.mark_unused(str(i))
        for i in range(1000):
            assert str(i) in cache

    def test_delete_fn_called(self):
        num_delete_fn_calls = 0
        uris_to_sizes = {"a": 8, "b": 6, "c": 4, "d": 20}

        def delete_fn(uri, logger):
            nonlocal num_delete_fn_calls
            num_delete_fn_calls += 1
            return uris_to_sizes[uri]

        cache = URICache(delete_fn, max_total_size_bytes=10, debug_mode=True)
        cache.add("a", 8)
        cache.add("b", 6)
        cache.mark_unused("b")
        # Total size is 14 > 10, so we need to delete "b".
        assert num_delete_fn_calls == 1

        cache.add("c", 4)
        cache.mark_unused("c")
        # Total size is 12 > 10, so we delete "c".
        assert num_delete_fn_calls == 2

        cache.mark_unused("a")
        # Total size is 8 <= 10, so we shouldn't delete anything.
        assert num_delete_fn_calls == 2

        cache.add("d", 20)
        # Total size is 28 > 10, so we delete "a".
        assert num_delete_fn_calls == 3

        cache.mark_unused("d")
        # Total size is 20 > 10, so we delete "d".
        assert num_delete_fn_calls == 4


@pytest.fixture
def enable_dev_mode(local_env_var_enabled):
    enabled = "1" if local_env_var_enabled else "0"
    os.environ["RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED"] = enabled
    yield
    del os.environ["RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED"]


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


def test_subprocess_error():
    ex = SubprocessCalledProcessError
    with pytest.raises(subprocess.SubprocessError) as e:
        raise ex(123, "abc")
    assert "test_out" not in str(e.value)
    assert "test_err" not in str(e.value)
    with pytest.raises(subprocess.SubprocessError) as e:
        raise ex(123, "abc", stderr="test_err")
    assert "test_out" not in str(e.value)
    assert "test_err" in str(e.value)
    with pytest.raises(subprocess.SubprocessError) as e:
        raise ex(123, "abc", output="test_out")
    assert "test_out" in str(e.value)
    assert "test_err" not in str(e.value)
    with pytest.raises(subprocess.SubprocessError) as e:
        raise ex(123, "abc", output="test_out", stderr="test_err")
    assert "test_out" in str(e.value)
    assert "test_err" in str(e.value)


def test_subprocess_error_with_last_n_lines():
    stdout = "1\n2\n3\n4\n5\n"
    stderr = "5\n4\n3\n2\n1\n"
    exception = SubprocessCalledProcessError(888, "abc", output=stdout, stderr=stderr)
    exception.LAST_N_LINES = 3
    exception_str = str(exception)
    assert "cmd" not in exception_str
    assert "Last 3 lines" in exception_str
    s = "".join([s.strip() for s in exception_str.splitlines()])
    assert "345" in s
    assert "321" in s


@pytest.mark.asyncio
async def test_check_output_cmd():
    cmd = "dir" if sys.platform.startswith("win") else "pwd"
    logs = []

    class _FakeLogger:
        def __getattr__(self, item):
            def _log(formatter, *args):
                logs.append(formatter % args)

            return _log

    for _ in range(2):
        output = await check_output_cmd([cmd], logger=_FakeLogger())
        assert len(output) > 0

    all_log_string = "\n".join(logs)

    # Check the cmd index generator works.
    assert "cmd[1]" in all_log_string
    assert "cmd[2]" in all_log_string

    # Test communicate fails.
    with mock.patch(
        "asyncio.subprocess.Process.communicate",
        side_effect=Exception("fake exception"),
    ):
        with pytest.raises(RuntimeError) as e:
            await check_output_cmd([cmd], logger=_FakeLogger())
        # Make sure the exception has cmd trace info.
        assert "cmd[3]" in str(e.value)

    # Test asyncio.create_subprocess_exec fails.
    with pytest.raises(RuntimeError) as e:
        await check_output_cmd(["not_exist_cmd"], logger=_FakeLogger())
    # Make sure the exception has cmd trace info.
    assert "cmd[4]" in str(e.value)

    # Test returncode != 0.
    with pytest.raises(SubprocessCalledProcessError) as e:
        await check_output_cmd([cmd, "--abc"], logger=_FakeLogger())
    # Make sure the exception has cmd trace info.
    assert "cmd[5]" in str(e.value)


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


@pytest.mark.parametrize(
    "option",
    ["pip_list", "pip_dict", "conda_name", "conda_dict", "container"],
)
def test_serialize_deserialize(option):
    runtime_env = dict()
    if option == "pip_list":
        runtime_env["pip"] = ["pkg1", "pkg2"]
    elif option == "pip_dict":
        runtime_env["pip"] = {
            "packages": ["pkg1", "pkg2"],
            "pip_check": False,
            "pip_version": "<22,>20",
        }
    elif option == "conda_name":
        runtime_env["conda"] = "env_name"
    elif option == "conda_dict":
        runtime_env["conda"] = {"dependencies": ["dep1", "dep2"]}
    elif option == "container":
        runtime_env["container"] = {
            "image": "anyscale/ray-ml:nightly-py38-cpu",
            "worker_path": "/root/python/ray/_private/workers/default_worker.py",
            "run_options": ["--cap-drop SYS_ADMIN", "--log-level=debug"],
        }
    else:
        raise ValueError("unexpected option " + str(option))

    typed_runtime_env = RuntimeEnv(**runtime_env)
    serialized_runtime_env = typed_runtime_env.serialize()
    cls_runtime_env = RuntimeEnv.deserialize(serialized_runtime_env)
    cls_runtime_env_dict = cls_runtime_env.to_dict()

    if "pip" in typed_runtime_env and isinstance(typed_runtime_env["pip"], list):
        pip_config_in_cls_runtime_env = cls_runtime_env_dict.pop("pip")
        pip_config_in_runtime_env = typed_runtime_env.pop("pip")
        assert {
            "packages": pip_config_in_runtime_env,
            "pip_check": False,
        } == pip_config_in_cls_runtime_env

    assert cls_runtime_env_dict == typed_runtime_env


def test_runtime_env_interface():

    # Test the interface related to working_dir
    default_working_dir = "s3://bucket/key.zip"
    modify_working_dir = "s3://bucket/key_A.zip"
    runtime_env = RuntimeEnv(working_dir=default_working_dir)
    runtime_env_dict = runtime_env.to_dict()
    assert runtime_env.working_dir_uri() == default_working_dir
    runtime_env["working_dir"] = modify_working_dir
    runtime_env_dict["working_dir"] = modify_working_dir
    assert runtime_env.working_dir_uri() == modify_working_dir
    assert runtime_env.to_dict() == runtime_env_dict

    runtime_env.pop("working_dir")
    assert runtime_env.to_dict() == {}

    # Test the interface related to py_modules
    init_py_modules = ["s3://bucket/key_1.zip", "s3://bucket/key_2.zip"]
    addition_py_modules = ["s3://bucket/key_3.zip", "s3://bucket/key_4.zip"]
    runtime_env = RuntimeEnv(py_modules=init_py_modules)
    runtime_env_dict = runtime_env.to_dict()
    assert set(runtime_env.py_modules_uris()) == set(init_py_modules)
    runtime_env["py_modules"].extend(addition_py_modules)
    runtime_env_dict["py_modules"].extend(addition_py_modules)
    assert set(runtime_env.py_modules_uris()) == set(
        init_py_modules + addition_py_modules
    )
    assert runtime_env.to_dict() == runtime_env_dict

    runtime_env.pop("py_modules")
    assert runtime_env.to_dict() == {}

    # Test the interface related to env_vars
    init_env_vars = {"A": "a", "B": "b"}
    update_env_vars = {"C": "c"}
    runtime_env = RuntimeEnv(env_vars=init_env_vars)
    runtime_env_dict = runtime_env.to_dict()
    runtime_env["env_vars"].update(update_env_vars)
    runtime_env_dict["env_vars"].update(update_env_vars)
    init_env_vars_copy = init_env_vars.copy()
    init_env_vars_copy.update(update_env_vars)
    assert runtime_env["env_vars"] == init_env_vars_copy
    assert runtime_env_dict == runtime_env.to_dict()

    runtime_env.pop("env_vars")
    assert runtime_env.to_dict() == {}

    # Test the interface related to conda
    conda_name = "conda"
    modify_conda_name = "conda_A"
    conda_config = {"dependencies": ["dep1", "dep2"]}
    runtime_env = RuntimeEnv(conda=conda_name)
    runtime_env_dict = runtime_env.to_dict()
    assert runtime_env.has_conda()
    assert runtime_env.conda_env_name() == conda_name
    assert runtime_env.conda_config() is None
    runtime_env["conda"] = modify_conda_name
    runtime_env_dict["conda"] = modify_conda_name
    assert runtime_env_dict == runtime_env.to_dict()
    assert runtime_env.has_conda()
    assert runtime_env.conda_env_name() == modify_conda_name
    assert runtime_env.conda_config() is None
    runtime_env["conda"] = conda_config
    runtime_env_dict["conda"] = conda_config
    assert runtime_env_dict == runtime_env.to_dict()
    assert runtime_env.has_conda()
    assert runtime_env.conda_env_name() is None
    assert runtime_env.conda_config() == json.dumps(conda_config, sort_keys=True)

    runtime_env.pop("conda")
    assert runtime_env.to_dict() == {"_ray_commit": "{{RAY_COMMIT_SHA}}"}

    # Test the interface related to pip
    with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
        requirement_file = os.path.join(tmpdir, "requirements.txt")
        requirement_packages = ["dep5", "dep6"]
        with open(requirement_file, "wt") as f:
            for package in requirement_packages:
                f.write(package)
                f.write("\n")

        pip_packages = ["dep1", "dep2"]
        addition_pip_packages = ["dep3", "dep4"]
        runtime_env = RuntimeEnv(pip=pip_packages)
        runtime_env_dict = runtime_env.to_dict()
        assert runtime_env.has_pip()
        assert set(runtime_env.pip_config()["packages"]) == set(pip_packages)
        assert runtime_env.virtualenv_name() is None
        runtime_env["pip"]["packages"].extend(addition_pip_packages)
        runtime_env_dict["pip"]["packages"].extend(addition_pip_packages)
        # The default value of pip_check is False
        runtime_env_dict["pip"]["pip_check"] = False
        assert runtime_env_dict == runtime_env.to_dict()
        assert runtime_env.has_pip()
        assert set(runtime_env.pip_config()["packages"]) == set(
            pip_packages + addition_pip_packages
        )
        assert runtime_env.virtualenv_name() is None
        runtime_env["pip"] = requirement_file
        runtime_env_dict["pip"] = requirement_packages
        assert runtime_env.has_pip()
        assert set(runtime_env.pip_config()["packages"]) == set(requirement_packages)
        assert runtime_env.virtualenv_name() is None
        # The default value of pip_check is False
        runtime_env_dict["pip"] = dict(
            packages=runtime_env_dict["pip"], pip_check=False
        )
        assert runtime_env_dict == runtime_env.to_dict()

        runtime_env.pop("pip")
        assert runtime_env.to_dict() == {"_ray_commit": "{{RAY_COMMIT_SHA}}"}

    # Test conflict
    with pytest.raises(ValueError):
        RuntimeEnv(pip=pip_packages, conda=conda_name)

    runtime_env = RuntimeEnv(pip=pip_packages)
    runtime_env["conda"] = conda_name
    with pytest.raises(ValueError):
        runtime_env.serialize()

    # Test the interface related to container
    container_init = {
        "image": "anyscale/ray-ml:nightly-py38-cpu",
        "worker_path": "/root/python/ray/_private/workers/default_worker.py",
        "run_options": ["--cap-drop SYS_ADMIN", "--log-level=debug"],
    }
    update_container = {"image": "test_modify"}
    runtime_env = RuntimeEnv(container=container_init)
    runtime_env_dict = runtime_env.to_dict()
    assert runtime_env.has_py_container()
    assert runtime_env.py_container_image() == container_init["image"]
    assert runtime_env.py_container_worker_path() == container_init["worker_path"]
    assert runtime_env.py_container_run_options() == container_init["run_options"]
    runtime_env["container"].update(update_container)
    runtime_env_dict["container"].update(update_container)
    container_copy = container_init
    container_copy.update(update_container)
    assert runtime_env_dict == runtime_env.to_dict()
    assert runtime_env.has_py_container()
    assert runtime_env.py_container_image() == container_copy["image"]
    assert runtime_env.py_container_worker_path() == container_copy["worker_path"]
    assert runtime_env.py_container_run_options() == container_copy["run_options"]

    runtime_env.pop("container")
    assert runtime_env.to_dict() == {}


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
