import dataclasses
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Any, Dict
from unittest import mock

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._private.runtime_env.uri_cache import URICache
from ray._private.runtime_env.utils import (
    SubprocessCalledProcessError,
    check_output_cmd,
)
from ray._private.test_utils import (
    chdir,
)
from ray.runtime_env import RuntimeEnv
from ray.runtime_env.runtime_env import (
    RuntimeEnvConfig,
    _merge_runtime_env,
)


def test_runtime_env_merge():
    # Both are None.
    parent = None
    child = None
    assert _merge_runtime_env(parent, child) == {}

    parent = {}
    child = None
    assert _merge_runtime_env(parent, child) == {}

    parent = None
    child = {}
    assert _merge_runtime_env(parent, child) == {}

    parent = {}
    child = {}
    assert _merge_runtime_env(parent, child) == {}

    # Only parent is given.
    parent = {"conda": ["requests"], "env_vars": {"A": "1"}}
    child = None
    assert _merge_runtime_env(parent, child) == parent

    # Only child is given.
    parent = None
    child = {"conda": ["requests"], "env_vars": {"A": "1"}}
    assert _merge_runtime_env(parent, child) == child

    # Successful case.
    parent = {"conda": ["requests"], "env_vars": {"A": "1"}}
    child = {"pip": ["requests"], "env_vars": {"B": "2"}}
    assert _merge_runtime_env(parent, child) == {
        "conda": ["requests"],
        "pip": ["requests"],
        "env_vars": {"A": "1", "B": "2"},
    }

    # Failure case
    parent = {"pip": ["requests"], "env_vars": {"A": "1"}}
    child = {"pip": ["colors"], "env_vars": {"B": "2"}}
    assert _merge_runtime_env(parent, child) is None

    # Failure case (env_vars)
    parent = {"pip": ["requests"], "env_vars": {"A": "1"}}
    child = {"conda": ["requests"], "env_vars": {"A": "2"}}
    assert _merge_runtime_env(parent, child) is None

    # override = True
    parent = {"pip": ["requests"], "env_vars": {"A": "1"}}
    child = {"pip": ["colors"], "env_vars": {"B": "2"}}
    assert _merge_runtime_env(parent, child, override=True) == {
        "pip": ["colors"],
        "env_vars": {"A": "1", "B": "2"},
    }

    # override = True + env vars
    parent = {"pip": ["requests"], "env_vars": {"A": "1"}}
    child = {"pip": ["colors"], "conda": ["requests"], "env_vars": {"A": "2"}}
    assert _merge_runtime_env(parent, child, override=True) == {
        "pip": ["colors"],
        "env_vars": {"A": "2"},
        "conda": ["requests"],
    }


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
            "pip_version": "==23.3.2;python_version=='3.9.16'",
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
def test_container_option_serialize(runtime_env_class):
    runtime_env = runtime_env_class(
        container={"image": "ray:latest", "run_options": ["--name=test"]}
    )
    job_config = ray.job_config.JobConfig(runtime_env=runtime_env)
    job_config_serialized = job_config._serialize()
    # job_config_serialized is JobConfig protobuf serialized string,
    # job_config.runtime_env_info.serialized_runtime_env
    # has container_option info
    assert job_config_serialized.count(b"ray:latest") == 1
    assert job_config_serialized.count(b"--name=test") == 1


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
def enable_dev_mode(local_env_var_enabled, monkeypatch):
    enabled = "1" if local_env_var_enabled else "0"
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED", enabled)
    yield


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
        "run_options": ["--cap-drop SYS_ADMIN", "--log-level=debug"],
    }
    update_container = {"image": "test_modify"}
    runtime_env = RuntimeEnv(container=container_init)
    runtime_env_dict = runtime_env.to_dict()
    assert runtime_env.has_py_container()
    assert runtime_env.py_container_image() == container_init["image"]
    assert runtime_env.py_container_run_options() == container_init["run_options"]
    runtime_env["container"].update(update_container)
    runtime_env_dict["container"].update(update_container)
    container_copy = container_init
    container_copy.update(update_container)
    assert runtime_env_dict == runtime_env.to_dict()
    assert runtime_env.has_py_container()
    assert runtime_env.py_container_image() == container_copy["image"]
    assert runtime_env.py_container_run_options() == container_copy["run_options"]

    runtime_env.pop("container")
    assert runtime_env.to_dict() == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
