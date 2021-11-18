"""Test that runtime_env raises good errors if ray[default] is not installed.


In CI, this file is run with a minimal ray installation (`pip install ray`.)

To run this test file locally, remove some dependency that appears in
ray[default] but not in ray (e.g., `pip uninstall aiohttp`) and set
`export RAY_MINIMAL=1`.

"""

import os
import sys
import pytest

import ray
from ray.exceptions import RuntimeEnvSetupError
from ray._private.test_utils import wait_for_condition


def _test_task_and_actor(capsys):
    @ray.remote
    def f():
        pass

    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.options(runtime_env={"pip": ["requests"]}).remote())

    def stderr_checker():
        captured = capsys.readouterr()
        return "ray[default]" in captured.err

    wait_for_condition(stderr_checker)

    @ray.remote
    class A:
        def task(self):
            pass

    A.options(runtime_env={"pip": ["requests"]}).remote()

    wait_for_condition(stderr_checker)


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25553 --port 0"],
    indirect=True)
def test_ray_client_task_actor(call_ray_start, capsys):
    ray.init("ray://localhost:25553")
    _test_task_and_actor(capsys)


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.")
def test_task_actor(shutdown_only, capsys):
    ray.init()
    _test_task_and_actor(capsys)


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.")
def test_ray_init(shutdown_only, capsys):
    with pytest.raises(RuntimeEnvSetupError):
        ray.init(runtime_env={"pip": ["requests"]})

        @ray.remote
        def f():
            pass

        ray.get(f.remote())

    def stderr_checker():
        captured = capsys.readouterr()
        return "ray[default]" in captured.err

    wait_for_condition(stderr_checker)


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test is only run in CI with a minimal Ray installation.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25552 --port 0"],
    indirect=True)
def test_ray_client_init(call_ray_start):
    with pytest.raises(ConnectionAbortedError) as excinfo:
        ray.init("ray://localhost:25552", runtime_env={"pip": ["requests"]})
    assert "ray[default]" in str(excinfo.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
