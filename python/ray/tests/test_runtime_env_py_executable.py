import os
import sys
import tempfile
from pathlib import Path
from typing import List

import pytest

import ray
from ray.exceptions import RuntimeEnvSetupError


@pytest.fixture(scope="function")
def tmp_working_dir():
    """A test fixture, which writes a test file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        script_file = path / "start.sh"
        with script_file.open(mode="w") as f:
            f.write(sys.executable + " $@")

        yield tmp_dir


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_simple_py_executable(shutdown_only):
    runtime_env = {
        "py_executable": "env RAY_TEST_PY_EXECUTABLE_ENV_EXAMPLE=1 " + sys.executable
    }
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def f():
        return os.environ["RAY_TEST_PY_EXECUTABLE_ENV_EXAMPLE"]

    assert ray.get(f.remote()) == "1"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_py_executable_with_working_dir(shutdown_only, tmp_working_dir):
    tmp_dir = tmp_working_dir

    runtime_env = {"working_dir": tmp_dir, "py_executable": "bash start.sh"}
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def f():
        return "hello"

    assert ray.get(f.remote()) == "hello"


def _py_executable_via_wrapper(tmp_path: Path, wrapper_source: str) -> str:
    """A py_executable that runs `wrapper_source` before the worker script.

    The wrapper receives the worker script as sys.argv[1] and must run it.
    """
    wrapper = tmp_path / "wrapper.py"
    wrapper.write_text(
        "import runpy, sys\n"
        f"{wrapper_source}\n"
        "sys.argv = sys.argv[1:]\n"
        "runpy.run_path(sys.argv[0], run_name='__main__')\n"
    )
    return f"{sys.executable} {wrapper}"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
@pytest.mark.parametrize(
    "wrapper_source,expected_in_message",
    [
        pytest.param(
            "import ray; ray.__version__ = '0.0.0.fake'",
            ["Version mismatch", "0.0.0.fake", ray.__version__],
            id="different_ray_version",
        ),
        pytest.param(
            "sys.modules['ray'] = None",
            ["cannot import Ray", "ModuleNotFoundError"],
            id="cannot_import_ray",
        ),
    ],
)
def test_py_executable_with_incompatible_ray_fails_task(
    shutdown_only, tmp_path: Path, wrapper_source: str, expected_in_message: List[str]
):
    py_executable = _py_executable_via_wrapper(tmp_path, wrapper_source)
    ray.init()

    @ray.remote(runtime_env={"py_executable": py_executable})
    def f():
        return "should not run"

    with pytest.raises(RuntimeEnvSetupError) as exc_info:
        ray.get(f.remote(), timeout=60)

    message = str(exc_info.value)
    assert py_executable in message
    for expected in expected_in_message:
        assert expected in message


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
