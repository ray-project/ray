import os
import sys
import tempfile
from pathlib import Path

import pytest

import ray


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


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_py_executable_path_with_space_and_parens(shutdown_only, tmp_path):
    # A wrapper script rather than a symlink, which can lose a venv's packages.
    py_executable = tmp_path / "dir with space (and parens)" / "python"
    py_executable.parent.mkdir()
    py_executable.write_text(
        "#!/bin/sh\n"
        "export RAY_TEST_PY_EXECUTABLE_WRAPPER=1\n"
        f'exec "{sys.executable}" "$@"\n'
    )
    py_executable.chmod(0o755)
    ray.init(runtime_env={"py_executable": str(py_executable)}, include_dashboard=False)

    @ray.remote
    def f():
        return os.environ.get("RAY_TEST_PY_EXECUTABLE_WRAPPER")

    assert ray.get(f.remote(), timeout=60) == "1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
