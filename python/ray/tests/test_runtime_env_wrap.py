import os
import pytest
import sys
import tempfile
from pathlib import Path

import ray


@pytest.fixture(scope="function")
def tmp_working_dir():
    """A test fixture which writes a test file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        script_file = path / "start.sh"
        with script_file.open(mode="w") as f:
            f.write(sys.executable + " $@")

        yield tmp_dir


def test_simple_wrap(shutdown_only):
    runtime_env = {"wrap": "env RAY_TEST_WRAP_ENV_EXAMPLE=1 " + sys.executable}
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def f():
        return os.environ["RAY_TEST_WRAP_ENV_EXAMPLE"]

    assert ray.get(f.remote()) == "1"


def test_wrap_with_working_dir(shutdown_only, tmp_working_dir):
    tmp_dir = tmp_working_dir

    runtime_env = {"working_dir": tmp_dir, "wrap": "bash start.sh "}
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def f():
        return "hello"

    assert ray.get(f.remote()) == "hello"
