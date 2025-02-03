# End-to-end tests for using "uv run" with py_executable

import os
from pathlib import Path
import pytest
import sys
import tempfile

import ray


PYPROJECT_TOML = """
[project]
name = "test"
version = "0.1"
dependencies = [
  "emoji",
]
requires-python = ">=3.9"
"""


@pytest.fixture(scope="function")
def tmp_working_dir():
    """A test fixture which writes a pyproject.toml."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        script_file = path / "pyproject.toml"
        with script_file.open(mode="w") as f:
            f.write(PYPROJECT_TOML)

        yield tmp_dir


def test_uv_run_simple(shutdown_only):
    runtime_env = {
        "py_executable": "uv run --with emoji --no-project",
    }
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


def test_uv_run_pyproject(shutdown_only, tmp_working_dir):
    tmp_dir = tmp_working_dir

    ray.init(runtime_env={
        "working_dir": tmp_dir,
        # We want to run in the system environment so the current installation of Ray can be found here
        "py_executable": f"env PYTHONPATH={':'.join(sys.path)} uv run --python-preference=only-system"
    })

    @ray.remote
    def emojize():
        import emoji
        return emoji.emojize("Ray rocks :thumbs_up:")
    
    assert ray.get(emojize.remote()) == "Ray rocks 👍"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
