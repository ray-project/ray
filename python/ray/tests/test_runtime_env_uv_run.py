# End-to-end tests for using "uv run" with py_executable

import os
from pathlib import Path
import pytest
import subprocess
import sys
import tempfile

import ray


@pytest.fixture(scope="function")
def with_uv():
    import platform
    import stat
    import tarfile
    from urllib import request

    arch = "aarch64" if platform.machine() in ["aarch64", "arm64"] else "i686"
    system = "unknown-linux-gnu" if platform.system() == "Linux" else "apple-darwin"
    name = f"uv-{arch}-{system}"
    url = f"https://github.com/astral-sh/uv/releases/download/0.5.27/{name}.tar.gz"
    with tempfile.TemporaryDirectory() as tmp_dir:
        with request.urlopen(request.Request(url), timeout=15.0) as response:
            with tarfile.open(fileobj=response, mode="r|*") as tar:
                tar.extractall(tmp_dir)
        uv = Path(tmp_dir) / name / "uv"
        uv.chmod(uv.stat().st_mode | stat.S_IEXEC)
        yield uv


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
    """A test fixture, which writes a pyproject.toml."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        script_file = path / "pyproject.toml"
        with script_file.open(mode="w") as f:
            f.write(PYPROJECT_TOML)

        yield tmp_dir


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_simple(shutdown_only, with_uv):
    uv = with_uv

    runtime_env = {
        "py_executable": f"{uv} run --with emoji --no-project",
    }
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_pyproject(shutdown_only, with_uv, tmp_working_dir):
    uv = with_uv
    tmp_dir = tmp_working_dir

    ray.init(
        runtime_env={
            "working_dir": tmp_dir,
            # We want to run in the system environment so the current installation of Ray can be found here
            "py_executable": f"env PYTHONPATH={':'.join(sys.path)} {uv} run --python-preference=only-system",
        }
    )

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_editable(shutdown_only, with_uv, tmp_working_dir):
    uv = with_uv
    tmp_dir = tmp_working_dir

    subprocess.run(
        ["git", "clone", "https://github.com/carpedm20/emoji/", "emoji_copy"],
        cwd=tmp_dir,
    )

    subprocess.run(
        ["git", "reset", "--hard", "08c5cc4789d924ad4215e2fb2ee8f0b19a0d421f"],
        cwd=Path(tmp_dir) / "emoji_copy",
    )

    subprocess.run(
        [uv, "add", "--editable", "./emoji_copy"],
        cwd=tmp_dir,
    )

    # Now edit the package
    content = ""
    with open(Path(tmp_dir) / "emoji_copy" / "emoji" / "core.py") as f:
        content = f.read()

    content = content.replace(
        "return pattern.sub(replace, string)", 'return "The package was edited"'
    )

    with open(Path(tmp_dir) / "emoji_copy" / "emoji" / "core.py", "w") as f:
        f.write(content)

    ray.init(
        runtime_env={
            "working_dir": tmp_dir,
            # We want to run in the system environment so the current installation of Ray can be found here
            "py_executable": f"env PYTHONPATH={':'.join(sys.path)} {uv} run --python-preference=only-system",
        }
    )

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "The package was edited"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
