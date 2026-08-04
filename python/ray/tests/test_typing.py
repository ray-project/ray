import os
import shutil
import subprocess
import sys
import tempfile

import mypy.api as mypy_api
import pyright
import pytest

# Paths are relative to the directory where Bazel is run in the CI
TYPING_GOOD_PATH = "python/ray/tests/typing_files/check_typing_good.py"
TYPING_BAD_PATH = "python/ray/tests/typing_files/check_typing_bad.py"
TYPING_ACTOR_ASYNC_PATH = "python/ray/tests/typing_files/check_typing_actor_async.py"


def test_typing_good():
    typing_good_tmp_path = create_tmp_copy(TYPING_GOOD_PATH)
    out, msg, status_code = mypy_api.run([typing_good_tmp_path])
    assert status_code == 0, out


def test_typing_bad():
    typing_bad_tmp_path = create_tmp_copy(TYPING_BAD_PATH)
    _, msg, status_code = mypy_api.run([typing_bad_tmp_path])
    assert status_code == 1, msg


def test_typing_actor_async():
    typing_actor_async_tmp_path = create_tmp_copy(TYPING_ACTOR_ASYNC_PATH)
    result = pyright.run(
        typing_actor_async_tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=tempfile.gettempdir(),
    )
    assert (
        result.returncode == 0
    ), f"Pyright check failed. stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"


def create_tmp_copy(file_path: str) -> str:
    """Copies file at file_path to a temporary file and returns the path."""

    base_file_name = os.path.basename(file_path)
    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, base_file_name)
    shutil.copy(file_path, tmp_path)
    print(f"Copied file at {file_path} to {tmp_path}")

    return tmp_path


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"

    sys.exit(pytest.main(["-sv", __file__]))
