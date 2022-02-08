import sys
import os
import shutil

import mypy.api as mypy_api
import pytest

TYPING_TEST_DIRS = os.path.join(os.path.dirname(__file__), "typing_files")


def test_typing_good():
    script = "check_typing_good.py"
    if os.getenv("RUNNING_BAZEL", None) != "true":
        script = os.path.join(TYPING_TEST_DIRS, script)
    _, msg, status_code = mypy_api.run([script])
    assert status_code == 0, msg


def test_typing_bad():
    script = "check_typing_bad.py"
    if os.getenv("RUNNING_BAZEL", None) != "true":
        script = os.path.join(TYPING_TEST_DIRS, script)
    _, msg, status_code = mypy_api.run([script])
    assert status_code == 1, msg


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    os.environ["RUNNING_BAZEL"] = "true"
    # copying is necessary because otherwise, ray is imported from the source
    # files in directory, `Bazel.runfiles_*/runfiles/com_github_ray_project_ray/python`
    # and mypy is unable to detect attributes that should be present in ray.
    typing_files_dir = "python/ray/tests/typing_files/"
    shutil.copy(typing_files_dir + "check_typing_good.py", os.getcwd())
    shutil.copy(typing_files_dir + "check_typing_bad.py", os.getcwd())
    sys.exit(pytest.main(["-v", __file__]))
