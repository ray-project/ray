import os
import shutil
import sys

import mypy.api as mypy_api
import pytest

TYPING_TEST_DIRS = os.path.join(os.path.dirname(__file__), "typing_files")


def test_typing_good():
    script = "check_typing_good.py"
    # the following check indicates that check_typing_good.py
    # is not present in the current directory, so follow
    # the path specified TYPING_TEST_DIRS
    if not os.path.exists(script):
        script = os.path.join(TYPING_TEST_DIRS, script)
    _, msg, status_code = mypy_api.run([script])
    assert status_code == 0, msg


def test_typing_bad():
    script = "check_typing_bad.py"
    # the following check indicates that check_typing_bad.py
    # is not present in the current directory, so follow
    # the path specified TYPING_TEST_DIRS
    if not os.path.exists(script):
        script = os.path.join(TYPING_TEST_DIRS, script)
    _, msg, status_code = mypy_api.run([script])
    assert status_code == 1, msg


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    # copying is necessary because otherwise, ray is imported from the source
    # files in directory, `Bazel.runfiles_*/runfiles/com_github_ray_project_ray/python`
    # and mypy is unable to detect attributes that should be present in ray.
    typing_files_dir = "python/ray/tests/typing_files/"
    checking_typing_good = typing_files_dir + "check_typing_good.py"
    checking_typing_bad = typing_files_dir + "check_typing_bad.py"
    if os.path.exists(checking_typing_good):
        shutil.copy(checking_typing_good, os.getcwd())
    if os.path.exists(checking_typing_bad):
        shutil.copy(checking_typing_bad, os.getcwd())
    sys.exit(pytest.main(["-v", __file__]))
