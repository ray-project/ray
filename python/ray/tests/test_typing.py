import sys
import os

import mypy.api as mypy_api
import pytest

TYPING_TEST_DIRS = os.path.join(os.path.dirname(__file__), "typing_files")


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_typing_good():
    script = os.path.join(TYPING_TEST_DIRS, "check_typing_good.py")
    msg, _, status_code = mypy_api.run([script])
    assert status_code == 0, msg


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_typing_bad():
    script = os.path.join(TYPING_TEST_DIRS, "check_typing_bad.py")
    msg, _, status_code = mypy_api.run([script])
    assert status_code == 1, msg


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
