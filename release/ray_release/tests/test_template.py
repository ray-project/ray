import sys

import pytest

from ray_release.exception import ReleaseTestConfigError
from ray_release.template import bazel_runfile, get_working_dir
from ray_release.test import Test


def test_get_working_dir_with_path_from_root():
    test_with_path_from_root = Test(
        {
            "name": "test",
            "working_dir": "//ray_testing/ray_release/tests",
        }
    )
    assert (
        get_working_dir(test_with_path_from_root, None, "/tmp/bazel_workspace")
        == "/tmp/bazel_workspace/ray_testing/ray_release/tests"
    )
    assert get_working_dir(test_with_path_from_root, None, None) == bazel_runfile(
        "ray_testing/ray_release/tests"
    )


def test_get_working_dir_with_relative_path():
    test_with_relative_path = Test(
        {
            "name": "test",
            "working_dir": "ray_release/tests",
        }
    )
    assert (
        get_working_dir(test_with_relative_path, None, "/tmp/bazel_workspace")
        == "/tmp/bazel_workspace/release/ray_release/tests"
    )
    assert get_working_dir(test_with_relative_path, None, None) == bazel_runfile(
        "release/ray_release/tests"
    )


def test_get_working_dir_fail():
    test_with_path_from_root = Test(
        {
            "name": "test",
            "working_dir": "//ray_testing/ray_release/tests",
        }
    )
    with pytest.raises(ReleaseTestConfigError):
        get_working_dir(
            test_with_path_from_root, "/tmp/test_definition_root", "tmp/bazel_workspace"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
