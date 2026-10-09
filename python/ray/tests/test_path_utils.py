import sys

import pytest

import ray._private.path_utils as PathUtils

# NOTE: Since PathUtils is cross-platform, each function _must_
# have tests for Windows and Posix (Linux/MacOS).

_is_posix = sys.platform in {"linux", "darwin"}


@pytest.mark.skipif(_is_posix, reason="Cannot create a Windows Path on POSIX systems.")
def test_is_path_returns_true_for_windows_path():
    path_str = "C:\\Some\\Dir"
    assert PathUtils.is_path(path_str)


@pytest.mark.skipif(not _is_posix, reason="Cannot create a POSIX Path on Windows.")
def test_is_path_returns_true_for_posix_path():
    path_str = "/home/some/dir"
    assert PathUtils.is_path(path_str)


def test_is_path_returns_false_for_uri():
    uri_str = "s3://some/remote/file"
    assert not PathUtils.is_path(uri_str)


def test_is_path_raises_error_for_non_string_input():
    with pytest.raises(TypeError, match="must be a string"):
        PathUtils.is_path(1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
