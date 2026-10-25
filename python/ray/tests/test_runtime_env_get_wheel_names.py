import sys

import pytest
import requests

import ray._private.ray_constants as ray_constants
from ray._private.utils import (
    get_master_wheel_url,
    get_release_wheel_url,
    get_wheel_filename,
)


def test_get_wheel_filename():
    """Test the code that generates the filenames of the `latest` wheels."""
    # NOTE: These should not be changed for releases.
    ray_version = "3.0.0.dev0"
    for arch in ["x86_64", "aarch64", "arm64"]:
        for sys_platform in ["darwin", "linux", "win32"]:
            # Windows only has x86_64 wheels
            if sys_platform == "win32" and arch != "x86_64":
                continue
            # MacOS only has arm64 wheels
            if sys_platform == "darwin" and arch == "x86_64":
                continue

            for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
                filename = get_wheel_filename(
                    sys_platform, ray_version, py_version, arch
                )
                prefix = "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/"
                url = f"{prefix}{filename}"
                assert requests.head(url).status_code == 200, url


def test_get_master_wheel_url():
    """Test the code that generates the filenames of `master` commit wheels."""
    # NOTE: These should not be changed for releases.
    ray_version = "3.0.0.dev0"
    # This should be a commit for which wheels have already been built for
    # all platforms and python versions at
    # `s3://ray-wheels/master/<test_commit>/`.
    #
    # Link to commit:
    # https://github.com/ray-project/ray/commit/faf06e09e55558fb36c72e91a5cf8a7e3da8b8c6
    test_commit = "faf06e09e55558fb36c72e91a5cf8a7e3da8b8c6"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            url = get_master_wheel_url(
                test_commit, sys_platform, ray_version, py_version
            )
            assert requests.head(url).status_code == 200, url


def test_get_release_wheel_url():
    """Test the code that generates the filenames of the `release` branch wheels."""
    # This should be a commit for which wheels have already been built for
    # all platforms and python versions at
    # `s3://ray-wheels/releases/2.2.0/<commit>/`.
    test_commits = {"2.49.2": "479fa716904109d9df4b56b98ca3c3350e1ec13c"}
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            for version, commit in test_commits.items():
                url = get_release_wheel_url(commit, sys_platform, version, py_version)
                assert requests.head(url).status_code == 200, url


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
