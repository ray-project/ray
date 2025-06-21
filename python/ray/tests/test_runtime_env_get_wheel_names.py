import sys

import pytest
import requests

from ray._private.utils import (
    get_master_wheel_url,
    get_release_wheel_url,
    get_wheel_filename,
)

import ray._private.ray_constants as ray_constants


def test_get_wheel_filename():
    """Test the code that generates the filenames of the `latest` wheels."""
    # NOTE: These should not be changed for releases.
    ray_version = "3.0.0.dev0"
    for arch in ["x86_64", "aarch64", "arm64"]:
        for sys_platform in ["darwin", "linux", "win32"]:
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
    test_commit = "593d04aba2726a0104280d1bdbc2779e3a8ba7d4"
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
    test_commits = {"2.31.0": "1240d3fc326517f9be28bb7897c1c88619f0d984"}
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            for version, commit in test_commits.items():
                url = get_release_wheel_url(commit, sys_platform, version, py_version)
                assert requests.head(url).status_code == 200, url


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
