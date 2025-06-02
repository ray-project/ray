from dataclasses import dataclass
import dataclasses
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest
from ray.runtime_env.runtime_env import (
    RuntimeEnvConfig,
    _merge_runtime_env,
    _validate_no_local_paths,
)
import requests

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.uri_cache import URICache
from ray._private.runtime_env.utils import (
    SubprocessCalledProcessError,
    check_output_cmd,
)
from ray._private.test_utils import (
    chdir,
    get_error_message,
    get_log_sources,
    wait_for_condition,
)
from ray._private.utils import (
    get_master_wheel_url,
    get_release_wheel_url,
    get_wheel_filename,
)
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv

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
