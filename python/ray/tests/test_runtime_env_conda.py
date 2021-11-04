import json
import os
from contextlib import contextmanager
from pathlib import Path
import pytest
import subprocess
import sys
import tempfile
import time
from typing import List
from unittest import mock, skipIf
import yaml

import ray
from ray._private.runtime_env.conda import (
    inject_dependencies,
    _inject_ray_to_conda_site,
    _resolve_install_from_source_ray_dependencies,
    _current_py_version,
)

from ray._private.runtime_env.conda_utils import get_conda_env_list
from ray._private.test_utils import (
    run_string_as_driver, run_string_as_driver_nonblocking, wait_for_condition)
from ray._private.utils import get_conda_env_dir, get_conda_bin_executable

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.fixture(scope="function", params=["ray_client", "no_ray_client"])
def start_cluster(ray_start_cluster, request):
    assert request.param in {"ray_client", "no_ray_client"}
    use_ray_client: bool = request.param == "ray_client"

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    if use_ray_client:
        cluster.head_node._ray_params.ray_client_server_port = "10004"
        cluster.head_node.start_ray_client_server()
        address = "ray://localhost:10004"
    else:
        address = cluster.address

    yield cluster, address
