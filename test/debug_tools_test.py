from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess

import pytest

import ray


@pytest.fixture(params=[("RAY_PLASMA_STORE_GDB"), ("RAY_RAYLET_GDB")])
def ray_gdb_start(request):
    # Setup environment and start ray
    _environ = os.environ.copy()
    os.environ[request.param] = "1"
    ray.init(num_cpus=1)

    # Yield expected process keyword
    if "PLASMA" in request.param:
        yield "plasma/plasma_store_server"
    elif "RAYLET" in request.param:
        yield "raylet/raylet"

    # Restore original environment and stop ray
    os.environ.clear()
    os.environ.update(_environ)
    ray.shutdown()


def test_raylet_gdb(ray_gdb_start, process_name):
    @ray.remote
    def f():
        return 42
    assert ray.get(f.remote()) == 42

    # Check process name in `ps aux | grep gdb`
    print(subprocess.check_output("ps aux | grep gdb | grep {}".format(process_name), shell=True))
