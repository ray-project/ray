from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess

import pytest

import ray


@pytest.fixture(params=[("PLASMA_STORE"), ("RAYLET")])
def ray_gdb_start(request):
    # Setup environment and start ray
    _environ = os.environ.copy()
    os.environ["RAY_{}_GDB".format(request.param)] = "1"
    os.environ["RAY_{}_TMUX".format(request.param)] = "1"
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


def test_raylet_gdb(ray_gdb_start):
    # ray_gdb_start yields the expected process name
    process_name = ray_gdb_start

    @ray.remote
    def f():
        return 42
    assert ray.get(f.remote()) == 42

    # Check process name in `ps aux | grep gdb`
    assert subprocess.Popen(["pgrep", "-f", "gdb.*{}".format(process_name)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.call(["pkill", "-f", "gdb.*{}".format(process_name)])
