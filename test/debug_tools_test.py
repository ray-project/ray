from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess

import pytest

import ray


@pytest.fixture
def ray_gdb_start():
    # Setup environment and start ray
    _environ = os.environ.copy()
    for process_name in ["RAYLET", "PLASMA_STORE"]:
        os.environ["RAY_{}_GDB".format(process_name)] = "1"
        os.environ["RAY_{}_TMUX".format(process_name)] = "1"
    ray.init(num_cpus=1)

    # Yield expected process keyword
    yield ["raylet/raylet", "plasma/plasma_store_server"]

    # Restore original environment and stop ray
    os.environ.clear()
    os.environ.update(_environ)
    ray.shutdown()


def test_raylet_gdb(ray_gdb_start):
    # ray_gdb_start yields the expected process name
    process_names = ray_gdb_start

    @ray.remote
    def f():
        return 42

    assert ray.get(f.remote()) == 42

    # Check process name in `ps aux | grep gdb`
    for process_name in process_names:
        pgrep_command = subprocess.Popen(
            ["pgrep", "-f", "gdb.*{}".format(process_name)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        assert pgrep_command.communicate()[0]
        subprocess.call(["pkill", "-f", "gdb.*{}".format(process_name)])
