from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import ray
import subprocess


@pytest.mark.parametrize(
    "env_var, process_name",
    [("RAY_PLASMA_STORE_GDB", "plasma/plasma_store_server"),
     ("RAY_RAYLET_GDB", "raylet/raylet")])
def test_raylet_gdb(env_var, process_name):
    # Save original environment variables and set new ones
    _environ = os.environ.copy()
    os.environ[env_var] = "1"

    # Test ray works as expected
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        return 42

    assert ray.get(f.remote()) == 42

    # Check process name in `ps aux | grep gdb`
    assert subprocess.check_output(
        "ps aux | grep gdb | grep {}".format(process_name), shell=True)

    ray.shutdown()

    # Restore environment variables
    os.environ.clear()
    os.environ.update(_environ)
