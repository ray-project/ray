from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
try:
    import psutil
except ImportError:
    psutil = None
import pytest
import ray


@ray.remote
def f():
    return 42


@pytest.mark.parametrize("env_var, process_name", [("RAY_RAYLET_GDB", "raylet/raylet"), ("RAY_PLASMA_STORE", "plasma/plasma_store_server")])
def test_raylet_gdb(env_var, process_name):
    # Save original environment variables and set new ones
    _environ = os.environ.copy()
    os.environ[env_var] = "1"

    # Test ray works as expected
    ray.init(num_cpus=1, ignore_reinit_error=True)
    assert ray.get(f.remote()) == 42

    # Test that gdb is running only if psutil is installed
    gdb_process = False
    if psutil is not None:
        for pid in psutil.pids():
            try:
                proc = psutil.Process(pid)
            except:
                continue
            if "gdb" in proc.name() and process_name in proc.name():
                gdb_process = True
                proc.terminate()
                break
        assert gdb_process

    # Restore environment variables
    os.environ.clear()
    os.environ.update(_environ)

    ray.shutdown()
