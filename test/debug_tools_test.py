from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import process
try:
    import psutil
except ImportError:
    psutil = None
import ray


@ray.remote
def f():
    return 42


def test_raylet_gdb():
    # Save original environment variables and set new ones
    _environ = os.environ.copy()
    os.environ["RAY_RAYLET_GDB"] = "1"

    # Test ray works as expected
    ray.init(num_cpus=1)
    assert ray.get(f.remote()) == 42

    # Test that gdb is running only if psutil is installed
    gdb_process = False
    if psutil is not None:
        for pid in psutil.pids():
            proc = psutil.Process(pid)
            if "gdb" in proc.name and "raylet/raylet" in proc.name:
                gdb_process = True
                break;
        assert gdb_process

    # Restore environment variables
    os.environ.clear()
    os.environ.update(_environ)


def test_plasma_gdb():
    # Save original environment variables and set new ones
    _environ = os.environ.copy()
    os.environ["RAY_PLASMA_STORE_GDB"] = "1"

    # Test ray works as expected
    ray.init(num_cpus=1)
    assert ray.get(f.remote()) == 42

    # Test that gdb is running only if psutil is installed
    gdb_process = False
    if psutil is not None:
        for pid in psutil.pids():
            proc = psutil.Process(pid)
            if "gdb" in proc.name and "plasma/plasma_store_server" in proc.name:
                gdb_process = True
                break;
        assert gdb_process

    # Restore environment variables
    os.environ.clear()
    os.environ.update(_environ)
