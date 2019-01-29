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


@pytest.parameterize((env_var, process_name), [("RAY_RAYLET_GDB", "raylet/raylet"), ("RAY_PLASMA_STORE", "plasma/plasma_store_server")])
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
                proc.terminate()
                break;
        assert gdb_process

    # Restore environment variables
    os.environ.clear()
    os.environ.update(_environ)

    ray.shutdown()
