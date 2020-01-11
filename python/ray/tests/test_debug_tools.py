import os
import subprocess
import sys

import pytest

import ray


@pytest.fixture
def ray_gdb_start():
    # Setup environment and start ray
    _environ = os.environ.copy()
    for process_name in ["RAYLET", "PLASMA_STORE"]:
        os.environ["RAY_{}_GDB".format(process_name)] = "1"
        os.environ["RAY_{}_TMUX".format(process_name)] = "1"

    yield None

    # Restore original environment and stop ray
    os.environ.clear()
    os.environ.update(_environ)
    ray.shutdown()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="This test requires Linux.")
def test_raylet_gdb(ray_gdb_start):
    # ray_gdb_start yields the expected process name
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        return 42

    assert ray.get(f.remote()) == 42

    # Check process name in `ps aux | grep gdb`
    for process_name in ["raylet/raylet", "plasma/plasma_store_server"]:
        pgrep_command = subprocess.Popen(
            ["pgrep", "-f", "gdb.*{}".format(process_name)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        assert pgrep_command.communicate()[0]


if __name__ == "__main__":
    import pytest
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
