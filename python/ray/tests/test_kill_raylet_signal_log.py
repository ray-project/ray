import signal
import sys

# Import psutil after ray so the packaged version is used.
import psutil
import pytest

import ray
from ray._common.test_utils import wait_for_condition


def get_pid(name):
    pids = psutil.process_iter()
    for pid in pids:
        if name in pid.name():
            return pid.pid

    return -1


def check_result(filename, num_signal, check_key):
    ray.init(num_cpus=1)
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    raylet_out_path = filename.format(session_dir)
    pid = get_pid("raylet")
    assert pid > 0
    p = psutil.Process(pid)
    p.send_signal(num_signal)
    p.wait(timeout=15)

    def check_file():
        with open(raylet_out_path) as f:
            s = f.read()
            return check_key in s

    wait_for_condition(check_file)


@pytest.mark.skipif(sys.platform == "win32", reason="Not support on Windows.")
def test_kill_raylet_signal_log(shutdown_only):
    check_result("{}/logs/raylet.err", signal.SIGABRT, "SIGABRT")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
