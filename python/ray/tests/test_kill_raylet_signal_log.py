import os
import signal
import sys
import time
import pytest
import threading

import ray

from ray.test_utils import (
    wait_for_pid_to_exit, )

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


def check_result(path):
    with open(path) as f:
        time.sleep(1)
        s = f.read()
        assert len(s) > 0
        print(s)
        assert "SIGTERM" in s


class check_thread(threading.Thread):
    def __init__(self, raylet_out_path):
        threading.Thread.__init__(self)
        self._daemonic = True
        self.raylet_out_path = raylet_out_path

    def run(self):
        check_result(self.raylet_out_path)


def test_kill_raylet_signal_log(shutdown_only):
    @ray.remote
    def f():
        return os.getpid()

    ray.init(num_cpus=1)
    session_dir = ray.worker._global_node.get_session_dir_path()
    raylet_out_path = "{}/logs/raylet.out".format(session_dir)
    assert len(raylet_out_path) > 0
    pid = ray.get(f.remote())
    os.kill(pid, SIGKILL)
    wait_for_pid_to_exit(pid)
    thread = check_thread(raylet_out_path)
    thread.start()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
