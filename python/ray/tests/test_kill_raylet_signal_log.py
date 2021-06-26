import psutil
import signal
import sys
import pytest
import ray


def get_pid(name):
    pids = psutil.process_iter()
    for pid in pids:
        if (pid.name() == name):
            return pid.pid


def test_kill_raylet_signal_log(shutdown_only):
    ray.init(num_cpus=1)
    session_dir = ray.worker._global_node.get_session_dir_path()
    raylet_out_path = "{}/logs/raylet.out".format(session_dir)
    pid = get_pid("raylet")
    p = psutil.Process(pid)
    p.send_signal(signal.SIGTERM)
    p.wait()
    with open(raylet_out_path) as f:
        s = f.read()
        assert "SIGTERM" in s


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
