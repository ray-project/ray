import os
import sys
import time

import pytest
import ray


def test_ray_log_redirected(shutdown_only):
    ray.init(num_cpus=1)
    session_dir = ray.worker._global_node.get_session_dir_path()
    time.sleep(1.0)
    assert os.path.exists(session_dir), "Specified socket path not found."
    raylet_out_path = "{}/logs/raylet.out".format(session_dir)
    raylet_err_path = "{}/logs/raylet.err".format(session_dir)
    assert os.path.exists(raylet_out_path), "Raylet out not found"
    assert os.path.exists(raylet_err_path), "Raylet err not found"
    assert os.path.getsize(raylet_out_path) > 0
    assert os.path.getsize(raylet_err_path) > 0


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
