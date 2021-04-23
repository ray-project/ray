# coding: utf-8
import logging
import pytest
import signal
import subprocess
import sys
import time

import ray

logger = logging.getLogger(__name__)


def test_healthcheck():
    res = subprocess.run(["ray", "health-check"])
    assert res.returncode != 0

    ray.init()
    res = subprocess.run(["ray", "health-check"])
    assert res.returncode == 0, res.stdout

    ray.shutdown()

    res = subprocess.run(["ray", "health-check"])
    assert res.returncode != 0


@pytest.mark.skipif(sys.platform == "win32", reason="Uses unix SIGKILL")
def test_healthcheck_ray_client_server():
    res = subprocess.run(
        ["ray", "health-check", "--component", "ray_client_server"])
    assert res.returncode != 0

    ray.init()
    res = subprocess.run(
        ["ray", "health-check", "--component", "ray_client_server"])
    assert res.returncode != 0, res.stdout

    client_server_handle = subprocess.Popen(
        [sys.executable, "-m", "ray.util.client.server"])
    # Gotta give the server time to initialize.
    time.sleep(5)

    res = subprocess.run(
        ["ray", "health-check", "--component", "ray_client_server"])
    assert res.returncode == 0, res.stdout

    client_server_handle.send_signal(signal.SIGKILL)
    time.sleep(ray.ray_constants.HEALTHCHECK_EXPIRATION_S)
    res = subprocess.run(
        ["ray", "health-check", "--component", "ray_client_server"])
    assert res.returncode != 0, res.stdout

    ray.shutdown()
    res = subprocess.run(
        ["ray", "health-check", "--component", "ray_client_server"])
    assert res.returncode != 0, res.stdout


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
