import re
import socket
import subprocess
import sys
import time

import pytest
import requests
from ray.test_utils import run_string_as_driver, wait_for_condition

import ray
from ray import ray_constants


def test_ray_start_default_port_conflict(call_ray_stop_only, shutdown_only):
    subprocess.check_call(["ray", "start", "--head"])
    ray.init(address="auto")
    assert str(ray_constants.DEFAULT_DASHBOARD_PORT) in ray.get_dashboard_url()

    error_raised = False
    try:
        subprocess.check_output(
            [
                "ray",
                "start",
                "--head",
                "--port",
                "9999",  # use a different gcs port
                "--include-dashboard=True"
            ],
            stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        assert b"already occupied" in e.stderr
        error_raised = True

    assert error_raised, "ray start should cause a conflict error"


def test_port_auto_increment(shutdown_only):
    ray.init()
    url = ray.get_dashboard_url()

    def dashboard_available():
        try:
            requests.get("http://" + url).status_code == 200
            return True
        except Exception:
            return False

    wait_for_condition(dashboard_available)

    run_string_as_driver(f"""
import ray
from ray.test_utils import wait_for_condition
import requests
ray.init()
url = ray.get_dashboard_url()
assert url != "{url}"
def dashboard_available():
    try:
        requests.get("http://"+url).status_code == 200
        return True
    except:
        return False
wait_for_condition(dashboard_available)
ray.shutdown()
        """)


def test_port_conflict(call_ray_stop_only, shutdown_only):
    sock = socket.socket()
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 0)
    sock.bind(("127.0.0.1", 9999))

    try:
        subprocess.check_output(
            [
                "ray", "start", "--head", "--port", "9989", "--dashboard-port",
                "9999", "--include-dashboard=True"
            ],
            stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        assert b"already occupied" in e.stderr

    with pytest.raises(ValueError, match="already occupied"):
        ray.init(dashboard_port=9999, include_dashboard=True)

    sock.close()


@pytest.mark.skipif(
    sys.version_info < (3, 5, 3), reason="requires python3.5.3 or higher")
def test_dashboard(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    dashboard_url = addresses["webui_url"]
    assert ray.get_dashboard_url() == dashboard_url

    assert re.match(r"^(localhost|\d+\.\d+\.\d+\.\d+):\d+$", dashboard_url)

    start_time = time.time()
    while True:
        try:
            node_info_url = f"http://{dashboard_url}/nodes"
            resp = requests.get(node_info_url, params={"view": "summary"})
            resp.raise_for_status()
            summaries = resp.json()
            assert summaries["result"] is True
            assert "msg" in summaries
            break
        except (requests.exceptions.ConnectionError, AssertionError):
            if time.time() > start_time + 30:
                out_log = None
                with open(
                        "{}/logs/dashboard.log".format(
                            addresses["session_dir"]), "r") as f:
                    out_log = f.read()
                raise Exception(
                    "Timed out while waiting for dashboard to start. "
                    f"Dashboard output log: {out_log}\n")


if __name__ == "__main__":
    import sys

    import pytest
    sys.exit(pytest.main(["-v", __file__]))
