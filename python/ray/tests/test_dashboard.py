import os
import re
import socket
import subprocess
import sys
import time

import psutil
import pytest
import requests
from ray._private.test_utils import (
    run_string_as_driver,
    wait_for_condition,
    get_error_message,
    get_log_batch,
)

import ray
from ray import ray_constants


def search_agents(cluster):
    all_processes = cluster.head_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    def _search_agent(processes):
        for p in processes:
            try:
                for c in p.cmdline():
                    if os.path.join("dashboard", "agent.py") in c:
                        return p
            except Exception:
                pass

    agent_proc = _search_agent(raylet_proc.children())
    return agent_proc


def test_ray_start_default_port_conflict(call_ray_stop_only, shutdown_only):
    subprocess.check_call(["ray", "start", "--head"])
    ray.init(address="auto")
    assert str(ray_constants.DEFAULT_DASHBOARD_PORT) in ray.worker.get_dashboard_url()

    error_raised = False
    try:
        subprocess.check_output(
            [
                "ray",
                "start",
                "--head",
                "--port",
                "9999",  # use a different gcs port
                "--include-dashboard=True",
            ],
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        assert b"already occupied" in e.stderr
        error_raised = True

    assert error_raised, "ray start should cause a conflict error"


def test_port_auto_increment(shutdown_only):
    ray.init()
    url = ray.worker.get_dashboard_url()

    def dashboard_available():
        try:
            requests.get("http://" + url).status_code == 200
            return True
        except Exception:
            return False

    wait_for_condition(dashboard_available)

    run_string_as_driver(
        f"""
import ray
from ray._private.test_utils import wait_for_condition
import requests
ray.init()
url = ray.worker.get_dashboard_url()
assert url != "{url}"
def dashboard_available():
    try:
        requests.get("http://"+url).status_code == 200
        return True
    except:
        return False
wait_for_condition(dashboard_available)
ray.shutdown()
        """
    )


def test_port_conflict(call_ray_stop_only, shutdown_only):
    sock = socket.socket()
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 0)
    sock.bind(("127.0.0.1", 9999))

    try:
        subprocess.check_output(
            [
                "ray",
                "start",
                "--head",
                "--port",
                "9989",
                "--dashboard-port",
                "9999",
                "--include-dashboard=True",
            ],
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        assert b"already occupied" in e.stderr

    with pytest.raises(ValueError, match="already occupied"):
        ray.init(dashboard_port=9999, include_dashboard=True)

    sock.close()


def test_dashboard(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    dashboard_url = addresses["webui_url"]
    assert ray.worker.get_dashboard_url() == dashboard_url

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
                    "{}/logs/dashboard.log".format(addresses["session_dir"]), "r"
                ) as f:
                    out_log = f.read()
                raise Exception(
                    "Timed out while waiting for dashboard to start. "
                    f"Dashboard output log: {out_log}\n"
                )


@pytest.fixture
def set_agent_failure_env_var():
    os.environ["_RAY_AGENT_FAILING"] = "1"
    yield
    del os.environ["_RAY_AGENT_FAILING"]


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "_system_config": {
                "agent_restart_interval_ms": 10,
                "agent_max_restart_count": 5,
            }
        }
    ],
    indirect=True,
)
def test_dashboard_agent_restart(
    set_agent_failure_env_var, ray_start_cluster_head, error_pubsub, log_pubsub
):
    """Test that when the agent fails to start many times in a row
    if the error message is suppressed correctly without spamming
    the driver.
    """
    # Choose a duplicated port for the agent so that it will crash.
    errors = get_error_message(
        error_pubsub, 1, ray_constants.DASHBOARD_AGENT_DIED_ERROR, timeout=10
    )
    assert len(errors) == 1
    for e in errors:
        assert (
            "There are 3 possible problems " "if you see this error." in e.error_message
        )
    # Make sure the agent process is not started anymore.
    cluster = ray_start_cluster_head
    wait_for_condition(lambda: search_agents(cluster) is None)

    # Make sure there's no spammy message for 5 seconds.
    def matcher(log_batch):
        return log_batch["pid"] != "autoscaler"

    match = get_log_batch(log_pubsub, 1, timeout=5, matcher=matcher)
    assert len(match) == 0, (
        "There are spammy logs during Ray agent restart process. " f"Logs: {match}"
    )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
