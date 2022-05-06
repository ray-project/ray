import os
import re
import subprocess
import sys
import time

import psutil
import pytest
import requests
from ray._private.test_utils import (
    run_string_as_driver,
    wait_for_condition,
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


@pytest.mark.parametrize(
    "listen_port",
    [9999],
    indirect=True,
)
def test_port_conflict(listen_port, call_ray_stop_only, shutdown_only):

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


conflict_port = 34567


def run_tasks_without_runtime_env():
    assert ray.is_initialized()

    @ray.remote
    def f():
        pass

    for _ in range(10):
        time.sleep(1)
        ray.get(f.remote())


def run_tasks_with_runtime_env():
    assert ray.is_initialized()

    @ray.remote(runtime_env={"pip": ["pip-install-test==0.5"]})
    def f():
        import pip_install_test  # noqa

        pass

    for _ in range(3):
        time.sleep(1)
        ray.get(f.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="`runtime_env` with `pip` not supported on Windows."
)
@pytest.mark.parametrize(
    "listen_port",
    [conflict_port],
    indirect=True,
)
@pytest.mark.parametrize(
    "call_ray_start",
    [f"ray start --head --num-cpus=1 --dashboard-agent-grpc-port={conflict_port}"],
    indirect=True,
)
def test_dashboard_agent_grpc_port_conflict(listen_port, call_ray_start):
    address = call_ray_start
    ray.init(address=address)
    # Tasks without runtime env still work when dashboard agent grpc port conflicts.
    run_tasks_without_runtime_env()
    # Tasks with runtime env couldn't work.
    with pytest.raises(
        ray.exceptions.RuntimeEnvSetupError,
        match="the grpc service of agent is invalid",
    ):
        run_tasks_with_runtime_env()


@pytest.mark.skipif(
    sys.platform == "win32", reason="`runtime_env` with `pip` not supported on Windows."
)
@pytest.mark.parametrize(
    "listen_port",
    [conflict_port],
    indirect=True,
)
@pytest.mark.parametrize(
    "call_ray_start",
    [
        f"ray start --head --num-cpus=1 --metrics-export-port={conflict_port}",
        f"ray start --head --num-cpus=1 --dashboard-agent-listen-port={conflict_port}",
    ],
    indirect=True,
)
def test_dashboard_agent_metrics_or_http_port_conflict(listen_port, call_ray_start):
    address = call_ray_start
    ray.init(address=address)
    # Tasks with runtime env still work when other agent port conflicts,
    # except grpc port.
    run_tasks_with_runtime_env()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
