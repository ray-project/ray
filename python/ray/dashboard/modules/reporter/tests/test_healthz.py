import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import requests

import ray
import ray._private.ray_constants as ray_constants
from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.controller import LOGGING_CONFIG_CHECKPOINT_KEY
from ray.serve._private.storage.kv_store import RayInternalKVStore
from ray.tests.conftest import *  # noqa: F401 F403


class _ServeHealthHandler(BaseHTTPRequestHandler):
    status_code = 200
    body = "success"

    def do_GET(self):
        if self.path == "/-/healthz":
            self.send_response(self.status_code)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(self.body.encode())
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return


def _start_mock_serve_health_server(port: int, status_code: int, body: str):
    _ServeHealthHandler.status_code = status_code
    _ServeHealthHandler.body = body
    server = HTTPServer(("127.0.0.1", port), _ServeHealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_healthz_head(monkeypatch, ray_start_cluster):
    dashboard_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_port=dashboard_port)
    uri = f"http://localhost:{dashboard_port}/api/gcs_healthz"
    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    h.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()
    # It'll either timeout or just return an error
    try:
        wait_for_condition(lambda: requests.get(uri, timeout=1) != 200, timeout=4)
    except RuntimeError as e:
        assert "Read timed out" in str(e)


def test_healthz_agent_1(monkeypatch, ray_start_cluster):
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/local_raylet_healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)

    h.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()
    # GCS's failure will not lead to healthz failure
    assert requests.get(uri).status_code == 200


@pytest.mark.skipif(sys.platform == "win32", reason="SIGSTOP only on posix")
def test_healthz_agent_2(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/local_raylet_healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)

    h.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.send_signal(
        signal.SIGSTOP
    )

    # GCS still think raylet is alive.
    assert requests.get(uri).status_code == 200
    # But after heartbeat timeout, it'll think the raylet is down.
    wait_for_condition(lambda: requests.get(uri).status_code != 200)


def test_unified_healthz_head(monkeypatch, ray_start_cluster):
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "raylet: success" in resp.text
    assert "gcs: success" in resp.text

    h.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()
    wait_for_condition(lambda: requests.get(uri).status_code == 503)
    resp = requests.get(uri)
    assert "gcs: " in resp.text
    assert "gcs: success" not in resp.text


@pytest.mark.skipif(sys.platform == "win32", reason="SIGSTOP only on posix")
def test_unified_healthz_worker(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    ray_start_cluster.add_node()
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "gcs: success (no local gcs)" in resp.text

    # Stop local raylet and verify this makes /healthz fail.
    h.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.send_signal(
        signal.SIGSTOP
    )
    wait_for_condition(lambda: requests.get(uri).status_code == 503)
    resp = requests.get(uri)
    assert "raylet: Local Raylet failed" in resp.text


def test_unified_healthz_worker_gcs_down(monkeypatch, ray_start_cluster):
    h_head = ray_start_cluster.add_node()
    agent_port = find_free_port()
    h_worker = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h_worker.node_ip_address}:{agent_port}/api/healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "gcs: success (no local gcs)" in resp.text

    # Stop the head GCS server.
    h_head.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()

    # Worker health check should still succeed.
    assert requests.get(uri).status_code == 200


def test_ray_service_healthz_serve_not_running(ray_start_cluster):
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/ray_service_healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "raylet: success" in resp.text
    assert "serve: success (serve not running)" in resp.text


def test_ray_service_healthz_serve_unhealthy(monkeypatch, ray_start_cluster):
    serve_port = find_free_port()
    monkeypatch.setenv("RAY_DASHBOARD_SERVE_HEALTH_CHECK_PORT", str(serve_port))
    mock_server = _start_mock_serve_health_server(
        port=serve_port, status_code=503, body="This node is being drained."
    )

    ray_start_cluster.add_node()
    agent_port = find_free_port()
    worker = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{worker.node_ip_address}:{agent_port}/api/ray_service_healthz"

    ray.init(address=ray_start_cluster.address)
    try:
        gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
        kv_store = RayInternalKVStore(
            namespace=f"ray-serve-{SERVE_NAMESPACE}",
            gcs_client=gcs_client,
        )
        kv_store.put(LOGGING_CONFIG_CHECKPOINT_KEY, b"serve-running")

        wait_for_condition(lambda: requests.get(uri).status_code == 503)
        resp = requests.get(uri)
        assert "raylet: success" in resp.text
        assert (
            "serve: Serve proxy health check failed with status code 503" in resp.text
        )
    finally:
        ray.shutdown()
        mock_server.shutdown()
        mock_server.server_close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
