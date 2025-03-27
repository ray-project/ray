import sys
import asyncio
import requests
import pytest
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import find_free_port, wait_for_condition
from ray.dashboard.tests.conftest import *  # noqa


@pytest.fixture(scope="module")
def ray_dashboard(ray_start_cluster):
    """
    Starts Ray cluster with the dashboard and returns the dashboard URL.
    """
    dashboard_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_port=dashboard_port)
    uri = f"http://localhost:{dashboard_port}"

    # Ensure the dashboard is accessible
    wait_for_condition(
        lambda: requests.get(f"{uri}/api/gcs_healthz").status_code == 200
    )

    return {"webui_url": uri, "node": h}


@pytest.fixture(scope="module")
def webui_url(ray_dashboard):
    """
    Extracts the Web UI URL from the Ray dashboard fixture.
    """
    return ray_dashboard["webui_url"]


def is_service_ready(url):
    """
    Checks if the given API endpoint is available.
    """
    try:
        resp = requests.get(url, timeout=3)
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False


def test_grafana_health(webui_url):
    """
    Tests the /api/grafana_health endpoint from MetricsHead module.
    """
    url = f"{webui_url}/api/grafana_health"
    assert wait_for_condition(lambda: is_service_ready(url), timeout=10)

    resp = requests.get(url)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result"] is True
    assert "grafana_host" in data["data"]


def test_prometheus_health(webui_url):
    """
    Tests the /api/prometheus_health endpoint from MetricsHead module.
    """
    url = f"{webui_url}/api/prometheus_health"
    assert wait_for_condition(lambda: is_service_ready(url), timeout=10)

    resp = requests.get(url)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result"] is True


def test_grafana_health_fail(webui_url, ray_dashboard):
    """
    Tests /api/grafana_health when Grafana is not running.
    """
    url = f"{webui_url}/api/grafana_health"

    # Simulate Grafana being down by killing the process
    ray_dashboard["node"].all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][
        0
    ].process.kill()

    try:
        wait_for_condition(
            lambda: requests.get(url, timeout=1).status_code != 200, timeout=4
        )
    except RuntimeError as e:
        assert "Read timed out" in str(e)


def test_prometheus_health_fail(webui_url, ray_dashboard):
    """
    Tests /api/prometheus_health when Prometheus is not running.
    """
    url = f"{webui_url}/api/prometheus_health"

    # Simulate Prometheus being down by killing the process
    ray_dashboard["node"].all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][
        0
    ].process.kill()

    try:
        wait_for_condition(
            lambda: requests.get(url, timeout=1).status_code != 200, timeout=4
        )
    except RuntimeError as e:
        assert "Read timed out" in str(e)


@pytest.mark.asyncio
async def test_async_health_check(webui_url):
    """
    Tests asynchronous API health check for robustness.
    """
    url = f"{webui_url}/api/grafana_health"
    for _ in range(5):
        resp = await asyncio.to_thread(requests.get, url)
        if resp.status_code == 200:
            break
        await asyncio.sleep(1)  # Retry every second
    assert resp.status_code == 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
