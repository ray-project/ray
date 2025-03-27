import asyncio
import requests
import pytest
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.dashboard.tests.conftest import *  # noqa


@pytest.fixture(scope="module")
def ray_dashboard(ray_start_with_dashboard):
    """
    Starts the Ray dashboard once for all tests in the module.
    """
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"])
    return ray_start_with_dashboard


@pytest.fixture(scope="module")
def webui_url(ray_dashboard):
    """
    Returns the formatted Web UI URL after ensuring it is available.
    """
    url = ray_dashboard["webui_url"]
    return format_web_url(url)


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


def test_grafana_health_fail(webui_url):
    """
    Tests /api/grafana_health when Grafana is not running.
    """
    url = f"{webui_url}/api/grafana_health"
    resp = requests.get(url)

    if resp.status_code != 200:  # When Grafana is down
        data = resp.json()
        assert "exception" in data["data"]
        assert "Cannot connect" in data["data"]["exception"]


def test_prometheus_health_fail(webui_url):
    """
    Tests /api/prometheus_health when Prometheus is not running.
    """
    url = f"{webui_url}/api/prometheus_health"
    resp = requests.get(url)

    if resp.status_code != 200:  # When Prometheus is down
        data = resp.json()
        assert "reason" in data["data"]
        assert "Cannot connect" in data["data"]["reason"]


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
    import sys

    sys.exit(pytest.main(["-v", __file__]))
