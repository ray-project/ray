import sys
import asyncio
import requests
import urllib.parse
import pytest
from ray._private.test_utils import wait_for_condition, wait_until_server_available
from ray.dashboard.tests.conftest import *  # noqa


def test_grafana_health(ray_start_with_dashboard):
    """
    Tests the /api/grafana_health endpoint from MetricsHead module.
    """
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    if not webui_url.startswith("http"):
        webui_url = "http://" + webui_url

    parsed = urllib.parse.urlparse(webui_url)
    host_port = f"{parsed.hostname}:{parsed.port}"
    assert wait_until_server_available(host_port)

    url = f"{webui_url}/api/grafana_health"
    assert wait_for_condition(
        lambda: requests.get(url, timeout=3).status_code == 200, timeout=10
    )

    resp = requests.get(url)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result"] is True
    assert "grafana_host" in data["data"]


def test_prometheus_health(ray_start_with_dashboard):
    """
    Tests the /api/prometheus_health endpoint from MetricsHead module.
    """
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    if not webui_url.startswith("http"):
        webui_url = "http://" + webui_url

    parsed = urllib.parse.urlparse(webui_url)
    host_port = f"{parsed.hostname}:{parsed.port}"
    assert wait_until_server_available(host_port)

    url = f"{webui_url}/api/prometheus_health"
    assert wait_for_condition(
        lambda: requests.get(url, timeout=3).status_code == 200, timeout=10
    )

    resp = requests.get(url)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result"] is True


@pytest.mark.asyncio
async def test_async_health_check(ray_start_with_dashboard):
    """
    Tests asynchronous API health check for robustness.
    """
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    if not webui_url.startswith("http"):
        webui_url = "http://" + webui_url

    parsed = urllib.parse.urlparse(webui_url)
    host_port = f"{parsed.hostname}:{parsed.port}"
    assert wait_until_server_available(host_port)

    url = f"{webui_url}/api/grafana_health"
    for _ in range(5):
        resp = await asyncio.to_thread(requests.get, url)
        if resp.status_code == 200:
            break
        await asyncio.sleep(1)
    assert resp.status_code == 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
