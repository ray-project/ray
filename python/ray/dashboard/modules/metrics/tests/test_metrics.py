import sys
import asyncio
import urllib.parse
import requests
import pytest

from ray._private.test_utils import wait_for_condition, wait_until_server_available
from ray.dashboard.tests.conftest import *  # noqa


def test_grafana_health(ray_start_with_dashboard):
    """
    Tests the /api/grafana_health endpoint from MetricsHead module.
    Prints response body on failure for easier debugging.
    """
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    if not webui_url.startswith("http"):
        webui_url = "http://" + webui_url

    parsed = urllib.parse.urlparse(webui_url)
    host_port = f"{parsed.hostname}:{parsed.port}"
    assert wait_until_server_available(
        host_port
    ), f"Dashboard not available at {host_port}"

    url = f"{webui_url}/api/grafana_health"

    def condition():
        resp = requests.get(url, timeout=3)
        if resp.status_code != 200:
            print("[test_grafana_health] Status code:", resp.status_code)
            print("[test_grafana_health] Response body:", resp.text)
        return resp.status_code == 200

    assert wait_for_condition(condition, timeout=10)


def test_prometheus_health(ray_start_with_dashboard):
    """
    Tests the /api/prometheus_health endpoint from MetricsHead module.
    Prints response body on failure for easier debugging.
    """
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    if not webui_url.startswith("http"):
        webui_url = "http://" + webui_url

    parsed = urllib.parse.urlparse(webui_url)
    host_port = f"{parsed.hostname}:{parsed.port}"
    assert wait_until_server_available(
        host_port
    ), f"Dashboard not available at {host_port}"

    url = f"{webui_url}/api/prometheus_health"

    def condition():
        resp = requests.get(url, timeout=3)
        if resp.status_code != 200:
            print("[test_prometheus_health] Status code:", resp.status_code)
            print("[test_prometheus_health] Response body:", resp.text)
        return resp.status_code == 200

    assert wait_for_condition(condition, timeout=10)


@pytest.mark.asyncio
async def test_async_health_check(ray_start_with_dashboard):
    """
    Tests asynchronous API health check for robustness.
    Prints response body on failure for easier debugging.
    """
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    if not webui_url.startswith("http"):
        webui_url = "http://" + webui_url

    parsed = urllib.parse.urlparse(webui_url)
    host_port = f"{parsed.hostname}:{parsed.port}"
    assert wait_until_server_available(
        host_port
    ), f"Dashboard not available at {host_port}"

    url = f"{webui_url}/api/grafana_health"

    resp = None
    for i in range(5):
        resp = await asyncio.to_thread(requests.get, url)
        if resp.status_code == 200:
            break
        else:
            print(
                f"[test_async_health_check] Attempt {i+1} failed. Status code:",
                resp.status_code,
            )
            print("[test_async_health_check] Response body:", resp.text)
        await asyncio.sleep(1)

    assert resp is not None, "No response received at all."
    assert resp.status_code == 200, (
        f"Async health check still failed with status {resp.status_code}. "
        f"Response: {resp.text}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
