import sys
import requests
import pytest
from ray._private.test_utils import format_web_url, wait_until_server_available
from ray.dashboard.tests.conftest import *  # noqa


def test_grafana_health(ray_start_with_dashboard):
    """
    Test the /api/grafana_health endpoint from MetricsHead module.
    """
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    resp = requests.get(f"{webui_url}/api/grafana_health")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result"] is True
    assert "grafana_host" in data["data"]


def test_prometheus_health(ray_start_with_dashboard):
    """
    Test the /api/prometheus_health endpoint from MetricsHead module.
    """
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    resp = requests.get(f"{webui_url}/api/prometheus_health")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result"] is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
