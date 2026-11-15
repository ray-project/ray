"""Tests for dashboard token authentication."""

import sys

import pytest
import requests

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import reset_autoscaler_v2_enabled_cache
from ray.autoscaler.v2.utils import is_autoscaler_v2
from ray.experimental.internal_kv import _internal_kv_get


def test_dashboard_request_requires_auth_with_valid_token(
    setup_cluster_with_token_auth,
):
    """Test that requests succeed with valid token when auth is enabled."""

    cluster_info = setup_cluster_with_token_auth
    headers = {"Authorization": f"Bearer {cluster_info['token']}"}

    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        headers=headers,
    )

    assert response.status_code == 200


def test_dashboard_request_requires_auth_missing_token(setup_cluster_with_token_auth):
    """Test that requests fail without token when auth is enabled."""

    cluster_info = setup_cluster_with_token_auth

    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        json={"test": "data"},
    )

    assert response.status_code == 401


def test_dashboard_request_requires_auth_invalid_token(setup_cluster_with_token_auth):
    """Test that requests fail with invalid token when auth is enabled."""

    cluster_info = setup_cluster_with_token_auth
    headers = {"Authorization": "Bearer wrong_token_00000000000000000000000000000000"}

    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        json={"test": "data"},
        headers=headers,
    )

    assert response.status_code == 403


def test_dashboard_request_with_ray_auth_header(setup_cluster_with_token_auth):
    """Test that requests succeed with valid token in X-Ray-Authorization header."""

    cluster_info = setup_cluster_with_token_auth
    headers = {"X-Ray-Authorization": f"Bearer {cluster_info['token']}"}

    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        headers=headers,
    )

    assert response.status_code == 200


def test_authorization_header_takes_precedence(setup_cluster_with_token_auth):
    """Test that standard Authorization header takes precedence over X-Ray-Authorization."""

    cluster_info = setup_cluster_with_token_auth

    # Provide both headers: valid token in Authorization, invalid in X-Ray-Authorization
    headers = {
        "Authorization": f"Bearer {cluster_info['token']}",
        "X-Ray-Authorization": "Bearer invalid_token_000000000000000000000000",
    }

    # Should succeed because Authorization header takes precedence
    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        headers=headers,
    )

    assert response.status_code == 200

    # Now test with invalid Authorization but valid X-Ray-Authorization
    headers = {
        "Authorization": "Bearer invalid_token_000000000000000000000000",
        "X-Ray-Authorization": f"Bearer {cluster_info['token']}",
    }

    # Should fail because Authorization header takes precedence (even though it's invalid)
    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        headers=headers,
    )

    assert response.status_code == 403


def test_dashboard_auth_disabled(setup_cluster_without_token_auth):
    """Test that auth is not enforced when AUTH_MODE is disabled."""

    cluster_info = setup_cluster_without_token_auth

    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        json={"test": "data"},
    )

    assert response.status_code == 200


def test_authentication_mode_endpoint_with_token_auth(setup_cluster_with_token_auth):
    """Test authentication_mode endpoint returns 'token' when auth is enabled."""

    cluster_info = setup_cluster_with_token_auth

    # This endpoint should be accessible WITHOUT authentication
    response = requests.get(f"{cluster_info['dashboard_url']}/api/authentication_mode")

    assert response.status_code == 200
    assert response.json() == {"authentication_mode": "token"}


def test_authentication_mode_endpoint_without_auth(setup_cluster_without_token_auth):
    """Test authentication_mode endpoint returns 'disabled' when auth is off."""

    cluster_info = setup_cluster_without_token_auth

    response = requests.get(f"{cluster_info['dashboard_url']}/api/authentication_mode")

    assert response.status_code == 200
    assert response.json() == {"authentication_mode": "disabled"}


def test_authentication_mode_endpoint_is_public(setup_cluster_with_token_auth):
    """Test authentication_mode endpoint works without Authorization header."""

    cluster_info = setup_cluster_with_token_auth

    # Call WITHOUT any authorization header - should still succeed
    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/authentication_mode",
        headers={},  # Explicitly no auth
    )

    # Should succeed even with token auth enabled
    assert response.status_code == 200
    assert response.json() == {"authentication_mode": "token"}


def _metrics_endpoints():
    """Return the node, autoscaler and dashboard metrics URLs once all three are up."""
    node = ray.nodes()[0]
    urls = {"node": f"http://{node['NodeManagerAddress']}:{node['MetricsExportPort']}"}
    for name, key in (
        ("autoscaler", "AutoscalerMetricsAddress"),
        ("dashboard", "DashboardMetricsAddress"),
    ):
        wait_for_condition(lambda: _internal_kv_get(key) is not None)
        urls[name] = f"http://{_internal_kv_get(key).decode()}"
    for url in urls.values():
        wait_for_condition(lambda: _metrics_reachable(url))
    return urls


def _metrics_reachable(url):
    try:
        requests.get(f"{url}/metrics", timeout=5)
        return True
    except requests.RequestException:
        return False


@pytest.fixture(params=["v1", "v2"])
def autoscaler_version(request, monkeypatch):
    """Run before the cluster fixture so the monitor starts in the chosen version."""
    monkeypatch.setenv(
        "RAY_enable_autoscaler_v2", "1" if request.param == "v2" else "0"
    )
    yield request.param
    # The head built in this process caches the version for later tests.
    reset_autoscaler_v2_enabled_cache()


def test_metrics_endpoints_require_token(
    autoscaler_version, setup_cluster_with_token_auth
):
    assert is_autoscaler_v2(fetch_from_server=True) == (autoscaler_version == "v2")
    token = setup_cluster_with_token_auth["token"]
    accepted_forms = [
        {"headers": {"Authorization": f"Bearer {token}"}},
        {"headers": {"X-Ray-Authorization": f"Bearer {token}"}},
        {"cookies": {"ray-authentication-token": token}},
    ]

    for name, url in _metrics_endpoints().items():
        missing = requests.get(f"{url}/metrics", timeout=5)
        assert missing.status_code == 401, name
        assert missing.headers["WWW-Authenticate"] == "Bearer", name

        malformed_cookie = requests.get(
            f"{url}/metrics", headers={"Cookie": "a(b=1"}, timeout=5
        )
        assert malformed_cookie.status_code == 401, name

        wrong = requests.get(
            f"{url}/metrics",
            headers={"Authorization": "Bearer " + "0" * 64},
            timeout=5,
        )
        assert wrong.status_code == 403, name

        for form in accepted_forms:
            response = requests.get(f"{url}/metrics", timeout=5, **form)
            assert response.status_code == 200, (name, form)


@pytest.fixture
def skip_metrics_auth(monkeypatch):
    """Run before the cluster fixture so every Ray process inherits the setting."""
    monkeypatch.setenv("RAY_AUTH_SKIP_METRICS", "1")


def test_skip_metrics_opens_only_metrics_endpoints(
    autoscaler_version, skip_metrics_auth, setup_cluster_with_token_auth
):
    assert is_autoscaler_v2(fetch_from_server=True) == (autoscaler_version == "v2")
    for name, url in _metrics_endpoints().items():
        response = requests.get(f"{url}/metrics", timeout=5)
        assert response.status_code == 200, name

    dashboard_api = requests.get(
        f"{setup_cluster_with_token_auth['dashboard_url']}/api/component_activities",
        timeout=5,
    )
    assert dashboard_api.status_code == 401


def test_metrics_endpoints_open_when_auth_disabled(setup_cluster_without_token_auth):
    for name, url in _metrics_endpoints().items():
        response = requests.get(f"{url}/metrics", timeout=5)
        assert response.status_code == 200, name


if __name__ == "__main__":

    sys.exit(pytest.main(["-vv", __file__]))
