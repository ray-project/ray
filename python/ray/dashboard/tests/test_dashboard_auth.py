"""Tests for dashboard token authentication."""
import os
import sys

import pytest
import requests

import ray
from ray._raylet import Config


@pytest.fixture
def start_ray_with_env_vars(request):
    """Clean up environment variables after each test."""
    env_vars = getattr(request, "param", {}).pop("env_vars", {})
    os.environ.update(**env_vars)
    Config.initialize("")

    yield ray.init()

    ray.shutdown()
    for k in env_vars.keys():
        del os.environ[k]


TEST_TOKEN = "test_token_12345678901234567890123456789012"


@pytest.mark.parametrize(
    "start_ray_with_env_vars",
    [
        {
            "env_vars": {"RAY_auth_mode": "token", "RAY_AUTH_TOKEN": TEST_TOKEN},
        },
    ],
    indirect=True,
)
def test_auth_enabled_valid_token(start_ray_with_env_vars):
    """Test that requests succeed with valid token when auth is enabled."""
    dashboard_url = start_ray_with_env_vars.address_info["webui_url"]

    # Request with valid auth should succeed
    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}
    response = requests.get(
        f"http://{dashboard_url}/api/component_activities",
        headers=headers,
    )
    assert response.status_code == 200


@pytest.mark.parametrize(
    "start_ray_with_env_vars",
    [
        {
            "env_vars": {"RAY_auth_mode": "token", "RAY_AUTH_TOKEN": TEST_TOKEN},
        },
    ],
    indirect=True,
)
def test_auth_enabled_missing_token(start_ray_with_env_vars):
    """Test that requests fail without token when auth is enabled."""
    dashboard_url = start_ray_with_env_vars.address_info["webui_url"]

    # GET without auth should fail with 401
    response = requests.get(
        f"http://{dashboard_url}/api/component_activities",
        json={"test": "data"},
    )
    assert response.status_code == 401


@pytest.mark.parametrize(
    "start_ray_with_env_vars",
    [
        {
            "env_vars": {"RAY_auth_mode": "token", "RAY_AUTH_TOKEN": TEST_TOKEN},
        },
    ],
    indirect=True,
)
def test_auth_enabled_invalid_token(start_ray_with_env_vars):
    """Test that requests fail with invalid token when auth is enabled."""
    dashboard_url = start_ray_with_env_vars.address_info["webui_url"]

    # Request with wrong token should fail with 403
    headers = {"Authorization": "Bearer INCORRECT_TOKEN"}
    response = requests.get(
        f"http://{dashboard_url}/api/component_activities",
        json={"test": "data"},
        headers=headers,
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    "start_ray_with_env_vars",
    [
        {
            "env_vars": {"RAY_auth_mode": "disabled"},
        },
    ],
    indirect=True,
)
def test_auth_disabled(start_ray_with_env_vars):
    """Test that auth is not enforced when auth_mode is disabled."""
    dashboard_url = start_ray_with_env_vars.address_info["webui_url"]

    # GET without auth should succeed when auth is disabled
    response = requests.get(
        f"http://{dashboard_url}/api/component_activities", json={"test": "data"}
    )
    # Should not return 401 or 403
    assert response.status_code == 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
