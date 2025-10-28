"""Tests for dashboard token authentication."""

import os

import pytest
import requests

import ray
from ray._raylet import Config
from ray.cluster_utils import Cluster


@pytest.fixture
def cleanup_env():
    """Clean up environment variables after each test."""
    yield
    # Clean up environment variables
    if "RAY_auth_mode" in os.environ:
        del os.environ["RAY_auth_mode"]
    if "RAY_AUTH_TOKEN" in os.environ:
        del os.environ["RAY_AUTH_TOKEN"]


def test_dashboard_request_requires_auth_with_valid_token(cleanup_env):
    """Test that requests succeed with valid token when auth is enabled."""
    test_token = "test_token_12345678901234567890123456789012"
    os.environ["RAY_auth_mode"] = "token"
    os.environ["RAY_AUTH_TOKEN"] = test_token
    Config.initialize("")
    cluster = Cluster()
    cluster.add_node()

    try:
        context = ray.init(address=cluster.address)
        dashboard_url = context.address_info["webui_url"]

        # Request with valid auth should succeed
        headers = {"Authorization": f"Bearer {test_token}"}
        response = requests.get(
            f"http://{dashboard_url}/api/component_activities",
            headers=headers,
        )
        assert response.status_code == 200

    finally:
        ray.shutdown()
        cluster.shutdown()


def test_dashboard_request_requires_auth_missing_token(cleanup_env):
    """Test that requests fail without token when auth is enabled."""
    test_token = "test_token_12345678901234567890123456789012"
    os.environ["RAY_auth_mode"] = "token"
    os.environ["RAY_AUTH_TOKEN"] = test_token
    Config.initialize("")
    cluster = Cluster()
    cluster.add_node()

    try:
        context = ray.init(address=cluster.address)
        dashboard_url = context.address_info["webui_url"]

        # GET without auth should fail with 401
        response = requests.get(
            f"http://{dashboard_url}/api/component_activities",
            json={"test": "data"},
        )
        assert response.status_code == 401

    finally:
        ray.shutdown()
        cluster.shutdown()


def test_dashboard_request_requires_auth_invalid_token(cleanup_env):
    """Test that requests fail with invalid token when auth is enabled."""
    correct_token = "test_token_12345678901234567890123456789012"
    wrong_token = "wrong_token_00000000000000000000000000000000"
    os.environ["RAY_auth_mode"] = "token"
    os.environ["RAY_AUTH_TOKEN"] = correct_token
    Config.initialize("")
    cluster = Cluster()
    cluster.add_node()

    try:
        context = ray.init(address=cluster.address)
        dashboard_url = context.address_info["webui_url"]

        # Request with wrong token should fail with 403
        headers = {"Authorization": f"Bearer {wrong_token}"}
        response = requests.get(
            f"http://{dashboard_url}/api/component_activities",
            json={"test": "data"},
            headers=headers,
        )
        assert response.status_code == 403

    finally:
        ray.shutdown()
        cluster.shutdown()


def test_dashboard_auth_disabled(cleanup_env):
    """Test that auth is not enforced when auth_mode is disabled."""
    os.environ["RAY_auth_mode"] = "disabled"

    cluster = Cluster()
    cluster.add_node()

    try:
        context = ray.init(address=cluster.address)
        dashboard_url = context.address_info["webui_url"]

        # GET without auth should succeed when auth is disabled
        response = requests.get(
            f"http://{dashboard_url}/api/component_activities", json={"test": "data"}
        )
        # Should not return 401 or 403
        assert response.status_code == 200

    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
