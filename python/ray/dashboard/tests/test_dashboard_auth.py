"""Tests for dashboard token authentication."""

import sys

import pytest
import requests


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


def test_dashboard_auth_disabled(setup_cluster_without_token_auth):
    """Test that auth is not enforced when auth_mode is disabled."""

    cluster_info = setup_cluster_without_token_auth

    response = requests.get(
        f"{cluster_info['dashboard_url']}/api/component_activities",
        json={"test": "data"},
    )

    assert response.status_code == 200


if __name__ == "__main__":

    sys.exit(pytest.main(["-vv", __file__]))
