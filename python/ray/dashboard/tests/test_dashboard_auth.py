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


if __name__ == "__main__":

    sys.exit(pytest.main(["-vv", __file__]))
