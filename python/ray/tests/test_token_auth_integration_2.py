"""Integration tests for token-based authentication in Ray (part 2).

CLI, token loader, metrics, and dashboard agent tests.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import ray
import ray.dashboard.consts as dashboard_consts
from ray._common.network_utils import build_address
from ray._common.test_utils import (
    PrometheusTimeseries,
    fetch_prometheus_timeseries,
    wait_for_condition,
)
from ray._private.test_utils import client_test_enabled

try:
    from ray._raylet import AuthenticationTokenLoader

    _RAYLET_AVAILABLE = True
except ImportError:
    _RAYLET_AVAILABLE = False
    AuthenticationTokenLoader = None

from ray._private.authentication_test_utils import (
    authentication_env_guard,
    clear_auth_token_sources,
    reset_auth_token_state,
    set_auth_mode,
    set_auth_token_path,
    set_env_auth_token,
)

pytestmark = pytest.mark.skipif(
    not _RAYLET_AVAILABLE,
    reason="Authentication tests require ray._raylet (not available in minimal installs)",
)


@pytest.fixture(autouse=True)
def _auto_clean_token_sources(clean_token_sources):
    """Opt in to the shared clean_token_sources fixture for every test."""
    yield


def _get_dashboard_agent_address(cluster_info):
    """Get the dashboard agent HTTP address from a running cluster."""
    # Get agent address from internal KV
    node_id = ray.nodes()[0]["NodeID"]
    key = f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}"
    agent_addr = ray.experimental.internal_kv._internal_kv_get(
        key, namespace=ray._private.ray_constants.KV_NAMESPACE_DASHBOARD
    )
    if agent_addr:
        ip, http_port, grpc_port = json.loads(agent_addr)
        return f"http://{ip}:{http_port}"
    return None


def _wait_and_get_dashboard_agent_address(cluster_info, timeout=30):
    """Waits for the dashboard agent address to become available and returns it."""

    def agent_address_is_available():
        return _get_dashboard_agent_address(cluster_info) is not None

    wait_for_condition(agent_address_is_available, timeout=timeout)
    return _get_dashboard_agent_address(cluster_info)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray CLI, not compatible with client mode",
)
@pytest.mark.parametrize("use_generate", [True, False])
def test_get_auth_token_cli(use_generate):
    """Test ray get-auth-token CLI command."""
    test_token = "a" * 64

    with authentication_env_guard():
        if use_generate:
            # Test --generate flag (no token set)
            clear_auth_token_sources(remove_default=True)
            args = ["ray", "get-auth-token", "--generate"]
        else:
            # Test with existing token from env var
            set_env_auth_token(test_token)
            reset_auth_token_state()
            args = ["ray", "get-auth-token"]

        env = os.environ.copy()
        result = subprocess.run(
            args,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0, (
            f"ray get-auth-token should succeed. "
            f"stdout: {result.stdout}, stderr: {result.stderr}"
        )

        # Verify token is printed to stdout
        token = result.stdout.strip()
        assert len(token) == 64, token
        assert all(c in "0123456789abcdef" for c in token), "Token should be hex"

        if not use_generate:
            # When using env var, should get exact token back
            assert token == test_token

        # Verify logs went to stderr (if --generate was used)
        if use_generate:
            assert (
                "generating new authentication token..." in result.stderr.lower()
            ), "Should log generation to stderr"


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray CLI, not compatible with client mode",
)
def test_get_auth_token_cli_no_token_no_generate():
    """Test ray get-auth-token fails without token and without --generate."""
    with authentication_env_guard():
        reset_auth_token_state()
        clear_auth_token_sources(remove_default=True)
        env = os.environ.copy()

        result = subprocess.run(
            ["ray", "get-auth-token"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0, "Should fail when no token and no --generate"
        assert "error" in result.stderr.lower(), "Should print error to stderr"
        assert "no" in result.stderr.lower() and "token" in result.stderr.lower()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray CLI, not compatible with client mode",
)
def test_get_auth_token_cli_piping():
    """Test that ray get-auth-token output can be piped."""
    test_token = "b" * 64

    with authentication_env_guard():
        set_env_auth_token(test_token)
        reset_auth_token_state()
        env = os.environ.copy()

        # Test piping: use token in shell pipeline
        result = subprocess.run(
            "ray get-auth-token | wc -c",
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0
        char_count = int(result.stdout.strip())
        assert char_count == 64, f"Expected 64 chars (no newline), got {char_count}"


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Tests AuthenticationTokenLoader directly, no benefit testing this in client mode",
)
def test_missing_token_file_raises_authentication_error():
    """Test that RAY_AUTH_TOKEN_PATH pointing to missing file raises AuthenticationError."""
    with authentication_env_guard():
        # Clear first, then set up the specific test scenario
        clear_auth_token_sources(remove_default=True)
        set_auth_mode("token")
        set_auth_token_path(None, "/nonexistent/path/to/token")
        reset_auth_token_state()

        token_loader = AuthenticationTokenLoader.instance()

        with pytest.raises(ray.exceptions.AuthenticationError) as exc_info:
            token_loader.has_token()

        # Verify error message is informative
        assert str(Path("/nonexistent/path/to/token")) in str(exc_info.value)
        assert "RAY_AUTH_TOKEN_PATH" in str(exc_info.value)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Tests AuthenticationTokenLoader directly, no benefit testing this in client mode",
)
def test_empty_token_file_raises_authentication_error(tmp_path):
    """Test that RAY_AUTH_TOKEN_PATH pointing to empty file raises AuthenticationError."""
    token_file = tmp_path / "empty_token_file.txt"
    with authentication_env_guard():
        # Clear first, then set up the specific test scenario
        clear_auth_token_sources(remove_default=True)
        set_auth_mode("token")
        set_auth_token_path("", token_file)
        reset_auth_token_state()

        token_loader = AuthenticationTokenLoader.instance()

        with pytest.raises(ray.exceptions.AuthenticationError) as exc_info:
            token_loader.has_token()

        assert "cannot be opened or is empty" in str(exc_info.value)
        assert str(token_file) in str(exc_info.value)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Tests AuthenticationTokenLoader directly, no benefit testing this in client mode",
)
def test_no_token_with_auth_enabled_returns_false():
    """Test that has_token(ignore_auth_mode=True) returns False when no token exists.

    This allows the caller (ensure_token_if_auth_enabled) to decide whether
    to generate a new token or raise an error.
    """
    with authentication_env_guard():
        set_auth_mode("token")
        clear_auth_token_sources(remove_default=True)
        reset_auth_token_state()

        token_loader = AuthenticationTokenLoader.instance()

        # has_token(ignore_auth_mode=True) should return False, not raise an exception
        result = token_loader.has_token(ignore_auth_mode=True)
        assert result is False


@pytest.mark.skipif(
    client_test_enabled(),
    reason="no benefit testing this in client mode",
)
def test_opentelemetry_metrics_with_token_auth(setup_cluster_with_token_auth):
    """Test that OpenTelemetry metrics are exported with token authentication.

    This test verifies that the C++ OpenTelemetryMetricRecorder correctly includes
    the authentication token in its gRPC metadata when exporting metrics to the
    metrics agent. If the auth headers are missing or incorrect, the metrics agent
    would reject the requests and metrics wouldn't be collected.
    """

    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]

    # Get the metrics export address from the head node
    head_node = cluster.head_node
    prom_addresses = [
        build_address(head_node.node_ip_address, head_node.metrics_export_port)
    ]

    timeseries = PrometheusTimeseries()

    def verify_metrics_collected():
        """Verify that metrics are being exported successfully."""
        fetch_prometheus_timeseries(prom_addresses, timeseries)
        metric_names = list(timeseries.metric_descriptors.keys())

        # Check for core Ray metrics that are always exported
        # These metrics are exported via the C++ OpenTelemetry recorder
        expected_metrics = [
            "ray_node_cpu_utilization",
            "ray_node_mem_used",
            "ray_node_disk_usage",
        ]

        # At least some metrics should be present
        return len(metric_names) > 0 and any(
            any(expected in name for name in metric_names)
            for expected in expected_metrics
        )

    # Wait for metrics to be collected
    # If auth wasn't working, the metrics agent would reject the exports
    # and we wouldn't see any metrics
    wait_for_condition(verify_metrics_collected, retry_interval_ms=1000)


@pytest.mark.parametrize(
    "token_type,expected_status",
    [
        ("none", 401),  # No token -> Unauthorized
        ("valid", "not_auth_error"),  # Valid token -> passes auth (may get 404)
        ("invalid", 403),  # Invalid token -> Forbidden
    ],
    ids=["no_token", "valid_token", "invalid_token"],
)
def test_dashboard_agent_auth(
    token_type, expected_status, setup_cluster_with_token_auth
):
    """Test dashboard agent authentication with various token scenarios."""
    import requests

    cluster_info = setup_cluster_with_token_auth

    agent_address = _wait_and_get_dashboard_agent_address(cluster_info)

    # Build headers based on token type
    headers = {}
    if token_type == "valid":
        headers["Authorization"] = f"Bearer {cluster_info['token']}"
    elif token_type == "invalid":
        headers["Authorization"] = "Bearer invalid_token_12345678901234567890"
    # token_type == "none" -> no Authorization header

    response = requests.get(
        f"{agent_address}/api/job_agent/jobs/nonexistent/logs",
        headers=headers,
        timeout=5,
    )

    if expected_status == "not_auth_error":
        # Valid token should pass auth (may get 404 for nonexistent job)
        assert response.status_code not in (401, 403), (
            f"Valid token should be accepted, got {response.status_code}: "
            f"{response.text}"
        )
    else:
        assert (
            response.status_code == expected_status
        ), f"Expected {expected_status}, got {response.status_code}: {response.text}"


@pytest.mark.parametrize(
    "endpoint",
    ["/api/healthz", "/api/local_raylet_healthz"],
    ids=["healthz", "local_raylet_healthz"],
)
def test_dashboard_agent_health_check_public(endpoint, setup_cluster_with_token_auth):
    """Test that agent health check endpoints remain public without auth."""
    import requests

    cluster_info = setup_cluster_with_token_auth

    agent_address = _wait_and_get_dashboard_agent_address(cluster_info)

    # Health check endpoints should be accessible without auth
    response = requests.get(f"{agent_address}{endpoint}", timeout=5)
    assert response.status_code == 200, (
        f"Health check {endpoint} should return 200 without auth, "
        f"got {response.status_code}: {response.text}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
