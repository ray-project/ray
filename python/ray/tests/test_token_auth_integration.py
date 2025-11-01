"""Integration tests for token-based authentication in Ray."""

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray._raylet import AuthenticationTokenLoader
from ray.tests.authentication_test_utils import (
    clear_auth_token_sources,
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)


def _run_ray_start_and_verify_status(
    args: list, env: dict, expect_success: bool = True, timeout: int = 30
) -> subprocess.CompletedProcess:
    """Helper to run ray start command with proper error handling."""
    result = subprocess.run(
        ["ray", "start"] + args,
        env={"RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1", **env},
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if expect_success:
        assert result.returncode == 0, (
            f"ray start should have succeeded. "
            f"stdout: {result.stdout}, stderr: {result.stderr}"
        )
    else:
        assert result.returncode != 0, (
            f"ray start should have failed but succeeded. "
            f"stdout: {result.stdout}, stderr: {result.stderr}"
        )
        # Check that error message mentions token
        error_output = result.stdout + result.stderr
        assert (
            "authentication token" in error_output.lower()
            or "token" in error_output.lower()
        ), f"Error message should mention token. Got: {error_output}"

    return result


def _cleanup_ray_start(env: Optional[dict] = None):
    """Helper to clean up ray start processes."""
    # Ensure any ray.init() connection is closed first
    if ray.is_initialized():
        ray.shutdown()

    # Stop with a longer timeout
    subprocess.run(
        ["ray", "stop", "--force"],
        env=env,
        capture_output=True,
        timeout=60,  # Increased timeout for flaky cleanup
        check=False,  # Don't raise on non-zero exit
    )

    # Wait for ray processes to actually stop
    def ray_stopped():
        result = subprocess.run(
            ["ray", "status"],
            capture_output=True,
            check=False,
        )
        # ray status returns non-zero when no cluster is running
        return result.returncode != 0

    try:
        wait_for_condition(ray_stopped, timeout=10, retry_interval_ms=500)
    except Exception:
        # Best effort - don't fail the test if we can't verify it stopped
        pass


@pytest.fixture(autouse=True)
def clean_token_sources(cleanup_auth_token_env):
    """Ensure authentication-related state is clean around each test."""

    clear_auth_token_sources(remove_default=True)
    reset_auth_token_state()

    yield

    if ray.is_initialized():
        ray.shutdown()

    subprocess.run(
        ["ray", "stop", "--force"],
        capture_output=True,
        timeout=60,
        check=False,
    )

    reset_auth_token_state()


def test_local_cluster_generates_token():
    """Test ray.init() generates token for local cluster when auth_mode=token is set."""
    # Ensure no token exists
    default_token_path = Path.home() / ".ray" / "auth_token"
    assert (
        not default_token_path.exists()
    ), f"Token file already exists at {default_token_path}"

    # Enable token auth via environment variable
    set_auth_mode("token")
    reset_auth_token_state()

    # Initialize Ray with token auth
    ray.init()

    try:
        # Verify token file was created
        assert default_token_path.exists(), (
            f"Token file was not created at {default_token_path}. "
            f"HOME={os.environ.get('HOME')}, "
            f"Files in {default_token_path.parent}: {list(default_token_path.parent.iterdir()) if default_token_path.parent.exists() else 'directory does not exist'}"
        )
        token = default_token_path.read_text().strip()
        assert len(token) == 32
        assert all(c in "0123456789abcdef" for c in token)

        # Verify cluster is working
        assert ray.is_initialized()

    finally:
        ray.shutdown()


def test_connect_without_token_raises_error(setup_cluster_with_token_auth):
    """Test ray.init(address=...) without token fails when auth_mode=token is set."""
    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]

    # Disconnect the current driver session and drop token state before retrying.
    ray.shutdown()
    set_auth_mode("disabled")
    clear_auth_token_sources(remove_default=True)
    reset_auth_token_state()

    # Ensure no token exists
    token_loader = AuthenticationTokenLoader.instance()
    assert not token_loader.has_token()

    # Try to connect to the cluster without a token - should raise RuntimeError
    with pytest.raises(ConnectionError):
        ray.init(address=cluster.address)


@pytest.mark.parametrize("tokens_match", [True, False])
def test_cluster_token_authentication(tokens_match, setup_cluster_with_token_auth):
    """Test cluster authentication with matching and non-matching tokens."""
    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]
    cluster_token = cluster_info["token"]

    # Reconfigure the driver token state to simulate fresh connections.
    ray.shutdown()
    set_auth_mode("token")

    if tokens_match:
        client_token = cluster_token  # Same token - should succeed
    else:
        client_token = "b" * 32  # Different token - should fail

    set_env_auth_token(client_token)
    reset_auth_token_state()

    if tokens_match:
        # Should succeed - test gRPC calls work
        ray.init(address=cluster.address)

        obj_ref = ray.put("test_data")
        result = ray.get(obj_ref)
        assert result == "test_data"

        @ray.remote
        def test_func():
            return "success"

        result = ray.get(test_func.remote())
        assert result == "success"

        ray.shutdown()

    else:
        # Should fail - connection or gRPC calls should fail
        with pytest.raises((ConnectionError, RuntimeError)):
            ray.init(address=cluster.address)
            try:
                ray.put("test")
            finally:
                ray.shutdown()


@pytest.mark.parametrize("is_head", [True, False])
def test_ray_start_without_token_raises_error(is_head, request):
    """Test that ray start fails when auth_mode=token but no token exists."""
    # Set up environment with token auth enabled but no token
    env = os.environ.copy()
    env["RAY_auth_mode"] = "token"
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)

    # Ensure no default token file exists (already cleaned by fixture)
    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    # When specifying an address, we need a head node to connect to
    cluster_info = None
    if not is_head:
        cluster_info = request.getfixturevalue("setup_cluster_with_token_auth")
        cluster = cluster_info["cluster"]
        ray.shutdown()

    # Prepare arguments
    if is_head:
        args = ["--head", "--port=0"]
    else:
        args = [f"--address={cluster.address}"]

    # Try to start node - should fail
    _run_ray_start_and_verify_status(args, env, expect_success=False)


def test_ray_start_head_with_token_succeeds():
    """Test that ray start --head succeeds when token auth is enabled with a valid token."""
    # Set up environment with token auth and a valid token
    test_token = "a" * 32
    env = os.environ.copy()
    env["RAY_AUTH_TOKEN"] = test_token
    env["RAY_auth_mode"] = "token"

    try:
        # Start head node - should succeed
        _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=True
        )

        # Verify we can connect to the cluster with ray.init()
        set_env_auth_token(test_token)
        set_auth_mode("token")
        reset_auth_token_state()

        # Wait for cluster to be ready
        def cluster_ready():
            try:
                ray.init(address="auto")
                return True
            except Exception:
                return False

        wait_for_condition(cluster_ready, timeout=10)
        assert ray.is_initialized()

        # Test basic operations work
        @ray.remote
        def test_func():
            return "success"

        result = ray.get(test_func.remote())
        assert result == "success"

    finally:
        # Cleanup handles ray.shutdown() internally
        _cleanup_ray_start(env)


@pytest.mark.parametrize("token_match", ["correct", "incorrect"])
def test_ray_start_address_with_token(token_match, setup_cluster_with_token_auth):
    """Test ray start --address=... with correct or incorrect token."""
    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]
    cluster_token = cluster_info["token"]

    # Reset the driver connection to reuse the fixture-backed cluster.
    ray.shutdown()
    set_auth_mode("token")

    # Set up environment for worker
    env = os.environ.copy()
    env["RAY_auth_mode"] = "token"

    if token_match == "correct":
        env["RAY_AUTH_TOKEN"] = cluster_token
        expect_success = True
    else:
        env["RAY_AUTH_TOKEN"] = "b" * 32
        expect_success = False

    # Start worker node
    _run_ray_start_and_verify_status(
        [f"--address={cluster.address}", "--num-cpus=1"],
        env,
        expect_success=expect_success,
    )

    if token_match == "correct":
        try:
            # Connect and verify the cluster has 2 nodes (head + worker)
            set_env_auth_token(cluster_token)
            reset_auth_token_state()
            ray.init(address=cluster.address)

            def worker_joined():
                return len(ray.nodes()) >= 2

            wait_for_condition(worker_joined, timeout=10)

            nodes = ray.nodes()
            assert (
                len(nodes) >= 2
            ), f"Expected at least 2 nodes, got {len(nodes)}: {nodes}"

        finally:
            if ray.is_initialized():
                ray.shutdown()
            _cleanup_ray_start(env)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
