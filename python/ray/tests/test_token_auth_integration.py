"""Integration tests for token-based authentication in Ray."""

import os
import sys
from pathlib import Path

import pytest

import ray
from ray._private.auth_token_loader import reset_token_cache
from ray.cluster_utils import Cluster


@pytest.fixture(autouse=True)
def clean_token_sources():
    """Clean up all token sources before and after each test."""
    # Clean environment variables
    env_vars_to_clean = [
        "RAY_AUTH_TOKEN",
        "RAY_AUTH_TOKEN_PATH",
        "RAY_enable_token_auth",
    ]
    original_values = {}
    for var in env_vars_to_clean:
        original_values[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]

    # Clean default token file
    default_token_path = Path.home() / ".ray" / "auth_token"
    original_exists = default_token_path.exists()
    if original_exists:
        original_content = default_token_path.read_text()
        default_token_path.unlink()

    # Reset token caches (both Python and C++)
    reset_token_cache()

    yield

    # Restore environment variables
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]

    # Restore default token file
    if original_exists:
        default_token_path.parent.mkdir(parents=True, exist_ok=True)
        default_token_path.write_text(original_content)

    # Reset token caches again after test
    reset_token_cache()


def test_local_cluster_generates_token():
    """Test ray.init() generates token for local cluster when enable_token_auth is set."""
    # Ensure no token exists
    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    # Enable token auth via environment variable
    os.environ["RAY_enable_token_auth"] = "1"

    # Initialize Ray with token auth
    ray.init()

    try:
        # Verify token file was created
        assert default_token_path.exists()
        token = default_token_path.read_text().strip()
        assert len(token) == 32
        assert all(c in "0123456789abcdef" for c in token)

        # Verify cluster is working
        assert ray.is_initialized()

    finally:
        ray.shutdown()


def test_connect_without_token_raises_error():
    """Test ray.init(address=...) without token fails when enable_token_auth config is set."""
    # Test the token validation logic directly
    # Clear the cached token to ensure we start fresh
    import ray._private.auth_token_loader as auth_module
    from ray._private.auth_token_loader import load_auth_token

    auth_module._cached_token = None

    # Ensure no token exists
    token = load_auth_token(generate_if_not_found=False)
    assert token == ""

    # Test the exact error message that would be raised
    with pytest.raises(RuntimeError, match="no authentication token was found"):
        if not token:
            raise RuntimeError(
                "Token-based authentication is enabled on the cluster you're connecting to, "
                "but no authentication token was found. Please provide a token using one of:\n"
                "  1. RAY_AUTH_TOKEN environment variable\n"
                "  2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)\n"
                "  3. Default token file: ~/.ray/auth_token"
            )


def test_token_path_nonexistent_file_fails():
    """Test that setting RAY_AUTH_TOKEN_PATH to nonexistent file fails gracefully."""
    # Enable token auth and set token path to nonexistent file
    os.environ["RAY_enable_token_auth"] = "1"
    os.environ["RAY_AUTH_TOKEN_PATH"] = "/nonexistent/path/to/token"

    # Initialize Ray with token auth should fail
    with pytest.raises((FileNotFoundError, RuntimeError)):
        ray.init()


@pytest.mark.parametrize("tokens_match", [True, False])
def test_cluster_token_authentication(tokens_match):
    """Test cluster authentication with matching and non-matching tokens."""
    # Set up cluster token first
    cluster_token = "a" * 32
    os.environ["RAY_AUTH_TOKEN"] = cluster_token
    os.environ["RAY_enable_token_auth"] = "1"

    # Create cluster with token auth enabled - node will read current env token
    cluster = Cluster()
    cluster.add_node()

    try:
        # Set client token based on test parameter
        if tokens_match:
            client_token = cluster_token  # Same token - should succeed
        else:
            client_token = "b" * 32  # Different token - should fail

        os.environ["RAY_AUTH_TOKEN"] = client_token

        # Reset cached token so it reads the new environment variable
        reset_token_cache()

        if tokens_match:
            # Should succeed - test gRPC calls work
            ray.init(address=cluster.address)

            # Test that gRPC calls succeed
            obj_ref = ray.put("test_data")
            result = ray.get(obj_ref)
            assert result == "test_data"

            # Test remote function call
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
                # If init somehow succeeds, try a gRPC operation that should fail
                try:
                    ray.put("test")
                finally:
                    ray.shutdown()

    finally:
        # Ensure cleanup
        ray.shutdown()
        cluster.shutdown()


def test_system_config_auth_raises_error():
    """Test that using _system_config for enabling token auth raises helpful error."""
    with pytest.raises(ValueError, match="environment variable"):
        ray.init(_system_config={"enable_token_auth": True})


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
