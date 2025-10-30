import tempfile
from pathlib import Path
from typing import Optional

import pytest

import ray
from ray._private.authentication.authentication_constants import (
    HTTP_REQUEST_INVALID_TOKEN_ERROR_MESSAGE,
    HTTP_REQUEST_MISSING_TOKEN_ERROR_MESSAGE,
)
from ray.cluster_utils import Cluster
from ray.dashboard.modules.job.sdk import JobSubmissionClient
from ray.tests.authentication_test_utils import (
    clear_auth_token_sources,
    reset_auth_token_state,
    set_auth_mode,
    set_auth_token_path,
    set_default_auth_token,
    set_env_auth_token,
)
from ray.util.state import StateApiClient


def test_submission_client_adds_token_automatically(setup_cluster_with_token_auth):
    """Test that SubmissionClient automatically adds token to headers."""
    # Token is already set in environment from setup_cluster_with_token_auth fixture
    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    # Verify Authorization header was added
    assert "Authorization" in client._headers
    assert client._headers["Authorization"].startswith("Bearer ")


def test_submission_client_without_token_shows_helpful_error(
    setup_cluster_with_token_auth,
):
    """Test that requests without token show helpful error message."""
    # Remove token from environment
    clear_auth_token_sources(remove_default=True)
    set_auth_mode("disabled")
    reset_auth_token_state()

    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    # Make a request - should fail with helpful message
    with pytest.raises(RuntimeError) as exc_info:
        client.get_version()

    expected_message = (
        "Authentication required: Unauthorized: Missing authentication token\n\n"
        f"{HTTP_REQUEST_MISSING_TOKEN_ERROR_MESSAGE}"
    )
    assert str(exc_info.value) == expected_message


def test_submission_client_with_invalid_token_shows_helpful_error(
    setup_cluster_with_token_auth,
):
    """Test that requests with wrong token show helpful error message."""
    # Set wrong token
    wrong_token = "wrong_token_00000000000000000000000000000000"
    set_env_auth_token(wrong_token)
    set_auth_mode("token")
    reset_auth_token_state()

    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    # Make a request - should fail with helpful message
    with pytest.raises(RuntimeError) as exc_info:
        client.get_version()

    expected_message = (
        "Authentication failed: Forbidden: Invalid authentication token\n\n"
        f"{HTTP_REQUEST_INVALID_TOKEN_ERROR_MESSAGE}"
    )
    assert str(exc_info.value) == expected_message


def test_submission_client_with_valid_token_succeeds(setup_cluster_with_token_auth):
    """Test that requests with valid token succeed."""
    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    # Make a request - should succeed
    version = client.get_version()
    assert version is not None


def test_job_submission_client_inherits_auth(setup_cluster_with_token_auth):
    """Test that JobSubmissionClient inherits auth from SubmissionClient."""
    client = JobSubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    # Verify Authorization header was added
    assert "Authorization" in client._headers
    assert client._headers["Authorization"].startswith("Bearer ")

    # Verify client can make authenticated requests
    version = client.get_version()
    assert version is not None


def test_state_api_client_inherits_auth(setup_cluster_with_token_auth):
    """Test that StateApiClient inherits auth from SubmissionClient."""
    client = StateApiClient(address=setup_cluster_with_token_auth["dashboard_url"])

    # Verify Authorization header was added
    assert "Authorization" in client._headers
    assert client._headers["Authorization"].startswith("Bearer ")


def test_user_provided_header_not_overridden(setup_cluster_with_token_auth):
    """Test that user-provided Authorization header is not overridden."""
    custom_auth = "Bearer custom_token"

    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(
        address=setup_cluster_with_token_auth["dashboard_url"],
        headers={"Authorization": custom_auth},
    )

    # Verify custom value is preserved
    assert client._headers["Authorization"] == custom_auth


def test_error_messages_contain_instructions(setup_cluster_with_token_auth):
    """Test that all auth error messages contain setup instructions."""
    # Test 401 error (missing token)
    clear_auth_token_sources(remove_default=True)
    set_auth_mode("disabled")
    reset_auth_token_state()

    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    with pytest.raises(RuntimeError) as exc_info:
        client.get_version()

    expected_missing = (
        "Authentication required: Unauthorized: Missing authentication token\n\n"
        f"{HTTP_REQUEST_MISSING_TOKEN_ERROR_MESSAGE}"
    )
    assert str(exc_info.value) == expected_missing

    # Test 403 error (invalid token)
    set_env_auth_token("wrong_token_00000000000000000000000000000000")
    set_auth_mode("token")
    reset_auth_token_state()

    client2 = SubmissionClient(address=setup_cluster_with_token_auth["dashboard_url"])

    with pytest.raises(RuntimeError) as exc_info:
        client2.get_version()

    expected_invalid = (
        "Authentication failed: Forbidden: Invalid authentication token\n\n"
        f"{HTTP_REQUEST_INVALID_TOKEN_ERROR_MESSAGE}"
    )
    assert str(exc_info.value) == expected_invalid


@pytest.mark.parametrize("token_source", ["env_var", "token_path", "default_path"])
def test_token_loaded_from_sources(cleanup_auth_token_env, token_source):
    """Test that SubmissionClient loads tokens from all supported sources."""

    test_token = "test_token_12345678901234567890123456789012"
    set_auth_mode("token")

    token_file_path: Optional[Path] = None

    if token_source == "env_var":
        set_env_auth_token(test_token)
    elif token_source == "token_path":
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            token_file_path = Path(tmp.name)
        set_auth_token_path(test_token, token_file_path)
    else:
        token_file_path = Path.home() / ".ray" / "auth_token"
        set_default_auth_token(test_token)

    reset_auth_token_state()

    cluster = Cluster()
    cluster.add_node()

    try:
        context = ray.init(address=cluster.address)
        dashboard_url = context.address_info["webui_url"]

        from ray.dashboard.modules.dashboard_sdk import SubmissionClient

        client = SubmissionClient(address=f"http://{dashboard_url}")
        assert client._headers["Authorization"] == f"Bearer {test_token}"
    finally:
        ray.shutdown()
        cluster.shutdown()
        if token_source == "token_path" and token_file_path:
            token_file_path.unlink(missing_ok=True)


def test_no_token_added_when_auth_disabled(setup_cluster_without_token_auth):
    """Test that no Authorization header is injected when auth is disabled."""

    from ray.dashboard.modules.dashboard_sdk import SubmissionClient

    client = SubmissionClient(address=setup_cluster_without_token_auth["dashboard_url"])

    assert "Authorization" not in client._headers


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
