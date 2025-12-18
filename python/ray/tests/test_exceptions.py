"""Tests for Ray exceptions."""
import sys
from enum import Enum
from unittest.mock import MagicMock, patch

import pytest

from ray.exceptions import AuthenticationError, RayError


class FakeAuthMode(Enum):
    DISABLED = 0
    TOKEN = 1
    K8S = 2


class TestAuthenticationError:
    """Tests for AuthenticationError exception."""

    auth_doc_url = "https://docs.ray.io/en/latest/ray-security/token-auth.html"

    def test_basic_creation(self):
        """Test basic AuthenticationError creation and message format."""
        error = AuthenticationError("Token is missing")
        error_str = str(error)

        # Original message preserved
        assert "Token is missing" in error_str
        # Doc URL included
        assert self.auth_doc_url in error_str

    def test_is_ray_error_subclass(self):
        """Test that AuthenticationError is a RayError subclass."""
        error = AuthenticationError("Test")
        assert isinstance(error, RayError)

    @pytest.mark.parametrize(
        "auth_mode,expected_note",
        [
            (FakeAuthMode.DISABLED, "RAY_AUTH_MODE is currently 'disabled'"),
            (FakeAuthMode.K8S, "RAY_AUTH_MODE is currently 'k8s'"),
            (FakeAuthMode.TOKEN, None),
        ],
        ids=["disabled", "k8s", "token"],
    )
    def test_auth_mode_note_in_message(self, auth_mode, expected_note):
        """Test that error message includes auth mode note when not in token mode."""
        with patch.dict(
            "sys.modules",
            {
                "ray._raylet": MagicMock(
                    AuthenticationMode=FakeAuthMode,
                    get_authentication_mode=lambda: auth_mode,
                )
            },
        ):
            error = AuthenticationError("Token is missing")
            error_str = str(error)

            assert "Token is missing" in error_str
            if expected_note:
                assert expected_note in error_str
            else:
                assert "RAY_AUTH_MODE is currently" not in error_str


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
