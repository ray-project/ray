"""Tests for Ray exceptions."""
import sys
from enum import Enum
from unittest.mock import MagicMock, patch

import pytest

from ray._private.authentication.authentication_constants import (
    TOKEN_AUTH_ERROR_MESSAGE,
)
from ray.exceptions import AuthenticationError, RayError


class FakeAuthMode(Enum):
    DISABLED = 0
    TOKEN = 1


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

    @pytest.mark.parametrize(
        "message",
        [
            "Token is missing",
            "Token is invalid.",
            TOKEN_AUTH_ERROR_MESSAGE,
        ],
        ids=["no_period", "trailing_period", "no_token_message"],
    )
    def test_punctuation(self, message):
        error_str = str(AuthenticationError(message))

        # A period right after the URL ends up inside terminal links.
        assert self.auth_doc_url + "." not in error_str
        assert ".." not in error_str

    def test_is_ray_error_subclass(self):
        """Test that AuthenticationError is a RayError subclass."""
        error = AuthenticationError("Test")
        assert isinstance(error, RayError)

    @pytest.mark.parametrize(
        "auth_mode,expected_note",
        [
            (FakeAuthMode.DISABLED, "RAY_AUTH_MODE is currently 'disabled'"),
            (FakeAuthMode.TOKEN, None),
        ],
        ids=["disabled", "token"],
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

    def test_token_auth_error_gets_no_second_fix(self):
        """The token auth error already names both fixes, so nothing is appended."""
        message = TOKEN_AUTH_ERROR_MESSAGE
        with patch.dict(
            "sys.modules",
            {
                "ray._raylet": MagicMock(
                    AuthenticationMode=FakeAuthMode,
                    get_authentication_mode=lambda: FakeAuthMode.TOKEN,
                )
            },
        ):
            assert str(AuthenticationError(message)) == message


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
