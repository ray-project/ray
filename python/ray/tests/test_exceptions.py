"""Tests for Ray exceptions."""
import sys

import pytest

from ray.exceptions import AuthenticationError, RayError


class TestAuthenticationError:
    """Tests for AuthenticationError exception."""

    auth_doc_url = "https://docs.ray.io/en/latest/ray-security/auth.html"

    def test_basic_creation(self):
        """Test basic AuthenticationError creation."""
        error = AuthenticationError("Token is missing")
        error_str = str(error)

        assert "Token is missing" in error_str
        assert self.auth_doc_url in error_str

    def test_is_ray_error_subclass(self):
        """Test that AuthenticationError is a RayError subclass."""
        error = AuthenticationError("Test")
        assert isinstance(error, RayError)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
