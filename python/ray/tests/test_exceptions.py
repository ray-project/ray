"""Tests for Ray exceptions."""
import pytest

from ray.exceptions import RayAuthenticationError, RayError


class TestRayAuthenticationError:
    """Tests for RayAuthenticationError exception."""

    auth_doc_url = "https://docs.ray.io/en/latest/ray-security/auth.html"

    def test_basic_creation(self):
        """Test basic RayAuthenticationError creation."""
        error = RayAuthenticationError("Token is missing")
        error_str = str(error)

        assert "Token is missing" in error_str
        assert self.auth_doc_url in error_str

    def test_is_ray_error_subclass(self):
        """Test that RayAuthenticationError is a RayError subclass."""
        error = RayAuthenticationError("Test")
        assert isinstance(error, RayError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
