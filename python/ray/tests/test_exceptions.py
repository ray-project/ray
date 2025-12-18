"""Tests for Ray exceptions."""
import sys
from enum import Enum
from unittest.mock import MagicMock, patch

import pytest

from ray.exceptions import AuthenticationError, RayError, RayTaskError


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


class ExceptionWithReadOnlyArgs(Exception):
    """An exception class whose `args` property has no setter.

    This simulates exception types like rasterio._err.CPLE_AppDefinedError
    that don't allow `args` to be set.
    """

    def __init__(self, message, code, detail):
        self.message = message
        self.code = code
        self.detail = detail
        super().__init__(message)

    @property
    def args(self):
        return (self.message, self.code, self.detail)


class TestRayTaskErrorWithReadOnlyArgs:
    """Tests for RayTaskError with exception types that have read-only args."""

    def test_dual_exception_with_readonly_args(self):
        """Test that RayTaskError can wrap exceptions with read-only args property.

        This tests the fix for issue #59437 where exceptions that don't allow
        `args` to be set would cause an AttributeError.
        """
        # Create an exception with read-only args
        cause = ExceptionWithReadOnlyArgs("Error", 200, "Something went wrong.")

        # Create a RayTaskError wrapping this exception
        ray_task_error = RayTaskError(
            function_name="test_func",
            traceback_str="Traceback (most recent call last):\n  test",
            cause=cause,
            proctitle="test_proctitle",
            pid=12345,
            ip="127.0.0.1",
        )

        # This should not raise AttributeError
        dual_exception = ray_task_error.as_instanceof_cause()

        # Verify the dual exception inherits from both RayTaskError and the cause class
        assert isinstance(dual_exception, RayTaskError)
        assert isinstance(dual_exception, ExceptionWithReadOnlyArgs)

        # Verify we can access the cause's attributes
        assert dual_exception.message == "Error"
        assert dual_exception.code == 200
        assert dual_exception.detail == "Something went wrong."

    def test_dual_exception_pickling_with_readonly_args(self):
        """Test that dual exceptions with read-only args can be pickled."""
        import pickle

        cause = ExceptionWithReadOnlyArgs("Error", 200, "Something went wrong.")
        ray_task_error = RayTaskError(
            function_name="test_func",
            traceback_str="Traceback (most recent call last):\n  test",
            cause=cause,
            proctitle="test_proctitle",
            pid=12345,
            ip="127.0.0.1",
        )

        dual_exception = ray_task_error.as_instanceof_cause()

        # Test pickling and unpickling
        pickled = pickle.dumps(dual_exception)
        unpickled = pickle.loads(pickled)

        # Verify the unpickled exception still works
        assert isinstance(unpickled, RayTaskError)
        assert unpickled.message == "Error"
        assert unpickled.code == 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
