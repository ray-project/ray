"""Unit tests for Azure credential error handling utilities.

Tests the handle_azure_credential_error(), validate_azure_credentials(),
and catch_azure_credential_errors() helpers from
ray.autoscaler._private._azure.utils without requiring the Azure SDK.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers – build mock Azure exception classes so the real SDK is NOT needed.
# ---------------------------------------------------------------------------

def _make_mock_azure_modules():
    """Return a dict suitable for ``patch.dict(sys.modules, ...)`` that
    provides lightweight stand-ins for the Azure SDK modules referenced by
    the utils module.
    """
    # azure.core.exceptions
    mock_core_exceptions = MagicMock()

    class _ClientAuthenticationError(Exception):
        pass

    class _HttpResponseError(Exception):
        def __init__(self, message="", status_code=None):
            super().__init__(message)
            self.status_code = status_code

    mock_core_exceptions.ClientAuthenticationError = _ClientAuthenticationError
    mock_core_exceptions.HttpResponseError = _HttpResponseError

    # azure.identity
    mock_identity = MagicMock()

    class _CredentialUnavailableError(Exception):
        pass

    mock_identity.CredentialUnavailableError = _CredentialUnavailableError

    class _DefaultAzureCredential:
        def __init__(self, **kwargs):
            pass

        def get_token(self, scope):
            return MagicMock()

    mock_identity.DefaultAzureCredential = _DefaultAzureCredential

    modules = {
        "azure": MagicMock(),
        "azure.core": MagicMock(),
        "azure.core.exceptions": mock_core_exceptions,
        "azure.identity": mock_identity,
    }

    return (
        modules,
        _ClientAuthenticationError,
        _HttpResponseError,
        _CredentialUnavailableError,
        _DefaultAzureCredential,
    )


# ---------------------------------------------------------------------------
# Tests – handle_azure_credential_error
# ---------------------------------------------------------------------------


class TestHandleAzureCredentialError:
    """Tests for handle_azure_credential_error()."""

    def _import_handler(self, modules_dict):
        """Import the function under the given mock-module context."""
        with patch.dict(sys.modules, modules_dict):
            # Force re-import so lazy imports inside the function resolve
            # against the patched modules.
            from ray.autoscaler._private._azure.utils import (
                handle_azure_credential_error,
            )
            return handle_azure_credential_error

    def test_catches_client_authentication_error(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = ClientAuthErr("Token has expired")
        with pytest.raises(RuntimeError, match="az login"):
            handler(exc)

    def test_catches_credential_unavailable_error(self):
        mods, _, _, CredUnavail, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = CredUnavail("No credential could be found")
        with pytest.raises(RuntimeError, match="az login"):
            handler(exc)

    def test_catches_http_response_error_401(self):
        mods, _, HttpRespErr, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = HttpRespErr("Unauthorized", status_code=401)
        with pytest.raises(RuntimeError, match="AZURE_CLIENT_ID"):
            handler(exc)

    def test_catches_http_response_error_403(self):
        mods, _, HttpRespErr, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = HttpRespErr("Forbidden", status_code=403)
        with pytest.raises(RuntimeError, match="AZURE_TENANT_ID"):
            handler(exc)

    def test_ignores_http_response_error_404(self):
        mods, _, HttpRespErr, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = HttpRespErr("Not found", status_code=404)
        # Should be a no-op – no RuntimeError raised.
        handler(exc)

    def test_ignores_http_response_error_500(self):
        mods, _, HttpRespErr, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = HttpRespErr("Server error", status_code=500)
        handler(exc)

    def test_ignores_unrelated_exceptions(self):
        mods, _, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = ValueError("something else entirely")
        handler(exc)

    def test_resource_type_appears_in_message(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = ClientAuthErr("Token expired")
        with pytest.raises(RuntimeError, match="compute"):
            handler(exc, resource_type="compute")

    def test_no_resource_type_omits_context(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = ClientAuthErr("Token expired")
        with pytest.raises(RuntimeError, match="Azure credential error:"):
            handler(exc, resource_type=None)

    def test_recovery_message_includes_env_vars(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = ClientAuthErr("expired")
        with pytest.raises(RuntimeError, match="AZURE_CLIENT_SECRET"):
            handler(exc)

    def test_recovery_message_includes_managed_identity(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        exc = ClientAuthErr("expired")
        with pytest.raises(RuntimeError, match="managed identity"):
            handler(exc)

    def test_original_exception_chained(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()
        handler = self._import_handler(mods)
        original = ClientAuthErr("original error")
        with pytest.raises(RuntimeError) as exc_info:
            handler(original)
        assert exc_info.value.__cause__ is original

    def test_noop_when_azure_sdk_not_installed(self):
        """When azure.core.exceptions cannot be imported the handler
        should silently return without raising."""
        # Provide modules dict *without* azure.core.exceptions so the
        # import inside the handler fails.
        handler_module = {}  # empty – no azure at all
        with patch.dict(sys.modules, handler_module):
            from ray.autoscaler._private._azure.utils import (
                handle_azure_credential_error,
            )
            # Should be a silent no-op for any exception.
            handle_azure_credential_error(RuntimeError("anything"))


# ---------------------------------------------------------------------------
# Tests – catch_azure_credential_errors decorator
# ---------------------------------------------------------------------------


class TestCatchAzureCredentialErrorsDecorator:
    """Tests for catch_azure_credential_errors()."""

    def test_decorator_converts_credential_error(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                catch_azure_credential_errors,
            )

            @catch_azure_credential_errors(resource_type="compute")
            def boom():
                raise ClientAuthErr("token expired")

            with pytest.raises(RuntimeError, match="compute"):
                boom()

    def test_decorator_passes_through_non_credential_errors(self):
        mods, _, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                catch_azure_credential_errors,
            )

            @catch_azure_credential_errors(resource_type="compute")
            def boom():
                raise ValueError("not a credential error")

            with pytest.raises(ValueError, match="not a credential error"):
                boom()

    def test_decorator_preserves_return_value(self):
        mods, _, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                catch_azure_credential_errors,
            )

            @catch_azure_credential_errors(resource_type="compute")
            def add(a, b):
                return a + b

            assert add(1, 2) == 3

    def test_decorator_preserves_function_name(self):
        mods, _, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                catch_azure_credential_errors,
            )

            @catch_azure_credential_errors()
            def my_function():
                pass

            assert my_function.__name__ == "my_function"


# ---------------------------------------------------------------------------
# Tests – validate_azure_credentials
# ---------------------------------------------------------------------------


class TestValidateAzureCredentials:
    """Tests for validate_azure_credentials()."""

    def test_success_path(self):
        mods, _, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                validate_azure_credentials,
            )
            cred = MagicMock()
            cred.get_token.return_value = MagicMock()
            # Should not raise.
            validate_azure_credentials(credential=cred)
            cred.get_token.assert_called_once_with(
                "https://management.azure.com/.default"
            )

    def test_failure_with_credential_error(self):
        mods, ClientAuthErr, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                validate_azure_credentials,
            )
            cred = MagicMock()
            cred.get_token.side_effect = ClientAuthErr("token expired")
            with pytest.raises(RuntimeError, match="az login"):
                validate_azure_credentials(credential=cred)

    def test_failure_with_non_credential_error(self):
        mods, _, _, _, _ = _make_mock_azure_modules()

        with patch.dict(sys.modules, mods):
            from ray.autoscaler._private._azure.utils import (
                validate_azure_credentials,
            )
            cred = MagicMock()
            cred.get_token.side_effect = ConnectionError("network down")
            with pytest.raises(ConnectionError, match="network down"):
                validate_azure_credentials(credential=cred)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
