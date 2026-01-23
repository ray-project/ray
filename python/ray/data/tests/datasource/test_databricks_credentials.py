"""Unit tests for Databricks credential providers."""

import os
from unittest import mock

import pytest

from ray.data._internal.datasource.databricks_credentials import (
    DatabricksCredentialProvider,
    EnvironmentCredentialProvider,
    StaticCredentialProvider,
    resolve_credential_provider,
)


class TestDatabricksCredentialProvider:
    """Tests for the abstract DatabricksCredentialProvider base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Verify DatabricksCredentialProvider cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            DatabricksCredentialProvider()

    def test_abstract_methods_defined(self):
        """Verify all abstract methods are defined."""
        abstract_methods = DatabricksCredentialProvider.__abstractmethods__
        assert "get_token" in abstract_methods
        assert "get_host" in abstract_methods
        assert "invalidate" in abstract_methods


class TestStaticCredentialProvider:
    """Tests for StaticCredentialProvider."""

    def test_init_with_valid_token_and_host(self):
        """Test successful initialization with token and host."""
        provider = StaticCredentialProvider(
            token="test_token", host="https://my-workspace.cloud.databricks.com"
        )
        assert provider.get_token() == "test_token"
        assert provider.get_host() == "https://my-workspace.cloud.databricks.com"

    def test_init_with_empty_token_raises_error(self):
        """Test that empty token raises ValueError."""
        with pytest.raises(ValueError, match="Token cannot be empty"):
            StaticCredentialProvider(token="", host="host")

    def test_init_with_none_token_raises_error(self):
        """Test that None token raises ValueError."""
        with pytest.raises(ValueError, match="Token cannot be empty"):
            StaticCredentialProvider(token=None, host="host")

    def test_init_with_empty_host_raises_error(self):
        """Test that empty host raises ValueError."""
        with pytest.raises(ValueError, match="Host cannot be empty"):
            StaticCredentialProvider(token="valid_token", host="")

    def test_init_with_none_host_raises_error(self):
        """Test that None host raises ValueError."""
        with pytest.raises(ValueError, match="Host cannot be empty"):
            StaticCredentialProvider(token="valid_token", host=None)

    def test_invalidate_is_noop(self):
        """Test that invalidate doesn't affect the static token."""
        provider = StaticCredentialProvider(token="test_token", host="test_host")
        provider.invalidate()
        assert provider.get_token() == "test_token"
        assert provider.get_host() == "test_host"

    def test_get_token_returns_same_value(self):
        """Test that get_token always returns the same value."""
        provider = StaticCredentialProvider(token="consistent_token", host="host")
        assert provider.get_token() == "consistent_token"
        assert provider.get_token() == "consistent_token"


class TestEnvironmentCredentialProvider:
    """Tests for EnvironmentCredentialProvider."""

    def test_get_token_from_env(self):
        """Test get_token reads from environment variable."""
        with mock.patch.dict(
            os.environ, {"DATABRICKS_TOKEN": "env_token", "DATABRICKS_HOST": "host"}
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_token() == "env_token"

    def test_get_host_from_env(self):
        """Test get_host reads from environment variable."""
        with mock.patch.dict(
            os.environ, {"DATABRICKS_TOKEN": "token", "DATABRICKS_HOST": "env_host"}
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_host() == "env_host"

    def test_init_raises_when_token_not_set(self):
        """Test __init__ raises ValueError when token env var is not set."""
        with mock.patch.dict(os.environ, {"DATABRICKS_HOST": "host"}, clear=True):
            with pytest.raises(ValueError, match="DATABRICKS_TOKEN.*not set"):
                EnvironmentCredentialProvider()

    def test_init_raises_when_host_not_set(self):
        """Test __init__ raises ValueError when host env var is not set."""
        with mock.patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=True):
            with pytest.raises(ValueError, match="DATABRICKS_HOST.*not set"):
                EnvironmentCredentialProvider()

    def test_host_detected_from_databricks_runtime(self):
        """Test host is detected from Databricks runtime when env var not set."""
        with (
            mock.patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=True),
            mock.patch.object(
                EnvironmentCredentialProvider,
                "_detect_databricks_host",
                return_value="detected-host.databricks.com",
            ),
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_host() == "detected-host.databricks.com"

    def test_custom_env_var_names(self):
        """Test using custom environment variable names."""
        with mock.patch.dict(
            os.environ, {"MY_TOKEN": "custom_token", "MY_HOST": "custom_host"}
        ):
            provider = EnvironmentCredentialProvider(
                token_env_var="MY_TOKEN", host_env_var="MY_HOST"
            )
            assert provider.get_token() == "custom_token"
            assert provider.get_host() == "custom_host"

    def test_invalidate_is_noop(self):
        """Test that invalidate doesn't affect environment credentials."""
        with mock.patch.dict(
            os.environ, {"DATABRICKS_TOKEN": "env_token", "DATABRICKS_HOST": "host"}
        ):
            provider = EnvironmentCredentialProvider()
            provider.invalidate()
            assert provider.get_token() == "env_token"


class TestResolveCredentialProvider:
    """Tests for resolve_credential_provider function."""

    def test_resolve_with_explicit_provider(self):
        """Test that explicit credential_provider is returned as-is."""
        provider = StaticCredentialProvider(token="my_token", host="my_host")
        result = resolve_credential_provider(credential_provider=provider)
        assert result is provider

    def test_resolve_with_none_returns_environment_provider(self):
        """Test that EnvironmentCredentialProvider is returned when none provided."""
        from ray.data._internal.datasource.databricks_credentials import (
            EnvironmentCredentialProvider,
        )

        with mock.patch.dict(
            os.environ, {"DATABRICKS_TOKEN": "token", "DATABRICKS_HOST": "host"}
        ):
            result = resolve_credential_provider()
            assert isinstance(result, EnvironmentCredentialProvider)

    def test_resolve_with_explicit_none_returns_environment_provider(self):
        """Test that explicit None returns EnvironmentCredentialProvider."""
        from ray.data._internal.datasource.databricks_credentials import (
            EnvironmentCredentialProvider,
        )

        with mock.patch.dict(
            os.environ, {"DATABRICKS_TOKEN": "token", "DATABRICKS_HOST": "host"}
        ):
            result = resolve_credential_provider(credential_provider=None)
            assert isinstance(result, EnvironmentCredentialProvider)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
