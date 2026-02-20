"""Unit tests for Databricks credential providers."""

import os
from unittest import mock

import pytest

from ray.data._internal.datasource.databricks_credentials import (
    DatabricksCredentialProvider,
    EnvironmentCredentialProvider,
    StaticCredentialProvider,
    resolve_credential_provider_for_databricks_table,
    resolve_credential_provider_for_unity_catalog,
)

SAMPLE_TOKEN = "dapi_test_token_abc123"
SAMPLE_HOST = "https://my-workspace.cloud.databricks.com"
SAMPLE_URL = "https://uc-workspace.databricks.com"
ALT_TOKEN = "dapi_alt_token_xyz789"
ALT_HOST = "https://alt-workspace.databricks.com"


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
        provider = StaticCredentialProvider(token=SAMPLE_TOKEN, host=SAMPLE_HOST)
        assert provider.get_token() == SAMPLE_TOKEN
        assert provider.get_host() == SAMPLE_HOST

    @pytest.mark.parametrize(
        "token,host,expected_error",
        [
            ("", SAMPLE_HOST, "Token cannot be empty"),
            (None, SAMPLE_HOST, "Token cannot be empty"),
            (SAMPLE_TOKEN, "", "Host cannot be empty"),
            (SAMPLE_TOKEN, None, "Host cannot be empty"),
        ],
    )
    def test_init_with_invalid_inputs_raises_error(self, token, host, expected_error):
        """Test that invalid token or host raises ValueError."""
        with pytest.raises(ValueError, match=expected_error):
            StaticCredentialProvider(token=token, host=host)

    def test_invalidate_is_noop(self):
        """Test that invalidate doesn't affect the static token."""
        provider = StaticCredentialProvider(token=SAMPLE_TOKEN, host=SAMPLE_HOST)
        provider.invalidate()
        assert provider.get_token() == SAMPLE_TOKEN
        assert provider.get_host() == SAMPLE_HOST

    def test_get_token_returns_same_value(self):
        """Test that get_token always returns the same value."""
        provider = StaticCredentialProvider(token=SAMPLE_TOKEN, host=SAMPLE_HOST)
        assert provider.get_token() == SAMPLE_TOKEN
        assert provider.get_token() == SAMPLE_TOKEN


class TestEnvironmentCredentialProvider:
    """Tests for EnvironmentCredentialProvider."""

    def test_get_token_from_env(self):
        """Test get_token reads from environment variable."""
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_TOKEN": SAMPLE_TOKEN, "DATABRICKS_HOST": SAMPLE_HOST},
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_token() == SAMPLE_TOKEN

    def test_get_host_from_env(self):
        """Test get_host reads from environment variable."""
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_TOKEN": SAMPLE_TOKEN, "DATABRICKS_HOST": SAMPLE_HOST},
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_host() == SAMPLE_HOST

    @pytest.mark.parametrize(
        "env_vars,expected_error",
        [
            ({"DATABRICKS_HOST": SAMPLE_HOST}, "DATABRICKS_TOKEN.*not set"),
            (
                {"DATABRICKS_TOKEN": SAMPLE_TOKEN},
                "set environment variable.*DATABRICKS_HOST",
            ),
        ],
    )
    def test_init_raises_when_env_var_not_set(self, env_vars, expected_error):
        """Test __init__ raises ValueError when required env var is not set."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match=expected_error):
                EnvironmentCredentialProvider()

    def test_host_detected_from_databricks_runtime(self):
        """Test host is detected from Databricks runtime when env var not set."""
        detected_host = "detected-host.databricks.com"
        with (
            mock.patch.dict(os.environ, {"DATABRICKS_TOKEN": SAMPLE_TOKEN}, clear=True),
            mock.patch.object(
                EnvironmentCredentialProvider,
                "_detect_databricks_host",
                return_value=detected_host,
            ),
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_host() == detected_host

    def test_custom_env_var_names(self):
        """Test using custom environment variable names."""
        with mock.patch.dict(
            os.environ, {"MY_TOKEN": SAMPLE_TOKEN, "MY_HOST": SAMPLE_HOST}
        ):
            provider = EnvironmentCredentialProvider(
                token_env_var="MY_TOKEN", host_env_var="MY_HOST"
            )
            assert provider.get_token() == SAMPLE_TOKEN
            assert provider.get_host() == SAMPLE_HOST

    def test_invalidate_refreshes_token_from_env(self):
        """Test that invalidate re-reads token from environment."""
        refreshed_token = "dapi_refreshed_token_456"
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_TOKEN": SAMPLE_TOKEN, "DATABRICKS_HOST": SAMPLE_HOST},
        ):
            provider = EnvironmentCredentialProvider()
            assert provider.get_token() == SAMPLE_TOKEN

            # Simulate external token refresh
            os.environ["DATABRICKS_TOKEN"] = refreshed_token
            provider.invalidate()
            assert provider.get_token() == refreshed_token

    def test_invalidate_keeps_token_if_env_unset(self):
        """Test that invalidate keeps existing token if env var is unset."""
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_TOKEN": SAMPLE_TOKEN, "DATABRICKS_HOST": SAMPLE_HOST},
        ):
            provider = EnvironmentCredentialProvider()

            # Remove env var after initialization
            del os.environ["DATABRICKS_TOKEN"]
            provider.invalidate()
            # Should keep the old token rather than failing
            assert provider.get_token() == SAMPLE_TOKEN


class TestResolveCredentialProviderForDatabricksTable:
    """Tests for resolve_credential_provider_for_databricks_table function."""

    def test_resolve_with_explicit_provider(self):
        """Test that explicit credential_provider is returned as-is."""
        provider = StaticCredentialProvider(token=SAMPLE_TOKEN, host=SAMPLE_HOST)
        result = resolve_credential_provider_for_databricks_table(
            credential_provider=provider
        )
        assert result is provider

    @pytest.mark.parametrize("credential_provider_arg", [None, "no_arg"])
    def test_resolve_with_none_returns_environment_provider(
        self, credential_provider_arg
    ):
        """Test that EnvironmentCredentialProvider is returned when none provided."""
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_TOKEN": SAMPLE_TOKEN, "DATABRICKS_HOST": SAMPLE_HOST},
        ):
            if credential_provider_arg == "no_arg":
                result = resolve_credential_provider_for_databricks_table()
            else:
                result = resolve_credential_provider_for_databricks_table(
                    credential_provider=credential_provider_arg
                )
            assert isinstance(result, EnvironmentCredentialProvider)


class TestResolveCredentialProviderForUnityCatalog:
    """Tests for resolve_credential_provider_for_unity_catalog function."""

    def test_resolve_with_explicit_provider(self):
        """Test that explicit credential_provider is returned as-is."""
        provider = StaticCredentialProvider(token=SAMPLE_TOKEN, host=SAMPLE_HOST)
        result = resolve_credential_provider_for_unity_catalog(
            credential_provider=provider
        )
        assert result is provider

    def test_resolve_with_explicit_provider_ignores_url_and_token(self):
        """Test that url/token are ignored when credential_provider is given."""
        provider = StaticCredentialProvider(token=SAMPLE_TOKEN, host=SAMPLE_HOST)
        result = resolve_credential_provider_for_unity_catalog(
            credential_provider=provider, url=ALT_HOST, token=ALT_TOKEN
        )
        assert result is provider

    def test_resolve_with_url_and_token(self):
        """Test that url and token create a StaticCredentialProvider."""
        result = resolve_credential_provider_for_unity_catalog(
            url=SAMPLE_URL, token=SAMPLE_TOKEN
        )
        assert isinstance(result, StaticCredentialProvider)
        assert result.get_token() == SAMPLE_TOKEN
        assert result.get_host() == SAMPLE_URL

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"url": SAMPLE_URL},
            {"token": SAMPLE_TOKEN},
        ],
        ids=["no_args", "only_url", "only_token"],
    )
    def test_resolve_raises_with_incomplete_args(self, kwargs):
        """Test that ValueError is raised when args are missing or incomplete."""
        with pytest.raises(ValueError, match="Either 'credential_provider' or both"):
            resolve_credential_provider_for_unity_catalog(**kwargs)

    @pytest.mark.parametrize(
        "url,token",
        [
            ("", SAMPLE_TOKEN),
            (SAMPLE_URL, ""),
        ],
        ids=["empty_url", "empty_token"],
    )
    def test_resolve_with_empty_string_raises(self, url, token):
        """Test that empty strings for url or token raise ValueError."""
        with pytest.raises(ValueError):
            resolve_credential_provider_for_unity_catalog(url=url, token=token)


class TestCredentialProviderSerialization:
    """Tests for credential provider serialization (needed for Ray workers)."""

    @pytest.mark.parametrize(
        "provider_type,expected_token,expected_host",
        [
            ("static", SAMPLE_TOKEN, SAMPLE_HOST),
            ("environment", SAMPLE_TOKEN, SAMPLE_HOST),
        ],
    )
    def test_provider_is_picklable(self, provider_type, expected_token, expected_host):
        """Verify credential providers can be pickled and unpickled."""
        import pickle

        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_TOKEN": expected_token, "DATABRICKS_HOST": expected_host},
        ):
            if provider_type == "static":
                provider = StaticCredentialProvider(
                    token=expected_token, host=expected_host
                )
            else:
                provider = EnvironmentCredentialProvider()

            pickled = pickle.dumps(provider)
            unpickled = pickle.loads(pickled)
            assert unpickled.get_token() == expected_token
            assert unpickled.get_host() == expected_host


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
