"""Databricks credential providers for Ray Data.

This module provides credential abstraction for Databricks authentication,
supporting static tokens with extensibility for future credential sources.
"""

import logging
import os
from abc import ABC, abstractmethod
from typing import Callable, Optional

import requests

logger = logging.getLogger(__name__)

# Default environment variable names for Databricks credentials
DEFAULT_TOKEN_ENV_VAR = "DATABRICKS_TOKEN"
DEFAULT_HOST_ENV_VAR = "DATABRICKS_HOST"


class DatabricksCredentialProvider(ABC):
    """Abstract base class for Databricks credential providers.

    This abstraction allows different credential sources (static tokens,
    file-based credentials, etc.) to be used with DatabricksUCDatasource.

    Subclasses must implement:
        - get_token(): Returns the current authentication token
        - get_host(): Returns the Databricks host URL (optional)
        - invalidate(): Clears any cached credentials
    """

    @abstractmethod
    def get_token(self) -> str:
        """Get the current authentication token.

        Returns:
            The Databricks authentication token string.

        Raises:
            ValueError: If no valid token is available.
        """
        pass

    @abstractmethod
    def get_host(self) -> str:
        """Get the Databricks host URL.

        Returns:
            The Databricks host URL.

        Raises:
            ValueError: If no valid host is available.
        """
        pass

    @abstractmethod
    def invalidate(self) -> None:
        """Invalidate any cached credentials.

        This method should be called when credentials need to be refreshed,
        such as after an authentication error.
        """
        pass


class StaticCredentialProvider(DatabricksCredentialProvider):
    """A credential provider that wraps static token and host.

    This is the simplest credential provider, useful when you have a
    token that doesn't need to be refreshed.

    Args:
        token: The Databricks authentication token.
        host: The Databricks host URL.

    Raises:
        ValueError: If token or host is empty or None.
    """

    def __init__(self, token: str, host: str):
        if not token:
            raise ValueError("Token cannot be empty or None")
        if not host:
            raise ValueError("Host cannot be empty or None")
        self._token = token
        self._host = host

    def get_token(self) -> str:
        """Get the static token.

        Returns:
            The authentication token provided at construction.
        """
        return self._token

    def get_host(self) -> str:
        """Get the host URL.

        Returns:
            The host URL provided at construction.
        """
        return self._host

    def invalidate(self) -> None:
        """No-op for static credentials.

        Static credentials cannot be refreshed, so this method
        does nothing.
        """
        pass


class EnvironmentCredentialProvider(DatabricksCredentialProvider):
    """A credential provider that reads from environment variables.

    Reads token and host from environment variables.
    If host env var is not set and running in Databricks runtime,
    automatically detects the host.

    Args:
        token_env_var: Environment variable name for the token.
            Defaults to DEFAULT_TOKEN_ENV_VAR ("DATABRICKS_TOKEN").
        host_env_var: Environment variable name for the host.
            Defaults to DEFAULT_HOST_ENV_VAR ("DATABRICKS_HOST").

    Raises:
        ValueError: If token or host cannot be resolved.
    """

    def __init__(
        self,
        token_env_var: str = DEFAULT_TOKEN_ENV_VAR,
        host_env_var: str = DEFAULT_HOST_ENV_VAR,
    ):
        self._token_env_var = token_env_var
        self._host_env_var = host_env_var

        # Validate token is set at initialization
        token = os.environ.get(self._token_env_var)
        if not token:
            raise ValueError(
                f"Environment variable '{self._token_env_var}' is not set. "
                "Please set it to your Databricks access token."
            )
        self._token = token

        # Resolve host: env var > Databricks runtime detection
        host = os.environ.get(self._host_env_var) or self._detect_databricks_host()
        if not host:
            raise ValueError(
                "You are not in databricks runtime, please set environment variable "
                f"'{self._host_env_var}' to databricks workspace URL "
                '(e.g. "adb-<workspace-id>.<random-number>.azuredatabricks.net").'
            )
        self._host = host

    def _detect_databricks_host(self) -> Optional[str]:
        """Detect host from Databricks runtime if available."""
        try:
            from ray.util.spark.utils import is_in_databricks_runtime

            if is_in_databricks_runtime():
                import IPython

                ip_shell = IPython.get_ipython()
                if ip_shell is not None:
                    dbutils = ip_shell.ns_table["user_global"]["dbutils"]
                    ctx = (
                        dbutils.notebook.entry_point.getDbutils()
                        .notebook()
                        .getContext()
                    )
                    return ctx.tags().get("browserHostName").get()
        except Exception as e:
            logger.warning(f"Failed to detect Databricks host from runtime: {e}")
        return None

    def get_token(self) -> str:
        """Get the token from environment variable.

        Returns:
            The authentication token from the environment.
        """
        return self._token

    def get_host(self) -> str:
        """Get the host from environment variable or Databricks runtime.

        Returns:
            The host URL.
        """
        return self._host

    def invalidate(self) -> None:
        """Re-read token from environment variable.

        This allows picking up refreshed tokens when the environment
        variable is updated (e.g., by an external token refresh process).
        """
        token = os.environ.get(self._token_env_var)
        if token:
            self._token = token


def resolve_credential_provider(
    credential_provider: Optional[DatabricksCredentialProvider] = None,
) -> DatabricksCredentialProvider:
    """Resolve credential provider.

    Args:
        credential_provider: An explicit credential provider instance.
            If None, falls back to EnvironmentCredentialProvider.

    Returns:
        A DatabricksCredentialProvider instance.
    """
    if credential_provider is not None:
        return credential_provider

    # Fall back to environment variables
    return EnvironmentCredentialProvider()


def build_headers(
    credential_provider: DatabricksCredentialProvider,
) -> dict[str, str]:
    """Build request headers with fresh token from credential provider.

    Args:
        credential_provider: The credential provider to get the token from.

    Returns:
        Dictionary containing Content-Type and Authorization headers.
    """
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {credential_provider.get_token()}",
    }


def request_with_401_retry(
    request_fn: Callable[..., requests.Response],
    url: str,
    credential_provider: DatabricksCredentialProvider,
    **kwargs,
) -> requests.Response:
    """Make an HTTP request with one retry on 401 after invalidating credentials.

    Args:
        request_fn: Request function (e.g., requests.get or requests.post)
        url: Request URL
        credential_provider: Credential provider for authentication
        **kwargs: Additional arguments passed to requests

    Returns:
        Response object (after calling raise_for_status)
    """
    response = request_fn(url, headers=build_headers(credential_provider), **kwargs)

    if response.status_code == 401:
        logger.info("Received 401 response, invalidating credentials and retrying.")
        credential_provider.invalidate()
        response = request_fn(url, headers=build_headers(credential_provider), **kwargs)

    response.raise_for_status()
    return response
