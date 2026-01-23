"""Databricks credential providers for Ray Data.

This module provides credential abstraction for Databricks authentication,
supporting static tokens with extensibility for future credential sources.
"""

import os
from abc import ABC, abstractmethod
from typing import Optional


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

    Reads DATABRICKS_TOKEN and DATABRICKS_HOST from environment variables.
    If DATABRICKS_HOST is not set and running in Databricks runtime,
    automatically detects the host.

    Args:
        token_env_var: Environment variable name for the token.
            Defaults to "DATABRICKS_TOKEN".
        host_env_var: Environment variable name for the host.
            Defaults to "DATABRICKS_HOST".

    Raises:
        ValueError: If token or host cannot be resolved.
    """

    def __init__(
        self,
        token_env_var: str = "DATABRICKS_TOKEN",
        host_env_var: str = "DATABRICKS_HOST",
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
                f"Environment variable '{self._host_env_var}' is not set and "
                "Databricks runtime host detection failed. Please set it to your "
                'Databricks workspace URL (e.g. "adb-<workspace-id>.<random>.azuredatabricks.net").'
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
        except Exception:
            pass
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
        """No-op for environment credentials.

        Environment variables are read fresh each time, so no
        invalidation is needed.
        """
        pass


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
