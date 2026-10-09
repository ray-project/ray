"""Test utilities for Databricks datasource tests."""

from dataclasses import dataclass, field
from typing import Optional

from ray.data._internal.datasource.databricks_credentials import (
    DatabricksCredentialProvider,
)


@dataclass
class MockResponse:
    """Mock HTTP response for testing.

    Args:
        status_code: HTTP status code. Defaults to 200.
        content: Response content as bytes. Defaults to None.
        _json_data: JSON response data. Defaults to None.
        raise_on_error: If True, raise_for_status() raises for status >= 400.
            Defaults to True.
    """

    status_code: int = 200
    content: Optional[bytes] = None
    _json_data: Optional[dict] = None
    raise_on_error: bool = field(default=True, repr=False)

    def raise_for_status(self):
        """Raise an exception if status code indicates an error."""
        if self.raise_on_error and self.status_code >= 400:
            raise Exception(f"HTTP Error {self.status_code}")

    def json(self):
        """Return the JSON data."""
        return self._json_data


class RefreshableCredentialProvider(DatabricksCredentialProvider):
    """A credential provider that simulates token refresh on invalidate.

    Useful for testing 401 retry logic. When invalidate() is called,
    the token changes from initial_token to "refreshed_token".

    Args:
        initial_token: The initial token value. Defaults to "expired_token".
        host: The host URL to return. Defaults to "https://test-host.databricks.com".
    """

    def __init__(
        self,
        initial_token: str = "expired_token",
        host: str = "https://test-host.databricks.com",
    ):
        self.current_token = initial_token
        self.invalidate_count = 0
        self._host = host

    def get_token(self) -> str:
        """Get the current token."""
        return self.current_token

    def get_host(self) -> str:
        """Get the host URL."""
        return self._host

    def invalidate(self) -> None:
        """Simulate token refresh by changing to 'refreshed_token'."""
        self.invalidate_count += 1
        self.current_token = "refreshed_token"
