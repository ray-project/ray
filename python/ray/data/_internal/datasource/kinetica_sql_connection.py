"""
Connection factory for Kinetica DB-API integration with Ray Data.

This module provides a connection factory that creates DB-API 2.0 compliant
connections compatible with Ray Data's read_sql and write_sql methods.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


def _check_gpudb():
    """Check that gpudb is installed and return the dbapi module."""
    try:
        from gpudb import dbapi

        return dbapi
    except ImportError:
        raise ImportError(
            "gpudb is required to use Kinetica SQL integration. "
            "Install it with: pip install gpudb"
        )


@dataclass
class KineticaConnectionFactory:
    """
    A callable factory that creates Kinetica DB-API connections.

    This class is designed to work with Ray Data's SQL integration,
    which expects a callable that returns DB-API 2.0 compliant connections.

    Attributes:
        url: URL of the Kinetica server.
        username: Username for authentication.
        password: Password for authentication.
        oauth_token: OAuth token for authentication (alternative to username/password).
        default_schema: Default schema to use for queries.
        options: Additional GPUdb client options.

    Example:
        >>> factory = KineticaConnectionFactory(  # doctest: +SKIP
        ...     url="http://localhost:9191",
        ...     username="admin",
        ...     password="password",
        ... )
    """

    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    oauth_token: Optional[str] = None
    default_schema: Optional[str] = None
    options: Optional[Dict[str, Any]] = None

    def __call__(self):
        """
        Create and return a new Kinetica connection.

        Returns:
            A DB-API 2.0 compliant KineticaConnection instance.
        """
        dbapi = _check_gpudb()
        return dbapi.connect(
            connection_string="kinetica://",
            url=self.url,
            username=self.username,
            password=self.password,
            oauth_token=self.oauth_token,
            default_schema=self.default_schema,
            options=self.options,
        )


def create_kinetica_connection_factory(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    oauth_token: Optional[str] = None,
    default_schema: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
) -> Callable:
    """
    Create a connection factory for use with Ray Data's SQL methods.

    This function returns a callable that creates new Kinetica connections
    when invoked. It's designed to work with Ray Data's `read_sql` and
    `write_sql` methods, which require a connection factory.

    Args:
        url: URL of the Kinetica server (e.g., "http://localhost:9191").
        username: Username for authentication.
        password: Password for authentication.
        oauth_token: OAuth token for authentication (alternative to username/password).
        default_schema: Default schema to use for queries.
        options: Additional GPUdb client options
            (e.g., {"skip_ssl_cert_verification": True}).

    Returns:
        A callable that creates new KineticaConnection instances.

    Example:
        >>> import ray  # doctest: +SKIP
        >>> factory = create_kinetica_connection_factory(  # doctest: +SKIP
        ...     url="http://localhost:9191",
        ...     username="admin",
        ...     password="password",
        ... )
        >>> ds = ray.data.read_sql(  # doctest: +SKIP
        ...     sql="SELECT * FROM my_table",
        ...     connection_factory=factory,
        ... )
    """
    return KineticaConnectionFactory(
        url=url,
        username=username,
        password=password,
        oauth_token=oauth_token,
        default_schema=default_schema,
        options=options,
    )
