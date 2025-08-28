import urllib.parse

"""
Utilities for working with HTTP endpoints.
"""


def validate_http_only_endpoint(endpoint: str) -> None:
    """Validates whether a string is a valid HTTP only endpoint.

    Args:
        endpoint: The endpoint string to validate

    Raises:
        ValueError: If the endpoint is not a valid HTTP only URL

    Example:
        >>> validate_http_only_endpoint("http://example.com:8080/path")  # No exception
        >>> validate_http_only_endpoint("invalid-url")  # Raises ValueError
        >>> validate_http_only_endpoint("https://example.com:8080/path")  # Raises ValueError
        >>> validate_http_only_endpoint("http://:8080/path")  # Raises ValueError
    """
    parsed = urllib.parse.urlparse(endpoint)

    # Must have a scheme
    if not parsed.scheme:
        raise ValueError(
            f"Invalid HTTP endpoint: {endpoint}. The endpoint must have a scheme."
        )

    # Determine valid schemes based on requirements
    if parsed.scheme.lower() != "http":
        raise ValueError(
            f"Invalid HTTP endpoint: {endpoint}. The endpoint must have a scheme of 'http'."
        )

    # Must have a netloc (hostname)
    if not parsed.netloc:
        raise ValueError(
            f"Invalid HTTP endpoint: {endpoint}. The endpoint must have a hostname."
        )
