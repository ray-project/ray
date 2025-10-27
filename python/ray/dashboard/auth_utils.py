"""Authentication utilities for Ray dashboard."""

from ray.includes.rpc_token_authentication import (
    AuthenticationMode,
    get_authentication_mode,
    validate_authentication_token,
)


def is_token_auth_enabled() -> bool:
    """Check if token authentication is enabled.

    Returns:
        bool: True if auth_mode is set to "token", False otherwise
    """
    return get_authentication_mode() == AuthenticationMode.TOKEN


def should_authenticate_request(method: str) -> bool:
    """Determine if request method requires authentication.

    Only mutable operations (POST, PUT, PATCH, DELETE) require authentication.

    Args:
        method: HTTP method (e.g., "GET", "POST", "PUT", "DELETE")

    Returns:
        bool: True if the method requires authentication, False otherwise
    """
    return method in ["POST", "PUT", "PATCH", "DELETE"]


def validate_request_token(auth_header: str) -> bool:
    """Validate the Authorization header from an HTTP request.

    Args:
        auth_header: The Authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if token is valid, False otherwise
    """
    if not auth_header:
        return False

    # validate_authentication_token expects full "Bearer <token>" format
    # and performs equality comparison via C++ layer
    return validate_authentication_token(auth_header)
