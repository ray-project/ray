try:
    from ray._raylet import (
        AuthenticationMode,
        get_authentication_mode,
        validate_authentication_token,
    )

    _RAYLET_AVAILABLE = True
except ImportError:
    # ray._raylet not available during doc builds
    _RAYLET_AVAILABLE = False


def is_token_auth_enabled() -> bool:
    """Check if token authentication is enabled.

    Returns:
        bool: True if auth_mode is set to "token", False otherwise
    """
    if not _RAYLET_AVAILABLE:
        return False
    return get_authentication_mode() == AuthenticationMode.TOKEN


def validate_request_token(auth_header: str) -> bool:
    """Validate the Authorization header from an HTTP request.

    Args:
        auth_header: The Authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if token is valid, False otherwise
    """
    if not _RAYLET_AVAILABLE or not auth_header:
        return False

    # validate_authentication_token expects full "Bearer <token>" format
    # and performs equality comparison via C++ layer
    return validate_authentication_token(auth_header)
