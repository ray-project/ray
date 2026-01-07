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
        bool: True if AUTH_MODE is set to "token" or "k8s", False otherwise
    """
    if not _RAYLET_AVAILABLE:
        return False

    return get_authentication_mode() in {
        AuthenticationMode.TOKEN,
        AuthenticationMode.K8S,
    }


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


def get_authentication_mode_name(mode: AuthenticationMode) -> str:
    """Convert AuthenticationMode enum value to string name.

    Args:
        mode: AuthenticationMode enum value from ray._raylet

    Returns:
        String name: "disabled", "token", or "k8s"
    """
    from ray._raylet import AuthenticationMode

    _MODE_NAMES = {
        AuthenticationMode.DISABLED: "disabled",
        AuthenticationMode.TOKEN: "token",
        AuthenticationMode.K8S: "k8s",
    }
    return _MODE_NAMES.get(mode, "unknown")
