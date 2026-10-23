from ray._common.utils import env_bool

try:
    from ray._raylet import (
        AuthenticationMode,
        get_authentication_mode,
        is_k8s_token_auth_enabled as _is_k8s_token_auth_enabled,
        validate_authentication_token,
    )

    _RAYLET_AVAILABLE = True
except ImportError:
    # ray._raylet not available during doc builds
    _RAYLET_AVAILABLE = False


def is_token_auth_enabled() -> bool:
    """Check if token authentication is enabled.

    Returns:
        bool: True if AUTH_MODE is set to "token".
    """
    if not _RAYLET_AVAILABLE:
        return False

    return get_authentication_mode() == AuthenticationMode.TOKEN


def is_k8s_token_auth_enabled() -> bool:
    """Check if Kubernetes token authentication is enabled."""
    return _RAYLET_AVAILABLE and _is_k8s_token_auth_enabled()


def is_metrics_auth_skipped() -> bool:
    """Whether RAY_AUTH_SKIP_METRICS opens Ray's metrics endpoints while token
    auth protects everything else."""
    return env_bool("RAY_AUTH_SKIP_METRICS", False)


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
        String name: "disabled", "token"
    """
    from ray._raylet import AuthenticationMode

    _MODE_NAMES = {
        AuthenticationMode.DISABLED: "disabled",
        AuthenticationMode.TOKEN: "token",
    }
    return _MODE_NAMES.get(mode, "unknown")
