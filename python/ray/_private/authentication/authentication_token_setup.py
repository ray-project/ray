"""Authentication token setup for Ray.

This module provides functions to generate and save authentication tokens
for Ray's token-based authentication system. Token loading and caching is
handled by the C++ AuthenticationTokenLoader.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from ray._private.authentication.authentication_constants import (
    TOKEN_AUTH_ENABLED_BUT_NO_TOKEN_FOUND_ERROR_MESSAGE,
)
from ray._private.authentication.authentication_token_generator import (
    generate_new_authentication_token,
)
from ray._raylet import (
    AuthenticationMode,
    AuthenticationTokenLoader,
    Config,
    get_authentication_mode,
)
from ray.exceptions import AuthenticationError

AUTH_MODE_ENV_VAR = "RAY_AUTH_MODE"

logger = logging.getLogger(__name__)


def generate_and_save_token() -> None:
    """Generate a new random token and save it in the default token path.

    Returns:
        The newly generated authentication token.
    """
    # Generate a UUID-based token
    token = generate_new_authentication_token()

    token_path = _get_default_token_path()
    try:
        # Create directory if it doesn't exist
        token_path.parent.mkdir(parents=True, exist_ok=True)

        # Write token to file with explicit flush and fsync
        with open(token_path, "w") as f:
            f.write(token)

        logger.info(f"Generated new authentication token and saved to {token_path}")
    except Exception:
        raise


def _get_default_token_path() -> Path:
    """Get the default token file path (~/.ray/auth_token).

    Returns:
        Path object pointing to ~/.ray/auth_token
    """
    return Path.home() / ".ray" / "auth_token"


def _enable_token_auth() -> None:
    """Enable token authentication for this process and its child processes.

    Sets ``RAY_AUTH_MODE=token``, which the child processes (GCS, raylet,
    dashboard) inherit via ``os.environ``, and refreshes the cached ``RayConfig``
    so this process observes token mode too (``RayConfig`` caches env vars on
    first read).
    """
    os.environ[AUTH_MODE_ENV_VAR] = "token"
    Config.initialize("")


def _warn_token_auth_disabled() -> None:
    """Warn that the local cluster is starting without token authentication."""
    logger.warning(
        "Token authentication is disabled for this Ray cluster. Anyone with "
        "network access to the cluster can run arbitrary code and access its "
        "data. To enable token authentication, generate a token with "
        f"`ray get-auth-token --generate` or set {AUTH_MODE_ENV_VAR}=token. For "
        "more information, see "
        "https://docs.ray.io/en/latest/ray-security/token-auth.html"
    )


def maybe_enable_token_auth_if_token_available() -> bool:
    """Enable token auth (for ``ray start --head``) if a token already exists.

    When ``RAY_AUTH_MODE`` isn't set, enable token auth if a token is available
    from any source (``RAY_AUTH_TOKEN``, ``RAY_AUTH_TOKEN_PATH``, or
    ``~/.ray/auth_token``); otherwise warn that the cluster is unauthenticated.
    Never generates a token. An explicit ``RAY_AUTH_MODE`` is respected.

    Returns:
        True if token authentication is enabled after this call, False otherwise.

    Raises:
        AuthenticationError: If a token source is configured but can't be read
            (e.g. an empty ``RAY_AUTH_TOKEN_PATH`` file), so startup fails closed
            instead of running unauthenticated.
    """
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        enabled = auth_mode_env.lower() == "token"
        if not enabled:
            _warn_token_auth_disabled()
        return enabled

    # Let a configured-but-unreadable source (AuthenticationError) propagate so
    # startup fails closed rather than running unauthenticated.
    if not AuthenticationTokenLoader.instance().has_token(ignore_auth_mode=True):
        _warn_token_auth_disabled()
        return False

    _enable_token_auth()
    logger.info(
        "Found an existing authentication token; enabling token authentication "
        f"for this cluster. Set {AUTH_MODE_ENV_VAR}=disabled to opt out."
    )
    return True


def enable_token_auth_by_default() -> bool:
    """Enable token auth by default for a new local ``ray.init()`` cluster.

    When ``RAY_AUTH_MODE`` isn't set, enable token auth so the caller's
    ``ensure_token_if_auth_enabled(..., create_token_if_missing=True)`` generates
    a token if none exists or reuses an existing one. An explicit
    ``RAY_AUTH_MODE`` is respected. Never warns. Only call this for a new local
    cluster, not when connecting to an existing one (``ray.init(address=...)``).

    Returns:
        True if token authentication is enabled after this call, False otherwise.
    """
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        return auth_mode_env.lower() == "token"

    _enable_token_auth()
    return True


def ensure_token_if_auth_enabled(
    system_config: Optional[Dict[str, Any]] = None, create_token_if_missing: bool = True
) -> None:
    """Check authentication settings and set up token resources if authentication is enabled.

    Ray calls this early during ray.init() to do the following for token-based authentication:
    1. Check whether you enabled token-based authentication.
    2. Make sure a token is available if authentication is enabled.
    3. Generate and save a default token for new local clusters if one doesn't already exist.

    Args:
        system_config: Ray raises an error if you set AUTH_MODE in system_config instead of the environment.
        create_token_if_missing: Generate a new token if one doesn't already exist.

    Raises:
        RuntimeError: Ray raises this error if authentication is enabled but no token is found when connecting
            to an existing cluster.
    """

    # Check if you enabled token authentication.
    if get_authentication_mode() != AuthenticationMode.TOKEN:
        if (
            system_config
            and "AUTH_MODE" in system_config
            and system_config["AUTH_MODE"] != "disabled"
        ):
            raise RuntimeError(
                "Set authentication mode can only be set with the `RAY_AUTH_MODE` environment variable, not using the system_config."
            )
        return

    token_loader = AuthenticationTokenLoader.instance()

    if not token_loader.has_token(ignore_auth_mode=True):
        if create_token_if_missing:
            # Generate a new token.
            generate_and_save_token()

            # Reload the cache so subsequent calls to token_loader read the new token.
            token_loader.reset_cache()
        else:
            raise AuthenticationError(
                TOKEN_AUTH_ENABLED_BUT_NO_TOKEN_FOUND_ERROR_MESSAGE
            )
