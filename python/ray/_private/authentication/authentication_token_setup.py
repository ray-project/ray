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
    """Enable token authentication for this process and its child processes."""
    os.environ[AUTH_MODE_ENV_VAR] = "token"
    # Refresh the cached RayConfig so this process observes token mode too
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


def _warn_token_auth_enabled() -> None:
    """Warn that the local cluster enabled token auth by default (mode unset)."""
    logger.warning(
        "Token authentication is enabled for this Ray cluster. Set "
        f"{AUTH_MODE_ENV_VAR}=disabled to opt out. For more information, see "
        "https://docs.ray.io/en/latest/ray-security/token-auth.html"
    )


def maybe_enable_token_auth_if_token_available() -> bool:
    """Enable token auth for ``ray start --head`` if a token already exists."""
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        # Mode set explicitly; respect it without warning.
        return auth_mode_env.lower() == "token"

    if not AuthenticationTokenLoader.instance().has_token(ignore_auth_mode=True):
        _warn_token_auth_disabled()
        return False

    _enable_token_auth()
    _warn_token_auth_enabled()
    return True


def enable_token_auth_by_default() -> bool:
    """Enable token auth by default for a new local ``ray.init()`` cluster."""
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        # Mode set explicitly; respect it without warning.
        return auth_mode_env.lower() == "token"

    _enable_token_auth()
    _warn_token_auth_enabled()
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
