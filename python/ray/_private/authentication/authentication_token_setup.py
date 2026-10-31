"""Token authentication setup for Ray: enable token mode and make sure a token
exists. Token loading, caching and creation live in the C++
AuthenticationTokenLoader.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from ray._private.authentication.authentication_constants import (
    TOKEN_AUTH_ERROR_MESSAGE,
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


def maybe_enable_token_auth_if_token_available(warn_if_disabled: bool = True) -> bool:
    """Enable token auth if RAY_AUTH_MODE is unset and a token already exists."""
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        # Mode set explicitly; respect it without warning.
        return auth_mode_env.lower() == "token"

    if not AuthenticationTokenLoader.instance().has_token(ignore_auth_mode=True):
        if warn_if_disabled:
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
    """Make sure a token is available when token authentication is enabled.

    Only callers that start a throwaway cluster on this machine pass
    ``create_token_if_missing=True``: a bare ``ray.init()``, and
    ``ray.cluster_utils.Cluster()`` and ``ray.cluster_utils.AutoscalingCluster``
    unless passed ``enable_token_auth=False``.
    A token made anywhere else can't match the one the rest of the cluster uses.

    Args:
        system_config: Ray raises an error if you set AUTH_MODE in system_config instead of the environment.
        create_token_if_missing: Generate a new token if one doesn't already exist.

    Raises:
        RuntimeError: AUTH_MODE is set in system_config instead of the
            ``RAY_AUTH_MODE`` environment variable.
        AuthenticationError: Token authentication is enabled, no token exists, and
            ``create_token_if_missing`` is False.
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

    if create_token_if_missing:
        token_loader.create_token_if_missing()
    elif not token_loader.has_token(ignore_auth_mode=True):
        raise AuthenticationError(TOKEN_AUTH_ERROR_MESSAGE)
