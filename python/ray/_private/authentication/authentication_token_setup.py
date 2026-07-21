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
    """Enable token authentication if a token is already available.

    For a local cluster started with ``ray start --head``, enable token
    authentication automatically when an authentication token is available from
    any source (the ``RAY_AUTH_TOKEN`` environment variable, the file referenced
    by ``RAY_AUTH_TOKEN_PATH``, or the default ``~/.ray/auth_token`` file), even
    if the ``RAY_AUTH_MODE`` environment variable isn't set. This lets users who
    generated a token with ``ray get-auth-token --generate`` (or who configured
    one of the token environment variables) start an authenticated cluster
    without also having to set ``RAY_AUTH_MODE=token``.

    Only enable when a token already exists; don't generate one. Don't call this
    for worker nodes (``ray start --address=...``) or when connecting to an
    existing cluster, so remote cluster behavior is unchanged.

    When ``RAY_AUTH_MODE`` is already set, the explicit choice is respected. When
    token authentication ends up disabled, this logs a warning so the user knows
    the cluster is unauthenticated.

    Returns:
        True if token authentication is enabled after this call, False otherwise.

    Raises:
        AuthenticationError: If a token source is configured but can't be read
            (for example, an empty file at ``RAY_AUTH_TOKEN_PATH``). This fails
            closed rather than silently starting an unauthenticated cluster.
    """
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        # Respect an explicit RAY_AUTH_MODE setting (e.g. "token" or "disabled").
        enabled = auth_mode_env.lower() == "token"
        if not enabled:
            _warn_token_auth_disabled()
        return enabled

    # Enable token auth only if a token is already available from some source. A
    # configured-but-unreadable source raises AuthenticationError here, which
    # propagates so startup fails closed instead of running unauthenticated.
    if not AuthenticationTokenLoader.instance().has_token(ignore_auth_mode=True):
        # No token available and RAY_AUTH_MODE wasn't set: keep authentication
        # disabled, preserving the previous behavior.
        _warn_token_auth_disabled()
        return False

    _enable_token_auth()
    logger.info(
        "Found an existing authentication token; enabling token authentication "
        f"for this cluster. Set {AUTH_MODE_ENV_VAR}=disabled to opt out."
    )
    return True


def enable_token_auth_by_default() -> bool:
    """Enable token authentication by default for a new local cluster.

    For a new local cluster started with ``ray.init()`` (that is, ``ray.init``
    called without an ``address``, so it bootstraps a fresh cluster), enable
    token authentication by default when the ``RAY_AUTH_MODE`` environment
    variable isn't set. The caller then runs
    ``ensure_token_if_auth_enabled(..., create_token_if_missing=True)``, which
    generates a token if none exists yet and reuses an existing one otherwise.

    This only applies to a new local cluster. Don't call it when connecting to an
    existing cluster (``ray.init(address=...)``), so that behavior is unchanged.

    Unlike ``maybe_enable_token_auth_if_token_available``, this enables token
    authentication even when no token exists yet (a token is generated), and it
    never warns: a new local ``ray.init()`` cluster is authenticated by default
    unless the user explicitly sets ``RAY_AUTH_MODE=disabled``.

    Returns:
        True if token authentication is enabled after this call, False otherwise.
    """
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        # Respect an explicit RAY_AUTH_MODE setting (e.g. "token" or "disabled").
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
