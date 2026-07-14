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


def _token_source_available() -> bool:
    """Return whether an authentication token is available from any source.

    Checks all supported token sources in precedence order via the C++ token
    loader: the ``RAY_AUTH_TOKEN`` environment variable, the file referenced by
    ``RAY_AUTH_TOKEN_PATH``, and the default token path (``~/.ray/auth_token``).
    """
    token_loader = AuthenticationTokenLoader.instance()
    try:
        return token_loader.has_token(ignore_auth_mode=True)
    except AuthenticationError:
        # A token source is configured but couldn't be loaded (e.g. an empty
        # file at RAY_AUTH_TOKEN_PATH). Treat the source as present so token auth
        # is enabled and ensure_token_if_auth_enabled surfaces a clear error.
        return True


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


def maybe_enable_token_auth_for_local_head_cluster() -> bool:
    """Enable token authentication for a local head cluster if a token is available.

    For a local cluster started with ``ray start --head``, enable token
    authentication automatically when an authentication token is available from
    any source (the ``RAY_AUTH_TOKEN`` environment variable, the file referenced
    by ``RAY_AUTH_TOKEN_PATH``, or the default ``~/.ray/auth_token`` file), even
    if the ``RAY_AUTH_MODE`` environment variable isn't set. This lets users who
    generated a token with ``ray get-auth-token --generate`` (or who configured
    one of the token environment variables) start an authenticated cluster
    without also having to set ``RAY_AUTH_MODE=token``.

    This only applies to local head clusters. Don't call it for worker nodes
    (``ray start --address=...``) or when connecting to an existing cluster, so
    remote cluster behavior is unchanged.

    When ``RAY_AUTH_MODE`` is already set, the explicit choice is respected. In
    particular ``RAY_AUTH_MODE=token`` with no token available still raises later
    in ``ensure_token_if_auth_enabled``. When token authentication ends up
    disabled, this logs a warning so the user knows the cluster is unauthenticated.

    Returns:
        True if token authentication is enabled after this call, False otherwise.
    """
    auth_mode_env = os.environ.get(AUTH_MODE_ENV_VAR)
    if auth_mode_env is not None:
        # Respect an explicit RAY_AUTH_MODE setting (e.g. "token" or "disabled").
        enabled = auth_mode_env.lower() == "token"
        if not enabled:
            _warn_token_auth_disabled()
        return enabled

    if not _token_source_available():
        # No token available from any source: keep authentication disabled, which
        # preserves the previous behavior when RAY_AUTH_MODE isn't set.
        _warn_token_auth_disabled()
        return False

    # A token is available but RAY_AUTH_MODE wasn't set. Enable token
    # authentication so the head node and all child processes it spawns enforce
    # authentication. Child processes inherit this environment variable.
    os.environ[AUTH_MODE_ENV_VAR] = "token"

    # RayConfig caches env vars the first time they're read. Re-read them so this
    # process (which connects to the cluster it's starting) also observes token
    # mode.
    Config.initialize("")

    logger.info(
        "Found an existing authentication token; enabling token authentication "
        f"for this cluster. Set {AUTH_MODE_ENV_VAR}=disabled to opt out."
    )
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
