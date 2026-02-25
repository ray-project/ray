"""Authentication token setup for Ray.

This module provides functions to generate and save authentication tokens
for Ray's token-based authentication system. Token loading and caching is
handled by the C++ AuthenticationTokenLoader.
"""

import logging
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
    get_authentication_mode,
)
from ray.exceptions import AuthenticationError

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
