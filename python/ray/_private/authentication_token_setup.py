"""Authentication token setup for Ray.

This module provides functions to generate and save authentication tokens
for Ray's token-based authentication system. Token loading and caching is
handled by the C++ AuthenticationTokenLoader.
"""

import logging
import os
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_and_save_token() -> None:
    """Generate a new random token and save it in the default token path.

    Returns:
        The newly generated authentication token.
    """
    # Generate a UUID-based token
    token = uuid.uuid4().hex

    token_path = _get_default_token_path()
    try:
        # Create directory if it doesn't exist
        token_path.parent.mkdir(parents=True, exist_ok=True)

        # Write token to file with explicit flush
        with open(token_path, "w") as f:
            f.write(token)
            f.flush()
            os.fsync(f.fileno())

        logger.info(f"Generated new authentication token and saved to {token_path}")
    except Exception as e:
        logger.warning(f"Failed to save generated token to {token_path}: {e}. ")
        raise


def _get_default_token_path() -> Path:
    """Get the default token file path (~/.ray/auth_token).

    Returns:
        Path object pointing to ~/.ray/auth_token
    """
    return Path.home() / ".ray" / "auth_token"


def setup_and_verify_auth(system_config=None, is_new_cluster: bool = True) -> None:
    """Check authentication settings and setup necessary resources.

    for token based authentication, Ray calls this early during ray.init() to do the following:
    1. Check if you enabled token-based authentication.
    2. Make sure a token is available if authentication is enabled.
    3. Generate and save a default token for new local clusters if one doesn't exist.

    Args:
        system_config: Raises an error if you set auth_mode in system_config instead of the environment.
        is_new_cluster: Set to True if starting a new local cluster, or False if connecting
            to an existing cluster.

    Raises:
        RuntimeError: If authentication is enabled but no token is found when connecting
            to an existing cluster.
    """
    from ray._raylet import (
        AuthenticationMode,
        AuthenticationTokenLoader,
        get_authentication_mode,
    )

    # Check if token authentication is enabled
    if get_authentication_mode() != AuthenticationMode.TOKEN:
        if system_config and system_config.get("auth_mode") != "disabled":
            raise RuntimeError(
                "Set authentication mode with the environment, not system_config."
            )
        return

    token_loader = AuthenticationTokenLoader.instance()

    if not token_loader.has_token():
        if is_new_cluster:
            # Generate token for new local cluster
            generate_and_save_token()

            # Reload cache so that subsequent calls to token_loader read the new token
            token_loader.reset_cache()
        else:
            # You're connecting to an existing cluster—token must already exist
            raise RuntimeError(
                "Token authentication is enabled but no authentication token was found. Please provide a token using one of:\n"
                "  1. RAY_AUTH_TOKEN environment variable\n"
                "  2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)\n"
                "  3. Default token file: ~/.ray/auth_token"
            )
