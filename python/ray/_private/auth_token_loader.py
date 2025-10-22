"""Authentication token loader for Ray.

This module provides functions to load, generate, and cache authentication tokens
for Ray's token-based authentication system. Tokens are loaded with the following
precedence:
1. RAY_AUTH_TOKEN environment variable
2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)
3. Default token path: ~/.ray/auth_token
"""

import logging
import os
import threading
import uuid
from pathlib import Path
from typing import Dict, Optional

from ray._raylet import Config, reset_auth_token_cache

logger = logging.getLogger(__name__)

# Module-level cached variables
_cached_token: Optional[str] = None
_token_lock = threading.Lock()


def load_auth_token(generate_if_not_found: bool = False) -> str:
    """Load the authentication token with caching.

    This function loads the token from available sources with the following precedence:
    1. RAY_AUTH_TOKEN environment variable
    2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)
    3. Default token path: ~/.ray/auth_token

    The token is cached after the first successful load to avoid repeated file I/O.

    Args:
        generate_if_not_found: If True, generate and save a new token if not found.
            If False, return empty string if no token is found.

    Returns:
        The authentication token, or empty string if not found and generation is disabled.
    """
    global _cached_token

    with _token_lock:
        # Return cached token if already loaded
        if _cached_token is not None:
            return _cached_token

        # Try to load from sources
        token = _load_token_from_sources()

        # Generate if requested and not found
        if not token and generate_if_not_found:
            token = _generate_and_save_token_internal()

        # Cache the result (even if empty)
        _cached_token = token
        return _cached_token


def _load_token_from_sources() -> str:
    """Load token from available sources (env vars and file).

    Returns:
        The authentication token, or empty string if not found.
    """
    # Precedence 1: RAY_AUTH_TOKEN environment variable
    env_token = os.environ.get("RAY_AUTH_TOKEN", "").strip()
    if env_token:
        logger.debug(
            "Loaded authentication token from RAY_AUTH_TOKEN environment variable"
        )
        return env_token

    # Precedence 2: RAY_AUTH_TOKEN_PATH environment variable
    env_token_path = os.environ.get("RAY_AUTH_TOKEN_PATH", "").strip()
    if env_token_path:
        token_path = Path(env_token_path).expanduser()
        if not token_path.exists():
            raise FileNotFoundError(f"Token file not found: {token_path}")
        token = token_path.read_text().strip()
        if token:
            logger.debug(f"Loaded authentication token from file: {token_path}")
            return token

    # Precedence 3: Default token path ~/.ray/auth_token
    default_path = _get_default_token_path()
    try:
        if default_path.exists():
            token = default_path.read_text().strip()
            if token:
                logger.debug(
                    f"Loaded authentication token from default path: {default_path}"
                )
                return token
    except Exception as e:
        logger.debug(f"Failed to read token from default path ({default_path}): {e}")

    # No token found
    logger.debug("No authentication token found in any source")
    return ""


def generate_and_save_token() -> str:
    """Generate a new random token and save it in the default token path.

    Returns:
        The newly generated authentication token.
    """
    global _cached_token

    with _token_lock:
        # Check if we already have a cached token
        if _cached_token is not None:
            logger.warning(
                "Returning cached authentication token instead of generating new one. "
                "Call load_auth_token() to use existing token or clear cache first."
            )
            return _cached_token

        # Generate and save token without nested lock
        return _generate_and_save_token_internal()


def _generate_and_save_token_internal() -> str:
    """Internal function to generate and save token. Assumes lock is already held."""
    global _cached_token

    # Generate a UUID-based token
    token = uuid.uuid4().hex

    # Try to save the token to the default path
    token_path = _get_default_token_path()
    try:
        # Create directory if it doesn't exist
        token_path.parent.mkdir(parents=True, exist_ok=True)

        # Write token to file
        token_path.write_text(token)

        # Ensure file is flushed to disk immediately
        # This is critical for subprocess/C++ code to read it immediately
        import os

        fd = os.open(str(token_path), os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)

        logger.info(f"Generated new authentication token and saved to {token_path}")
    except Exception as e:
        logger.warning(
            f"Failed to save generated token to {token_path}: {e}. "
            "Token will only be available in memory."
        )

    # Cache the generated token
    _cached_token = token
    return token


def _get_default_token_path() -> Path:
    """Get the default token file path (~/.ray/auth_token).

    Returns:
        Path object pointing to ~/.ray/auth_token
    """
    return Path.home() / ".ray" / "auth_token"


def setup_and_verify_auth(
    system_config: Optional[Dict] = None, is_new_cluster: bool = True
) -> None:
    """Verify auth configuration and ensure token is available when auth is enabled.

    This is called early during ray.init() to:
    1. Check for _system_config misuse and provide helpful error
    2. Verify token is available if auth is enabled
    3. Generate default token for new local clusters if needed

    Args:
        system_config: The _system_config dict from ray.init() (checked for misuse)
        is_new_cluster: True if starting new local cluster, False if connecting to an existing cluster

    Raises:
        ValueError: If _system_config is used for enabling auth (should use env var instead)
    """
    # Check for _system_config misuse
    if system_config and system_config.get("enable_token_auth", False):
        raise ValueError(
            "Authentication mode should be configured via environment variable, "
            "not _system_config (which is for testing only).\n"
            "Please set: RAY_enable_token_auth=1\n"
            "Or in Python: os.environ['RAY_enable_token_auth'] = '1'"
        )

    Config.initialize("")

    if Config.enable_token_auth():
        # For new clusters: generate token if not found
        # For existing clusters: only use existing token (don't generate)
        token = load_auth_token(generate_if_not_found=is_new_cluster)

        if not is_new_cluster and not token:
            raise RuntimeError(
                "Token authentication is enabled on the cluster you're connecting to, "
                "but no authentication token was found. Please provide a token using one of:\n"
                "  1. RAY_AUTH_TOKEN environment variable\n"
                "  2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)\n"
                "  3. Default token file: ~/.ray/auth_token"
            )


def _reset_token_cache_for_testing():
    """Reset both Python and C++ token caches.

    Should only be used for testing purposes.
    """
    global _cached_token
    _cached_token = None

    # Also reset the C++ token cache
    reset_auth_token_cache()
