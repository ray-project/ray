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
from typing import Optional

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
            token = _generate_and_save_token()

        # Cache the result (even if empty)
        _cached_token = token
        return _cached_token


def has_auth_token() -> bool:
    """Check if an authentication token exists.

    Returns:
        True if a token is available (cached or can be loaded), False otherwise.
    """
    token = load_auth_token(generate_if_not_found=False)
    return bool(token)


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
        try:
            token_path = Path(env_token_path).expanduser()
            if token_path.exists():
                token = token_path.read_text().strip()
                if token:
                    logger.debug(f"Loaded authentication token from file: {token_path}")
                    return token
            else:
                logger.warning(
                    f"RAY_AUTH_TOKEN_PATH is set but file does not exist: {token_path}"
                )
        except Exception as e:
            logger.warning(
                f"Failed to read token from RAY_AUTH_TOKEN_PATH ({env_token_path}): {e}"
            )

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


def _generate_and_save_token() -> str:
    """Generate a new UUID token and save it to the default path.

    Returns:
        The newly generated authentication token.
    """
    # Generate a UUID-based token
    token = uuid.uuid4().hex

    # Try to save the token to the default path
    token_path = _get_default_token_path()
    try:
        # Create directory if it doesn't exist
        token_path.parent.mkdir(parents=True, exist_ok=True)

        # Write token to file
        token_path.write_text(token)

        # Set file permissions to 0600 on Unix systems
        try:
            # This will work on Unix systems, but not on Windows
            os.chmod(token_path, 0o600)
        except (OSError, AttributeError):
            # chmod may not work on Windows or may fail for other reasons
            # This is not critical, so we just log a debug message
            logger.debug(
                f"Could not set file permissions to 0600 for {token_path}. "
                "This is expected on Windows."
            )

        logger.info(f"Generated new authentication token and saved to {token_path}")
    except Exception as e:
        logger.warning(
            f"Failed to save generated token to {token_path}: {e}. "
            "Token will only be available in memory."
        )

    return token


def _get_default_token_path() -> Path:
    """Get the default token file path (~/.ray/auth_token).

    Returns:
        Path object pointing to ~/.ray/auth_token
    """
    return Path.home() / ".ray" / "auth_token"
