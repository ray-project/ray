"""Token authentication setup for Ray: make sure a token exists. Token loading,
caching and creation live in the C++ AuthenticationTokenLoader.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from ray._private.authentication.authentication_constants import (
    TOKEN_AUTH_ERROR_MESSAGE,
)
from ray._raylet import (
    AuthenticationMode,
    AuthenticationTokenLoader,
    get_authentication_mode,
)
from ray.exceptions import AuthenticationError

AUTH_MODE_ENV_VAR = "RAY_AUTH_MODE"


def _get_default_token_path() -> Path:
    """Get the default token file path (~/.ray/auth_token).

    Returns:
        Path object pointing to ~/.ray/auth_token
    """
    return Path.home() / ".ray" / "auth_token"


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
