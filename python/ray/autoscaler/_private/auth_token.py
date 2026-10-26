import os

from ray._private.authentication.authentication_token_setup import (
    AUTH_MODE_ENV_VAR,
)
from ray._raylet import AuthenticationTokenLoader
from ray.exceptions import AuthenticationError

AUTH_TOKEN_REMOTE_PATH = "~/.ray/auth_token"


def token_auth_disabled_locally() -> bool:
    """Whether RAY_AUTH_MODE turns token auth off in this process.

    Matches Ray's own parsing: any set value other than "token" means disabled.
    """
    auth_mode = os.environ.get(AUTH_MODE_ENV_VAR)
    return auth_mode is not None and auth_mode.lower() != "token"


def load_or_create_cluster_token() -> str:
    """Return the local token, by the usual source precedence, for a launcher
    that creates a cluster and ships it the token, such as `ray up`.

    Creates ~/.ray/auth_token only when no token source holds a token.

    Returns:
        The token.

    Raises:
        AuthenticationError: RAY_AUTH_TOKEN is set but empty, or
            RAY_AUTH_TOKEN_PATH names a file that can't be read or is empty.
    """
    token_loader = AuthenticationTokenLoader.instance()
    token_loader.reset_cache()
    if not token_loader.has_token(ignore_auth_mode=True) and os.environ.get(
        "RAY_AUTH_TOKEN"
    ):
        raise AuthenticationError("RAY_AUTH_TOKEN is set but empty")
    token_loader.create_token_if_missing()
    token_loader.reset_cache()
    return token_loader.get_raw_token(ignore_auth_mode=True)
