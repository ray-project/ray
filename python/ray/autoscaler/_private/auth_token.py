import os

from ray._raylet import (
    AuthenticationMode,
    AuthenticationTokenLoader,
    get_authentication_mode,
)
from ray.exceptions import AuthenticationError

AUTH_TOKEN_REMOTE_PATH = "~/.ray/auth_token"


def token_auth_enabled_locally() -> bool:
    """Whether this process runs in token auth mode, so a launcher ships a token.

    Until AUTH_MODE defaults to "token", an unset RAY_AUTH_MODE means disabled:
    then nodes don't turn token mode on, and a shipped token would split a
    cluster into a token-mode head and workers it rejects.
    """
    return get_authentication_mode() == AuthenticationMode.TOKEN


def load_local_token() -> str:
    """Return the local token, by the usual source precedence, without
    creating one.

    Returns:
        The token.

    Raises:
        AuthenticationError: No token source holds a token, or
            RAY_AUTH_TOKEN_PATH names a file that can't be read.
    """
    token_loader = AuthenticationTokenLoader.instance()
    token_loader.reset_cache()
    if not token_loader.has_token(ignore_auth_mode=True):
        raise AuthenticationError("No auth token found on this machine")
    return token_loader.get_raw_token(ignore_auth_mode=True)


def load_or_create_cluster_token() -> str:
    """Return the local token, by the usual source precedence, for a launcher
    that creates a cluster and ships it the token, such as `ray up`.

    Creates ~/.ray/auth_token only when no token source holds a token.

    Returns:
        The token.

    Raises:
        AuthenticationError: RAY_AUTH_TOKEN is set but yields no token, or
            RAY_AUTH_TOKEN_PATH names a file that can't be read or is empty.
        RuntimeError: ~/.ray/auth_token can't be written.
    """
    token_loader = AuthenticationTokenLoader.instance()
    token_loader.reset_cache()
    if not token_loader.has_token(ignore_auth_mode=True) and os.environ.get(
        "RAY_AUTH_TOKEN"
    ):
        raise AuthenticationError("RAY_AUTH_TOKEN is set but yields no token")
    token_loader.create_token_if_missing()
    token_loader.reset_cache()
    return token_loader.get_raw_token(ignore_auth_mode=True)
