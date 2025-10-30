import logging
import sys
from typing import Dict, Optional

import pytest
from aiohttp import web

from ray._private.authentication import authentication_constants


def create_token_authentication_middleware() -> web.middleware:
    """Return an aiohttp middleware that validates bearer tokens when enabled."""

    from ray.dashboard import authentication_utils as auth_utils

    @web.middleware
    async def auth_middleware(request: web.Request, handler):
        if not auth_utils.is_token_auth_enabled():
            return await handler(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header:
            return web.Response(
                status=401, text="Unauthorized: Missing authentication token"
            )

        if not auth_utils.validate_request_token(auth_header):
            return web.Response(
                status=403, text="Forbidden: Invalid authentication token"
            )

        return await handler(request)

    return auth_middleware


def apply_token_if_enabled(
    headers: Dict[str, str], logger: Optional[logging.Logger] = None
) -> bool:
    """Inject Authorization header when token auth is enabled.

    Args:
        headers: Mutable mapping of HTTP headers. Updated in place.
        logger: Optional logger used for warning when token is missing.

    Returns:
        bool: True if the token was added to headers, False otherwise.
    """

    if headers is None:
        raise ValueError("headers must be provided")

    if "Authorization" in headers:
        return False

    from ray._raylet import AuthenticationTokenLoader
    from ray.dashboard import authentication_utils as auth_utils

    if not auth_utils.is_token_auth_enabled():
        return False

    token_loader = AuthenticationTokenLoader.instance()
    token_added = token_loader.set_token_for_http_header(headers)

    if not token_added and logger is not None:
        logger.warning(
            "Token authentication is enabled but no token was found. "
            "Requests to authenticated clusters will fail."
        )

    return token_added


def format_authentication_http_error(status: int, body: str) -> Optional[str]:
    """Return a user-friendly authentication error message, if applicable."""

    if status == 401:
        return "Authentication required: {body}\n\n{details}".format(
            body=body,
            details=authentication_constants.HTTP_REQUEST_MISSING_TOKEN_ERROR_MESSAGE,
        )

    if status == 403:
        return "Authentication failed: {body}\n\n{details}".format(
            body=body,
            details=authentication_constants.HTTP_REQUEST_INVALID_TOKEN_ERROR_MESSAGE,
        )

    return None


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
