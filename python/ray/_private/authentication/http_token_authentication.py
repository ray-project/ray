import sys
from typing import Dict, Optional

import pytest
from aiohttp import web

from ray._private.authentication import authentication_constants
from ray._raylet import AuthenticationTokenLoader
from ray.dashboard import authentication_utils as auth_utils


@web.middleware
async def token_auth_middleware(request: web.Request, handler):
    """Middleware to validate bearer tokens when token authentication is enabled."""
    if not auth_utils.is_token_auth_enabled():
        return await handler(request)

    auth_header = request.headers.get(
        authentication_constants.AUTHORIZATION_HEADER_NAME, ""
    )
    if not auth_header:
        return web.Response(
            status=401, text="Unauthorized: Missing authentication token"
        )

    if not auth_utils.validate_request_token(auth_header):
        return web.Response(status=403, text="Forbidden: Invalid authentication token")

    return await handler(request)


def get_auth_headers_if_auth_enabled(user_headers: Dict[str, str]) -> Dict[str, str]:

    if not auth_utils.is_token_auth_enabled():
        return {}

    # Check if user provided their own Authorization header (case-insensitive)
    has_user_auth = any(
        key.lower() == authentication_constants.AUTHORIZATION_HEADER_NAME
        for key in user_headers.keys()
    )
    if has_user_auth:
        # User has provided their own auth header, don't override
        return {}

    token_loader = AuthenticationTokenLoader.instance()
    auth_headers = token_loader.get_token_for_http_header()

    if not auth_headers:
        # Token auth enabled but no token found
        logger.warning(
            "Token authentication is enabled but no token was found. "
            "Requests to authenticated clusters will fail."
        )

    return auth_headers


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
