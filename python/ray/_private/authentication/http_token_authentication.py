import logging
from types import ModuleType
from typing import Dict, Optional

from ray._private.authentication import authentication_constants
from ray.dashboard import authentication_utils as auth_utils

logger = logging.getLogger(__name__)


def get_token_auth_middleware(aiohttp_module: ModuleType):
    """Internal helper to create token auth middleware with provided modules.

    Args:
        aiohttp_module: The aiohttp module to use
    Returns:
        An aiohttp middleware function
    """

    @aiohttp_module.web.middleware
    async def token_auth_middleware(request, handler):
        """Middleware to validate bearer tokens when token authentication is enabled.

        In minimal Ray installations (without ray._raylet), this middleware is a no-op
        and passes all requests through without authentication.
        """
        # No-op if token auth is not enabled or raylet is not available
        if not auth_utils.is_token_auth_enabled():
            return await handler(request)

        auth_header = request.headers.get(
            authentication_constants.AUTHORIZATION_HEADER_NAME, ""
        )
        if not auth_header:
            return aiohttp_module.web.Response(
                status=401, text="Unauthorized: Missing authentication token"
            )

        if not auth_utils.validate_request_token(auth_header):
            return aiohttp_module.web.Response(
                status=403, text="Forbidden: Invalid authentication token"
            )

        return await handler(request)

    return token_auth_middleware


def get_auth_headers_if_auth_enabled(user_headers: Dict[str, str]) -> Dict[str, str]:

    if not auth_utils.is_token_auth_enabled():
        return {}

    from ray._raylet import AuthenticationTokenLoader

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
