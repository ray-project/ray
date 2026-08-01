import logging
from types import ModuleType
from typing import Dict, List, Optional

from ray._private.authentication import (
    authentication_constants,
    authentication_utils as auth_utils,
)

logger = logging.getLogger(__name__)


def get_token_auth_middleware(
    aiohttp_module: ModuleType,
    whitelisted_exact_paths: Optional[List[str]] = None,
    whitelisted_path_prefixes: Optional[List[str]] = None,
):
    """Internal helper to create token auth middleware with provided modules.

    Args:
        aiohttp_module: The aiohttp module to use
        whitelisted_exact_paths: List of exact paths that don't require authentication
        whitelisted_path_prefixes: List of path prefixes that don't require authentication
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

        # skip authentication for whitelisted paths
        if (whitelisted_exact_paths and request.path in whitelisted_exact_paths) or (
            whitelisted_path_prefixes
            and request.path.startswith(tuple(whitelisted_path_prefixes))
        ):
            return await handler(request)

        # Try to get authentication token from multiple sources (in priority order):
        # 1. Standard "Authorization" header (for API clients, SDKs)
        # 2. Fallback "X-Ray-Authorization" header (for proxies and KubeRay)
        # 3. Cookie (for web dashboard sessions)

        auth_header = request.headers.get(
            authentication_constants.AUTHORIZATION_HEADER_NAME, ""
        )

        if not auth_header:
            auth_header = request.headers.get(
                authentication_constants.RAY_AUTHORIZATION_HEADER_NAME, ""
            )

        if not auth_header:
            token = request.cookies.get(
                authentication_constants.AUTHENTICATION_TOKEN_COOKIE_NAME
            )
            if token:
                # Format as Bearer token for validation
                auth_header = (
                    authentication_constants.AUTHORIZATION_BEARER_PREFIX + token
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
            details=authentication_constants.TOKEN_AUTH_ENABLED_BUT_NO_TOKEN_FOUND_ERROR_MESSAGE,
        )

    if status == 403:
        return "Authentication failed: {body}\n\n{details}".format(
            body=body,
            details=authentication_constants.TOKEN_INVALID_ERROR_MESSAGE,
        )

    return None
