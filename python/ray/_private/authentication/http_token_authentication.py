import logging
import socket
import threading
from http.cookies import CookieError, SimpleCookie
from socketserver import ThreadingMixIn
from types import ModuleType
from typing import TYPE_CHECKING, Dict, List, Optional
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from ray._private.authentication import (
    authentication_constants,
    authentication_utils as auth_utils,
)

if TYPE_CHECKING:
    from multidict import CIMultiDictProxy

logger = logging.getLogger(__name__)


def _carries_auth_header(headers: "CIMultiDictProxy[str]") -> bool:
    """Whether a request carries a non-empty Authorization header, of any scheme, or
    X-Ray-Authorization header. The token cookie doesn't count: a browser can keep it
    from an earlier token-mode cluster at the same address."""
    return any(
        headers.getall(authentication_constants.AUTHORIZATION_HEADER_NAME, [])
    ) or bool(headers.get(authentication_constants.RAY_AUTHORIZATION_HEADER_NAME))


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

        With auth disabled, or without ray._raylet (doc builds), it authenticates
        nothing and only rejects requests that carry an auth header, so token-mode
        clients opt out too.
        """
        if not auth_utils.is_token_auth_enabled():
            if _carries_auth_header(request.headers):
                logger.debug(
                    f"Rejecting {request.method} {request.path} with an auth header "
                    "because auth is disabled"
                )
                return aiohttp_module.web.Response(
                    status=401,
                    text=authentication_constants.TOKEN_AUTH_ERROR_MESSAGE,
                )
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


def get_token_auth_wsgi_app(app):
    """Wrap a metrics WSGI app so it requires the token when token auth is on,
    unless RAY_AUTH_SKIP_METRICS is set.

    Accepts the same token sources as the dashboard. A missing token gets 401
    with ``WWW-Authenticate: Bearer``, a wrong one 403.
    """

    def token_auth_app(environ, start_response):
        if (
            not auth_utils.is_token_auth_enabled()
            or auth_utils.is_metrics_auth_skipped()
        ):
            return app(environ, start_response)

        auth_header = environ.get("HTTP_AUTHORIZATION") or environ.get(
            "HTTP_X_RAY_AUTHORIZATION"
        )
        if not auth_header:
            try:
                cookie = SimpleCookie(environ.get("HTTP_COOKIE", "")).get(
                    authentication_constants.AUTHENTICATION_TOKEN_COOKIE_NAME
                )
            except CookieError:
                cookie = None
            if cookie and cookie.value:
                auth_header = (
                    authentication_constants.AUTHORIZATION_BEARER_PREFIX + cookie.value
                )

        if not auth_header:
            start_response(
                "401 Unauthorized",
                [("Content-Type", "text/plain"), ("WWW-Authenticate", "Bearer")],
            )
            return [b"Unauthorized: Missing authentication token"]

        if not auth_utils.validate_request_token(auth_header):
            start_response("403 Forbidden", [("Content-Type", "text/plain")])
            return [b"Forbidden: Invalid authentication token"]

        return app(environ, start_response)

    return token_auth_app


# prometheus_client.exposition.ThreadingWSGIServer doesn't exist in the
# declared minimum prometheus_client 0.7.1.
class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class _ReusePortThreadingWSGIServer(_ThreadingWSGIServer):
    """Sets SO_REUSEPORT so several processes on one host can share the port.
    Set by hand: socketserver's allow_reuse_port only exists from Python 3.11."""

    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        super().server_bind()


class _SilentHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        pass


def start_metrics_http_server(port: int, registry, addr: str = "0.0.0.0") -> None:
    """Serve ``registry`` on ``addr:port`` from a daemon thread, like
    ``prometheus_client.start_http_server``, but requiring the token when token
    auth is on."""
    from prometheus_client import make_wsgi_app

    start_metrics_wsgi_server(port, make_wsgi_app(registry), addr)


def start_metrics_wsgi_server(
    port: int, app, addr: str = "0.0.0.0", reuse_port: bool = False
) -> WSGIServer:
    """Serve the WSGI metrics ``app`` on ``addr:port`` from a daemon thread,
    behind the same token check as start_metrics_http_server. With
    ``reuse_port``, other processes that set SO_REUSEPORT can bind the same
    port. Returns the server so the caller can shut it down."""
    family, _, _, _, sockaddr = socket.getaddrinfo(
        addr, port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE
    )[0]
    base_class = _ReusePortThreadingWSGIServer if reuse_port else _ThreadingWSGIServer
    server_class = type("_MetricsWSGIServer", (base_class,), {"address_family": family})
    httpd = make_server(
        sockaddr[0],
        port,
        get_token_auth_wsgi_app(app),
        server_class,
        _SilentHandler,
    )
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


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
        from ray.exceptions import AuthenticationError

        raise AuthenticationError(authentication_constants.TOKEN_AUTH_ERROR_MESSAGE)

    return auth_headers


def format_authentication_http_error(status: int, body: str) -> Optional[str]:
    """Return a user-friendly authentication error message, if applicable."""

    # A 401 means a missing token or a token sent to a cluster with auth disabled,
    # which the one message covers, so the body would only repeat it.
    if status == 401:
        return authentication_constants.TOKEN_AUTH_ERROR_MESSAGE

    if status == 403:
        return "Authentication failed: {body}\n\n{details}".format(
            body=body,
            details=authentication_constants.TOKEN_INVALID_ERROR_MESSAGE,
        )

    return None
