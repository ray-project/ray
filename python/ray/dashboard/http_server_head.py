import asyncio
import errno
import ipaddress
import logging
import os
import pathlib
import posixpath
import sys
import time
from math import floor
from typing import List, Set

from packaging.version import Version

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.timezone_utils as timezone_utils
import ray.dashboard.utils as dashboard_utils
from ray import ray_constants
from ray._common.network_utils import build_address, parse_address
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray._common.utils import get_or_create_event_loop
from ray._private.authentication import (
    authentication_constants,
    authentication_utils as auth_utils,
)
from ray._private.authentication.http_token_authentication import (
    get_token_auth_middleware,
)
from ray._raylet import get_authentication_mode
from ray.dashboard.dashboard_metrics import DashboardPrometheusMetrics
from ray.dashboard.head import DashboardHeadModule

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp, hdrs
from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
from ray.dashboard.subprocesses.routes import SubprocessRouteTable

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

# Env var that enables follow_symlinks for serving UI static files.
# This is an advanced setting that should only be used with special Ray installations
# where the dashboard build files are symlinked to a different directory.
# This is not recommended for most users and can pose a security risk.
# Please reference the aiohttp docs here:
# https://docs.aiohttp.org/en/stable/web_reference.html#aiohttp.web.UrlDispatcher.add_static
ENV_VAR_FOLLOW_SYMLINKS = "RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS"
FOLLOW_SYMLINKS_ENABLED = os.environ.get(ENV_VAR_FOLLOW_SYMLINKS) == "1"
if FOLLOW_SYMLINKS_ENABLED:
    logger.warning(
        "Enabling RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS is not recommended as it "
        "allows symlinks to directories outside the dashboard build folder. "
        "You may accidentally expose files on your system outside of the "
        "build directory."
    )


def setup_static_dir():
    build_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "client", "build"
    )
    module_name = os.path.basename(os.path.dirname(__file__))
    if not os.path.isdir(build_dir):
        raise dashboard_utils.FrontendNotFoundError(
            errno.ENOENT,
            "Dashboard build directory not found. If installing "
            "from source, please follow the additional steps "
            "required to build the dashboard"
            f"(cd python/ray/{module_name}/client "
            "&& npm ci "
            "&& npm run build)",
            build_dir,
        )

    static_dir = os.path.join(build_dir, "static")
    routes.static("/static", static_dir, follow_symlinks=FOLLOW_SYMLINKS_ENABLED)
    return build_dir


class HttpServerDashboardHead:
    def __init__(
        self,
        ip: str,
        http_host: str,
        http_port: int,
        http_port_retries: int,
        gcs_address: str,
        session_name: str,
        metrics: DashboardPrometheusMetrics,
    ):
        self.ip = ip
        self.http_host = http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries
        self.head_node_ip = parse_address(gcs_address)[0]
        self.metrics = metrics
        self._session_name = session_name

        # Below attirubtes are filled after `run` API is invoked.
        self.runner = None

        # Setup Dashboard Routes
        try:
            build_dir = setup_static_dir()
            logger.info("Setup static dir for dashboard: %s", build_dir)
        except dashboard_utils.FrontendNotFoundError as ex:
            # Not to raise FrontendNotFoundError due to NPM incompatibilities
            # with Windows.
            # Please refer to ci.sh::build_dashboard_front_end()
            if sys.platform in ["win32", "cygwin"]:
                logger.warning(ex)
            else:
                raise ex
        dashboard_optional_utils.DashboardHeadRouteTable.bind(self)

        # Create a http session for all modules.
        # aiohttp<4.0.0 uses a 'loop' variable, aiohttp>=4.0.0 doesn't anymore
        if Version(aiohttp.__version__) < Version("4.0.0"):
            self.http_session = aiohttp.ClientSession(loop=get_or_create_event_loop())
        else:
            self.http_session = aiohttp.ClientSession()

    @routes.get("/")
    async def get_index(self, req) -> aiohttp.web.FileResponse:
        try:
            # This API will be no-op after the first report.
            # Note: We always record the usage, but it is not reported
            # if the usage stats is disabled.
            record_extra_usage_tag(TagKey.DASHBOARD_USED, "True")
        except Exception as e:
            logger.warning(
                "Failed to record the dashboard usage. "
                "This error message is harmless and can be ignored. "
                f"Error: {e}"
            )
        resp = aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "client/build/index.html"
            )
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @routes.get("/favicon.ico")
    async def get_favicon(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "client/build/favicon.ico"
            )
        )

    @routes.get("/timezone")
    async def get_timezone(self, req) -> aiohttp.web.Response:
        try:
            current_timezone = timezone_utils.get_current_timezone_info()
            return aiohttp.web.json_response(current_timezone)

        except Exception as e:
            logger.error(f"Error getting timezone: {e}")
            return aiohttp.web.Response(
                status=500, text="Internal Server Error:" + str(e)
            )

    @routes.get("/api/authentication_mode")
    async def get_authentication_mode(self, req) -> aiohttp.web.Response:
        try:
            mode = get_authentication_mode()
            mode_str = auth_utils.get_authentication_mode_name(mode)

            response = aiohttp.web.json_response({"authentication_mode": mode_str})

            # If auth is disabled, clear any existing authentication cookie
            if mode_str == "disabled":
                response.set_cookie(
                    authentication_constants.AUTHENTICATION_TOKEN_COOKIE_NAME,
                    "",
                    max_age=0,
                    path="/",
                )

            return response
        except Exception as e:
            logger.error(f"Error getting authentication mode: {e}")
            return aiohttp.web.Response(
                status=500, text="Internal Server Error: " + str(e)
            )

    @routes.post("/api/authenticate")
    async def authenticate(self, req) -> aiohttp.web.Response:
        """
        Authenticate a user by validating their token and setting a secure HttpOnly cookie.

        This endpoint accepts a token via the Authorization header, validates it,
        and if valid, sets an HttpOnly cookie for subsequent requests from the web dashboard.
        """
        try:
            # Check if token authentication is enabled
            if not auth_utils.is_token_auth_enabled():
                return aiohttp.web.Response(
                    status=401,
                    text="Unauthorized: Token authentication is not enabled",
                )

            # Get token from Authorization header
            auth_header = req.headers.get(
                authentication_constants.AUTHORIZATION_HEADER_NAME, ""
            )

            if not auth_header:
                return aiohttp.web.Response(
                    status=401,
                    text="Unauthorized: Missing authentication token",
                )

            # Validate the token
            if not auth_utils.validate_request_token(auth_header):
                return aiohttp.web.Response(
                    status=403,
                    text="Forbidden: Invalid authentication token",
                )

            # Token is valid - extract the token value (remove "Bearer " prefix if present)
            token = auth_header
            if auth_header.lower().startswith(
                authentication_constants.AUTHORIZATION_BEARER_PREFIX.lower()
            ):
                token = auth_header[
                    len(authentication_constants.AUTHORIZATION_BEARER_PREFIX) :
                ]  # Remove "Bearer " prefix

            # Create successful response
            response = aiohttp.web.json_response(
                {"status": "authenticated", "message": "Token is valid"}
            )

            # Set secure HttpOnly cookie
            # Check if the connection is secure (HTTPS)
            is_secure = req.scheme == "https"

            response.set_cookie(
                authentication_constants.AUTHENTICATION_TOKEN_COOKIE_NAME,
                token,
                max_age=authentication_constants.AUTHENTICATION_TOKEN_COOKIE_MAX_AGE,  # 30 days (matching previous behavior)
                path="/",
                httponly=True,  # Prevents JavaScript access (XSS protection)
                samesite="Strict",  # Prevents CSRF attacks
                secure=is_secure,  # Only send over HTTPS if connection is secure
            )

            return response

        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            return aiohttp.web.Response(
                status=500, text="Internal Server Error: " + str(e)
            )

    def get_address(self):
        assert self.http_host and self.http_port
        return self.http_host, self.http_port

    @aiohttp.web.middleware
    async def path_clean_middleware(self, request, handler):
        if request.path.startswith("/static") or request.path.startswith("/logs"):
            parent = pathlib.PurePosixPath(
                "/logs" if request.path.startswith("/logs") else "/static"
            )

            # If the destination is not relative to the expected directory,
            # then the user is attempting path traversal, so deny the request.
            request_path = pathlib.PurePosixPath(posixpath.realpath(request.path))
            if request_path != parent and parent not in request_path.parents:
                logger.info(
                    f"Rejecting {request_path=} because it is not relative to {parent=}"
                )
                raise aiohttp.web.HTTPForbidden()
        return await handler(request)

    def get_browsers_no_post_put_middleware(self, whitelisted_paths: Set[str]):
        """Create middleware that blocks POST/PUT requests from browsers.

        Args:
            whitelisted_paths: Set of paths that are allowed to accept POST/PUT
                              from browsers (e.g., {"/api/authenticate"})

        Returns:
            An aiohttp middleware function
        """

        @aiohttp.web.middleware
        async def browsers_no_post_put_middleware(request, handler):
            # Allow whitelisted paths
            if request.path in whitelisted_paths:
                return await handler(request)

            if (
                # Deny mutating requests from browsers.
                # See `is_browser_request` for details of the check.
                dashboard_optional_utils.is_browser_request(request)
                and request.method in [hdrs.METH_POST, hdrs.METH_PUT]
            ):
                return aiohttp.web.Response(
                    status=405, text="Method Not Allowed for browser traffic."
                )

            return await handler(request)

        return browsers_no_post_put_middleware

    @aiohttp.web.middleware
    async def metrics_middleware(self, request, handler):
        start_time = time.monotonic()

        try:
            response = await handler(request)
            status_tag = f"{floor(response.status / 100)}xx"
            return response
        except (Exception, asyncio.CancelledError):
            status_tag = "5xx"
            raise
        finally:
            resp_time = time.monotonic() - start_time
            try:
                self.metrics.metrics_request_duration.labels(
                    endpoint=handler.__name__,
                    http_status=status_tag,
                    Version=ray.__version__,
                    SessionName=self._session_name,
                    Component="dashboard",
                ).observe(resp_time)
                self.metrics.metrics_request_count.labels(
                    method=request.method,
                    endpoint=handler.__name__,
                    http_status=status_tag,
                    Version=ray.__version__,
                    SessionName=self._session_name,
                    Component="dashboard",
                ).inc()
            except Exception as e:
                logger.exception(f"Error emitting api metrics: {e}")

    @aiohttp.web.middleware
    async def cache_control_static_middleware(self, request, handler):
        if request.path.startswith("/static"):
            response = await handler(request)
            response.headers["Cache-Control"] = "max-age=31536000"
            return response
        return await handler(request)

    async def run(
        self,
        dashboard_head_modules: List[DashboardHeadModule],
        subprocess_module_handles: List[SubprocessModuleHandle],
    ):
        # Bind http routes of each module.
        for m in dashboard_head_modules:
            dashboard_optional_utils.DashboardHeadRouteTable.bind(m)

        for h in subprocess_module_handles:
            SubprocessRouteTable.bind(h)

        # Public endpoints that don't require authentication.
        # These are needed for the dashboard to load and request an auth token.
        public_exact_paths = {
            "/",  # Root index.html
            "/favicon.ico",
            "/api/authentication_mode",
            "/api/authenticate",  # Token authentication endpoint
            "/api/healthz",  # General healthcheck
            "/api/gcs_healthz",  # GCS health check
            "/api/local_raylet_healthz",  # Raylet health check
            "/-/healthz",  # Serve health check
        }
        public_path_prefixes = ("/static/",)  # Static assets (JS, CSS, images)

        # Paths that are allowed to accept POST/PUT requests from browsers
        browser_post_put_allowed_paths = {
            "/api/authenticate",  # Token authentication endpoint
        }

        # Http server should be initialized after all modules loaded.
        # working_dir uploads for job submission can be up to 100MiB.

        app = aiohttp.web.Application(
            client_max_size=ray_constants.DASHBOARD_CLIENT_MAX_SIZE,
            middlewares=[
                self.metrics_middleware,
                get_token_auth_middleware(
                    aiohttp, public_exact_paths, public_path_prefixes
                ),
                self.path_clean_middleware,
                self.get_browsers_no_post_put_middleware(
                    browser_post_put_allowed_paths
                ),
                self.cache_control_static_middleware,
            ],
        )
        app.add_routes(routes=routes.bound_routes())
        app.add_routes(routes=SubprocessRouteTable.bound_routes())

        self.runner = aiohttp.web.AppRunner(
            app,
            access_log_format=(
                "%a %t '%r' %s %b bytes %D us '%{Referer}i' '%{User-Agent}i'"
            ),
        )
        await self.runner.setup()
        last_ex = None
        for i in range(1 + self.http_port_retries):
            try:
                site = aiohttp.web.TCPSite(self.runner, self.http_host, self.http_port)
                await site.start()
                break
            except OSError as e:
                last_ex = e
                self.http_port += 1
                logger.warning("Try to use port %s: %s", self.http_port, e)
        else:
            raise Exception(
                f"Failed to find a valid port for dashboard after "
                f"{self.http_port_retries} retries: {last_ex}"
            )
        self.http_host, self.http_port, *_ = site._server.sockets[0].getsockname()
        self.http_host = (
            self.ip
            if ipaddress.ip_address(self.http_host).is_unspecified
            else self.http_host
        )
        logger.info(
            "Dashboard head http address: %s",
            build_address(self.http_host, self.http_port),
        )
        # Dump registered http routes.
        dump_routes = [r for r in app.router.routes() if r.method != hdrs.METH_HEAD]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))

    async def cleanup(self):
        # Wait for finish signal.
        await self.runner.cleanup()
