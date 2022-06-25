import asyncio
import errno
import ipaddress
import logging
import os
import sys
from distutils.version import LooseVersion

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp, hdrs

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


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
            "&& npm install "
            "&& npm ci "
            "&& npm run build)",
            build_dir,
        )

    static_dir = os.path.join(build_dir, "static")
    routes.static("/static", static_dir, follow_symlinks=True)
    return build_dir


class HttpServerDashboardHead:
    def __init__(self, ip, http_host, http_port, http_port_retries):
        self.ip = ip
        self.http_host = http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries

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
        dashboard_optional_utils.ClassMethodRouteTable.bind(self)

        # Create a http session for all modules.
        # aiohttp<4.0.0 uses a 'loop' variable, aiohttp>=4.0.0 doesn't anymore
        if LooseVersion(aiohttp.__version__) < LooseVersion("4.0.0"):
            self.http_session = aiohttp.ClientSession(loop=asyncio.get_event_loop())
        else:
            self.http_session = aiohttp.ClientSession()

    @routes.get("/")
    async def get_index(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "client/build/index.html"
            )
        )

    @routes.get("/favicon.ico")
    async def get_favicon(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "client/build/favicon.ico"
            )
        )

    def get_address(self):
        assert self.http_host and self.http_port
        return self.http_host, self.http_port

    async def run(self, modules):
        # Bind http routes of each module.
        for c in modules:
            dashboard_optional_utils.ClassMethodRouteTable.bind(c)
        # Http server should be initialized after all modules loaded.
        # working_dir uploads for job submission can be up to 100MiB.
        app = aiohttp.web.Application(client_max_size=100 * 1024 ** 2)
        app.add_routes(routes=routes.bound_routes())

        self.runner = aiohttp.web.AppRunner(
            app,
            access_log_format=(
                "%a %t '%r' %s %b bytes %D us " "'%{Referer}i' '%{User-Agent}i'"
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
            "Dashboard head http address: %s:%s", self.http_host, self.http_port
        )
        # Dump registered http routes.
        dump_routes = [r for r in app.router.routes() if r.method != hdrs.METH_HEAD]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))

    async def cleanup(self):
        # Wait for finish signal.
        await self.runner.cleanup()
