import asyncio
import logging
import random
from typing import List, Optional

from packaging.version import Version

import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._common.utils import get_or_create_event_loop
from ray._common.network_utils import build_address
from ray.dashboard.optional_deps import aiohttp, aiohttp_cors, hdrs

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardAgentRouteTable


class HttpServerAgent:
    def __init__(self, ip: str, listen_port: int) -> None:
        self.ip = ip
        self.listen_port = listen_port
        self.http_host = None
        self.http_port = None
        self.http_session = None
        self.runner = None

    async def _start_site_with_retry(
        self, max_retries: int = 5, base_delay: float = 0.1
    ) -> aiohttp.web.TCPSite:
        """Start the TCP site with retry logic and exponential backoff.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds for exponential backoff

        Returns:
            The started site object

        Raises:
            OSError: If all retry attempts fail
        """
        last_exception: Optional[OSError] = None

        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                site = aiohttp.web.TCPSite(
                    self.runner,
                    "127.0.0.1" if self.ip == "127.0.0.1" else "0.0.0.0",
                    self.listen_port,
                )
                await site.start()
                if attempt > 0:
                    logger.info(
                        f"Successfully started agent on port {self.listen_port} "
                        f"after {attempt} retry attempts"
                    )
                return site

            except OSError as e:
                last_exception = e
                if attempt < max_retries:
                    # Calculate exponential backoff with jitter
                    delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        f"Failed to bind to port {self.listen_port} (attempt {attempt + 1}/"
                        f"{max_retries + 1}). Retrying in {delay:.2f}s. Error: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.exception(
                        f"Agent port #{self.listen_port} failed to bind after "
                        f"{max_retries + 1} attempts."
                    )
                    break

        # If we get here, all retries failed
        raise last_exception

    async def start(self, modules: List) -> None:
        # Create a http session for all modules.
        # aiohttp<4.0.0 uses a 'loop' variable, aiohttp>=4.0.0 doesn't anymore
        if Version(aiohttp.__version__) < Version("4.0.0"):
            self.http_session = aiohttp.ClientSession(loop=get_or_create_event_loop())
        else:
            self.http_session = aiohttp.ClientSession()

        # Bind routes for every module so that each module
        # can use decorator-style routes.
        for c in modules:
            dashboard_optional_utils.DashboardAgentRouteTable.bind(c)

        app = aiohttp.web.Application()
        app.add_routes(routes=routes.bound_routes())

        # Enable CORS on all routes.
        cors = aiohttp_cors.setup(
            app,
            defaults={
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_methods="*",
                    allow_headers=("Content-Type", "X-Header"),
                )
            },
        )
        for route in list(app.router.routes()):
            cors.add(route)

        self.runner = aiohttp.web.AppRunner(app)
        await self.runner.setup()

        # Start the site with retry logic
        site = await self._start_site_with_retry()

        self.http_host, self.http_port, *_ = site._server.sockets[0].getsockname()
        logger.info(
            "Dashboard agent http address: %s",
            build_address(self.http_host, self.http_port),
        )

        # Dump registered http routes.
        dump_routes = [r for r in app.router.routes() if r.method != hdrs.METH_HEAD]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))

    async def cleanup(self) -> None:
        # Wait for finish signal.
        await self.runner.cleanup()
        await self.http_session.close()
