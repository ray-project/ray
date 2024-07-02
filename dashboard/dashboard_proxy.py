"""
This module is a reversed proxy for the Ray Dashboard. It is used to start the worker
dashboard subprocesses, and route the dashboard requests to the appropriate subprocess.

This proxy only accepts arguments enough to start the http service and the list of
modules per each subprocess. Any unknown arguments will be passed to the subprocesses.

Because each subprocess only talks to this proxy, the `host` and `port` arguments are
set to `localhost` and a random available port.

Each subprocess will be started with the following arguments:

- `--host`: `localhost`
- `--port`: a random available port
- `--port-retries`: 0 (If port unavailable, just die and the proxy respawns it with another port.)
- `--modules-to-load`: module names from `--modules-per-process` proxy argument.
- `--logging-filename`: the passed-in logging filename, appended with subprocess number *before* the extension. Example: `dashboard.log` -> `dashboard.subprocess0.log` whereas the no-change `dashboard.log` is the proxy's log.
- any unknown arguments passed to the proxy.

Note:

- All "module"s are Python Classes, not Python Modules. They are subclasses of `DashboardHeadModule`.
- In order to know the route of each module, the proxy still imports all modules.
- Supports Streaming responses and WebSockets.

TODO(ryw): figure out `minimal` how to work.
TODO(ryw): Don't support gRPC servers.
TODO(ryw): `DataSource` readers can only read if the writers are in the same process.
TODO(ryw): if the subprocesses die, this module should restart them.
TODO(ryw): now the static support is hacky: we just copy paste the code. Refactor the http_server_head to have stateless static serving.
TODO(ryw): this proxy itself does not configure logging. It should.
"""

import logging
import os
import subprocess
import sys
import aiohttp
import aiohttp.web
import asyncio
import argparse
import inspect
from typing import List, Dict
from collections import defaultdict
from dataclasses import dataclass, field

import ray
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.utils import DashboardHeadModule
from ray.dashboard.http_server_head import setup_static_dir
import ray.dashboard.consts as dashboard_consts

logger = logging.getLogger(__name__)


def find_free_port():
    """
    TODO(ryw): this is scattered many times across Ray. Consolidate.
    """
    from contextlib import closing
    import socket

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def append_before_extension(filename: str, append: str) -> str:
    """
    Appends a string before the extension of a filename.
    """
    base, ext = os.path.splitext(filename)
    return f"{base}.{append}{ext}"


def make_route_table_to_modules(
    all_modules: List[type[DashboardHeadModule]],
) -> Dict[str, Dict[str, type[DashboardHeadModule]]]:
    """
    Makes a route table from the modules.

    We don't use the `DashboardHeadRouteTable` because it does not store the module type information.

    TODO(ryw): if we need filename and lineno someday, we can add the module type to
    the `DashboardHeadRouteTable` and use that.

    Returns: {method: {path: module}. For example:
        d["get"]["/events"] = EventHead
    """

    def is_route_method(obj):
        return (
            inspect.isfunction(obj)
            and hasattr(obj, "__route_method__")
            and hasattr(obj, "__route_path__")
        )

    ret = defaultdict(dict)
    for module in all_modules:
        handler_routes = inspect.getmembers(module, is_route_method)
        for _, handler in handler_routes:
            method = handler.__route_method__
            path = handler.__route_path__
            ret[method][path] = module
    return ret


@dataclass
class SubDashboard:
    """
    A subprocess of the Ray Dashboard, that handles a subset of the dashboard requests.
    """

    host: str
    port_retries: int
    modules_to_load: List[type[DashboardHeadModule]]
    logging_filename: str
    unknown_args: List[str]
    port: int = 0
    process: subprocess.Popen = field(default=None)

    def start(self):
        """
        (Re)start with a random available port.
        """
        if self.process:
            self.stop()
        self.port = find_free_port()
        logger.info(
            f"starting subdashboard for {self.modules_to_load} on port {self.port}"
        )
        cmd = [
            sys.executable,
            # The neighbor dashboard.py
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.py"),
            "--host=localhost",
            f"--port={self.port}",
            "--port-retries=0",
            f"--logging-filename={self.logging_filename}",
            f"--modules-to-load={','.join(m.__name__ for m in self.modules_to_load)}",
            *self.unknown_args,
        ]
        logger.debug(f"Starting subprocess: {' '.join(cmd)}")
        self.process = subprocess.Popen(cmd)

    def stop(self):
        self.process.kill()


class DashboardProxy:
    """
    Main class. Maintains a list of subprocesses, each of which runs a subset of the dashboard modules. Proxies requests to the appropriate subprocess.
    """

    def __init__(
        self,
        host: str,
        port: int,
        port_retries: int,
        modules_per_process: List[List[str]],
        logging_filename: str,
        unknown_args: List[str],
    ):
        self.host = host
        self.port = port
        self.port_retries = port_retries
        self.modules_per_process = modules_per_process
        self.logging_filename = logging_filename
        #
        self.modules: Dict[str, type[DashboardHeadModule]] = {
            m.__name__: m for m in dashboard_utils.get_all_modules(DashboardHeadModule)
        }
        self.route_table = make_route_table_to_modules(self.modules.values())
        self.subdashboards = [
            SubDashboard(
                host="localhost",
                port_retries=0,
                modules_to_load=[self.modules[m] for m in modules],
                logging_filename=append_before_extension(
                    self.logging_filename, f"subprocess{i}"
                ),
                unknown_args=unknown_args,
            )
            for i, modules in enumerate(modules_per_process)
        ]
        self.module_to_subdashboard = {
            module: subdashboard
            for subdashboard in self.subdashboards
            for module in subdashboard.modules_to_load
        }
        for subdashboard in self.subdashboards:
            subdashboard.start()

    #### HACK for statics (The UI). It should be served by HttpServerDashboardHead but
    # it's the "main class" and does not inherit from DashboardHeadModule. Copy pasting
    # code to our new "main class" for now.
    async def get_index(self, req) -> aiohttp.web.FileResponse:
        try:
            # This API will be no-op after the first report.
            # Note: We always record the usage, but it is not reported
            # if the usage stats is disabled.
            from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

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
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    async def get_favicon(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "client/build/favicon.ico"
            )
        )

    async def start_server_async(self):
        app = aiohttp.web.Application()
        app.add_routes(await self.make_route())

        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        # TODO(ryw): implement port retries.
        site = aiohttp.web.TCPSite(runner, "localhost", self.port)
        await site.start()
        logger.info(f"Dashboard proxy started at http://localhost:{self.port}")
        # Dump registered http routes.
        # TODO(ryw): make this a util function.
        dump_routes = [
            r for r in app.router.routes() if r.method != aiohttp.hdrs.METH_HEAD
        ]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))
        while True:
            await asyncio.sleep(1000)

    async def handle_request(self, module, request: aiohttp.web.Request):
        """
        Proxies request to the corresponding module.

        Caveats:
        - headers are ignored.
        """
        method = request.method
        path = request.path
        subdashboard = self.module_to_subdashboard[module]
        url = f"http://{subdashboard.host}:{subdashboard.port}{path}"
        async with aiohttp.ClientSession() as session:
            data = await request.read()
            async with session.request(
                method, url, params=request.query, data=data
            ) as proxy_response:
                # If Content-Length is present, read it as a standard response.
                if "Content-Length" in proxy_response.headers:
                    body = await proxy_response.read()
                    return aiohttp.web.Response(
                        status=proxy_response.status,
                        body=body,
                        headers=proxy_response.headers,
                    )
                else:
                    # Streaming response (chunked or unspecified length).
                    response = aiohttp.web.StreamResponse(
                        status=proxy_response.status, headers=proxy_response.headers
                    )
                    await response.prepare(request)

                    # Stream data in chunks.
                    async for chunk in proxy_response.content.iter_any():
                        await response.write(chunk)
                    await response.write_eof()
                    return response

    async def make_route(self):
        """
        This method is to ensure we only forward the routes we know from the modules.

        `bound_handler` is a closure that captures the `module` variable.
        """
        route = aiohttp.web.RouteTableDef()
        for method, paths in self.route_table.items():
            for path, module in paths.items():

                def bound_handler(request, module=module):
                    return self.handle_request(module, request)

                route.route(method, path)(bound_handler)
        ### HACK for the UI from HttpServerDashboardHead
        # which itself is not a DashboardHeadModule.
        route.route("get", "/")(self.get_index)
        route.route("get", "/favicon.ico")(self.get_favicon)
        setup_static_dir(route)

        return route


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ray Dashboard Proxy")
    parser.add_argument(
        "--host",
        required=True,
        type=str,
        help="Host address of dashboard aiohttp server",
    )
    parser.add_argument(
        "--port",
        required=True,
        type=int,
        help="Port number of dashboard aiohttp server",
    )
    parser.add_argument(
        "--port-retries",
        required=True,
        type=int,
        help="The retry times to select a valid port",
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=dashboard_consts.DASHBOARD_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}"'.format(
            dashboard_consts.DASHBOARD_LOG_FILENAME
        ),
    )
    parser.add_argument(
        "--modules-per-process",
        required=True,
        type=str,
        help=(
            "Specify the list of module names in m11,m12;m21,m22 format. Each "
            "semicolon-separated group of module names will be started in a separate "
            "process. In each group, modules are comma-separated. Note this arg does "
            "exist in dashboard.py. "
            "E.g., "
            "--modules-per-process=HealthzHead,ServeHead,JobHead,APIHead;"
            "DataHead,MetricsHead,UsageStatsHead;"
            "StateHead,ActorHead,EventHead,NodeHead,ReportHead,HttpServerDashboardHead"
        ),
    )
    args, unknown = parser.parse_known_args()

    modules_per_process = [
        modules.split(",") for modules in args.modules_per_process.split(";")
    ]
    # TODO: check modules_per_process is valid
    proxy = DashboardProxy(
        host=args.host,
        port=args.port,
        port_retries=args.port_retries,
        modules_per_process=modules_per_process,
        logging_filename=args.logging_filename,
        unknown_args=unknown,
    )
    asyncio.run(proxy.start_server_async())
    logger.info("Dashboard proxy exiting.")
