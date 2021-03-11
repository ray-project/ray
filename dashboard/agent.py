import argparse
import asyncio
import logging
import logging.handlers
import os
import platform
import sys
import socket
import json
import time
import traceback

import aiohttp
import aiohttp.web
import aiohttp_cors
import psutil
from aiohttp import hdrs
from grpc.experimental import aio as aiogrpc

import ray
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
from ray.core.generated import agent_manager_pb2
from ray.core.generated import agent_manager_pb2_grpc
from ray._private.ray_logging import setup_component_logger

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()


class DashboardAgent(object):
    def __init__(self,
                 node_ip_address,
                 redis_address,
                 dashboard_agent_port,
                 redis_password=None,
                 temp_dir=None,
                 log_dir=None,
                 metrics_export_port=None,
                 node_manager_port=None,
                 object_store_name=None,
                 raylet_name=None):
        """Initialize the DashboardAgent object."""
        # Public attributes are accessible for all agent modules.
        self.ip = node_ip_address
        self.redis_address = dashboard_utils.address_tuple(redis_address)
        self.redis_password = redis_password
        self.temp_dir = temp_dir
        self.log_dir = log_dir
        self.dashboard_agent_port = dashboard_agent_port
        self.metrics_export_port = metrics_export_port
        self.node_manager_port = node_manager_port
        self.object_store_name = object_store_name
        self.raylet_name = raylet_name
        self.node_id = os.environ["RAY_NODE_ID"]
        # TODO(edoakes): RAY_RAYLET_PID isn't properly set on Windows. This is
        # only used for fate-sharing with the raylet and we need a different
        # fate-sharing mechanism for Windows anyways.
        if sys.platform not in ["win32", "cygwin"]:
            self.ppid = int(os.environ["RAY_RAYLET_PID"])
            assert self.ppid > 0
            logger.info("Parent pid is %s", self.ppid)
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0), ))
        self.grpc_port = self.server.add_insecure_port(
            f"[::]:{self.dashboard_agent_port}")
        logger.info("Dashboard agent grpc address: %s:%s", self.ip,
                    self.grpc_port)
        self.aioredis_client = None
        options = (("grpc.enable_http_proxy", 0), )
        self.aiogrpc_raylet_channel = aiogrpc.insecure_channel(
            f"{self.ip}:{self.node_manager_port}", options=options)
        self.http_session = None

    def _load_modules(self):
        """Load dashboard agent modules."""
        modules = []
        agent_cls_list = dashboard_utils.get_all_modules(
            dashboard_utils.DashboardAgentModule)
        for cls in agent_cls_list:
            logger.info("Loading %s: %s",
                        dashboard_utils.DashboardAgentModule.__name__, cls)
            c = cls(self)
            dashboard_utils.ClassMethodRouteTable.bind(c)
            modules.append(c)
        logger.info("Loaded %d modules.", len(modules))
        return modules

    async def run(self):
        async def _check_parent():
            """Check if raylet is dead and fate-share if it is."""
            try:
                curr_proc = psutil.Process()
                while True:
                    parent = curr_proc.parent()
                    if (parent is None or parent.pid == 1
                            or self.ppid != parent.pid):
                        logger.error("Raylet is dead, exiting.")
                        sys.exit(0)
                    await asyncio.sleep(
                        dashboard_consts.
                        DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_SECONDS)
            except Exception:
                logger.error("Failed to check parent PID, exiting.")
                sys.exit(1)

        if sys.platform not in ["win32", "cygwin"]:
            check_parent_task = create_task(_check_parent())

        # Create an aioredis client for all modules.
        try:
            self.aioredis_client = await dashboard_utils.get_aioredis_client(
                self.redis_address, self.redis_password,
                dashboard_consts.CONNECT_REDIS_INTERNAL_SECONDS,
                dashboard_consts.RETRY_REDIS_CONNECTION_TIMES)
        except (socket.gaierror, ConnectionRefusedError):
            logger.error(
                "Dashboard agent exiting: "
                "Failed to connect to redis at %s", self.redis_address)
            sys.exit(-1)

        # Create a http session for all modules.
        self.http_session = aiohttp.ClientSession(
            loop=asyncio.get_event_loop())

        # Start a grpc asyncio server.
        await self.server.start()

        modules = self._load_modules()

        # Http server should be initialized after all modules loaded.
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
            })
        for route in list(app.router.routes()):
            cors.add(route)

        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, self.ip, 0)
        await site.start()
        http_host, http_port, *_ = site._server.sockets[0].getsockname()
        logger.info("Dashboard agent http address: %s:%s", http_host,
                    http_port)

        # Dump registered http routes.
        dump_routes = [
            r for r in app.router.routes() if r.method != hdrs.METH_HEAD
        ]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))

        # Write the dashboard agent port to redis.
        await self.aioredis_client.set(
            f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{self.node_id}",
            json.dumps([http_port, self.grpc_port]))

        # Register agent to agent manager.
        raylet_stub = agent_manager_pb2_grpc.AgentManagerServiceStub(
            self.aiogrpc_raylet_channel)

        await raylet_stub.RegisterAgent(
            agent_manager_pb2.RegisterAgentRequest(
                agent_pid=os.getpid(),
                agent_port=self.grpc_port,
                agent_ip_address=self.ip))

        tasks = [m.run(self.server) for m in modules]
        if sys.platform not in ["win32", "cygwin"]:
            tasks.append(check_parent_task)
        await asyncio.gather(*tasks)

        await self.server.wait_for_termination()
        # Wait for finish signal.
        await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dashboard agent.")
    parser.add_argument(
        "--node-ip-address",
        required=True,
        type=str,
        help="the IP address of this node.")
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="The address to use for Redis.")
    parser.add_argument(
        "--metrics-export-port",
        required=True,
        type=int,
        help="The port to expose metrics through Prometheus.")
    parser.add_argument(
        "--dashboard-agent-port",
        required=True,
        type=int,
        help="The port on which the dashboard agent will receive GRPCs.")
    parser.add_argument(
        "--node-manager-port",
        required=True,
        type=int,
        help="The port to use for starting the node manager")
    parser.add_argument(
        "--object-store-name",
        required=True,
        type=str,
        default=None,
        help="The socket name of the plasma store")
    parser.add_argument(
        "--raylet-name",
        required=True,
        type=str,
        default=None,
        help="The socket path of the raylet process")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="The password to use for Redis")
    parser.add_argument(
        "--logging-level",
        required=False,
        type=lambda s: logging.getLevelName(s.upper()),
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=dashboard_consts.DASHBOARD_AGENT_LOG_FILENAME,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is \"{}\".".format(
            dashboard_consts.DASHBOARD_AGENT_LOG_FILENAME))
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is {} bytes.".format(
            ray_constants.LOGGING_ROTATE_BYTES))
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is {}.".
        format(ray_constants.LOGGING_ROTATE_BACKUP_COUNT))
    parser.add_argument(
        "--log-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of log directory.")
    parser.add_argument(
        "--temp-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.")

    args = parser.parse_args()
    try:
        setup_component_logger(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count)

        # The dashboard is currently broken on Windows.
        # https://github.com/ray-project/ray/issues/14026.
        if sys.platform == "win32":
            logger.warning(
                "The dashboard is currently disabled on windows."
                "See https://github.com/ray-project/ray/issues/14026"
                "for more details")
            while True:
                time.sleep(999)

        agent = DashboardAgent(
            args.node_ip_address,
            args.redis_address,
            args.dashboard_agent_port,
            redis_password=args.redis_password,
            temp_dir=args.temp_dir,
            log_dir=args.log_dir,
            metrics_export_port=args.metrics_export_port,
            node_manager_port=args.node_manager_port,
            object_store_name=args.object_store_name,
            raylet_name=args.raylet_name)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(agent.run())
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray._private.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray._private.utils.format_error_message(
            traceback.format_exc())
        message = ("The agent on node {} failed with the following "
                   "error:\n{}".format(platform.uname()[1], traceback_str))
        ray._private.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_AGENT_DIED_ERROR, message)
        logger.exception(message)
        raise e
