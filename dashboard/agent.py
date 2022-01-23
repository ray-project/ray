import argparse
import asyncio
import logging
import logging.handlers
import os
import platform
import sys
import socket
import json
import traceback

try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc
from distutils.version import LooseVersion

import ray
import ray.experimental.internal_kv as internal_kv
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
from ray._private.gcs_pubsub import gcs_pubsub_enabled, GcsPublisher
from ray._private.gcs_utils import GcsClient, \
    get_gcs_address_from_redis, use_gcs_for_bootstrap
from ray.core.generated import agent_manager_pb2
from ray.core.generated import agent_manager_pb2_grpc
from ray._private.ray_logging import setup_component_logger

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp, aiohttp_cors, hdrs

# Import psutil after ray so the packaged version is used.
import psutil

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
                 gcs_address,
                 redis_password=None,
                 temp_dir=None,
                 session_dir=None,
                 runtime_env_dir=None,
                 log_dir=None,
                 metrics_export_port=None,
                 node_manager_port=None,
                 listen_port=0,
                 object_store_name=None,
                 raylet_name=None,
                 logging_params=None):
        """Initialize the DashboardAgent object."""
        # Public attributes are accessible for all agent modules.
        self.ip = node_ip_address

        if use_gcs_for_bootstrap():
            assert gcs_address is not None
            self.gcs_address = gcs_address
        else:
            self.redis_address = dashboard_utils.address_tuple(redis_address)
            self.redis_password = redis_password
            self.aioredis_client = None
            self.gcs_address = None

        self.temp_dir = temp_dir
        self.session_dir = session_dir
        self.runtime_env_dir = runtime_env_dir
        self.log_dir = log_dir
        self.dashboard_agent_port = dashboard_agent_port
        self.metrics_export_port = metrics_export_port
        self.node_manager_port = node_manager_port
        self.listen_port = listen_port
        self.object_store_name = object_store_name
        self.raylet_name = raylet_name
        self.logging_params = logging_params
        self.node_id = os.environ["RAY_NODE_ID"]
        # TODO(edoakes): RAY_RAYLET_PID isn't properly set on Windows. This is
        # only used for fate-sharing with the raylet and we need a different
        # fate-sharing mechanism for Windows anyways.
        if sys.platform not in ["win32", "cygwin"]:
            self.ppid = int(os.environ["RAY_RAYLET_PID"])
            assert self.ppid > 0
            logger.info("Parent pid is %s", self.ppid)
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0), ))
        grpc_ip = "127.0.0.1" if self.ip == "127.0.0.1" else "0.0.0.0"
        self.grpc_port = ray._private.tls_utils.add_port_to_grpc_server(
            self.server, f"{grpc_ip}:{self.dashboard_agent_port}")
        logger.info("Dashboard agent grpc address: %s:%s", grpc_ip,
                    self.grpc_port)
        options = (("grpc.enable_http_proxy", 0), )
        self.aiogrpc_raylet_channel = ray._private.utils.init_grpc_channel(
            f"{self.ip}:{self.node_manager_port}", options, asynchronous=True)
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

        if not use_gcs_for_bootstrap():
            # Create an aioredis client for all modules.
            try:
                self.aioredis_client = \
                    await dashboard_utils.get_aioredis_client(
                        self.redis_address, self.redis_password,
                        dashboard_consts.CONNECT_REDIS_INTERNAL_SECONDS,
                        dashboard_consts.RETRY_REDIS_CONNECTION_TIMES)
            except (socket.gaierror, ConnectionRefusedError):
                logger.error(
                    "Dashboard agent exiting: "
                    "Failed to connect to redis at %s", self.redis_address)
                sys.exit(-1)

        # Create a http session for all modules.
        # aiohttp<4.0.0 uses a 'loop' variable, aiohttp>=4.0.0 doesn't anymore
        if LooseVersion(aiohttp.__version__) < LooseVersion("4.0.0"):
            self.http_session = aiohttp.ClientSession(
                loop=asyncio.get_event_loop())
        else:
            self.http_session = aiohttp.ClientSession()

        # Start a grpc asyncio server.
        await self.server.start()

        if not use_gcs_for_bootstrap():
            gcs_address = await self.aioredis_client.get(
                dashboard_consts.GCS_SERVER_ADDRESS)
            self.gcs_client = GcsClient(address=gcs_address.decode())
        else:
            self.gcs_client = GcsClient(address=self.gcs_address)
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
        site = aiohttp.web.TCPSite(
            runner, "127.0.0.1"
            if self.ip == "127.0.0.1" else "0.0.0.0", self.listen_port)
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
        # TODO: Use async version if performance is an issue
        internal_kv._internal_kv_put(
            f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{self.node_id}",
            json.dumps([http_port, self.grpc_port]),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD)

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
        "--gcs-address",
        required=False,
        type=str,
        help="The address (ip:port) of GCS.")
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
        "--listen-port",
        required=False,
        type=int,
        default=0,
        help="Port for HTTP server to listen on")
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
    parser.add_argument(
        "--session-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of this session.")
    parser.add_argument(
        "--runtime-env-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the resource directory used by runtime_env.")

    args = parser.parse_args()
    try:
        logging_params = dict(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count)
        setup_component_logger(**logging_params)

        agent = DashboardAgent(
            args.node_ip_address,
            args.redis_address,
            args.dashboard_agent_port,
            args.gcs_address,
            redis_password=args.redis_password,
            temp_dir=args.temp_dir,
            session_dir=args.session_dir,
            runtime_env_dir=args.runtime_env_dir,
            log_dir=args.log_dir,
            metrics_export_port=args.metrics_export_port,
            node_manager_port=args.node_manager_port,
            listen_port=args.listen_port,
            object_store_name=args.object_store_name,
            raylet_name=args.raylet_name,
            logging_params=logging_params)
        if os.environ.get("_RAY_AGENT_FAILING"):
            raise Exception("Failure injection failure.")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(agent.run())
    except Exception as e:
        # All these env vars should be available because
        # they are provided by the parent raylet.
        restart_count = os.environ["RESTART_COUNT"]
        max_restart_count = os.environ["MAX_RESTART_COUNT"]
        raylet_pid = os.environ["RAY_RAYLET_PID"]
        node_ip = args.node_ip_address
        if restart_count >= max_restart_count:
            # Agent is failed to be started many times.
            # Push an error to all drivers, so that users can know the
            # impact of the issue.
            redis_client = None
            gcs_publisher = None
            if gcs_pubsub_enabled():
                if use_gcs_for_bootstrap():
                    gcs_publisher = GcsPublisher(args.gcs_address)
                else:
                    redis_client = ray._private.services.create_redis_client(
                        args.redis_address, password=args.redis_password)
                    gcs_publisher = GcsPublisher(
                        address=get_gcs_address_from_redis(redis_client))
            else:
                redis_client = ray._private.services.create_redis_client(
                    args.redis_address, password=args.redis_password)

            traceback_str = ray._private.utils.format_error_message(
                traceback.format_exc())
            message = (
                f"(ip={node_ip}) "
                f"The agent on node {platform.uname()[1]} failed to "
                f"be restarted {max_restart_count} "
                "times. There are 3 possible problems if you see this error."
                "\n  1. The dashboard might not display correct "
                "information on this node."
                "\n  2. Metrics on this node won't be reported."
                "\n  3. runtime_env APIs won't work."
                "\nCheck out the `dashboard_agent.log` to see the "
                "detailed failure messages.")
            ray._private.utils.publish_error_to_driver(
                ray_constants.DASHBOARD_AGENT_DIED_ERROR,
                message,
                redis_client=redis_client,
                gcs_publisher=gcs_publisher)
            logger.error(message)
        logger.exception(e)
        exit(1)
