import argparse
import asyncio
import logging
import logging.handlers
import os
import sys
import json

try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

import ray
import ray.experimental.internal_kv as internal_kv
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
from ray._private.gcs_utils import GcsClient
from ray.core.generated import agent_manager_pb2
from ray.core.generated import agent_manager_pb2_grpc
from ray._private.ray_logging import setup_component_logger

# Import psutil after ray so the packaged version is used.
import psutil

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)

aiogrpc.init_grpc_aio()


class DashboardAgent(object):
    def __init__(
        self,
        node_ip_address,
        dashboard_agent_port,
        gcs_address,
        minimal,
        metrics_export_port=None,
        node_manager_port=None,
        listen_port=0,
        disable_metrics_collection: bool = False,
        *,  # the following are required kwargs
        object_store_name: str,
        raylet_name: str,
        log_dir: str,
        temp_dir: str,
        session_dir: str,
        runtime_env_dir: str,
        logging_params: dict,
        agent_id: int,
    ):
        """Initialize the DashboardAgent object."""
        # Public attributes are accessible for all agent modules.
        self.ip = node_ip_address
        self.minimal = minimal

        assert gcs_address is not None
        self.gcs_address = gcs_address

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
        self.metrics_collection_disabled = disable_metrics_collection
        self.agent_id = agent_id
        # TODO(edoakes): RAY_RAYLET_PID isn't properly set on Windows. This is
        # only used for fate-sharing with the raylet and we need a different
        # fate-sharing mechanism for Windows anyways.
        if sys.platform not in ["win32", "cygwin"]:
            self.ppid = int(os.environ["RAY_RAYLET_PID"])
            assert self.ppid > 0
            logger.info("Parent pid is %s", self.ppid)

        # Setup raylet channel
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        self.aiogrpc_raylet_channel = ray._private.utils.init_grpc_channel(
            f"{self.ip}:{self.node_manager_port}", options, asynchronous=True
        )

        # Setup grpc server
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        grpc_ip = "127.0.0.1" if self.ip == "127.0.0.1" else "0.0.0.0"
        try:
            self.grpc_port = ray._private.tls_utils.add_port_to_grpc_server(
                self.server, f"{grpc_ip}:{self.dashboard_agent_port}"
            )
        except Exception:
            # TODO(SongGuyang): Catch the exception here because there is
            # port conflict issue which brought from static port. We should
            # remove this after we find better port resolution.
            logger.exception(
                "Failed to add port to grpc server. Agent will stay alive but "
                "disable the grpc service."
            )
            self.server = None
            self.grpc_port = None
        else:
            logger.info("Dashboard agent grpc address: %s:%s", grpc_ip, self.grpc_port)

        # If the agent is started as non-minimal version, http server should
        # be configured to communicate with the dashboard in a head node.
        self.http_server = None

    async def _configure_http_server(self, modules):
        from ray.dashboard.http_server_agent import HttpServerAgent

        http_server = HttpServerAgent(self.ip, self.listen_port)
        await http_server.start(modules)
        return http_server

    def _load_modules(self):
        """Load dashboard agent modules."""
        modules = []
        agent_cls_list = dashboard_utils.get_all_modules(
            dashboard_utils.DashboardAgentModule
        )
        for cls in agent_cls_list:
            logger.info(
                "Loading %s: %s", dashboard_utils.DashboardAgentModule.__name__, cls
            )
            c = cls(self)
            modules.append(c)
        logger.info("Loaded %d modules.", len(modules))
        return modules

    @property
    def http_session(self):
        assert self.http_server, "Accessing unsupported API in a minimal ray."
        return self.http_server.http_session

    async def run(self):
        async def _check_parent():
            """Check if raylet is dead and fate-share if it is."""
            try:
                curr_proc = psutil.Process()
                while True:
                    parent = curr_proc.parent()
                    if parent is None or parent.pid == 1 or self.ppid != parent.pid:
                        logger.error("Raylet is dead, exiting.")
                        sys.exit(0)
                    await asyncio.sleep(
                        dashboard_consts.DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_SECONDS
                    )
            except Exception:
                logger.error("Failed to check parent PID, exiting.")
                sys.exit(1)

        if sys.platform not in ["win32", "cygwin"]:
            check_parent_task = create_task(_check_parent())

        # Start a grpc asyncio server.
        if self.server:
            await self.server.start()

        self.gcs_client = GcsClient(address=self.gcs_address)
        modules = self._load_modules()

        # Setup http server if necessary.
        if not self.minimal:
            # If the agent is not minimal it should start the http server
            # to communicate with the dashboard in a head node.
            # Http server is not started in the minimal version because
            # it requires additional dependencies that are not
            # included in the minimal ray package.
            try:
                self.http_server = await self._configure_http_server(modules)
            except Exception:
                # TODO(SongGuyang): Catch the exception here because there is
                # port conflict issue which brought from static port. We should
                # remove this after we find better port resolution.
                logger.exception(
                    "Failed to start http server. Agent will stay alive but "
                    "disable the http service."
                )

        # Write the dashboard agent port to kv.
        # TODO: Use async version if performance is an issue
        # -1 should indicate that http server is not started.
        http_port = -1 if not self.http_server else self.http_server.http_port
        internal_kv._internal_kv_put(
            f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{self.node_id}",
            json.dumps([http_port, self.grpc_port]),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        )

        # Register agent to agent manager.
        raylet_stub = agent_manager_pb2_grpc.AgentManagerServiceStub(
            self.aiogrpc_raylet_channel
        )

        await raylet_stub.RegisterAgent(
            agent_manager_pb2.RegisterAgentRequest(
                agent_id=self.agent_id,
                agent_port=self.grpc_port,
                agent_ip_address=self.ip,
            )
        )

        tasks = [m.run(self.server) for m in modules]
        if sys.platform not in ["win32", "cygwin"]:
            tasks.append(check_parent_task)
        await asyncio.gather(*tasks)

        await self.server.wait_for_termination()

        if self.http_server:
            await self.http_server.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dashboard agent.")
    parser.add_argument(
        "--node-ip-address",
        required=True,
        type=str,
        help="the IP address of this node.",
    )
    parser.add_argument(
        "--gcs-address", required=True, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--metrics-export-port",
        required=True,
        type=int,
        help="The port to expose metrics through Prometheus.",
    )
    parser.add_argument(
        "--dashboard-agent-port",
        required=True,
        type=int,
        help="The port on which the dashboard agent will receive GRPCs.",
    )
    parser.add_argument(
        "--node-manager-port",
        required=True,
        type=int,
        help="The port to use for starting the node manager",
    )
    parser.add_argument(
        "--object-store-name",
        required=True,
        type=str,
        default=None,
        help="The socket name of the plasma store",
    )
    parser.add_argument(
        "--listen-port",
        required=False,
        type=int,
        default=0,
        help="Port for HTTP server to listen on",
    )
    parser.add_argument(
        "--raylet-name",
        required=True,
        type=str,
        default=None,
        help="The socket path of the raylet process",
    )
    parser.add_argument(
        "--logging-level",
        required=False,
        type=lambda s: logging.getLevelName(s.upper()),
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP,
    )
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP,
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=dashboard_consts.DASHBOARD_AGENT_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}".'.format(
            dashboard_consts.DASHBOARD_AGENT_LOG_FILENAME
        ),
    )
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is {} bytes.".format(ray_constants.LOGGING_ROTATE_BYTES),
    )
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is {}.".format(
            ray_constants.LOGGING_ROTATE_BACKUP_COUNT
        ),
    )
    parser.add_argument(
        "--log-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of log directory.",
    )
    parser.add_argument(
        "--temp-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.",
    )
    parser.add_argument(
        "--session-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of this session.",
    )
    parser.add_argument(
        "--runtime-env-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the resource directory used by runtime_env.",
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help=(
            "Minimal agent only contains a subset of features that don't "
            "require additional dependencies installed when ray is installed "
            "by `pip install ray[default]`."
        ),
    )
    parser.add_argument(
        "--disable-metrics-collection",
        action="store_true",
        help=("If this arg is set, metrics report won't be enabled from the agent."),
    )
    parser.add_argument(
        "--agent-id",
        required=True,
        type=int,
        help="ID to report when registering with raylet",
    )

    args = parser.parse_args()
    try:
        logging_params = dict(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count,
        )
        setup_component_logger(**logging_params)

        agent = DashboardAgent(
            args.node_ip_address,
            args.dashboard_agent_port,
            args.gcs_address,
            args.minimal,
            temp_dir=args.temp_dir,
            session_dir=args.session_dir,
            runtime_env_dir=args.runtime_env_dir,
            log_dir=args.log_dir,
            metrics_export_port=args.metrics_export_port,
            node_manager_port=args.node_manager_port,
            listen_port=args.listen_port,
            object_store_name=args.object_store_name,
            raylet_name=args.raylet_name,
            logging_params=logging_params,
            disable_metrics_collection=args.disable_metrics_collection,
            agent_id=args.agent_id,
        )
        if os.environ.get("_RAY_AGENT_FAILING"):
            raise Exception("Failure injection failure.")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(agent.run())
    except Exception:
        logger.exception("Agent is working abnormally. It will exit immediately.")
        exit(1)
