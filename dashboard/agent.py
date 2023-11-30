import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import pathlib
import sys
import signal

import ray
import ray._private.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
from ray._raylet import GcsClient
from ray._private.process_watcher import create_check_raylet_task
from ray._private.gcs_utils import GcsAioClient
from ray._private.ray_logging import (
    setup_component_logger,
    configure_log_file,
)
from ray.experimental.internal_kv import (
    _initialize_internal_kv,
    _internal_kv_initialized,
)
from ray._private.ray_constants import AGENT_GRPC_MAX_MESSAGE_LENGTH

logger = logging.getLogger(__name__)


class DashboardAgent:
    def __init__(
        self,
        node_ip_address,
        dashboard_agent_port,
        gcs_address,
        minimal,
        metrics_export_port=None,
        node_manager_port=None,
        listen_port=ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
        disable_metrics_collection: bool = False,
        *,  # the following are required kwargs
        object_store_name: str,
        raylet_name: str,
        log_dir: str,
        temp_dir: str,
        session_dir: str,
        logging_params: dict,
        agent_id: int,
        session_name: str,
    ):
        """Initialize the DashboardAgent object."""
        # Public attributes are accessible for all agent modules.
        self.ip = node_ip_address
        self.minimal = minimal

        assert gcs_address is not None
        self.gcs_address = gcs_address

        self.temp_dir = temp_dir
        self.session_dir = session_dir
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
        self.session_name = session_name

        # grpc server is None in mininal.
        self.server = None
        # http_server is None in minimal.
        self.http_server = None

        # Used by the agent and sub-modules.
        # TODO(architkulkarni): Remove gcs_client once the agent exclusively uses
        # gcs_aio_client and not gcs_client.
        self.gcs_client = GcsClient(address=self.gcs_address)
        _initialize_internal_kv(self.gcs_client)
        assert _internal_kv_initialized()
        self.gcs_aio_client = GcsAioClient(address=self.gcs_address)

        if not self.minimal:
            self._init_non_minimal()

    def _init_non_minimal(self):
        from ray._private.gcs_pubsub import GcsAioPublisher

        self.aio_publisher = GcsAioPublisher(address=self.gcs_address)

        try:
            from grpc import aio as aiogrpc
        except ImportError:
            from grpc.experimental import aio as aiogrpc

        # We would want to suppress deprecating warnings from aiogrpc library
        # with the usage of asyncio.get_event_loop() in python version >=3.10
        # This could be removed once https://github.com/grpc/grpc/issues/32526
        # is released, and we used higher versions of grpcio that that.
        if sys.version_info.major >= 3 and sys.version_info.minor >= 10:
            import warnings

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=DeprecationWarning)
                aiogrpc.init_grpc_aio()
        else:
            aiogrpc.init_grpc_aio()

        self.server = aiogrpc.server(
            options=(
                ("grpc.so_reuseport", 0),
                (
                    "grpc.max_send_message_length",
                    AGENT_GRPC_MAX_MESSAGE_LENGTH,
                ),  # noqa
                (
                    "grpc.max_receive_message_length",
                    AGENT_GRPC_MAX_MESSAGE_LENGTH,
                ),
            )  # noqa
        )
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
        assert (
            self.http_server
        ), "Accessing unsupported API (HttpServerAgent) in a minimal ray."
        return self.http_server.http_session

    @property
    def publisher(self):
        assert (
            self.aio_publisher
        ), "Accessing unsupported API (GcsAioPublisher) in a minimal ray."
        return self.aio_publisher

    def get_node_id(self) -> str:
        return self.node_id

    async def run(self):
        # Start a grpc asyncio server.
        if self.server:
            await self.server.start()

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
        grpc_port = -1 if not self.server else self.grpc_port
        await self.gcs_aio_client.internal_kv_put(
            f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{self.node_id}".encode(),
            json.dumps([http_port, grpc_port]).encode(),
            True,
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        )

        tasks = [m.run(self.server) for m in modules]
        if sys.platform not in ["win32", "cygwin"]:

            def callback():
                logger.info(
                    f"Terminated Raylet: ip={self.ip}, node_id={self.node_id}. "
                )

            check_parent_task = create_check_raylet_task(
                self.log_dir, self.gcs_address, callback, loop
            )
            tasks.append(check_parent_task)
        await asyncio.gather(*tasks)

        if self.server:
            await self.server.wait_for_termination()
        else:
            while True:
                await asyncio.sleep(3600)  # waits forever

        if self.http_server:
            await self.http_server.cleanup()


def open_capture_files(log_dir):
    filename = f"agent-{args.agent_id}"
    return (
        ray._private.utils.open_log(pathlib.Path(log_dir) / f"{filename}.out"),
        ray._private.utils.open_log(pathlib.Path(log_dir) / f"{filename}.err"),
    )


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
        default=ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
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
        "--minimal",
        action="store_true",
        help=(
            "Minimal agent only contains a subset of features that don't "
            "require additional dependencies installed when ray is installed "
            "by `pip install 'ray[default]'`."
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
        default=os.getpid(),
    )
    parser.add_argument(
        "--session-name",
        required=False,
        type=str,
        default=None,
        help="The session name (cluster id) of this cluster.",
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
        logger = setup_component_logger(**logging_params)

        # Initialize event loop, see Dashboard init code for caveat
        # w.r.t grpc server init in the DashboardAgent initializer.
        loop = ray._private.utils.get_or_create_event_loop()

        # Setup stdout/stderr redirect files
        out_file, err_file = open_capture_files(args.log_dir)
        configure_log_file(out_file, err_file)

        agent = DashboardAgent(
            args.node_ip_address,
            args.dashboard_agent_port,
            args.gcs_address,
            args.minimal,
            temp_dir=args.temp_dir,
            session_dir=args.session_dir,
            log_dir=args.log_dir,
            metrics_export_port=args.metrics_export_port,
            node_manager_port=args.node_manager_port,
            listen_port=args.listen_port,
            object_store_name=args.object_store_name,
            raylet_name=args.raylet_name,
            logging_params=logging_params,
            disable_metrics_collection=args.disable_metrics_collection,
            agent_id=args.agent_id,
            session_name=args.session_name,
        )

        def sigterm_handler():
            logger.warning("Exiting with SIGTERM immediately...")
            # Exit code 0 will be considered as an expected shutdown
            os._exit(signal.SIGTERM)

        if sys.platform != "win32":
            # TODO(rickyyx): we currently do not have any logic for actual
            # graceful termination in the agent. Most of the underlying
            # async tasks run by the agent head doesn't handle CancelledError.
            # So a truly graceful shutdown is not trivial w/o much refactoring.
            # Re-open the issue: https://github.com/ray-project/ray/issues/25518
            # if a truly graceful shutdown is required.
            loop.add_signal_handler(signal.SIGTERM, sigterm_handler)

        loop.run_until_complete(agent.run())
    except Exception:
        logger.exception("Agent is working abnormally. It will exit immediately.")
        exit(1)
