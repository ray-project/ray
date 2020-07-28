import argparse
import asyncio
import logging
import logging.handlers
import os
import sys
import traceback

import aiohttp
import aioredis
from grpc.experimental import aio as aiogrpc

import ray
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray.services
import ray.utils
import psutil

logger = logging.getLogger(__name__)

aiogrpc.init_grpc_aio()


class DashboardAgent(object):
    def __init__(self,
                 redis_address,
                 redis_password=None,
                 temp_dir=None,
                 log_dir=None,
                 node_manager_port=None,
                 object_store_name=None,
                 raylet_name=None):
        """Initialize the DashboardAgent object."""
        self._agent_cls_list = dashboard_utils.get_all_modules(
            dashboard_utils.DashboardAgentModule)
        ip, port = redis_address.split(":")
        # Public attributes are accessible for all agent modules.
        self.redis_address = (ip, int(port))
        self.redis_password = redis_password
        self.temp_dir = temp_dir
        self.log_dir = log_dir
        self.node_manager_port = node_manager_port
        self.object_store_name = object_store_name
        self.raylet_name = raylet_name
        self.ip = ray.services.get_node_ip_address()
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0), ))
        listen_address = "[::]:0"
        logger.info("Dashboard agent listen at: %s", listen_address)
        self.port = self.server.add_insecure_port(listen_address)
        self.aioredis_client = None
        self.aiogrpc_raylet_channel = aiogrpc.insecure_channel("{}:{}".format(
            self.ip, self.node_manager_port))
        self.http_session = aiohttp.ClientSession(
            loop=asyncio.get_event_loop())

    def _load_modules(self):
        """Load dashboard agent modules."""
        modules = []
        for cls in self._agent_cls_list:
            logger.info("Load %s: %s",
                        dashboard_utils.DashboardAgentModule.__name__, cls)
            c = cls(self)
            modules.append(c)
        logger.info("Load {} modules.".format(len(modules)))
        return modules

    async def run(self):
        # Create an aioredis client for all modules.
        self.aioredis_client = await aioredis.create_redis_pool(
            address=self.redis_address, password=self.redis_password)

        # Start a grpc asyncio server.
        await self.server.start()

        # Write the dashboard agent port to redis.
        await self.aioredis_client.set(
            "{}{}".format(dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX,
                          self.ip), self.port)

        async def _check_parent():
            """Check if raylet is dead."""
            curr_proc = psutil.Process()
            while True:
                parent = curr_proc.parent()
                if parent is None or parent.pid == 1:
                    logger.error("raylet is dead, agent will die because "
                                 "it fate-shares with raylet.")
                    sys.exit(0)
                await asyncio.sleep(
                    dashboard_consts.
                    DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_SECONDS)

        modules = self._load_modules()
        await asyncio.gather(_check_parent(),
                             *(m.run(self.server) for m in modules))
        await self.server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dashboard agent.")
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="The address to use for Redis.")
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
        default=dashboard_consts.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is {} bytes.".format(
            dashboard_consts.LOGGING_ROTATE_BYTES))
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=dashboard_consts.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is {}.".
        format(dashboard_consts.LOGGING_ROTATE_BACKUP_COUNT))
    parser.add_argument(
        "--log-dir",
        required=False,
        type=str,
        default=None,
        help="Specify the path of log directory.")
    parser.add_argument(
        "--temp-dir",
        required=False,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.")

    args = parser.parse_args()
    try:
        if args.temp_dir:
            temp_dir = "/" + args.temp_dir.strip("/")
        else:
            temp_dir = "/tmp/ray"
        os.makedirs(temp_dir, exist_ok=True)

        if args.log_dir:
            log_dir = args.log_dir
        else:
            log_dir = os.path.join(temp_dir, "session_latest/logs")
        os.makedirs(log_dir, exist_ok=True)

        if args.logging_filename:
            logging_handlers = [
                logging.handlers.RotatingFileHandler(
                    os.path.join(log_dir, args.logging_filename),
                    maxBytes=args.logging_rotate_bytes,
                    backupCount=args.logging_rotate_backup_count)
            ]
        else:
            logging_handlers = None
        logging.basicConfig(
            level=args.logging_level,
            format=args.logging_format,
            handlers=logging_handlers)

        agent = DashboardAgent(
            args.redis_address,
            redis_password=args.redis_password,
            temp_dir=temp_dir,
            log_dir=log_dir,
            node_manager_port=args.node_manager_port,
            object_store_name=args.object_store_name,
            raylet_name=args.raylet_name)

        loop = asyncio.get_event_loop()
        loop.create_task(agent.run())
        loop.run_forever()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The agent on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_AGENT_DIED_ERROR, message)
        raise e
