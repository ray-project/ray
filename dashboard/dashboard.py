import sys

import argparse
import asyncio
import errno
import logging
import logging.handlers
import os
import platform
import traceback

import ray.dashboard.consts as dashboard_consts
import ray.dashboard.head as dashboard_head
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray._private.gcs_utils as gcs_utils
import ray._private.services
import ray._private.utils
from ray._private.gcs_pubsub import gcs_pubsub_enabled, GcsPublisher
from ray._private.ray_logging import setup_component_logger
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class FrontendNotFoundError(OSError):
    pass


def setup_static_dir():
    build_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "client", "build")
    module_name = os.path.basename(os.path.dirname(__file__))
    if not os.path.isdir(build_dir):
        raise FrontendNotFoundError(
            errno.ENOENT, "Dashboard build directory not found. If installing "
            "from source, please follow the additional steps "
            "required to build the dashboard"
            f"(cd python/ray/{module_name}/client "
            "&& npm install "
            "&& npm ci "
            "&& npm run build)", build_dir)

    static_dir = os.path.join(build_dir, "static")
    routes.static("/static", static_dir, follow_symlinks=True)
    return build_dir


class Dashboard:
    """A dashboard process for monitoring Ray nodes.

    This dashboard is made up of a REST API which collates data published by
        Reporter processes on nodes into a json structure, and a webserver
        which polls said API for display purposes.

    Args:
        host(str): Host address of dashboard aiohttp server.
        port(int): Port number of dashboard aiohttp server.
        port_retries(int): The retry times to select a valid port.
        gcs_address(str): GCS address of the cluster
        redis_address(str): Redis address of a Ray cluster
        redis_password(str): Redis password to access GCS
        log_dir(str): Log directory of dashboard.
    """

    def __init__(self,
                 host,
                 port,
                 port_retries,
                 gcs_address,
                 redis_address,
                 redis_password=None,
                 log_dir=None):
        self.dashboard_head = dashboard_head.DashboardHead(
            http_host=host,
            http_port=port,
            http_port_retries=port_retries,
            gcs_address=gcs_address,
            redis_address=redis_address,
            redis_password=redis_password,
            log_dir=log_dir)

        # Setup Dashboard Routes
        try:
            build_dir = setup_static_dir()
            logger.info("Setup static dir for dashboard: %s", build_dir)
        except FrontendNotFoundError as ex:
            # Not to raise FrontendNotFoundError due to NPM incompatibilities
            # with Windows.
            # Please refer to ci.sh::build_dashboard_front_end()
            if sys.platform in ["win32", "cygwin"]:
                logger.warning(ex)
            else:
                raise ex
        dashboard_utils.ClassMethodRouteTable.bind(self)

    @routes.get("/")
    async def get_index(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "client/build/index.html"))

    @routes.get("/favicon.ico")
    async def get_favicon(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "client/build/favicon.ico"))

    async def run(self):
        await self.dashboard_head.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ray dashboard.")
    parser.add_argument(
        "--host",
        required=True,
        type=str,
        help="The host to use for the HTTP server.")
    parser.add_argument(
        "--port",
        required=True,
        type=int,
        help="The port to use for the HTTP server.")
    parser.add_argument(
        "--port-retries",
        required=False,
        type=int,
        default=0,
        help="The retry times to select a valid port.")
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
        default=dashboard_consts.DASHBOARD_LOG_FILENAME,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is \"{}\"".format(
            dashboard_consts.DASHBOARD_LOG_FILENAME))
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

    if gcs_utils.use_gcs_for_bootstrap():
        args.redis_address = None
        args.redis_password = None
    else:
        args.gcs_address = None

    try:
        setup_component_logger(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count)

        dashboard = Dashboard(
            args.host,
            args.port,
            args.port_retries,
            args.gcs_address,
            args.redis_address,
            redis_password=args.redis_password,
            log_dir=args.log_dir)
        # TODO(fyrestone): Avoid using ray.state in dashboard, it's not
        # asynchronous and will lead to low performance. ray disconnect()
        # will be hang when the ray.state is connected and the GCS is exit.
        # Please refer to: https://github.com/ray-project/ray/issues/16328
        service_discovery = PrometheusServiceDiscoveryWriter(
            args.redis_address, args.redis_password, args.gcs_address,
            args.temp_dir)
        # Need daemon True to avoid dashboard hangs at exit.
        service_discovery.daemon = True
        service_discovery.start()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(dashboard.run())
    except Exception as e:
        traceback_str = ray._private.utils.format_error_message(
            traceback.format_exc())
        message = f"The dashboard on node {platform.uname()[1]} " \
                  f"failed with the following " \
                  f"error:\n{traceback_str}"
        if isinstance(e, FrontendNotFoundError):
            logger.warning(message)
        else:
            logger.error(message)
            raise e

        # Something went wrong, so push an error to all drivers.
        redis_client = None
        gcs_publisher = None
        if gcs_pubsub_enabled():
            if gcs_utils.use_gcs_for_bootstrap():
                gcs_publisher = GcsPublisher(args.gcs_address)
            else:
                redis_client = ray._private.services.create_redis_client(
                    args.redis_address, password=args.redis_password)
                gcs_publisher = GcsPublisher(
                    address=gcs_utils.get_gcs_address_from_redis(redis_client))
                redis_client = None
        else:
            redis_client = ray._private.services.create_redis_client(
                args.redis_address, password=args.redis_password)

        ray._private.utils.publish_error_to_driver(
            redis_client,
            ray_constants.DASHBOARD_DIED_ERROR,
            message,
            redis_client=redis_client,
            gcs_publisher=gcs_publisher)
