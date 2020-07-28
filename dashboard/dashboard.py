try:
    import aiohttp.web
except ImportError:
    print("The dashboard requires aiohttp to run.")
    import sys

    sys.exit(1)

import argparse
import asyncio
import errno
import logging
import logging.handlers
import os
import traceback
import uuid

import aioredis

import ray
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.head as dashboard_head
import ray.new_dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray.services
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def setup_static_dir(app):
    build_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "client/build")
    module_name = os.path.basename(os.path.dirname(__file__))
    if not os.path.isdir(build_dir):
        raise OSError(
            errno.ENOENT, "Dashboard build directory not found. If installing "
            "from source, please follow the additional steps "
            "required to build the dashboard"
            "(cd python/ray/{}/client "
            "&& npm install "
            "&& npm ci "
            "&& npm run build)".format(module_name), build_dir)

    static_dir = os.path.join(build_dir, "static")
    app.router.add_static("/static", static_dir, follow_symlinks=True)
    return build_dir


class Dashboard:
    """A dashboard process for monitoring Ray nodes.

    This dashboard is made up of a REST API which collates data published by
        Reporter processes on nodes into a json structure, and a webserver
        which polls said API for display purposes.

    Args:
        host(str): Host address of dashboard aiohttp server.
        port(int): Port number of dashboard aiohttp server.
        redis_address(str): GCS address of a Ray cluster
        temp_dir (str): The temporary directory used for log files and
            information for this Ray session.
        redis_password(str): Redis password to access GCS
    """

    def __init__(self,
                 host,
                 port,
                 redis_address,
                 temp_dir,
                 redis_password=None):
        self.host = host
        self.port = port
        self.temp_dir = temp_dir
        self.dashboard_id = str(uuid.uuid4())
        self.dashboard_head = dashboard_head.DashboardHead(
            redis_address=redis_address, redis_password=redis_password)

        self.app = aiohttp.web.Application()
        self.app.add_routes(routes=routes.routes())

        # Setup Dashboard Routes
        build_dir = setup_static_dir(self.app)
        logger.info("Setup static dir for dashboard: %s", build_dir)
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
        coroutines = [
            self.dashboard_head.run(),
            aiohttp.web._run_app(self.app, host=self.host, port=self.port)
        ]
        ip = ray.services.get_node_ip_address()
        aioredis_client = await aioredis.create_redis_pool(
            address=self.dashboard_head.redis_address,
            password=self.dashboard_head.redis_password)
        await aioredis_client.set(dashboard_consts.REDIS_KEY_DASHBOARD,
                                  ip + ":" + str(self.port))
        await asyncio.gather(*coroutines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "dashboard to connect to."))
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
        default="",
        help="Specify the name of log file, "
        "log to stdout if set empty, default is \"\"")
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

        dashboard = Dashboard(
            args.host,
            args.port,
            args.redis_address,
            temp_dir,
            redis_password=args.redis_password)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(dashboard.run())
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The dashboard on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_DIED_ERROR, message)
        if isinstance(e, OSError) and e.errno == errno.ENOENT:
            logger.warning(message)
        else:
            raise e
