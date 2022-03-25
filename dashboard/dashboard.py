import argparse
import asyncio
import logging
import logging.handlers
import platform
import traceback

import ray.dashboard.consts as dashboard_consts
import ray.dashboard.head as dashboard_head
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
from ray._private.gcs_pubsub import GcsPublisher
from ray._private.ray_logging import setup_component_logger

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


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
        log_dir(str): Log directory of dashboard.
    """

    def __init__(
        self,
        host,
        port,
        port_retries,
        gcs_address,
        log_dir=None,
        temp_dir=None,
        session_dir=None,
        minimal=False,
    ):
        self.dashboard_head = dashboard_head.DashboardHead(
            http_host=host,
            http_port=port,
            http_port_retries=port_retries,
            gcs_address=gcs_address,
            log_dir=log_dir,
            temp_dir=temp_dir,
            session_dir=session_dir,
            minimal=minimal,
        )

    async def run(self):
        await self.dashboard_head.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ray dashboard.")
    parser.add_argument(
        "--host", required=True, type=str, help="The host to use for the HTTP server."
    )
    parser.add_argument(
        "--port", required=True, type=int, help="The port to use for the HTTP server."
    )
    parser.add_argument(
        "--port-retries",
        required=False,
        type=int,
        default=0,
        help="The retry times to select a valid port.",
    )
    parser.add_argument(
        "--gcs-address", required=True, type=str, help="The address (ip:port) of GCS."
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
        default=dashboard_consts.DASHBOARD_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}"'.format(
            dashboard_consts.DASHBOARD_LOG_FILENAME
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
        help="Specify the path of the session directory of the cluster.",
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help=(
            "Minimal dashboard only contains a subset of features that don't "
            "require additional dependencies installed when ray is installed "
            "by `pip install ray[default]`."
        ),
    )

    args = parser.parse_args()

    try:
        setup_component_logger(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count,
        )

        dashboard = Dashboard(
            args.host,
            args.port,
            args.port_retries,
            args.gcs_address,
            log_dir=args.log_dir,
            temp_dir=args.temp_dir,
            session_dir=args.session_dir,
            minimal=args.minimal,
        )
        loop = asyncio.get_event_loop()
        loop.run_until_complete(dashboard.run())
    except Exception as e:
        traceback_str = ray._private.utils.format_error_message(traceback.format_exc())
        message = (
            f"The dashboard on node {platform.uname()[1]} "
            f"failed with the following "
            f"error:\n{traceback_str}"
        )
        if isinstance(e, dashboard_utils.FrontendNotFoundError):
            logger.warning(message)
        else:
            logger.error(message)
            raise e

        # Something went wrong, so push an error to all drivers.
        gcs_publisher = GcsPublisher(args.gcs_address)
        ray._private.utils.publish_error_to_driver(
            ray_constants.DASHBOARD_DIED_ERROR,
            message,
            gcs_publisher=gcs_publisher,
        )
