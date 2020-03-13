import argparse
import os
import traceback

import ray

import ray.ray_constants as ray_constants
from ray.dashboard.dashboard import Dashboard, DashboardController

if __name__ == "__main__":
    # TODO(sang): Use Click instead of argparser
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
        help="the password to use for Redis")
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
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
        "--temp-dir",
        required=False,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.")
    parser.add_argument(
        "--hosted-dashboard-addr",
        required=False,
        type=str,
        # TODO(simon): change to ray.io address
        default="54.200.58.116:9081",
        help="Specify the address where user dashboard will be hosted.")
    args = parser.parse_args()
    ray.utils.setup_logger(args.logging_level, args.logging_format)

    import os
    hosted_addr_override = os.environ.get('RAY_HOSTED_ADDR')

    try:
        dashboard_controller = DashboardController(
            args.redis_address, args.redis_password)
        dashboard = Dashboard(
            args.host,
            args.port,
            args.redis_address,
            args.temp_dir,
            dashboard_controller,
            hosted_dashboard_addr=hosted_addr_override or args.hosted_dashboard_addr,
            redis_password=args.redis_password)
        dashboard.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = (
            "The dashboard on node {} failed to start with the following "
            "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_DIED_ERROR, message)
        raise e
