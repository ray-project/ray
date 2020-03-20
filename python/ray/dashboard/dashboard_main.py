import argparse
import os
import traceback

import click
import ray

import ray.ray_constants as ray_constants
from ray.dashboard.dashboard import Dashboard

@click.command()
@click.option(
    "--host",
    required=True,
    type=str,
    help="The host to use for the HTTP server.")
@click.option(
    "--port",
    required=True,
    type=int,
    help="The port to use for the HTTP server.")
@click.option(
    "--redis-address",
    required=True,
    type=str,
    help="The address to use for Redis.")
@click.option(
    "--redis-password",
    required=False,
    type=str,
    default=None,
    help="the password to use for Redis")
@click.option(
    "--logging-level",
    required=False,
    type=click.Choice(ray_constants.LOGGER_LEVEL_CHOICES),
    default=ray_constants.LOGGER_LEVEL,
    help=ray_constants.LOGGER_LEVEL_HELP)
@click.option(
    "--logging-format",
    required=False,
    type=str,
    default=ray_constants.LOGGER_FORMAT,
    help=ray_constants.LOGGER_FORMAT_HELP)
@click.option(
    "--temp-dir",
    required=False,
    type=str,
    default=None,
    help="Specify the path of the temporary directory use by Ray process.")
def main(host,
         port,
         redis_address,
         redis_password,
         logging_level,
         logging_format,
         temp_dir):
    ray.utils.setup_logger(logging_level, logging_format)

    metrics_export_address = os.environ.get("METRICS_EXPORT_ADDRESS")

    try:
        dashboard = Dashboard(
            host,
            port,
            redis_address,
            temp_dir,
            metrics_export_address=metrics_export_address,
            redis_password=redis_password)
        dashboard.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = (
            "The dashboard on node {} failed to start with the following "
            "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_DIED_ERROR, message)
        raise e


if __name__ == "__main__":
    main()
