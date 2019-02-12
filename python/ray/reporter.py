from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import traceback
import time

import ray.ray_constants as ray_constants
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


class Reporter(object):
    """A monitor process for monitoring Ray nodes.

    Attributes:
        host (str): The hostname of this machine. Used to improve the log
            messages published to Redis.
        redis_client: A client used to communicate with the Redis server.
    """

    def __init__(self, redis_address, redis_password=None):
        """Initialize the reporter object."""
        self.host = os.uname()[1]
        self.redis_key = "REPORTER_{}".format(self.host)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

    def perform_iteration(self):
        """Get any changes to the log files and push updates to Redis."""
        A = [self.host]
        self.redis_client.put(ray.gcs_utils.REPORTER_CHANNEL,
                                  "\n".join(A))

    def run(self):
        """Run the reporter."""
        while True:
            self.perform_iteration()
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "reporter to connect to."))
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
    args = parser.parse_args()
    ray.utils.setup_logger(args.logging_level, args.logging_format)

    reporter = Reporter(
        args.redis_address, redis_password=args.redis_password)

    try:
        reporter.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The reporter on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.REPORTER_DIED_ERROR, message)
        raise e
