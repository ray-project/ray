from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import traceback
import time
import datetime
from socket import AddressFamily

import simplejson
import psutil

import ray.ray_constants as ray_constants
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

def running_worker(s):
    if "ray_worker" not in s:
        return False

    if s == "ray_worker":
        return False

    return True

def determine_ip_address():
    """Return the first IP address for an ethernet interface on the system."""
    addrs = [
        x.address
        for k, v in psutil.net_if_addrs().items() if k[0] == "e"
        for x in v
        if x.family == AddressFamily.AF_INET
    ]
    return addrs[0]

def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()

class Reporter(object):
    """A monitor process for monitoring Ray nodes.

    Attributes:
        host (str): The hostname of this machine. Used to improve the log
            messages published to Redis.
        redis_client: A client used to communicate with the Redis server.
    """

    def __init__(self, redis_address, redis_password=None):
        """Initialize the reporter object."""
        self.cpu_counts = (psutil.cpu_count(), psutil.cpu_count(logical=False))
        self.ip_addr = determine_ip_address()
        self.hostname = os.uname().nodename

        _ = psutil.cpu_percent()  # For initialization

        self.redis_key = "{}.{}".format(ray.gcs_utils.REPORTER_CHANNEL, self.hostname)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

    @staticmethod
    def get_cpu_percent():
        return psutil.cpu_percent()

    @staticmethod
    def get_boot_time():
        return psutil.boot_time()

    @staticmethod
    def get_network_stats():
        now = datetime.datetime.utcnow()
        ifaces = [
            v
            for k, v in psutil.net_io_counters(pernic=True).items()
            if k[0] == "e"
        ]

        sent = sum((iface.bytes_sent for iface in ifaces))
        recv = sum((iface.bytes_recv for iface in ifaces))
        return sent, recv

    @staticmethod
    def get_mem_usage():
        vm = psutil.virtual_memory()
        return vm.total, vm.available, vm.percent

    @staticmethod
    def get_disk_usage():
        return {
            x: psutil.disk_usage(x)
            for x in ["/", "/tmp"]
        }

    @staticmethod
    def get_workers():
        return [
            x.as_dict(attrs=["pid", "create_time", "cpu_times", "name", "memory_full_info"])
            for x in psutil.process_iter()
            if running_worker(x.name())
        ]

    def get_load_avg(self):
        load = os.getloadavg()
        per_cpu_load = tuple(
            ( round(x/self.cpu_counts[0], 2) for x in load )
        )
        return load, per_cpu_load

    def get_all_stats(self):
        return {
            "now": to_posix_time(datetime.datetime.utcnow()),
            "hostname": self.hostname,
            "ip": self.ip_addr,
            "cpu": self.get_cpu_percent(),
            "cpus": self.cpu_counts,
            "mem": self.get_mem_usage(),
            "workers": self.get_workers(),
            "boot_time": self.get_boot_time(),
            "load_avg": self.get_load_avg(),
            "disk": self.get_disk_usage(),
        }

    def perform_iteration(self):
        """Get any changes to the log files and push updates to Redis."""
        stats = self.get_all_stats()

        self.redis_client.publish(
            #ray.gcs_utils.REPORTER_CHANNEL,
            self.redis_key,
            simplejson.dumps(stats, namedtuple_as_object=True),
        )

    def run(self):
        """Run the reporter."""
        while True:
            try:
                self.perform_iteration()
            except:
                traceback.print_exc()
                pass

            time.sleep(ray_constants.REPORTER_UPDATE_INTERVAL_MS/1000)

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
