from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import redis
import time

from ray.services import get_ip_address
from ray.services import get_port


class LogMonitor(object):
    """A monitor process for monitoring Ray log files.

    Attributes:
        node_ip_address: The IP address of the node that the log monitor
            process is running on. This will be used to determine which log
            files to track.
        redis_client: A client used to communicate with the Redis server.
        log_filenames: A list of the names of the log files that this monitor
            process is monitoring.
        log_files: A dictionary mapping the name of a log file to a list of
            strings representing its contents.
        log_file_handles: A dictionary mapping the name of a log file to a file
            handle for that file.
    """
    def __init__(self, redis_ip_address, redis_port, node_ip_address):
        """Initialize the log monitor object."""
        self.node_ip_address = node_ip_address
        self.redis_client = redis.StrictRedis(host=redis_ip_address,
                                              port=redis_port)
        self.log_files = {}
        self.log_file_handles = {}

    def update_log_filenames(self):
        """Get the most up-to-date list of log files to monitor from Redis."""
        num_current_log_files = len(self.log_files)
        new_log_filenames = self.redis_client.lrange(
            "LOG_FILENAMES:{}".format(self.node_ip_address),
            num_current_log_files, -1)
        for log_filename in new_log_filenames:
            print("Beginning to track file {}".format(log_filename))
            assert log_filename not in self.log_files
            self.log_files[log_filename] = []

    def check_log_files_and_push_updates(self):
        """Get any changes to the log files and push updates to Redis."""
        for log_filename in self.log_files:
            if log_filename in self.log_file_handles:
                # Get any updates to the file.
                new_lines = []
                while True:
                    current_position = (
                        self.log_file_handles[log_filename].tell())
                    next_line = self.log_file_handles[log_filename].readline()
                    if next_line != "":
                        new_lines.append(next_line)
                    else:
                        self.log_file_handles[log_filename].seek(
                            current_position)
                        break

                # If there are any new lines, cache them and also push them to
                # Redis.
                if len(new_lines) > 0:
                    self.log_files[log_filename] += new_lines
                    redis_key = "LOGFILE:{}:{}".format(
                        self.node_ip_address, log_filename.decode("ascii"))
                    self.redis_client.rpush(redis_key, *new_lines)
            else:
                try:
                    self.log_file_handles[log_filename] = open(log_filename,
                                                               "r")
                except IOError as e:
                    if e.errno == os.errno.EMFILE:
                        print("Warning: Some files are not being logged "
                              "because there are too many open files.")
                    elif e.errno == os.errno.ENOENT:
                        print("Warning: The file {} was not "
                              "found.".format(log_filename))
                    else:
                        raise e

    def run(self):
        """Run the log monitor.

        This will query Redis once every second to check if there are new log
        files to monitor. It will also store those log files in Redis.
        """
        while True:
            self.update_log_filenames()
            self.check_log_files_and_push_updates()
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=("Parse Redis server for the "
                                                  "log monitor to connect "
                                                  "to."))
    parser.add_argument("--redis-address", required=True, type=str,
                        help="The address to use for Redis.")
    parser.add_argument("--node-ip-address", required=True, type=str,
                        help="The IP address of the node this process is on.")
    args = parser.parse_args()

    redis_ip_address = get_ip_address(args.redis_address)
    redis_port = get_port(args.redis_address)

    log_monitor = LogMonitor(redis_ip_address, redis_port,
                             args.node_ip_address)
    log_monitor.run()
