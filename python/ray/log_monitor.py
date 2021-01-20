import argparse
import errno
import glob
import json
import logging
import logging.handlers
import os
import platform
import re
import shutil
import time
import traceback

import ray.ray_constants as ray_constants
import ray._private.services as services
import ray.utils
from ray.ray_logging import setup_component_logger

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

# The groups are worker id, job id, and pid.
JOB_LOG_PATTERN = re.compile(".*worker-([0-9a-f]+)-(\d+)-(\d+)")


class LogFileInfo:
    def __init__(self,
                 filename=None,
                 size_when_last_opened=None,
                 file_position=None,
                 file_handle=None,
                 is_err_file=False,
                 job_id=None,
                 worker_pid=None):
        assert (filename is not None and size_when_last_opened is not None
                and file_position is not None)
        self.filename = filename
        self.size_when_last_opened = size_when_last_opened
        self.file_position = file_position
        self.file_handle = file_handle
        self.is_err_file = is_err_file
        self.job_id = job_id
        self.worker_pid = worker_pid


class LogMonitor:
    """A monitor process for monitoring Ray log files.

    This class mantains a list of open files and a list of closed log files. We
    can't simply leave all files open because we'll run out of file
    descriptors.

    The "run" method of this class will cycle between doing several things:
    1. First, it will check if any new files have appeared in the log
       directory. If so, they will be added to the list of closed files.
    2. Then, if we are unable to open any new files, we will close all of the
       files.
    3. Then, we will open as many closed files as we can that may have new
       lines (judged by an increase in file size since the last time the file
       was opened).
    4. Then we will loop through the open files and see if there are any new
       lines in the file. If so, we will publish them to Redis.

    Attributes:
        host (str): The hostname of this machine. Used to improve the log
            messages published to Redis.
        logs_dir (str): The directory that the log files are in.
        redis_client: A client used to communicate with the Redis server.
        log_filenames (set): This is the set of filenames of all files in
            open_file_infos and closed_file_infos.
        open_file_infos (list[LogFileInfo]): Info for all of the open files.
        closed_file_infos (list[LogFileInfo]): Info for all of the closed
            files.
        can_open_more_files (bool): True if we can still open more files and
            false otherwise.
    """

    def __init__(self, logs_dir, redis_address, redis_password=None):
        """Initialize the log monitor object."""
        self.ip = services.get_node_ip_address()
        self.logs_dir = logs_dir
        self.redis_client = ray._private.services.create_redis_client(
            redis_address, password=redis_password)
        self.log_filenames = set()
        self.open_file_infos = []
        self.closed_file_infos = []
        self.can_open_more_files = True

    def close_all_files(self):
        """Close all open files (so that we can open more)."""
        while len(self.open_file_infos) > 0:
            file_info = self.open_file_infos.pop(0)
            file_info.file_handle.close()
            file_info.file_handle = None
            try:
                # Test if the worker process that generated the log file
                # is still alive. Only applies to worker processes.
                if (file_info.worker_pid != "raylet"
                        and file_info.worker_pid != "gcs_server"):
                    os.kill(file_info.worker_pid, 0)
            except OSError:
                # The process is not alive any more, so move the log file
                # out of the log directory so glob.glob will not be slowed
                # by it.
                target = os.path.join(self.logs_dir, "old",
                                      os.path.basename(file_info.filename))
                try:
                    shutil.move(file_info.filename, target)
                except (IOError, OSError) as e:
                    if e.errno == errno.ENOENT:
                        logger.warning(
                            f"Warning: The file {file_info.filename} "
                            "was not found.")
                    else:
                        raise e
            else:
                self.closed_file_infos.append(file_info)
        self.can_open_more_files = True

    def update_log_filenames(self):
        """Update the list of log files to monitor."""
        # output of user code is written here
        log_file_paths = glob.glob(f"{self.logs_dir}/worker*[.out|.err]")
        # segfaults and other serious errors are logged here
        raylet_err_paths = glob.glob(f"{self.logs_dir}/raylet*.err")
        # monitor logs are needed to report autoscaler events
        monitor_log_paths = glob.glob(f"{self.logs_dir}/monitor.log")
        # If gcs server restarts, there can be multiple log files.
        gcs_err_path = glob.glob(f"{self.logs_dir}/gcs_server*.err")
        for file_path in (log_file_paths + raylet_err_paths + gcs_err_path +
                          monitor_log_paths):
            if os.path.isfile(
                    file_path) and file_path not in self.log_filenames:
                job_match = JOB_LOG_PATTERN.match(file_path)
                if job_match:
                    job_id = job_match.group(2)
                    worker_pid = int(job_match.group(3))
                else:
                    job_id = None
                    worker_pid = None

                is_err_file = file_path.endswith("err")

                self.log_filenames.add(file_path)
                self.closed_file_infos.append(
                    LogFileInfo(
                        filename=file_path,
                        size_when_last_opened=0,
                        file_position=0,
                        file_handle=None,
                        is_err_file=is_err_file,
                        job_id=job_id,
                        worker_pid=worker_pid))
                log_filename = os.path.basename(file_path)
                logger.info(f"Beginning to track file {log_filename}")

    def open_closed_files(self):
        """Open some closed files if they may have new lines.

        Opening more files may require us to close some of the already open
        files.
        """
        if not self.can_open_more_files:
            # If we can't open any more files. Close all of the files.
            self.close_all_files()

        files_with_no_updates = []
        while len(self.closed_file_infos) > 0:
            if (len(self.open_file_infos) >=
                    ray_constants.LOG_MONITOR_MAX_OPEN_FILES):
                self.can_open_more_files = False
                break

            file_info = self.closed_file_infos.pop(0)
            assert file_info.file_handle is None
            # Get the file size to see if it has gotten bigger since we last
            # opened it.
            try:
                file_size = os.path.getsize(file_info.filename)
            except (IOError, OSError) as e:
                # Catch "file not found" errors.
                if e.errno == errno.ENOENT:
                    logger.warning(f"Warning: The file {file_info.filename} "
                                   "was not found.")
                    self.log_filenames.remove(file_info.filename)
                    continue
                raise e

            # If some new lines have been added to this file, try to reopen the
            # file.
            if file_size > file_info.size_when_last_opened:
                try:
                    f = open(file_info.filename, "rb")
                except (IOError, OSError) as e:
                    if e.errno == errno.ENOENT:
                        logger.warning(
                            f"Warning: The file {file_info.filename} "
                            "was not found.")
                        self.log_filenames.remove(file_info.filename)
                        continue
                    else:
                        raise e

                f.seek(file_info.file_position)
                file_info.filesize_when_last_opened = file_size
                file_info.file_handle = f
                self.open_file_infos.append(file_info)
            else:
                files_with_no_updates.append(file_info)

        # Add the files with no changes back to the list of closed files.
        self.closed_file_infos += files_with_no_updates

    def check_log_files_and_publish_updates(self):
        """Get any changes to the log files and push updates to Redis.

        Returns:
            True if anything was published and false otherwise.
        """
        anything_published = False
        for file_info in self.open_file_infos:
            assert not file_info.file_handle.closed

            lines_to_publish = []
            max_num_lines_to_read = 100
            for _ in range(max_num_lines_to_read):
                try:
                    next_line = file_info.file_handle.readline()
                    # Replace any characters not in UTF-8 with
                    # a replacement character, see
                    # https://stackoverflow.com/a/38565489/10891801
                    next_line = next_line.decode("utf-8", "replace")
                    if next_line == "":
                        break
                    if next_line[-1] == "\n":
                        next_line = next_line[:-1]
                    lines_to_publish.append(next_line)
                except Exception:
                    logger.error(
                        f"Error: Reading file: {file_info.full_path}, "
                        f"position: {file_info.file_info.file_handle.tell()} "
                        "failed.")
                    raise

            if file_info.file_position == 0:
                if "/raylet" in file_info.filename:
                    file_info.worker_pid = "raylet"
                elif "/gcs_server" in file_info.filename:
                    file_info.worker_pid = "gcs_server"
                elif "/monitor" in file_info.filename:
                    file_info.worker_pid = "autoscaler"

            # Record the current position in the file.
            file_info.file_position = file_info.file_handle.tell()

            if len(lines_to_publish) > 0:
                self.redis_client.publish(
                    ray.gcs_utils.LOG_FILE_CHANNEL,
                    json.dumps({
                        "ip": self.ip,
                        "pid": file_info.worker_pid,
                        "job": file_info.job_id,
                        "is_err": file_info.is_err_file,
                        "lines": lines_to_publish
                    }))
                anything_published = True

        return anything_published

    def run(self):
        """Run the log monitor.

        This will query Redis once every second to check if there are new log
        files to monitor. It will also store those log files in Redis.
        """
        while True:
            self.update_log_filenames()
            self.open_closed_files()
            anything_published = self.check_log_files_and_publish_updates()
            # If nothing was published, then wait a little bit before checking
            # for logs to avoid using too much CPU.
            if not anything_published:
                time.sleep(0.05)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "log monitor to connect "
                     "to."))
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
        "--logging-filename",
        required=False,
        type=str,
        default=ray_constants.LOG_MONITOR_LOG_FILE_NAME,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is "
        f"\"{ray_constants.LOG_MONITOR_LOG_FILE_NAME}\"")
    parser.add_argument(
        "--logs-dir",
        required=True,
        type=str,
        help="Specify the path of the temporary directory used by Ray "
        "processes.")
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is "
        f"{ray_constants.LOGGING_ROTATE_BYTES} bytes.")
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is "
        f"{ray_constants.LOGGING_ROTATE_BACKUP_COUNT}.")
    args = parser.parse_args()
    setup_component_logger(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.logs_dir,
        filename=args.logging_filename,
        max_bytes=args.logging_rotate_bytes,
        backup_count=args.logging_rotate_backup_count)

    log_monitor = LogMonitor(
        args.logs_dir, args.redis_address, redis_password=args.redis_password)

    try:
        log_monitor.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray._private.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = (f"The log monitor on node {platform.node()} "
                   f"failed with the following error:\n{traceback_str}")
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.LOG_MONITOR_DIED_ERROR, message)
        logger.error(message)
        raise e
