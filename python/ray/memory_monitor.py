from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import time

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


class RayOutOfMemoryError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

    @staticmethod
    def get_message(used_gb, total_gb, threshold):
        pids = psutil.pids()
        proc_stats = []
        for pid in pids:
            proc = psutil.Process(pid)
            proc_stats.append(
                (proc.memory_info().rss - proc.memory_info().shared, pid,
                 proc.cmdline()))
        proc_str = "PID\tMEM\tCOMMAND"
        for rss, pid, cmdline in sorted(proc_stats, reverse=True)[:10]:
            proc_str += "\n{}\t{}GB\t{}".format(
                pid, round(rss / 1e9, 2), " ".join(cmdline)[:100].strip())
        return ("More than {}% of the memory on ".format(int(
            100 * threshold)) + "node {} is used ({} / {} GB). ".format(
                os.uname()[1], round(used_gb, 2), round(total_gb, 2)) +
                "The top 10 memory consumers are:\n\n{}".format(proc_str) +
                "\n\nIn addition, up to {} GB of shared memory is ".format(
                    round(psutil.virtual_memory().shared / 1e9, 2)) +
                "currently being used by the Ray object store. You can set "
                "the object store size with the `object_store_memory` "
                "parameter when starting Ray, and the max Redis size with "
                "`redis_max_memory`. Note that Ray assumes all system "
                "memory is available for use by workers. If your system "
                "has other applications running, you should manually set "
                "these memory limits to a lower value.")


class MemoryMonitor(object):
    """Helper class for raising errors on low memory.

    This presents a much cleaner error message to users than what would happen
    if we actually ran out of memory.

    The monitor tries to use the cgroup memory limit and usage if it is set
    and available so that it is more reasonable inside containers. Otherwise,
    it uses `psutil` to check the memory usage.

    The environment variable `RAY_MEMORY_MONITOR_ERROR_THRESHOLD` can be used
    to overwrite the default error_threshold setting.
    """

    def __init__(self, error_threshold=0.95, check_interval=1):
        # Note: it takes ~50us to check the memory usage through psutil, so
        # throttle this check at most once a second or so.
        self.check_interval = check_interval
        self.last_checked = time.time()
        try:
            self.error_threshold = float(
                os.getenv("RAY_MEMORY_MONITOR_ERROR_THRESHOLD"))
        except (ValueError, TypeError):
            self.error_threshold = error_threshold
        # Try to read the cgroup memory limit if it is available.
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes",
                      "rb") as f:
                self.cgroup_memory_limit_gb = int(f.read()) / 1e9
        except IOError:
            self.cgroup_memory_limit_gb = sys.maxsize / 1e9
        if not psutil:
            print("WARNING: Not monitoring node memory since `psutil` is not "
                  "installed. Install this with `pip install psutil` "
                  "(or ray[debug]) to enable debugging of memory-related "
                  "crashes.")

    def raise_if_low_memory(self):
        if not psutil:
            return  # nothing we can do

        if "RAY_DEBUG_DISABLE_MEMORY_MONITOR" in os.environ:
            return  # escape hatch, not intended for user use

        if time.time() - self.last_checked > self.check_interval:
            self.last_checked = time.time()
            total_gb = psutil.virtual_memory().total / 1e9
            used_gb = total_gb - psutil.virtual_memory().available / 1e9
            if self.cgroup_memory_limit_gb < total_gb:
                total_gb = self.cgroup_memory_limit_gb
                with open("/sys/fs/cgroup/memory/memory.usage_in_bytes",
                          "rb") as f:
                    used_gb = int(f.read()) / 1e9
            if used_gb > total_gb * self.error_threshold:
                raise RayOutOfMemoryError(
                    RayOutOfMemoryError.get_message(used_gb, total_gb,
                                                    self.error_threshold))
            else:
                logger.debug("Memory usage is {} / {}".format(
                    used_gb, total_gb))
