import logging
import os
import platform
import sys
import time

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil  # noqa E402

logger = logging.getLogger(__name__)


def get_rss(memory_info):
    """Get the estimated non-shared memory usage from psutil memory_info."""
    mem = memory_info.rss
    # OSX doesn't have the shared attribute
    if hasattr(memory_info, "shared"):
        mem -= memory_info.shared
    return mem


def get_shared(virtual_memory):
    """Get the estimated shared memory usage from psutil virtual mem info."""
    # OSX doesn't have the shared attribute
    if hasattr(virtual_memory, "shared"):
        return virtual_memory.shared
    else:
        return 0


class RayOutOfMemoryError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

    @staticmethod
    def get_message(used_gb, total_gb, threshold):
        pids = psutil.pids()
        proc_stats = []
        for pid in pids:
            proc = psutil.Process(pid)
            proc_stats.append((get_rss(proc.memory_info()), pid,
                               proc.cmdline()))
        proc_str = "PID\tMEM\tCOMMAND"
        for rss, pid, cmdline in sorted(proc_stats, reverse=True)[:10]:
            proc_str += "\n{}\t{}GiB\t{}".format(
                pid, round(rss / (1024**3), 2),
                " ".join(cmdline)[:100].strip())
        return ("More than {}% of the memory on ".format(int(
            100 * threshold)) + "node {} is used ({} / {} GB). ".format(
                platform.node(), round(used_gb, 2), round(total_gb, 2)) +
                "The top 10 memory consumers are:\n\n{}".format(proc_str) +
                "\n\nIn addition, up to {} GiB of shared memory is ".format(
                    round(get_shared(psutil.virtual_memory()) / (1024**3), 2))
                + "currently being used by the Ray object store. You can set "
                "the object store size with the `object_store_memory` "
                "parameter when starting Ray.\n---\n"
                "--- Tip: Use the `ray memory` command to list active "
                "objects in the cluster.\n---\n")


class MemoryMonitor:
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
        self.last_checked = 0
        self.heap_limit = None
        self.worker_name = None
        try:
            self.error_threshold = float(
                os.getenv("RAY_MEMORY_MONITOR_ERROR_THRESHOLD"))
        except (ValueError, TypeError):
            self.error_threshold = error_threshold
        # Try to read the cgroup memory limit if it is available.
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes",
                      "rb") as f:
                self.cgroup_memory_limit_gb = int(f.read()) / (1024**3)
        except IOError:
            self.cgroup_memory_limit_gb = sys.maxsize / (1024**3)
        if not psutil:
            logger.warn("WARNING: Not monitoring node memory since `psutil` "
                        "is not installed. Install this with "
                        "`pip install psutil` (or ray[debug]) to enable "
                        "debugging of memory-related crashes.")

    def set_heap_limit(self, worker_name, limit_bytes):
        self.heap_limit = limit_bytes
        self.worker_name = worker_name

    def raise_if_low_memory(self):
        if time.time() - self.last_checked > self.check_interval:
            if "RAY_DEBUG_DISABLE_MEMORY_MONITOR" in os.environ:
                return  # escape hatch, not intended for user use

            self.last_checked = time.time()
            total_gb = psutil.virtual_memory().total / (1024**3)
            used_gb = total_gb - psutil.virtual_memory().available / (1024**3)
            if self.cgroup_memory_limit_gb < total_gb:
                total_gb = self.cgroup_memory_limit_gb
                with open("/sys/fs/cgroup/memory/memory.usage_in_bytes",
                          "rb") as f:
                    used_gb = int(f.read()) / (1024**3)
                # Exclude the page cache
                with open("/sys/fs/cgroup/memory/memory.stat", "r") as f:
                    for line in f.readlines():
                        if line.split(" ")[0] == "cache":
                            used_gb = \
                                used_gb - int(line.split(" ")[1]) / (1024**3)
                assert used_gb >= 0
            if used_gb > total_gb * self.error_threshold:
                raise RayOutOfMemoryError(
                    RayOutOfMemoryError.get_message(used_gb, total_gb,
                                                    self.error_threshold))
            else:
                logger.debug("Memory usage is {} / {}".format(
                    used_gb, total_gb))

            if self.heap_limit:
                mem_info = psutil.Process(os.getpid()).memory_info()
                heap_size = get_rss(mem_info)
                if heap_size > self.heap_limit:
                    raise RayOutOfMemoryError(
                        "Heap memory usage for {} is {} / {} GiB limit".format(
                            self.worker_name, round(heap_size / (1024**3), 4),
                            round(self.heap_limit / (1024**3), 4)))
                elif heap_size > 0.8 * self.heap_limit:
                    logger.warning(
                        "Heap memory usage for {} is {} / {} GiB limit".format(
                            self.worker_name, round(heap_size / (1024**3), 4),
                            round(self.heap_limit / (1024**3), 4)))
