from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import subprocess
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
        try:
            ps_output = subprocess.check_output(
                "ps -eo pid,user,%mem,args --sort -%mem | "
                "head -n 6 | cut -c-200",
                shell=True)
            ps_output = ps_output.decode(sys.stdout.encoding).strip()
        except Exception as e:
            ps_output = str(e)
        return (
            "More than {}% of the memory on ".format(
                int(100 * threshold)) +
            "node {} is used ({} / {} GB). ".format(
                os.uname()[1], round(used_gb, 1), round(total_gb, 1)) +
            "The top 5 memory consumers are:\n\n{}".format(ps_output))


class MemoryMonitor(object):
    def __init__(self, error_threshold=0.95, check_interval=5):
        self.error_threshold = error_threshold
        self.last_checked = time.time()
        self.check_interval = check_interval
        if not psutil:
            logger.warning(
                "WARNING: Not monitoring node memory since `psutil` is not "
                "installed. Install this with `pip install psutil` to enable "
                "debugging of memory-related crashes.")
            

    def raise_if_low_memory(self):
        if not psutil:
            return
        if time.time() - self.last_checked > self.check_interval:
            self.last_checked = time.time()
            total_gb = psutil.virtual_memory().total / 1e9
            used_gb = total_gb - psutil.virtual_memory().available / 1e9
            if used_gb > total_gb * self.error_threshold:
                raise RayOutOfMemoryError(
                    RayOutOfMemoryError.get_message(
                        used_gb, total_gb, self.error_threshold))
            else:
                logger.debug(
                    "Memory usage is {} / {}".format(used_gb, total_gb))
