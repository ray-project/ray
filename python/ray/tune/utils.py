from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os

def gpu_count():
    if os.path.exists("/proc/driver/nvidia/gpus"):
        return len(os.listdir("/proc/driver/nvidia/gpus"))
    return 0