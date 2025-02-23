import ctypes
import logging

import torch_npu

from ray.util.collective.collective_group.base_collective_group import BaseGroup

logger = logging.getLogger(__file__)

libhccl = None
try:
    libhccl = ctypes.CDLL("libhccl.so")
except OSError:
    pass


class NCCLGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        pass
