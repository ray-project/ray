from collective.collective_group.nccl_util import *
from collective.collective_group.base_collective_group import BaseGroup

# TODO(Hao): implement this
class NCCLGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        BaseGroup.__init__(self, world_size, rank, group_name)

    def
