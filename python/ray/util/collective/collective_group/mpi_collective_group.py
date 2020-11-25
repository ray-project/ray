from collective.collective_group.base_collective_group import BaseGroup

# TODO(Dacheng): implement this
class MPIGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        BaseGroup.__init__(self, world_size, rank, group_name)
