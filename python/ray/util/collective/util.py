"""Some utility class for Collectives."""
import ray
import logging

logger = logging.getLogger(__name__)


@ray.remote
class NCCLUniqueIDStore(object):
    """NCCLUniqueID Store as a named actor."""

    def __init__(self, name):
        self.name = name
        self.nccl_id = None

    def set_id(self, uid):
        self.nccl_id = uid
        return self.nccl_id

    def get_id(self):
        if not self.nccl_id:
            logger.warning(
                "The NCCL ID has not been set yet for store {}".format(
                    self.name))
        return self.nccl_id

@ray.remote
class Info:
    """Store the collective information for groups created through declare_collective_group().
       Should be used as a NamedActor."""

    def __init__(self):
        self.ids = None
        self.world_size = -1
        self.rank = -1
        self.backend = None

    def set_info(self, ids, world_size, rank, backend):
        """Store collective information."""
        self.ids = ids
        self.world_size = world_size
        self.rank = rank
        self.backend = backend

    def get_info(self):
        """Get previously stored collective information."""
        return self.ids, self.world_size, self.rank, self.backend
