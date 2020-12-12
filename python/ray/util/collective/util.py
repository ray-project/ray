"""Some utility class for Collectives."""
import ray
import logging

logger = logging.getLogger(__name__)


@ray.remote
class NCCLUniqueIDStore:
    """NCCLUniqueID Store as a named actor class.

    Args:
        name (str): the unique name for this named actor.

    Attributes:
        name (str): the unique name for this named actor.
        nccl_id (str): the NCCLUniqueID held in this store.
    """

    def __init__(self, name):
        self.name = name
        self.nccl_id = None

    def set_id(self, uid):
        """
        Initialize the NCCL unique ID for this store.

        Args:
            uid (str): the unique ID generated via the NCCL get_unique_id API.

        Returns:
            None
        """
        self.nccl_id = uid
        return self.nccl_id

    def get_id(self):
        """Get the NCCL unique ID held in this store."""
        if not self.nccl_id:
            logger.warning("The NCCL ID has not been "
                           "set yet for store {}.".format(self.name))
        return self.nccl_id
