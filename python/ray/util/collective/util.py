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
                'The NCCL ID has not been set yet for store {}'.format(
                    self.name))
        return self.nccl_id
