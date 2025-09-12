"""Some utility class for Collectives."""
import asyncio
import logging

import ray

logger = logging.getLogger(__name__)


@ray.remote
class NCCLUniqueIDStore:
    """NCCLUniqueID Store as a named actor class.

    Args:
        name: the unique name for this named actor.

    Attributes:
        name: the unique name for this named actor.
        nccl_id: the NCCLUniqueID held in this store.
    """

    def __init__(self, name):
        self.name = name
        self.nccl_id = None
        self.event = asyncio.Event()

    async def set_id(self, uid):
        """
        Initialize the NCCL unique ID for this store.

        Args:
            uid: the unique ID generated via the NCCL generate_communicator_id API.

        Returns:
            The NCCL unique ID set.
        """
        self.nccl_id = uid
        self.event.set()
        return uid

    async def wait_and_get_id(self):
        """Wait for the NCCL unique ID to be set and return it."""
        await self.event.wait()
        return self.nccl_id

    def get_id(self):
        """Get the NCCL unique ID held in this store."""
        if not self.nccl_id:
            logger.warning(
                "The NCCL ID has not been set yet for store {}.".format(self.name)
            )
        return self.nccl_id


@ray.remote
class Info:
    """Store the group information created via `create_collective_group`.

    Note: Should be used as a NamedActor.
    """

    def __init__(self):
        self.ids = None
        self.world_size = -1
        self.rank = -1
        self.backend = None
        self.gloo_timeout = 30000

    def set_info(self, ids, world_size, rank, backend, gloo_timeout):
        """Store collective information."""
        self.ids = ids
        self.world_size = world_size
        self.rank = rank
        self.backend = backend
        self.gloo_timeout = gloo_timeout

    def get_info(self):
        """Get previously stored collective information."""
        return (
            self.ids,
            self.world_size,
            self.rank,
            self.backend,
            self.gloo_timeout,
        )
