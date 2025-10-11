from dataclasses import dataclass
from typing import List

import ray
from ray.util.collective.types import Backend


@dataclass
class Communicator:
    """
    A handle to a communicator that we are a member of.
    """

    # The name of the communicator.
    name: str
    # Our rank in the collective group.
    rank: int
    # A valid backend, as defined by
    # ray.util.collective.types.Backend.
    backend: str


class CommunicatorHandle:
    """
    A communicator handle used by the driver to store handles to the
    actors in the communicator.
    """

    def __init__(self, actors: List[ray.actor.ActorHandle], name: str, backend: str):
        """
        Initializes the CommunicatorHandle with the given actor handles.
        Assumes that the communicator has already been initialized on all actors.

        Args:
            actors: A list of actor handles to be stored.
            name: Name of the communicator.
            backend: Communicator backend. See
                ray.util.collective.types for valid values.
        """
        self._actors = actors
        self._name = name
        self._backend = Backend(backend)

    def get_rank(self, actor: ray.actor.ActorHandle):
        for i, a in enumerate(self._actors):
            if a == actor:
                return i
        return -1

    @property
    def actors(self) -> List[ray.actor.ActorHandle]:
        """
        Return all actor handles in this communicator.
        """
        return self._actors[:]

    @property
    def name(self) -> str:
        return self._name

    @property
    def backend(self) -> str:
        return self._backend
