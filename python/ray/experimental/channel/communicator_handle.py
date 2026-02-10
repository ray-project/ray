from typing import List

import ray


class CommunicatorHandle:
    """
    A lightweight communicator handle used by the driver to store handles to
    the actors in the communicator.
    """

    def __init__(
        self,
        actor_handles: List["ray.actor.ActorHandle"],
    ):
        """
        Initializes the CommunicatorHandle with the given actor handles.

        Args:
            actor_handles: A list of actor handles to be stored.
        """
        self._actor_handles = actor_handles

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        """
        Retuan all actor handles in this communicator.
        """
        return self._actor_handles
