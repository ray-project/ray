from typing import Callable

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class TrackedActor:
    """Actor tracked by an actor manager.

    This object is used to reference a Ray actor on an actor manager

    Existence of this object does not mean that the Ray actor has already been started.
    Actor state can be inquired from the actor manager tracking the Ray actor.

    Attributes:
        actor_id: ID for identification of the actor within the actor manager. This
            ID is not related to the Ray actor ID.

    """

    def __init__(self, actor_id: int):
        self.actor_id = actor_id

    def on_start(self, callback: Callable[["TrackedActor"], None]) -> "TrackedActor":
        """Set callback to invoke when actor started."""
        raise NotImplementedError

    def on_stop(self, callback: Callable[["TrackedActor"], None]) -> "TrackedActor":
        """Set callback to invoke when actor stopped gracefully."""
        raise NotImplementedError

    def on_error(
        self, callback: Callable[["TrackedActor", Exception], None]
    ) -> "TrackedActor":
        """Set callback to invoke when actor died."""
        raise NotImplementedError
