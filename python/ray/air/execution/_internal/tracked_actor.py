from typing import Callable


class TrackedActor:
    """Actor tracked by an actor manager.

    This object is used to reference a Ray actor on an actor manager

    Existence of this object does not mean that the Ray actor has already been started.
    Actor state can be inquired from the actor manager tracking the Ray actor.

    Note:
        Objects of this class are returned by the :class:`RayEventManager`.
        This class should not be instantiated manually.

    Attributes:
        actor_id: ID for identification of the actor within the actor manager. This
            ID is not related to the Ray actor ID.

    """

    def __init__(self, actor_id: int):
        self.actor_id = actor_id
        self._on_start = None
        self._on_stop = None
        self._on_error = None

    def on_start(self, callback: Callable[["TrackedActor"], None]) -> "TrackedActor":
        """Set callback to invoke when actor started."""
        self._on_start = callback
        return self

    def on_stop(self, callback: Callable[["TrackedActor"], None]) -> "TrackedActor":
        """Set callback to invoke when actor stopped gracefully."""
        self._on_stop = callback
        return self

    def on_error(
        self, callback: Callable[["TrackedActor", Exception], None]
    ) -> "TrackedActor":
        """Set callback to invoke when actor died."""
        self._on_error = callback
        return self
