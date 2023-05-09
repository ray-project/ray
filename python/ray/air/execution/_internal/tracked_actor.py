from typing import Callable, Optional


class TrackedActor:
    """Actor tracked by an actor manager.

    This object is used to reference a Ray actor on an actor manager

    Existence of this object does not mean that the Ray actor has already been started.
    Actor state can be inquired from the actor manager tracking the Ray actor.

    Note:
        Objects of this class are returned by the :class:`RayActorManager`.
        This class should not be instantiated manually.

    Attributes:
        actor_id: ID for identification of the actor within the actor manager. This
            ID is not related to the Ray actor ID.

    """

    def __init__(
        self,
        actor_id: int,
        on_start: Optional[Callable[["TrackedActor"], None]] = None,
        on_stop: Optional[Callable[["TrackedActor"], None]] = None,
        on_error: Optional[Callable[["TrackedActor", Exception], None]] = None,
    ):
        self.actor_id = actor_id
        self._on_start = on_start
        self._on_stop = on_stop
        self._on_error = on_error

    def set_on_start(self, on_start: Optional[Callable[["TrackedActor"], None]]):
        self._on_start = on_start

    def set_on_stop(self, on_stop: Optional[Callable[["TrackedActor"], None]]):
        self._on_stop = on_stop

    def set_on_error(
        self, on_error: Optional[Callable[["TrackedActor", Exception], None]]
    ):
        self._on_error = on_error

    def __repr__(self):
        return f"<TrackedActor {self.actor_id}>"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.actor_id == other.actor_id

    def __hash__(self):
        return hash(self.actor_id)
