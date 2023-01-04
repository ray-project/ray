import json
from dataclasses import dataclass

from typing import Dict, Type

from ray.air.execution.resources.request import ResourceRequest


@dataclass
class ActorSpec:
    """Specification of an actor.

    This class is used with the ``ActorManager`` to specify actor properties. These
    are used to schedule the actual Ray actor once the resources are available.

    The actor manager will request the required resources. Once available, the
    actor will be scheduled. Specifically, ``cls`` will be converted into a remote
    Ray object and scheduled on the resources requested in the ``resource_request``.
    The remote object will be initialized with ``kwargs`` as the constructor
    arguments.

    Attributes:
        cls: Actor class to be scheduled. This should not be a regular class
            (not a remote Ray task).
        kwargs: Keyword arguments passed to the remote actor on initialization.
        resource_request: Resources required for the actor to run.
    """

    cls: Type
    kwargs: Dict
    resource_request: ResourceRequest

    def __hash__(self):
        if not hasattr(self, "_hash"):
            # Cache hash
            _hash = hash(
                json.dumps(
                    self.__dict__,
                    sort_keys=True,
                    indent=0,
                    ensure_ascii=True,
                )
            )
            setattr(self, "_hash", _hash)

        return getattr(self, "_hash")

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@dataclass
class TrackedActor:
    """Actor tracked by an actor manager.

    This object is returned when an actor is added. It is also returned as part of
    the ``ActorStarted``, ``ActorStopped``, and ``ActorFailed`` events.

    Existence of this object does not mean that the Ray actor has already been started.
    Actor state can be inquired from the actor manager tracking the Ray actor.

    Attributes:
        actor_id: ID for identification of the actor within the actor manager. This
            ID is not related to the Ray actor ID.

    """

    actor_id: int

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return self.actor_id == other.actor_id
