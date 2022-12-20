from dataclasses import dataclass

from typing import Dict, Type

from ray.air.execution.resources.request import (
    ResourceRequest,
    AcquiredResources,
)


@dataclass
class ActorRequest:
    """Request for an actor to be started.

    This class is used with the ``ActorManager`` to request an actor to be started.
    The actor manager will request the required resources. Once available, the
    actor will be scheduled. Specifically, ``cls`` will be converted into a remote
    Ray object and scheduled on the resources requested in the ``resource_request``.
    The remote object will be initialized with ``kwargs`` as the constructor
    arguments.

    Contrary to a resource request, an actor request is always unique.
    Even if two actor requests have been created with the same arguments,
    they will not be considered the same.

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
        return hash(id(self))

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


@dataclass
class ActorInfo:
    """Info class for a scheduled actor tracked by an actor manager.

    This object is made available through the ``ActorStarted``, ``ActorStopped``,
    and ``ActorFailed`` events.

    Attributes:
        actor_id: Hexadecimal actor ID.
        actor_request: Request used to start this actor.
        acquired_resources: Resources the actor has been scheduled on.

    """

    actor_id: str
    actor_request: ActorRequest
    acquired_resources: AcquiredResources

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()
