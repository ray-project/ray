from dataclasses import dataclass

from typing import Dict, Type

from ray.air.experimental.execution.resources.request import (
    ResourceRequest,
    AllocatedResource,
)


@dataclass
class ActorRequest:
    cls: Type
    kwargs: Dict
    resource_request: ResourceRequest

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


@dataclass
class ActorInfo:
    actor_request: ActorRequest
    allocated_resource: AllocatedResource

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()
