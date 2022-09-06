from dataclasses import dataclass

from typing import Dict, Type

from ray.air.execution.resources.request import ResourceRequest, ReadyResource


@dataclass
class ActorRequest:
    cls: Type
    kwargs: Dict
    resources: ResourceRequest

    def __hash__(self):
        return hash(id(self))


@dataclass
class ActorInfo:
    actor_request: ActorRequest
    used_resource: ReadyResource

    def __hash__(self):
        return hash(id(self))
