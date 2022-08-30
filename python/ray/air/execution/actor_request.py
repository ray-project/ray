from dataclasses import dataclass

from typing import Dict, Type

import ray
from ray.air.execution.resources.request import ResourceRequest


@dataclass
class ActorRequest:
    cls: Type
    kwargs: Dict
    resources: ResourceRequest


@dataclass
class ActorInfo:
    request: ActorRequest
    actor: ray.ActorID
