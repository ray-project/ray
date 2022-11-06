from typing import Dict, List, Union

from dataclasses import dataclass

import ray
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import AllocatedResource
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class VirtualAllocatedResource(AllocatedResource):
    bundles: List[Dict[str, float]]

    def annotate_remote_objects(
        self, objects
    ) -> List[Union[ray.ObjectRef, ray.actor.ActorHandle]]:
        return objects


@DeveloperAPI
class VirtualResourceManager(FixedResourceManager):
    _resource_cls: AllocatedResource = VirtualAllocatedResource
