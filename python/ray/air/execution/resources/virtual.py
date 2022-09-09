from typing import Dict, List

from dataclasses import dataclass

from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import ReadyResource


@dataclass
class VirtualReadyResource(ReadyResource):
    bundles: List[Dict[str, float]]

    def annotate_remote_objects(self, objects):
        return objects[0]


class VirtualResourceManager(FixedResourceManager):
    _resource_cls: ReadyResource = VirtualReadyResource
