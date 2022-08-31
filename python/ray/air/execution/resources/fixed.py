from typing import Dict, Optional, List

from dataclasses import dataclass

from ray.air.execution.resources.request import ResourceRequest, ReadyResource
from ray.air.execution.resources.resource_manager import ResourceManager


def _sum_bundle_resources(bundles: List[Dict[str, float]]) -> Dict[str, float]:
    all_resources = {}
    for resources in bundles:
        for k, v in resources.items():
            all_resources[k] = all_resources.get(k, 0) + v

    return all_resources


@dataclass
class FixedReadyResource(ReadyResource):
    bundles: List[Dict[str, float]]

    def annotate_remote_objects(self, objects):
        all_resources = _sum_bundle_resources(self.bundles)
        num_cpus = all_resources.pop("CPU", 0)
        num_gpus = all_resources.pop("GPU", 0)
        return objects[0].options(
            num_cpus=num_cpus, num_gpus=num_gpus, resources=all_resources
        )


class FixedResourceManager(ResourceManager):
    def __init__(self, total_resources: Dict[str, float]):
        self._total_resources = total_resources
        self._used_resources = []

    @property
    def _available_resources(self) -> Dict[str, float]:
        available_resources = self._total_resources.copy()
        for used_resources in self._used_resources:
            all_resources = _sum_bundle_resources(used_resources.bundles)
            for k, v in all_resources.items():
                available_resources[k] -= v
        return available_resources

    def request_resources(self, resources: ResourceRequest):
        pass

    def has_resources_ready(self, resources: ResourceRequest) -> bool:
        available_resources = self._available_resources
        all_resources = _sum_bundle_resources(resources.bundles)
        for k, v in all_resources.items():
            if available_resources[k] < v:
                return False
        return True

    def acquire_resources(
        self, resources: ResourceRequest
    ) -> Optional[FixedReadyResource]:
        if not self.has_resources_ready(resources):
            return None

        self._used_resources.append(resources)
        return FixedReadyResource(bundles=resources.bundles, request=resources)

    def return_resources(self, ready_resources: ReadyResource):
        resources = ready_resources.request
        self._used_resources.remove(resources)
