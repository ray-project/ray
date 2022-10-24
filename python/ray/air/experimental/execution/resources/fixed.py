from typing import Dict, Optional, List, Union

from dataclasses import dataclass

import ray
from ray import SCRIPT_MODE
from ray.air.experimental.execution.resources.request import (
    ResourceRequest,
    AllocatedResource,
)
from ray.air.experimental.execution.resources.resource_manager import ResourceManager


# Avoid numerical errors by multiplying and subtracting with this number.
# Compare: 0.99 - 0.33 vs (0.99 * 1000 - 0.33 * 1000) / 1000
_DIGITS = 100000


def _sum_bundle_resources(bundles: List[Dict[str, float]]) -> Dict[str, float]:
    all_resources = {}
    for resources in bundles:
        for k, v in resources.items():
            all_resources[k] = all_resources.get(k, 0) + v

    return all_resources


@dataclass
class FixedAllocatedResource(AllocatedResource):
    bundles: List[Dict[str, float]]

    def annotate_remote_objects(
        self, objects
    ) -> List[Union[ray.ObjectRef, ray.actor.ActorHandle]]:
        if len(objects) == 1:
            all_resources = _sum_bundle_resources(self.bundles)
            num_cpus = all_resources.pop("CPU", 0)
            num_gpus = all_resources.pop("GPU", 0)

            return [
                objects[0].options(
                    num_cpus=num_cpus, num_gpus=num_gpus, resources=all_resources
                )
            ]

        if len(objects) != len(self.bundles):
            raise RuntimeError(
                f"Cannot annotate {len(objects)} remote objects with "
                f"{len(self.bundles)} bundles (lengths must be the same)."
            )

        annotated = []
        for obj, bundle in zip(objects, self.bundles):
            bundle = bundle.copy()
            num_cpus = bundle.pop("CPU", 0)
            num_gpus = bundle.pop("GPU", 0)

            annotated.append(
                obj.options(num_cpus=num_cpus, num_gpus=num_gpus, resources=bundle)
            )
        return annotated


class FixedResourceManager(ResourceManager):
    _resource_cls: AllocatedResource = FixedAllocatedResource

    def __init__(self, total_resources: Optional[Dict[str, float]] = None):
        if not total_resources:
            rtc = ray.get_runtime_context()
            if rtc.worker.mode in {None, SCRIPT_MODE}:
                total_resources = ray.available_resources()
            else:
                total_resources = rtc.get_assigned_resources()

        self._total_resources = total_resources
        self._used_resources = []

    @property
    def _available_resources(self) -> Dict[str, float]:
        available_resources = self._total_resources.copy()
        for used_resources in self._used_resources:
            all_resources = _sum_bundle_resources(used_resources.bundles)
            for k, v in all_resources.items():
                available_resources[k] = (
                    available_resources[k] * _DIGITS - v * _DIGITS
                ) / _DIGITS
        return available_resources

    def request_resources(self, resource_request: ResourceRequest):
        pass

    def cancel_resource_request(self, resource_request: ResourceRequest):
        pass

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        available_resources = self._available_resources
        all_resources = _sum_bundle_resources(resource_request.bundles)
        for k, v in all_resources.items():
            if available_resources[k] < v:
                return False
        return True

    def acquire_resources(
        self, resource_request: ResourceRequest
    ) -> Optional[AllocatedResource]:
        if not self.has_resources_ready(resource_request):
            return None

        self._used_resources.append(resource_request)
        return self._resource_cls(
            bundles=resource_request.bundles, resource_request=resource_request
        )

    def return_resources(
        self, allocated_resources: AllocatedResource, cancel_request: bool = True
    ):
        resources = allocated_resources.resource_request
        self._used_resources.remove(resources)
