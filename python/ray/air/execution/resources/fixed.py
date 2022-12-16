from typing import Dict, List, Optional, Union

from dataclasses import dataclass

import ray
from ray import SCRIPT_MODE, LOCAL_MODE
from ray.air.execution.resources.request import (
    ResourceRequest,
    AllocatedResource,
    RemoteRayObject,
)
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.util.annotations import DeveloperAPI


# Avoid numerical errors by multiplying and subtracting with this number.
# Compare: 0.99 - 0.33 vs (0.99 * 1000 - 0.33 * 1000) / 1000
_DIGITS = 100000


@DeveloperAPI
@dataclass
class FixedAllocatedResource(AllocatedResource):
    bundles: List[Dict[str, float]]

    def annotate_remote_objects(
        self, objects: List[RemoteRayObject]
    ) -> List[Union[RemoteRayObject]]:
        # If we have an empty head, we schedule the first object (the "head") with
        # empty resources.
        if self.resource_request.head_bundle_is_empty:
            bundles = [{}] + self.resource_request.bundles
        else:
            bundles = self.resource_request.bundles

        if len(objects) > len(bundles):
            raise RuntimeError(
                f"The number of objects to annotate ({len(objects)}) cannot "
                f"exceed the number of available bundles ({len(bundles)})."
            )

        annotated = []
        for i, (obj, bundle) in enumerate(zip(objects, bundles)):
            bundle = bundle.copy()
            num_cpus = bundle.pop("CPU", 0)
            num_gpus = bundle.pop("GPU", 0)
            memory = bundle.pop("memory", 0.0)

            annotated.append(
                obj.options(
                    num_cpus=num_cpus,
                    num_gpus=num_gpus,
                    memory=memory,
                    resources=bundle,
                )
            )
        return annotated


@DeveloperAPI
class FixedResourceManager(ResourceManager):
    """Fixed budget based resource manager.

    This resource manager keeps track of a fixed set of resources. When resources
    are acquired, they are subtracted from the budget. When resources are freed,
    they are added back to the budget.

    The resource manager still requires resources to be requested before they become
    available. However, because the resource requests are virtual, this will not
    trigger autoscaling.

    Additionally, resources are not reserved on request, only on acquisition. Thus,
    acquiring a resource can change the availability of other requests. Note that
    this behavior may be changed in future implementations.

    Args:
        total_resources: Budget of resources to manage. Defaults to all available
            resources in the current task or all cluster resources (if outside a task).

    """

    _resource_cls: AllocatedResource = FixedAllocatedResource

    def __init__(self, total_resources: Optional[Dict[str, float]] = None):
        if not total_resources:
            rtc = ray.get_runtime_context()
            if rtc.worker.mode in {None, SCRIPT_MODE, LOCAL_MODE}:
                total_resources = ray.available_resources()
            else:
                total_resources = rtc.get_assigned_resources()

        self._total_resources = total_resources
        self._requested_resources = []
        self._used_resources = []

    @property
    def _available_resources(self) -> Dict[str, float]:
        available_resources = self._total_resources.copy()

        for used_resources in self._used_resources:
            all_resources = used_resources.required_resources
            for k, v in all_resources.items():
                available_resources[k] = (
                    available_resources[k] * _DIGITS - v * _DIGITS
                ) / _DIGITS
        return available_resources

    def request_resources(self, resource_request: ResourceRequest):
        self._requested_resources.append(resource_request)

    def cancel_resource_request(self, resource_request: ResourceRequest):
        self._requested_resources.remove(resource_request)

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        if resource_request not in self._requested_resources:
            return False

        available_resources = self._available_resources
        all_resources = resource_request.required_resources
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

    def free_resources(self, allocated_resources: AllocatedResource):
        resources = allocated_resources.resource_request
        self._used_resources.remove(resources)

    def clear(self):
        pass
