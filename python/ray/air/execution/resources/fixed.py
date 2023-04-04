from typing import Dict, List, Optional

from dataclasses import dataclass

import ray
from ray import SCRIPT_MODE, LOCAL_MODE
from ray.air.execution.resources.request import (
    ResourceRequest,
    AcquiredResources,
    RemoteRayEntity,
)
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.util.annotations import DeveloperAPI


# Avoid numerical errors by multiplying and subtracting with this number.
# Compare: 0.99 - 0.33 = 0.65999... vs (0.99 * 1000 - 0.33 * 1000) / 1000 = 0.66
_DIGITS = 100000


@DeveloperAPI
@dataclass
class FixedAcquiredResources(AcquiredResources):
    bundles: List[Dict[str, float]]

    def _annotate_remote_entity(
        self, entity: RemoteRayEntity, bundle: Dict[str, float], bundle_index: int
    ) -> RemoteRayEntity:
        bundle = bundle.copy()
        num_cpus = bundle.pop("CPU", 0)
        num_gpus = bundle.pop("GPU", 0)
        memory = bundle.pop("memory", 0.0)

        return entity.options(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            resources=bundle,
        )


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

    The fixed resource manager does not support placement strategies. Using
    ``STRICT_SPREAD`` will result in an error. ``STRICT_PACK`` will succeed only
    within a placement group bundle. All other placement group arguments will be
    ignored.

    Args:
        total_resources: Budget of resources to manage. Defaults to all available
            resources in the current task or all cluster resources (if outside a task).

    """

    _resource_cls: AcquiredResources = FixedAcquiredResources

    def __init__(self, total_resources: Optional[Dict[str, float]] = None):
        rtc = ray.get_runtime_context()

        if not total_resources:
            if rtc.worker.mode in {None, SCRIPT_MODE, LOCAL_MODE}:
                total_resources = ray.cluster_resources()
            else:
                total_resources = rtc.get_assigned_resources()

        # If we are in a placement group, all of our resources will be in a bundle
        # and thus fulfill requirements of STRICT_PACK - but only if child tasks
        # are captured by the pg.
        self._allow_strict_pack = (
            ray.util.get_current_placement_group() is not None
            and rtc.should_capture_child_tasks_in_placement_group
        )

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
        if resource_request.strategy == "STRICT_SPREAD" or (
            not self._allow_strict_pack and resource_request.strategy == "STRICT_PACK"
        ):
            raise RuntimeError(
                f"Requested a resource with placement strategy "
                f"{resource_request.strategy}, but this cannot be fulfilled by a "
                f"FixedResourceManager. In a nested setting, please set the inner "
                f"placement strategy to be less restrictive (i.e. no STRICT_ strategy)."
            )

        self._requested_resources.append(resource_request)

    def cancel_resource_request(self, resource_request: ResourceRequest):
        self._requested_resources.remove(resource_request)

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        if resource_request not in self._requested_resources:
            return False

        available_resources = self._available_resources
        all_resources = resource_request.required_resources
        for k, v in all_resources.items():
            if available_resources.get(k, 0.0) < v:
                return False
        return True

    def acquire_resources(
        self, resource_request: ResourceRequest
    ) -> Optional[AcquiredResources]:
        if not self.has_resources_ready(resource_request):
            return None

        self._used_resources.append(resource_request)
        return self._resource_cls(
            bundles=resource_request.bundles, resource_request=resource_request
        )

    def free_resources(self, acquired_resource: AcquiredResources):
        resources = acquired_resource.resource_request
        self._used_resources.remove(resources)

    def clear(self):
        # Reset internal state
        self._requested_resources = []
        self._used_resources = []
