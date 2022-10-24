import abc

from typing import List, Optional

import ray
from ray.air.experimental.execution.resources.request import (
    ResourceRequest,
    AllocatedResource,
)


class ResourceManager(abc.ABC):
    def get_resource_futures(self) -> List[ray.ObjectRef]:
        """Return futures for resources to await."""
        return []

    def request_resources(self, resource_request: ResourceRequest):
        """Request resources, e.g. schedule placement group."""
        raise NotImplementedError

    def cancel_resource_request(self, resource_request: ResourceRequest):
        """Cancel resource request, e.g. remove placement group."""
        raise NotImplementedError

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        """Returns True if resources for the given request are available"""
        raise NotImplementedError

    def acquire_resources(
        self, resource_request: ResourceRequest
    ) -> Optional[AllocatedResource]:
        """Acquire resources. Returns None if resources are not available."""
        raise NotImplementedError

    def return_resources(
        self, allocated_resources: AllocatedResource, cancel_request: bool = True
    ):
        """Return resources to resource pool."""
        raise NotImplementedError
