import abc

from typing import List, Optional

import ray
from ray.air.execution.resources.request import ResourceRequest, ReadyResource


class ResourceManager(abc.ABC):
    def get_resource_futures(self) -> List[ray.ObjectRef]:
        """Return futures for resources to await."""
        return []

    def request_resources(self, resources: ResourceRequest):
        """Request resources, e.g. schedule placement group."""
        raise NotImplementedError

    def cancel_resource_request(self, resources: ResourceRequest):
        """Request resources, e.g. schedule placement group."""
        raise NotImplementedError

    def has_resources_ready(self, resources: ResourceRequest) -> bool:
        """Returns True if resources for the given request are available"""
        raise NotImplementedError

    def acquire_resources(self, resources: ResourceRequest) -> Optional[ReadyResource]:
        """Acquire resources. Returns None if resources are not available."""
        raise NotImplementedError

    def return_resources(self, ready_resources: ReadyResource):
        """Return resources to resource pool."""
        raise NotImplementedError
