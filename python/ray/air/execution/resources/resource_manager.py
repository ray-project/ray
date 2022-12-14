import abc

from typing import List, Optional

import ray
from ray.air.execution.resources.request import (
    ResourceRequest,
    AllocatedResource,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class ResourceManager(abc.ABC):
    """Resource manager interface.

    A resource manager can be used to request resources from a Ray cluster and
    allocate them to remote Ray tasks.

    Because Ray clusters can autoscale, we need to request resources before
    we can allocate them. Effectively this means we have to keep track
    of requested resources (which can trigger autoscaling). Once the requested
    resources are available to be scheduled, the control loop can acquire them.

    This separation also ensures that requests can be cancelled before they resolve
    if resources are not needed anymore.

    The flow is as follows:

    .. code-block:: python

        # Create resource manager
        resource_manager = ResourceManager()

        # Create resource request
        resource_request = ResourceRequest([{"CPU": 4}])

        # Pass to resource manager
        resource_manager.request_resources(resource_request)

        # Wait until ready
        while not resource_manager.has_resources_ready(resource_request):
            time.sleep(1)

        # Once ready, acquire resources
        allocated_resources = resource_manager.acquire_resources(resource_request)

        # Bind to remote task or actor
        annotated_remote_fn = allocated_resources.annotate_remote_objects(
            [remote_fn])

        # Run remote function. This will use the acquired resources
        ray.get(annotated_remote_fn.remote())

        # After using the resources, free
        resource_manager.free_resources(annotated_resources)

    """

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

    def free_resources(self, allocated_resources: AllocatedResource):
        """Free resources from usage."""
        raise NotImplementedError

    def update_state(self):
        """Reconcile internal state with cluster state.

        Depending on the backend, resource futures can be awaited externally
        (with ``get_resource_futures()``). If such a future resolved, the outer
        control loop can instruct the resource manager to update its internal state.
        """
        pass

    def clear(self):
        """Reset and clear all used resources."""
        raise NotImplementedError

    def __reduce__(self):
        """We disallow serialization.

        Shared resource managers should live on an actor.
        """
        raise ValueError(
            f"Resource managers cannot be serialized." f"Resource manager: {str(self)}"
        )
