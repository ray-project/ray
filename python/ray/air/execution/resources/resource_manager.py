import abc

from typing import List, Optional

import ray
from ray.air.execution.resources.request import (
    ResourceRequest,
    AcquiredResource,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class ResourceManager(abc.ABC):
    """Resource manager interface.

    A resource manager can be used to request resources from a Ray cluster and
    allocate them to remote Ray tasks or actors.

    Resources have to be requested before they can be acquired.

    Resources managed by the resource manager can be in three states:

    1. "Requested":  The resources have been requested but are not yet available to
       schedule remote Ray objects. The resource request may trigger autoscaling,
       and can be cancelled if no longer needed.
    2. "Ready": The requested resources are now available to schedule remote Ray
       objects. They can be acquired and subsequently used remote Ray objects.
       The resource request can still be cancelled if no longer needed.
    3. "Acquired": The resources have been acquired by a caller to use for scheduling
       remote Ray objects. Note that it is the responsibility of the caller to
       schedule the Ray objects with these resources.
       The associated resource request has been removed and can no longer be cancelled.
       The acquired resources can be returned to the resource manager when they are no
       longer used.

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
        acquired_resource = resource_manager.acquire_resources(resource_request)

        # Bind to remote task or actor
        annotated_remote_fn = acquired_resource.annotate_remote_objects(
            [remote_fn])

        # Run remote function. This will use the acquired resources
        ray.get(annotated_remote_fn.remote())

        # After using the resources, free
        resource_manager.free_resources(annotated_resources)

    """

    def request_resources(self, resource_request: ResourceRequest):
        """Request resources.

        Depending on the backend, resources can trigger autoscaling. Requested
        resources can be ready or not ready. Once they are "ready", they can
        be acquired and used by remote Ray objects.

        Resource requests can be cancelled anytime using ``cancel_resource_request()``.
        Once acquired, the resource request is removed. Acquired resources can be
        returned with ``free_resources()``.
        """
        raise NotImplementedError

    def cancel_resource_request(self, resource_request: ResourceRequest):
        """Cancel resource request.

        Resource requests can be cancelled anytime before a resource is acquired.
        Acquiring a resource will remove the associated resource request.
        Acquired resources can be returned with ``free_resources()``.
        """
        raise NotImplementedError

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        """Returns True if resources for the given request are ready to be acquired."""
        raise NotImplementedError

    def acquire_resources(
        self, resource_request: ResourceRequest
    ) -> Optional[AcquiredResource]:
        """Acquire resources. Returns None if resources are not ready to be acquired.

        Acquiring resources will remove the associated resource request.
        Acquired resources can be returned with ``free_resources()``.
        """
        raise NotImplementedError

    def free_resources(self, acquired_resource: AcquiredResource):
        """Free acquired resources from usage and return them to the resource manager.

        Freeing resources will return the resources to the manager, but there are
        no guarantees about the tasks and actors scheduled on the resources. The caller
        should make sure that any references to tasks or actors scheduled on the
        resources have been removed before calling ``free_resources()``.
        """
        raise NotImplementedError

    def get_resource_futures(self) -> List[ray.ObjectRef]:
        """Return futures for resources to await.

        Depending on the backend, we use resource futures to determine availability
        of resources (e.g. placement groups) or resolution of requests.
        In this case, the futures can be awaited externally by an outer control loop.

        When a resource future resolved, you may want to call ``update_state()``
        to force the resource manager to update its internal state immediately.
        """
        return []

    def update_state(self):
        """Update internal state of the resource manager.

        The resource manager may have internal state that needs periodic updating.
        For instance, depending on the backend, resource futures can be awaited
        externally (with ``get_resource_futures()``).

        If such a future resolved, the outer control loop can instruct the resource
        manager to update its internal state immediately.
        """
        pass

    def clear(self):
        """Reset internal state and clear all resources.

        Calling this method will reset the resource manager to its initialization state.
        All resources will be removed.
        """
        raise NotImplementedError

    def __reduce__(self):
        """We disallow serialization.

        Shared resource managers should live on an actor.
        """
        raise ValueError(
            f"Resource managers cannot be serialized. Resource manager: {str(self)}"
        )
