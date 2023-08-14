import time
from collections import defaultdict
from typing import Dict, List, Optional, Set

from dataclasses import dataclass

import ray
from ray.air.execution.resources.request import (
    ResourceRequest,
    AcquiredResources,
    RemoteRayEntity,
)
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.util.annotations import DeveloperAPI
from ray.util.placement_group import PlacementGroup, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@DeveloperAPI
@dataclass
class PlacementGroupAcquiredResources(AcquiredResources):
    placement_group: PlacementGroup

    def _annotate_remote_entity(
        self, entity: RemoteRayEntity, bundle: Dict[str, float], bundle_index: int
    ) -> RemoteRayEntity:
        bundle = bundle.copy()
        num_cpus = bundle.pop("CPU", 0)
        num_gpus = bundle.pop("GPU", 0)
        memory = bundle.pop("memory", 0.0)

        return entity.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=self.placement_group,
                placement_group_bundle_index=bundle_index,
                placement_group_capture_child_tasks=True,
            ),
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            resources=bundle,
        )


@DeveloperAPI
class PlacementGroupResourceManager(ResourceManager):
    """Resource manager using placement groups as the resource backend.

    This manager will use placement groups to fulfill resource requests. Requesting
    a resource will schedule the placement group. Acquiring a resource will
    return a ``PlacementGroupAcquiredResources`` that can be used to schedule
    Ray tasks and actors on the placement group. Freeing an acquired resource
    will destroy the associated placement group.

    Ray core does not emit events when resources are available. Instead, the
    scheduling state has to be periodically updated.

    Per default, placement group scheduling state is refreshed every time when
    resource state is inquired, but not more often than once every ``update_interval_s``
    seconds. Alternatively, staging futures can be retrieved (and awaited) with
    ``get_resource_futures()`` and state update can be force with ``update_state()``.

    Args:
        update_interval_s: Minimum interval in seconds between updating scheduling
            state of placement groups.

    """

    _resource_cls: AcquiredResources = PlacementGroupAcquiredResources

    def __init__(self, update_interval_s: float = 0.1):
        # Internally, the placement group lifecycle is like this:
        # - Resources are requested with ``request_resources()``
        # - A placement group is scheduled ("staged")
        # - A ``PlacementGroup.ready()`` future is scheduled ("staging future")
        # - We update the scheduling state when we need to
        #   (e.g. when ``has_resources_ready()`` is called)
        # - When staging futures resolve, a placement group is moved from "staging"
        #   to "ready"
        # - When a resource request is canceled, we remove a placement group from
        #   "staging". If there are not staged placement groups
        #   (because they are already "ready"), we remove one from "ready" instead.
        # - When a resource is acquired, the pg is removed from "ready" and moved
        #   to "acquired"
        # - When a resource is freed, the pg is removed from "acquired" and destroyed

        # Mapping of placement group to request
        self._pg_to_request: Dict[PlacementGroup, ResourceRequest] = {}

        # PGs that are staged but not "ready", yet (i.e. not CREATED)
        self._request_to_staged_pgs: Dict[
            ResourceRequest, Set[PlacementGroup]
        ] = defaultdict(set)

        # PGs that are CREATED and can be used by tasks and actors
        self._request_to_ready_pgs: Dict[
            ResourceRequest, Set[PlacementGroup]
        ] = defaultdict(set)

        # Staging futures used to update internal state.
        # We keep a double mapping here for better lookup efficiency.
        self._staging_future_to_pg: Dict[ray.ObjectRef, PlacementGroup] = dict()
        self._pg_to_staging_future: Dict[PlacementGroup, ray.ObjectRef] = dict()

        # Set of acquired PGs. We keep track of these here to make sure we
        # only free PGs that this manager managed.
        self._acquired_pgs: Set[PlacementGroup] = set()

        # Minimum time between updates of the internal state
        self.update_interval_s = update_interval_s
        self._last_update = time.monotonic() - self.update_interval_s - 1

    def get_resource_futures(self) -> List[ray.ObjectRef]:
        return list(self._staging_future_to_pg.keys())

    def _maybe_update_state(self):
        now = time.monotonic()
        if now > self._last_update + self.update_interval_s:
            self.update_state()

    def update_state(self):
        ready, not_ready = ray.wait(
            list(self._staging_future_to_pg.keys()),
            num_returns=len(self._staging_future_to_pg),
            timeout=0,
        )
        for future in ready:
            # Remove staging future
            pg = self._staging_future_to_pg.pop(future)
            self._pg_to_staging_future.pop(pg)
            # Fetch resource request
            request = self._pg_to_request[pg]
            # Remove from staging, add to ready
            self._request_to_staged_pgs[request].remove(pg)
            self._request_to_ready_pgs[request].add(pg)
        self._last_update = time.monotonic()

    def request_resources(self, resource_request: ResourceRequest):
        pg = resource_request.to_placement_group()
        self._pg_to_request[pg] = resource_request
        self._request_to_staged_pgs[resource_request].add(pg)

        future = pg.ready()
        self._staging_future_to_pg[future] = pg
        self._pg_to_staging_future[pg] = future

    def cancel_resource_request(self, resource_request: ResourceRequest):
        if self._request_to_staged_pgs[resource_request]:
            pg = self._request_to_staged_pgs[resource_request].pop()

            # PG was staging
            future = self._pg_to_staging_future.pop(pg)
            self._staging_future_to_pg.pop(future)

            # Cancel the pg.ready task.
            # Otherwise, it will be pending node assignment forever.
            ray.cancel(future)
        else:
            # PG might be ready
            pg = self._request_to_ready_pgs[resource_request].pop()
            if not pg:
                raise RuntimeError(
                    "Cannot cancel resource request: No placement group was "
                    f"staged or is ready. Make sure to not cancel more resource "
                    f"requests than you've created. Request: {resource_request}"
                )

        self._pg_to_request.pop(pg)
        ray.util.remove_placement_group(pg)

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        if not bool(len(self._request_to_ready_pgs[resource_request])):
            # Only update state if needed
            self._maybe_update_state()

        return bool(len(self._request_to_ready_pgs[resource_request]))

    def acquire_resources(
        self, resource_request: ResourceRequest
    ) -> Optional[PlacementGroupAcquiredResources]:
        if not self.has_resources_ready(resource_request):
            return None

        pg = self._request_to_ready_pgs[resource_request].pop()
        self._acquired_pgs.add(pg)

        return self._resource_cls(placement_group=pg, resource_request=resource_request)

    def free_resources(self, acquired_resource: PlacementGroupAcquiredResources):
        pg = acquired_resource.placement_group

        self._acquired_pgs.remove(pg)
        remove_placement_group(pg)
        self._pg_to_request.pop(pg)

    def clear(self):
        if not ray.is_initialized():
            return

        for staged_pgs in self._request_to_staged_pgs.values():
            for staged_pg in staged_pgs:
                remove_placement_group(staged_pg)

        for ready_pgs in self._request_to_ready_pgs.values():
            for ready_pg in ready_pgs:
                remove_placement_group(ready_pg)

        for acquired_pg in self._acquired_pgs:
            remove_placement_group(acquired_pg)

        # Reset internal state
        self.__init__(update_interval_s=self.update_interval_s)

    def __del__(self):
        self.clear()
