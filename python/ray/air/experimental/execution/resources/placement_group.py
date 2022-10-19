import time
from collections import defaultdict
from typing import Dict, Optional, List, Set, Union

from dataclasses import dataclass

import ray
from ray.air.experimental.execution.resources.request import (
    ResourceRequest,
    AllocatedResource,
)
from ray.air.experimental.execution.resources.resource_manager import ResourceManager
from ray.util.placement_group import placement_group, PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@dataclass
class PlacementGroupAllocatedResource(AllocatedResource):
    placement_group: PlacementGroup

    def annotate_remote_objects(
        self, objects
    ) -> List[Union[ray.ObjectRef, ray.actor.ActorHandle]]:
        # With an empty head, the second bundle should live in the
        # actual PG's first bundle, so we start counting from -1
        if self.request.head_bundle_is_empty:
            start = -1
            bundles = [{}] + self.request.bundles
        else:
            start = 0
            bundles = self.request.bundles

        annotated = []
        for i, (obj, bundle) in enumerate(zip(objects, bundles), start=start):
            bundle = bundle.copy()
            num_cpus = bundle.pop("CPU", 0)
            num_gpus = bundle.pop("GPU", 0)

            annotated.append(
                obj.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=self.placement_group,
                        # Max ensures that empty head bundles are correctly placed
                        placement_group_bundle_index=max(0, i),
                    ),
                    num_cpus=num_cpus,
                    num_gpus=num_gpus,
                    resources=bundle,
                )
            )
        return annotated


class PlacementGroupResourceManager(ResourceManager):
    _resource_cls: AllocatedResource = PlacementGroupAllocatedResource

    def __init__(self, update_interval: float = 0.1):
        self._pg_to_request: Dict[PlacementGroup, ResourceRequest] = {}
        self._request_to_staged_pgs: Dict[
            ResourceRequest, Set[PlacementGroup]
        ] = defaultdict(set)
        self._request_to_ready_pgs: Dict[
            ResourceRequest, Set[PlacementGroup]
        ] = defaultdict(set)

        self._staging_future_to_pg: Dict[ray.ObjectRef, PlacementGroup] = dict()
        self._pg_to_staging_future: Dict[PlacementGroup, ray.ObjectRef] = dict()
        self._acquired_pgs: Set[PlacementGroup] = set()

        self._update_interval = update_interval
        self._last_update = time.monotonic() - self._update_interval - 1

    def get_resource_futures(self) -> List[ray.ObjectRef]:
        return list(self._staging_future_to_pg.keys())

    def _maybe_update_state(self):
        now = time.monotonic()
        if now > self._last_update + self._update_interval:
            self._update_state()
            self._last_update = now

    def _update_state(self):
        ready, not_ready = ray.wait(
            list(self._staging_future_to_pg.keys()),
            num_returns=len(self._staging_future_to_pg),
            timeout=1e-6,
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

    def request_resources(self, resources: ResourceRequest):
        pg = placement_group(resources.bundles)
        self._pg_to_request[pg] = resources
        self._request_to_staged_pgs[resources].add(pg)

        future = pg.ready()
        self._staging_future_to_pg[future] = pg
        self._pg_to_staging_future[pg] = future

    def cancel_resource_request(self, resources: ResourceRequest):
        if self._request_to_staged_pgs[resources]:
            pg = self._request_to_staged_pgs[resources].pop()

            # PG was staging
            future = self._pg_to_staging_future.pop(pg)
            self._staging_future_to_pg.pop(future)
        else:
            # PG might be ready
            pg = self._request_to_ready_pgs[resources].pop()
            if not pg:
                raise RuntimeError(
                    "Cannot cancel resource request: No placement group was "
                    f"staged or is ready. Request: {resources}"
                )

        self._pg_to_request.pop(pg)
        ray.util.remove_placement_group(pg)

    def has_resources_ready(self, resources: ResourceRequest) -> bool:
        if not bool(len(self._request_to_ready_pgs[resources])):
            # Only update state if needed
            self._maybe_update_state()

        return bool(len(self._request_to_ready_pgs[resources]))

    def acquire_resources(
        self, resources: ResourceRequest
    ) -> Optional[PlacementGroupAllocatedResource]:
        if not self.has_resources_ready(resources):
            return None

        pg = self._request_to_ready_pgs[resources].pop()
        self._acquired_pgs.add(pg)

        return self._resource_cls(placement_group=pg, request=resources)

    def return_resources(
        self, ready_resources: PlacementGroupAllocatedResource, cancel_request: bool = True
    ):
        pg = ready_resources.placement_group
        request = self._pg_to_request[pg]

        self._acquired_pgs.remove(pg)
        self._request_to_ready_pgs[request].add(pg)

        if cancel_request:
            self.cancel_resource_request(resources=ready_resources.request)
