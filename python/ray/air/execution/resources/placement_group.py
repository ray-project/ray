from collections import defaultdict
from typing import Dict, Optional, List, Set

from dataclasses import dataclass

import ray
from ray.air.execution.resources.request import ResourceRequest, ReadyResource
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.util.placement_group import PlacementGroup


def _sum_bundle_resources(bundles: List[Dict[str, float]]) -> Dict[str, float]:
    all_resources = {}
    for resources in bundles:
        for k, v in resources.items():
            all_resources[k] = all_resources.get(k, 0) + v

    return all_resources


@dataclass
class PlacementGroupReadyResource(ReadyResource):
    bundles: List[Dict[str, float]]
    pg: PlacementGroup

    def annotate_remote_objects(self, objects):
        all_resources = _sum_bundle_resources(self.bundles)
        num_cpus = all_resources.pop("CPU", 0)
        num_gpus = all_resources.pop("GPU", 0)
        return objects[0].options(
            num_cpus=num_cpus, num_gpus=num_gpus, resources=all_resources
        )


class PlacementGroupResourceManager(ResourceManager):
    _resource_cls: ReadyResource = PlacementGroupReadyResource

    def __init__(self):
        self._pg_to_request: Dict[PlacementGroup, ResourceRequest] = {}
        self._request_to_staged_pgs: Dict[
            ResourceRequest, Set[PlacementGroup]
        ] = defaultdict(set)
        self._request_to_ready_pgs: Dict[
            ResourceRequest, Set[PlacementGroup]
        ] = defaultdict(set)

        self._staging_future_to_pg: Dict[ray.ObjectRef, PlacementGroup] = dict()
        self._pg_to_staging_future: Dict[PlacementGroup, ray.ObjectRef] = dict()

        self._staged_pgs: Set[PlacementGroup] = set()
        self._ready_pgs: Set[PlacementGroup] = set()
        self._acquired_pgs: Set[PlacementGroup] = set()

    def _update_state(self):
        ready, not_ready = ray.wait(
            self._staging_future_to_pg, num_returns=len(self._staging_future_to_pg)
        )
        for future in ready:
            # Remove staging future
            pg = self._staging_future_to_pg.pop(future)
            self._pg_to_staging_future.pop(pg)
            # Fetch resource requst
            request = self._pg_to_request[pg]
            # Remove from staging, add to ready
            self._request_to_staged_pgs[request].remove(pg)
            self._request_to_ready_pgs[request].add(pg)

    def request_resources(self, resources: ResourceRequest):
        pg = PlacementGroup(resources.bundles)
        self._pg_to_request[pg] = resources
        self._request_to_staged_pgs[resources].add(pg)
        self._staged_pgs.add(pg)

        future = pg.ready()
        self._staging_future_to_pg[future] = pg
        self._pg_to_staging_future[pg] = future

    def cancel_resource_request(self, resources: ResourceRequest):
        pg = self._request_to_staged_pgs[resources].pop()
        if pg:
            # PG was staging
            self._staged_pgs.remove(pg)
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

            self._ready_pgs.remove(pg)

        self._pg_to_request.pop(pg)

    def has_resources_ready(self, resources: ResourceRequest) -> bool:
        return bool(len(self._request_to_ready_pgs[resources]))

    def acquire_resources(self, resources: ResourceRequest) -> Optional[ReadyResource]:
        if not self.has_resources_ready(resources):
            return None

        pg = self._request_to_ready_pgs[resources].pop()

        self._ready_pgs.remove(pg)
        self._acquired_pgs.add(pg)

        return self._resource_cls(pg=pg, request=resources)

    def return_resources(self, ready_resources: PlacementGroupReadyResource):
        pg = ready_resources.pg
        request = self._pg_to_request[pg]

        self._acquired_pgs.remove(pg)
        self._ready_pgs.add(pg)
        self._request_to_ready_pgs[request].add(pg)

        # We always cancel the resource request
        self.cancel_resource_request(resources=request)
