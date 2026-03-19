"""Minimal ``ray.util`` compatibility for the Rust backend."""

from __future__ import annotations

from typing import Dict, List, Optional

import ray

from ray.util.annotations import PublicAPI

VALID_PLACEMENT_GROUP_STRATEGIES = {
    "PACK",
    "SPREAD",
    "STRICT_PACK",
    "STRICT_SPREAD",
}


@PublicAPI
class PlacementGroup:
    def __init__(self, id, bundle_cache: Optional[List[Dict[str, float]]] = None):
        self.id = id
        self.bundle_cache = bundle_cache or []

    @property
    def is_empty(self) -> bool:
        return self.id.is_nil()

    def ready(self):
        if not self.wait(30):
            raise TimeoutError("placement group did not become ready")
        return ray.put(True)

    def wait(self, timeout_seconds: float = 30) -> bool:
        return ray._runtime.gcs.wait_placement_group_until_ready(
            self.id.binary(), timeout_seconds
        )

    @property
    def bundle_specs(self) -> List[Dict[str, float]]:
        return list(self.bundle_cache)

    @property
    def bundle_count(self) -> int:
        return len(self.bundle_cache)

    def __eq__(self, other):
        return isinstance(other, PlacementGroup) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


@PublicAPI
def placement_group(
    bundles: List[Dict[str, float]],
    strategy: str = "PACK",
    name: str = "",
    lifetime: Optional[str] = None,
    _soft_target_node_id: Optional[str] = None,
    bundle_label_selector=None,
) -> PlacementGroup:
    del name, lifetime, _soft_target_node_id, bundle_label_selector
    if strategy not in VALID_PLACEMENT_GROUP_STRATEGIES:
        raise ValueError(f"invalid placement group strategy: {strategy}")
    pg_id = ray._runtime.gcs.create_placement_group(bundles, strategy)
    return PlacementGroup(pg_id, bundle_cache=bundles)


@PublicAPI
def remove_placement_group(placement_group: PlacementGroup) -> None:
    ray._runtime.gcs.remove_placement_group(placement_group.id.binary())


@PublicAPI
def get_placement_group(_placement_group_name: str) -> PlacementGroup:
    raise NotImplementedError("named placement groups are not yet implemented in the Rust backend")


@PublicAPI
def placement_group_table(placement_group: PlacementGroup = None) -> dict:
    if placement_group is None:
        return {}
    return {
        "placement_group_id": placement_group.id.hex(),
        "bundles": {
            idx: bundle for idx, bundle in enumerate(placement_group.bundle_specs)
        },
    }


@PublicAPI
def get_current_placement_group():
    return None


__all__ = [
    "PlacementGroup",
    "VALID_PLACEMENT_GROUP_STRATEGIES",
    "get_current_placement_group",
    "get_placement_group",
    "placement_group",
    "placement_group_table",
    "remove_placement_group",
]
