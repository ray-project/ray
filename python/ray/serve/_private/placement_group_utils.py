import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import ray
from ray.serve._private.constants import ACCELERATOR_KIND_TPU, SERVE_LOGGER_NAME
from ray.util.placement_group import PlacementGroup, remove_placement_group
from ray.util.tpu import SlicePlacementGroup, slice_placement_group

logger = logging.getLogger(SERVE_LOGGER_NAME)

# NOTE: Please read carefully before changing!
#
# Similar to `default_impl.py`, methods like `_default_create_placement_group` are
# common extension points and should be treated as a Developer API.


@dataclass(frozen=True)
class CreatePlacementGroupRequest:
    """Internal request for creating a per-replica placement group.

    Either ``bundles`` or ``accelerator_config`` must be provided:
    - For plain CPU/GPU deployments, the caller provides ``bundles`` and the
      default path creates a standard PlacementGroup.
    - For accelerator deployments (e.g. TPU), the caller provides
      ``accelerator_config`` and the dispatch derives bundles from the
      structured config (e.g. TPU topology -> per-host bundles).
    """

    bundles: Optional[List[Dict[str, float]]] = None
    strategy: str = "PACK"
    target_node_id: Optional[str] = None
    name: str = ""
    runtime_env: Optional[str] = None
    bundle_label_selector: Optional[List[Dict[str, str]]] = None
    fallback_strategy: Optional[List[Dict[str, Any]]] = None
    accelerator_config: Optional[Any] = None


@dataclass
class ReplicaPlacementGroup:
    """Internal Serve handle for a replica's placement group(s).

    Wraps the worker PG and any accelerator-specific cleanup hooks so the
    controller doesn't need to know whether the underlying request was a
    plain CPU/GPU PG or a TPU slice reservation.
    """

    placement_group: Optional[PlacementGroup]
    _slice_pg: Optional[SlicePlacementGroup] = None

    def release_reservation_holders(self) -> None:
        """Call after ``placement_group.ready()`` resolves successfully.

        Releases any internal reservation-holder PGs (e.g. TPU head PGs)
        that were only needed to claim resources during scheduling. No-op
        for non-accelerator deployments.
        """
        if self._slice_pg is not None:
            self._slice_pg.release_head_pgs()

    def shutdown(self) -> None:
        """Tear down the replica's PG(s). Idempotent."""
        if self._slice_pg is not None:
            self._slice_pg.shutdown()
            self._slice_pg = None
            self.placement_group = None
        elif self.placement_group is not None:
            try:
                remove_placement_group(self.placement_group)
            except Exception:
                logger.exception("Failed to remove placement group.")
            finally:
                self.placement_group = None


def _default_create_placement_group(
    request: CreatePlacementGroupRequest,
) -> PlacementGroup:
    return ray.util.placement_group(
        request.bundles,
        request.strategy,
        _soft_target_node_id=request.target_node_id,
        name=request.name,
        lifetime="detached",
        bundle_label_selector=request.bundle_label_selector,
    )


def _create_replica_placement_group(
    request: CreatePlacementGroupRequest,
) -> ReplicaPlacementGroup:
    """Internal entry point that supports accelerator-specific dispatch.

    Dispatches on ``request.accelerator_config``:
    - TPUAcceleratorConfig: derive bundles from topology via
      slice_placement_group; ``request.bundles`` is ignored.
    - None: use ``request.bundles`` to create a standard PlacementGroup.

    Raises ValueError if neither bundles nor a recognized accelerator
    config is provided - this catches users setting an unrecognized
    accelerator_config type without explicit bundles, which would
    otherwise schedule with no PG at all.
    """
    accelerator_config = request.accelerator_config

    if getattr(accelerator_config, "kind", None) == ACCELERATOR_KIND_TPU:
        slice_pg = slice_placement_group(
            topology=accelerator_config.topology,
            accelerator_version=accelerator_config.accelerator_version,
            num_slices=accelerator_config.num_slices,
            chips_per_vm=accelerator_config.chips_per_vm,
            resources_per_bundle=accelerator_config.resources_per_bundle,
            strategy=request.strategy,
            name=request.name,
            lifetime="detached",
            bundle_label_selector=request.bundle_label_selector,
        )
        return ReplicaPlacementGroup(
            placement_group=slice_pg.placement_group,
            _slice_pg=slice_pg,
        )

    if request.bundles is None:
        raise ValueError(
            "CreatePlacementGroupRequest requires either non-None bundles "
            "or a recognized accelerator_config. Got accelerator_config="
            f"{type(accelerator_config).__name__ if accelerator_config else None}, "
            "bundles=None."
        )

    pg = _default_create_placement_group(request)
    return ReplicaPlacementGroup(placement_group=pg)
