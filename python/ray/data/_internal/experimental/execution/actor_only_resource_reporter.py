"""This dataset's resource view under the actor-only backend.

The standard executor derives usage and limits from the ``ResourceManager``.
Under the actor-only backend the object store is accounted live by the
``ResourceBank`` and the dataset's capacity is whatever the shared
``AutoscalingCoordinator`` allocated to it, so this reporter is the single
source both the cluster autoscaler's utilization gauge and the operator sizer
read from -- their scale-up signals and the progress status line agree by
construction.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Tuple

from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    AutoscalingCoordinator,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import (
    NodeIdStr,
    PlacementGroupIdStr,
)
from ray.data._internal.execution.resource_bank import ResourceBankBase
from ray.data._internal.utils.cached_ray_internals import (
    get_alive_node_labels,
    get_draining_nodes,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetAllocationView:
    """Snapshot of the cluster as visible to one dataset, taken once per sizing
    tick so every sizing phase plans against the same numbers."""

    # This dataset's coordinator allocation, per node, including
    # ``pg_reserved_by_node``.
    reserved_by_node: Dict[NodeIdStr, ExecutionResources]
    # Node labels (``ray.nodes()[...]["Labels"]``) per alive node; resolves
    # label_selector eligibility. Empty when constraint-aware placement is off.
    labels_by_node: Dict[NodeIdStr, Dict[str, str]] = field(default_factory=dict)
    # Bundle shapes per placement group the topology references. Only populated
    # on the PG-aware path; carries no node mapping (Ray core places PGs).
    pg_bundles: Dict[PlacementGroupIdStr, List[ExecutionResources]] = field(
        default_factory=dict
    )
    # The part of ``reserved_by_node`` this dataset's registered placement
    # groups hold, on the nodes Ray placed them. Only PG actors can use it.
    pg_reserved_by_node: Dict[NodeIdStr, ExecutionResources] = field(
        default_factory=dict
    )
    # Nodes Ray core is draining. Their allocation is still reported, but
    # nothing new should be placed there.
    draining_nodes: FrozenSet[NodeIdStr] = frozenset()


class ActorOnlyResourceReporter:
    """Global usage / limits for one dataset execution.

    "Global" means across this dataset's operators, not cluster-wide:
    cross-dataset arbitration lives in the shared coordinator. Satisfies the
    ``ResourceReporter`` protocol the cluster autoscaler's utilization gauge
    reads through.
    """

    def __init__(
        self,
        topology: "Topology",
        autoscaling_coordinator: AutoscalingCoordinator,
        resource_bank: ResourceBankBase,
    ):
        self._topology = topology
        self._autoscaling_coordinator = autoscaling_coordinator
        self._resource_bank = resource_bank
        # Placement-group bundle shapes, keyed by ``PlacementGroup.id.hex()``.
        # Shapes are fixed at PG creation, so each PG is cached once
        # (``cache_pg_bundle_shapes``) and served from here afterwards.
        self._pg_bundles_by_id: Dict[PlacementGroupIdStr, List[ExecutionResources]] = {}
        # Placement groups last registered with the coordinator.
        self._registered_pg_ids: FrozenSet[PlacementGroupIdStr] = frozenset()
        # Signature of the last-seen per-node allocation and when it last
        # changed (see ``seconds_since_allocation_change``).
        self._alloc_signature: Optional[
            Tuple[Tuple[str, float, float, float], ...]
        ] = None
        self._alloc_changed_at = float("-inf")

    def get_global_usage(self) -> ExecutionResources:
        """Resources used by running work across all operators: CPU/GPU/memory
        from each operator's ``running_logical_usage``, object store from the
        bank's live bytes."""
        usage = ExecutionResources.zero()
        for op in self._topology:
            live = self._resource_bank.live_object_store(op=op)
            usage = usage.add(
                ExecutionResources(object_store_memory=live.total_bytes())
            )
            usage = usage.add(op.running_logical_usage())
        return usage

    def get_global_pending_usage(self) -> ExecutionResources:
        """Resources of actors requested but not yet running, across operators."""
        pending = ExecutionResources.zero()
        for op in self._topology:
            pending = pending.add(op.pending_logical_usage())
        return pending

    def get_global_limits(self) -> ExecutionResources:
        """This dataset's allocated cluster capacity, from the coordinator."""
        limits = ExecutionResources.zero()
        reserved = self._autoscaling_coordinator.get_reserved_resources().total()
        for bundle in reserved.values():
            limits = limits.add(ExecutionResources.from_resource_dict(bundle))
        return limits

    def is_blocked_on_consumer(self) -> bool:
        """Always False: the actor-only backend doesn't track consumer
        blocking, so the cluster autoscaler never skips a scale-up for it."""
        return False

    def get_reserved_resources_by_node(self) -> Dict[NodeIdStr, ExecutionResources]:
        """This dataset's coordinator allocation keyed by node id -- the view
        actors are placed against. Sums to ``get_global_limits``."""
        reserved_by_node, _ = self._get_split_reserved_by_node()
        return reserved_by_node

    def _get_split_reserved_by_node(
        self,
        *,
        recompute: bool = False,
    ) -> Tuple[
        Dict[NodeIdStr, ExecutionResources], Dict[NodeIdStr, ExecutionResources]
    ]:
        """Read the coordinator allocation once.

        Args:
            recompute: Wait for a fresh coordinator allocation instead of its cache.

        Returns:
            The whole allocation per node, and the placement-group part of it.
        """
        split = (
            self._autoscaling_coordinator.get_reserved_resources(recompute=True)
            if recompute
            else self._autoscaling_coordinator.get_reserved_resources()
        )
        alloc = {
            node_id: ExecutionResources.from_resource_dict(resources)
            for node_id, resources in split.total().items()
        }
        pg_alloc = {
            node_id: ExecutionResources.from_resource_dict(resources)
            for node_id, resources in split.placement_group.items()
        }
        # Object store is left out on purpose: it is shared from each node's
        # leftover capacity and moves whenever other requesters come and go,
        # while this timer gates actor placement, which uses CPU/GPU/memory.
        signature = tuple(sorted((n, r.cpu, r.gpu, r.memory) for n, r in alloc.items()))
        if signature != self._alloc_signature:
            self._alloc_signature = signature
            self._alloc_changed_at = time.monotonic()
        return alloc, pg_alloc

    def seconds_since_allocation_change(self) -> float:
        """How long the per-node allocation has been unchanged, as of the last
        ``get_reserved_resources_by_node`` call."""
        return time.monotonic() - self._alloc_changed_at

    def cache_pg_bundle_shapes(self, placement_groups: List["PlacementGroup"]) -> None:
        """Cache each placement group's bundle shapes for ``get_cluster_view``.

        Shapes come from ``PlacementGroup.bundle_specs``, which the creating
        process already holds, so no GCS read is needed. Where the bundles
        were placed comes from the coordinator (``pg_reserved_by_node``).
        """
        for pg in placement_groups:
            pg_id = pg.id.hex()
            if pg_id in self._pg_bundles_by_id:
                continue
            # Index-ordered, so consumers can index by
            # ``placement_group_bundle_index``.
            self._pg_bundles_by_id[pg_id] = [
                ExecutionResources.from_resource_dict(bundle)
                for bundle in pg.bundle_specs
            ]

    def register_placement_groups(
        self, placement_groups: List["PlacementGroup"]
    ) -> None:
        """Register this dataset's placement groups with the coordinator, so
        other requesters are not handed the capacity they hold.

        Replaces the registered set; sends nothing when it hasn't changed.
        """
        pg_ids = frozenset(pg.id.hex() for pg in placement_groups)
        if pg_ids != self._registered_pg_ids:
            self._autoscaling_coordinator.register_placement_groups(pg_ids)
            self._registered_pg_ids = pg_ids

    def get_cluster_view(
        self,
        *,
        include_labels: bool = False,
        include_pg_bundles: bool = False,
        include_draining_nodes: bool = False,
        recompute: bool = False,
    ) -> DatasetAllocationView:
        """Snapshot of this dataset's allocation; optionally force recomputation."""
        reserved_by_node, pg_reserved_by_node = self._get_split_reserved_by_node(
            recompute=recompute
        )
        return DatasetAllocationView(
            reserved_by_node=reserved_by_node,
            labels_by_node=get_alive_node_labels() if include_labels else {},
            pg_bundles=dict(self._pg_bundles_by_id) if include_pg_bundles else {},
            pg_reserved_by_node=pg_reserved_by_node,
            draining_nodes=(
                frozenset(get_draining_nodes())
                if include_draining_nodes
                else frozenset()
            ),
        )
