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
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    AutoscalingCoordinator,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import (
    NodeIdStr,
    PlacementGroupIdStr,
)
from ray.data._internal.execution.resource_bank import ResourceBankBase
from ray.data._internal.utils.cached_ray_internals import get_alive_node_labels

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClusterView:
    """Snapshot of the cluster as visible to one dataset, taken once per sizing
    tick so every sizing phase plans against the same numbers."""

    # This dataset's coordinator allocation, per node.
    alloc_by_node: Dict[NodeIdStr, ExecutionResources]
    # Node labels (``ray.nodes()[...]["Labels"]``) per alive node; resolves
    # label_selector eligibility. Empty when constraint-aware placement is off.
    labels_by_node: Dict[NodeIdStr, Dict[str, str]] = field(default_factory=dict)
    # Bundle shapes per placement group the topology references. Only populated
    # on the PG-aware path; carries no node mapping (Ray core places PGs).
    pg_bundles: Dict[PlacementGroupIdStr, List[ExecutionResources]] = field(
        default_factory=dict
    )


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
        # Shapes are fixed at PG creation, so each PG is fetched at most once
        # (``prefetch_pg_bundles``) and served from here afterwards.
        self._pg_bundles_by_id: Dict[PlacementGroupIdStr, List[ExecutionResources]] = {}
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
        for bundle in self._autoscaling_coordinator.get_reserved_resources().values():
            limits = limits.add(ExecutionResources.from_resource_dict(bundle))
        return limits

    def is_blocked_on_consumer(self) -> bool:
        """Always False: the actor-only backend doesn't track consumer
        blocking, so the cluster autoscaler never skips a scale-up for it."""
        return False

    def get_reserved_resources_by_node(self) -> Dict[NodeIdStr, ExecutionResources]:
        """This dataset's coordinator allocation keyed by node id -- the view
        actors are placed against. Sums to ``get_global_limits``."""
        alloc = {
            node_id: ExecutionResources.from_resource_dict(resources)
            for node_id, resources in (
                self._autoscaling_coordinator.get_reserved_resources().items()
            )
        }
        signature = tuple(sorted((n, r.cpu, r.gpu, r.memory) for n, r in alloc.items()))
        if signature != self._alloc_signature:
            self._alloc_signature = signature
            self._alloc_changed_at = time.monotonic()
        return alloc

    def seconds_since_allocation_change(self) -> float:
        """How long the per-node allocation has been unchanged, as of the last
        ``get_reserved_resources_by_node`` call."""
        return time.monotonic() - self._alloc_changed_at

    def prefetch_pg_bundles(self, placement_groups: List["PlacementGroup"]) -> None:
        """Fetch and cache the bundle shapes of the given placement groups.

        A PG whose table is not readable yet is left uncached and retried on
        the next call, so an early fetch never pins it to "no bundles".
        """
        from ray.util.placement_group import placement_group_table

        for pg in placement_groups:
            pg_id = pg.id.hex()
            if pg_id in self._pg_bundles_by_id:
                continue
            table = placement_group_table(pg)
            bundles = table.get("bundles", {}) or {}
            if not bundles:
                continue
            # ``bundles`` is keyed by bundle index; keep an index-ordered list
            # so consumers can index by ``placement_group_bundle_index``.
            self._pg_bundles_by_id[pg_id] = [
                ExecutionResources.from_resource_dict(bundles[idx])
                for idx in sorted(bundles)
            ]

    def get_cluster_view(
        self, *, include_labels: bool = False, include_pg_bundles: bool = False
    ) -> ClusterView:
        """Snapshot of the cluster as visible to this dataset right now."""
        return ClusterView(
            alloc_by_node=self.get_reserved_resources_by_node(),
            labels_by_node=get_alive_node_labels() if include_labels else {},
            pg_bundles=dict(self._pg_bundles_by_id) if include_pg_bundles else {},
        )
