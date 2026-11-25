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
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
)

from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    AutoscalingCoordinator,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import (
    NodeIdStr,
    PlacementGroupIdStr,
)
from ray.data._internal.execution.resource_bank import ResourceBankBase
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.utils.cached_ray_internals import (
    get_alive_node_labels,
    get_draining_nodes,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)


class BundleKey(NamedTuple):
    """One bundle of one placement group: the slot a PG actor must fit inside.

    Identifies the bundle itself, not where it runs: Ray core picks the node
    and never reports per-bundle placement back.

    Example: ``placement_group([{"GPU": 1}, {"GPU": 1}, {"CPU": 4}])`` has
    three bundles, ``BundleKey(pg_id="a1b2...", bundle_index=0)`` through
    ``bundle_index=2``.
    """

    # ``PlacementGroup.id.hex()``, e.g. ``"a1b2c3d4e5f6..."``.
    pg_id: PlacementGroupIdStr
    # Position in the PG's bundle list (0-based), as
    # ``placement_group_bundle_index`` numbers it.
    bundle_index: int


@dataclass(frozen=True)
class PlacementGroupSlots:
    """Bundle shapes of every placement group the topology uses, which of
    them each static-PG op may land in, and a cap for each PG-managed op
    whose PG capacity is unknown.

    Example, with ``pg = placement_group([{"GPU": 1}, {"GPU": 1}, {"CPU": 4}])``
    (id ``"a1b2"``) and three ops::

        infer:   map_batches(..., placement_group=pg)
        embed:   map_batches(..., placement_group=pg,
                             placement_group_bundle_index=2)
        rerank:  map_batches(..., ray_remote_args_fn=<creates a PG per actor>,
                             concurrency=(1, 8))

    measures as::

        shapes = {
            BundleKey("a1b2", 0): ExecutionResources(gpu=1),
            BundleKey("a1b2", 1): ExecutionResources(gpu=1),
            BundleKey("a1b2", 2): ExecutionResources(cpu=4),
        }
        static_bundles_by_op = {
            infer: (BundleKey("a1b2", 0), BundleKey("a1b2", 1),
                    BundleKey("a1b2", 2)),
            embed: (BundleKey("a1b2", 2),),
        }
        # rerank's PGs don't exist until its actors start, so there are no
        # bundles to read: it is capped at its pool's max size.
        unknown_cap_by_op = {rerank: 8}
    """

    # Each bundle's resources, fixed at PG creation.
    shapes: Dict[BundleKey, ExecutionResources] = field(default_factory=dict)
    # The bundles each static-PG op's actors may land in: all of its PG's
    # bundles, or just ``placement_group_bundle_index``.
    static_bundles_by_op: Dict[
        "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
    ] = field(default_factory=dict)
    # PG-managed ops with no known bundles: the most actors they may hold.
    unknown_cap_by_op: Dict["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )

    @classmethod
    def measure(
        cls,
        ops: Iterable["ExperimentalActorPoolMapOperator"],
        pg_bundles_by_id: Mapping[str, List[ExecutionResources]],
    ) -> "PlacementGroupSlots":
        """``pg_bundles_by_id``: each placement group's bundle shapes, in
        bundle-index order, e.g. ``{"a1b2": [ExecutionResources(gpu=1),
        ExecutionResources(gpu=1), ExecutionResources(cpu=4)]}``."""
        shapes: Dict[BundleKey, ExecutionResources] = {}
        static_bundles_by_op: Dict[
            "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
        ] = {}
        unknown_cap_by_op: Dict["ExperimentalActorPoolMapOperator", int] = {}
        for op in ops:
            constraint = op.placement_constraint()
            pg = constraint.placement_group
            pg_id = pg.id.hex() if pg is not None else None
            bundles = pg_bundles_by_id.get(pg_id) if pg_id is not None else None
            if pg_id is None or not bundles:
                if constraint.is_pg_managed:
                    unknown_cap_by_op[op] = op.actor_pool.bounded_max_size()
                continue
            index = constraint.placement_group_bundle_index
            if index is not None and index >= len(bundles):
                # Ray would reject this index; size against every bundle
                # rather than none.
                logger.warning(
                    "%s: placement_group_bundle_index=%d but placement group %s "
                    "has %d bundle(s); sizing against all of them.",
                    op.name,
                    index,
                    pg_id,
                    len(bundles),
                )
                index = None
            if index is not None and index >= 0:
                indices: Iterable[int] = (index,)
            else:
                # None, or Ray's -1: any bundle.
                indices = range(len(bundles))
            keys = tuple(BundleKey(pg_id, i) for i in indices)
            for key in keys:
                shapes[key] = bundles[key.bundle_index]
            static_bundles_by_op[op] = keys
        return cls(
            shapes=shapes,
            static_bundles_by_op=static_bundles_by_op,
            unknown_cap_by_op=unknown_cap_by_op,
        )


@dataclass(frozen=True)
class DatasetReservationView:
    """Snapshot of the cluster as visible to one dataset, taken once per sizing
    tick so every sizing phase plans against the same numbers."""

    # This includes pg reservations
    reserved_by_node: Dict[NodeIdStr, ExecutionResources]
    # Only pg reservations
    pg_reserved_by_node: Dict[NodeIdStr, ExecutionResources] = field(
        default_factory=dict
    )
    labels_by_node: Dict[NodeIdStr, Dict[str, str]] = field(default_factory=dict)
    draining_nodes: FrozenSet[NodeIdStr] = frozenset()
    pg_slots: PlacementGroupSlots = field(default_factory=PlacementGroupSlots)

    @cached_property
    def non_pg_reserved_by_node(self) -> Dict[NodeIdStr, ExecutionResources]:
        res = {}
        for node_id, reserved in self.reserved_by_node.items():
            pg_reserved = self.pg_reserved_by_node.get(
                node_id, ExecutionResources.zero()
            )
            res[node_id] = reserved.subtract(pg_reserved)
        return res


class NodeReservationSignature(NamedTuple):
    """Used to compare future and pass node reservations. Useful for
    detect changes in reservation. Detecting changes allows us to determine
    if we are in an node autoscaling cluster."""

    node_id: NodeIdStr
    cpu: float
    gpu: float
    memory: float


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
        *,
        get_node_labels: Callable[
            [], Mapping[NodeIdStr, Dict[str, str]]
        ] = get_alive_node_labels,
        get_draining_node_ids: Callable[[], Iterable[NodeIdStr]] = get_draining_nodes,
    ):
        """``get_node_labels`` and ``get_draining_node_ids`` read Ray's state
        API by default; tests inject fakes to run without a cluster."""
        self._topology = topology
        self._autoscaling_coordinator = autoscaling_coordinator
        self._resource_bank = resource_bank
        self._get_node_labels = get_node_labels
        self._get_draining_node_ids = get_draining_node_ids
        # Each PG's bundle shapes, from ``pg.bundle_specs``, in bundle-index
        # order, e.g. ``{"a1b2": [ExecutionResources(gpu=1), ...]}``.
        self._pg_bundles_by_id: Dict[PlacementGroupIdStr, List[ExecutionResources]] = {}
        self._registered_pg_ids: FrozenSet[PlacementGroupIdStr] = frozenset()
        self._pg_slots: Optional[PlacementGroupSlots] = None
        self._reservation_signature: Optional[
            Tuple[NodeReservationSignature, ...]
        ] = None
        self._reservation_changed_at = float("-inf")

    def bootstrap(self):
        # Cold cache: Send an initial RPC now to get the node resources.
        self.get_cluster_view()

    # --- BEGIN The resource reporter protocol ---

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

    # --- END The resource reporter protocol ---

    def get_reserved_resources_by_node(self) -> Dict[NodeIdStr, ExecutionResources]:
        """This dataset's coordinator allocation keyed by node id -- the view
        actors are placed against. Sums to ``get_global_limits``."""
        reserved_by_node, _ = self._get_split_reserved_by_node()
        return reserved_by_node

    def _get_split_reserved_by_node(
        self,
    ) -> Tuple[
        Dict[NodeIdStr, ExecutionResources], Dict[NodeIdStr, ExecutionResources]
    ]:
        """Read the coordinator reservation once.

        Returns:
            The whole reservation per node, and the placement-group part of it.
        """
        split = self._autoscaling_coordinator.get_reserved_resources()
        reservation = {
            node_id: ExecutionResources.from_resource_dict(resources)
            for node_id, resources in split.total().items()
        }
        pg_reservation = {
            node_id: ExecutionResources.from_resource_dict(resources)
            for node_id, resources in split.placement_group.items()
        }
        signature = tuple(
            sorted(
                NodeReservationSignature(
                    node_id=n, cpu=r.cpu, gpu=r.gpu, memory=r.memory
                )
                for n, r in reservation.items()
            )
        )
        if signature != self._reservation_signature:
            self._reservation_signature = signature
            self._reservation_changed_at = time.monotonic()
        return reservation, pg_reservation

    def seconds_since_allocation_change(self) -> float:
        return time.monotonic() - self._reservation_changed_at

    def _register_placement_groups(self):
        """Register this dataset's placement groups with the coordinator, so
        other requesters are not handed the capacity they hold.

        Replaces the registered set; sends nothing when it hasn't changed.
        """
        # 1. Gather all placement groups per operator. Note that operators
        # can create placement groups on the fly, so that's why it can't be recomputed
        all_op_pgs: List["PlacementGroup"] = []
        for op in self._topology:
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            pg = op.placement_constraint().placement_group
            this_op_pgs = []
            if pg is not None:
                this_op_pgs.append(pg)
            this_op_pgs.extend(op.fn_placement_groups())
            pg_ids = [pg.id.hex() for pg in this_op_pgs]
            for pg in this_op_pgs:
                pg_id = pg.id.hex()
                if pg_id in self._pg_bundles_by_id:
                    continue
                self._pg_bundles_by_id[pg_id] = [
                    ExecutionResources.from_resource_dict(bundle)
                    for bundle in pg.bundle_specs
                ]
            all_op_pgs.extend(this_op_pgs)

        pg_ids = frozenset(pg.id.hex() for pg in all_op_pgs)
        if pg_ids != self._registered_pg_ids:
            self._autoscaling_coordinator.register_placement_groups(pg_ids)
            self._registered_pg_ids = pg_ids

    def get_cluster_view(self) -> DatasetReservationView:
        """Snapshot of the cluster as visible to this dataset right now."""
        self._register_placement_groups()
        reserved_by_node, pg_reserved_by_node = self._get_split_reserved_by_node()
        if self._pg_slots is None:
            self._pg_slots = PlacementGroupSlots.measure(
                (
                    op
                    for op in self._topology
                    if isinstance(op, ExperimentalActorPoolMapOperator)
                ),
                self._pg_bundles_by_id,
            )
        return DatasetReservationView(
            reserved_by_node=reserved_by_node,
            labels_by_node=dict(self._get_node_labels()),
            pg_reserved_by_node=pg_reserved_by_node,
            draining_nodes=frozenset(self._get_draining_node_ids()),
            pg_slots=self._pg_slots,
        )
