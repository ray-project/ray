"""When the sizer evaluates, and the context one evaluation's phases share."""

from __future__ import annotations

import logging
import math
import time
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

from ray.data._internal.execution.execution_flags import SIZER_WARMUP_TIMEOUT_S
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.execution.resource_manager import (
    terminal_operator_from_topology,
)
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    DatasetAllocationView,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (  # noqa: E501
    is_constrained,
)
from ray.data._internal.experimental.execution.sizer.models import (
    TickCapacitySnapshot,
    TickContext,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    OpProfiles,
    is_sink_or_write,
)
from ray.data._internal.experimental.execution.sizer.operator_ordering import (
    ordered_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.pg_bundles import (
    PlacementGroupBundles,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)


def committed_by_node(
    profiles: OpProfiles,
) -> Dict[NodeIdStr, ExecutionResources]:
    """Resources the non-PG pools hold per node: running actors plus pendings
    at their target node.

    PG-managed ops are excluded: their actors live in a placement group
    reservation, which the coordinator already leaves out of this dataset's
    slice.
    """
    committed: Dict[NodeIdStr, ExecutionResources] = {}
    for op in profiles.non_pg_ops:
        for node_id, usage in op.actor_pool.committed_usage_by_node.items():
            committed[node_id] = committed.get(node_id, ExecutionResources.zero()).add(
                usage
            )
    return committed


def draining_by_node(
    profiles: OpProfiles,
) -> Dict[NodeIdStr, ExecutionResources]:
    """The part of ``committed_by_node`` held by draining actors: already on
    its way out, and held only until each actor is fully drained."""
    draining: Dict[NodeIdStr, ExecutionResources] = {}
    for op in profiles.non_pg_ops:
        for node_id, usage in op.actor_pool.draining_usage_by_node().items():
            draining[node_id] = draining.get(node_id, ExecutionResources.zero()).add(
                usage
            )
    return draining


class SizingTicker:
    """Paces the sizer and builds each tick's shared context.

    The executor calls in on every scheduling-loop step, but sizing on that
    ~100ms rate is noise: queue signals are meaningless below ~1s and actor
    startup dominates anything we decide. So this owns the cadence clock and
    hands back a TickContext only when an evaluation is due -- plus the state
    that spans ticks: the warmup latch, the empty-allocation skepticism
    counters, and the placement-group bundles read when the sizer is built.

    Everything it needs about the cluster is handed in once per step, so the
    sizer never reads the resource reporter itself and every phase of a tick
    sees the same numbers.
    """

    def __init__(
        self,
        profiles: OpProfiles,
        *,
        cadence_s: float,
        clock: Callable[[], float] = time.monotonic,
        pg_bundles: Optional[PlacementGroupBundles] = None,
        unknown_pg_cap_by_op: Optional[
            Dict["ExperimentalActorPoolMapOperator", int]
        ] = None,
        warmup_timeout_s: float = SIZER_WARMUP_TIMEOUT_S,
    ):
        self._profiles = profiles
        self.cadence_s = cadence_s
        self.clock = clock
        self.warmup: bool = True
        self._warmup_timeout_s = warmup_timeout_s
        # Time of the first tick; the warmup timeout counts from here.
        self._warmup_started_t: Optional[float] = None
        self.critical_ops: Tuple[
            "ExperimentalActorPoolMapOperator", ...
        ] = self._resolve_critical_ops(profiles.topology)
        # -inf rather than 0: time.monotonic()'s epoch is arbitrary, so only
        # this reads unambiguously as "never evaluated".
        self.last_eval_t: float = float("-inf")
        self._warned_loop_lag = False
        self._alloc_seen_nonempty = False
        self._consecutive_empty_alloc_ticks = 0
        self._pg_bundles = pg_bundles or PlacementGroupBundles()
        self._unknown_pg_cap_by_op: Dict[
            "ExperimentalActorPoolMapOperator", int
        ] = dict(unknown_pg_cap_by_op or {})

    def next_tick(
        self,
        cluster_view: "DatasetAllocationView",
    ) -> Optional[TickContext]:
        """The next sizing tick, or None while inside the cadence window."""
        now = self.clock()
        if now - self.last_eval_t < self.cadence_s:
            return None
        self._warn_if_loop_lagging(now)
        self.last_eval_t = now
        self._update_warmup_latch(now)

        capacity = self.build_capacity_snapshot(cluster_view)
        return TickContext(
            capacity=capacity,
            warmup=self.warmup,
            ordered_ops=tuple(
                ordered_actor_pool_ops(
                    self._profiles.topology,
                    self._profiles.actor_pool_ops,
                    capacity.schedulable_nodes_by_op,
                )
            ),
        )

    def _warn_if_loop_lagging(self, now: float) -> None:
        """The gap between ticks measures the control loop, since the executor
        calls in every step. A slow loop delays every sizing decision, so make
        it visible."""
        since = now - self.last_eval_t
        if not math.isfinite(since):
            return  # first tick of the run
        if since <= 1.5 * self.cadence_s or self._warned_loop_lag:
            return
        self._warned_loop_lag = True
        logger.warning(
            "OperatorSizer: scheduling-loop lag -- %.1fs since last sizer tick "
            "(cadence %.1fs); a slow control loop delays every sizing decision. "
            "Logged once.",
            since,
            self.cadence_s,
        )

    def _update_warmup_latch(self, now: float) -> None:
        """Warmup ends for good once every critical op has produced its first
        output: the pipeline is flowing end to end, so feedback beats
        feedforward from here on.

        A critical op that finished without producing (an empty branch of a union)
        does not hold warmup open. Nor does one that never produces within the
        warmup timeout: leaving warmup early only hands sizing to the warm
        phase, which can still move capacity to it.
        """
        if not self.warmup:
            return
        if self._warmup_started_t is None:
            self._warmup_started_t = now
        waiting_on = [
            op
            for op in self.critical_ops
            if op.metrics.num_task_outputs_generated == 0 and not op.has_completed()
        ]
        if waiting_on:
            if now - self._warmup_started_t < self._warmup_timeout_s:
                return
            logger.warning(
                "OperatorSizer: warmup timed out after %.0fs with no output yet "
                "from %s; switching to the warm phase.",
                now - self._warmup_started_t,
                ", ".join(op.name for op in waiting_on),
            )
            self.warmup = False
            return
        logger.info(
            "OperatorSizer: warmup complete (%s produced first output)",
            ", ".join(op.name for op in self.critical_ops) or "no actor-pool operators",
        )
        self.warmup = False

    # --- capacity snapshot -----------------------------------------------------

    def build_capacity_snapshot(
        self,
        cluster_view: "DatasetAllocationView",
    ) -> TickCapacitySnapshot:
        """Capacity and schedulability as of now, for every phase of this tick.

        Draining nodes are left out of ``reserved``, so anything we hold there
        shows up in ``nodes_to_reclaim``. Nodes to reclaim are left out of
        ``free``: an actor placed there would be evicted next tick.
        """
        reserved = {
            node_id: res
            for node_id, res in cluster_view.reserved_by_node.items()
            if node_id not in cluster_view.draining_nodes
        }
        nodes_to_reclaim = self._nodes_to_reclaim(reserved)
        committed = committed_by_node(self._profiles)
        zero = ExecutionResources.zero()
        free = {
            node_id: res.subtract(committed.get(node_id, zero)).max(zero)
            for node_id, res in reserved.items()
            if node_id not in nodes_to_reclaim
        }

        # Match against this allocation, so a schedulable node is never one we
        # cannot place on.
        view_for_match = DatasetAllocationView(
            reserved_by_node=reserved, labels_by_node=cluster_view.labels_by_node
        )
        schedulable_nodes_by_op = {}
        per_actor_resources_by_op = {}
        for op in self._profiles.actor_pool_ops:
            per_actor = op.actor_pool.per_actor_resource_usage()
            per_actor_resources_by_op[op] = per_actor
            schedulable_nodes_by_op[op] = op.placement_constraint().matching_nodes(
                view_for_match, per_actor
            )

        return TickCapacitySnapshot(
            free_by_node=free,
            reserved_by_node=reserved,
            nodes_to_reclaim=nodes_to_reclaim,
            schedulable_nodes_by_op=schedulable_nodes_by_op,
            per_actor_resources_by_op=per_actor_resources_by_op,
            constrained_op_count_by_node=self._constrained_op_count_by_node(
                schedulable_nodes_by_op
            ),
            pg_free_by_bundle=self._pg_bundles.free_by_bundle(),
            schedulable_bundles_by_op=dict(self._pg_bundles.bundles_by_op),
            unknown_pg_headroom_by_op=self._unknown_pg_headroom(),
        )

    def _constrained_op_count_by_node(
        self,
        schedulable_nodes_by_op: Dict[
            "ExperimentalActorPoolMapOperator", AbstractSet[NodeIdStr]
        ],
    ) -> Dict[NodeIdStr, int]:
        """How many constrained non-PG ops can use each node.

        An unconstrained op can use every node, so counting it would add the
        same +1 everywhere and never change an ordering; only constrained ops
        tell nodes apart. The ledger fills the least contended nodes first,
        and placement breaks ties the same way.
        """
        count: Dict[NodeIdStr, int] = {}
        for op, nodes in schedulable_nodes_by_op.items():
            if self._profiles.is_pg_managed(op) or not is_constrained(op):
                continue
            for node_id in nodes:
                count[node_id] = count.get(node_id, 0) + 1
        return count

    def _unknown_pg_headroom(self) -> Dict["ExperimentalActorPoolMapOperator", int]:
        """How many more actors each op whose PG capacity is unknown may add
        under its pool's max size.

        Counts ``total_size``, not ``serving_size``: a draining actor keeps
        its PG until it is actually dead.
        """
        return {
            op: max(0, cap - op.actor_pool.total_size())
            for op, cap in self._unknown_pg_cap_by_op.items()
        }

    # --- shrinking allocation -------------------------------------------------

    def _nodes_to_reclaim(
        self, reserved: Dict[NodeIdStr, ExecutionResources]
    ) -> Dict[NodeIdStr, ExecutionResources]:
        """Where we now hold more than the coordinator allows.

        An empty allocation usually means the coordinator cache has not been
        filled yet, not that our slice was revoked, so we do not evict on one
        until it has persisted for two ticks. A non-empty read is always acted
        on -- a partial shrink is real.
        """
        if reserved:
            self._alloc_seen_nonempty = True
            self._consecutive_empty_alloc_ticks = 0
            return self.compute_nodes_to_reclaim(reserved, self._profiles)
        if not self._alloc_seen_nonempty:
            logger.debug(
                "OperatorSizer: allocation empty before first grant -- "
                "skipping reclaim this tick."
            )
            return {}
        self._consecutive_empty_alloc_ticks += 1
        if self._consecutive_empty_alloc_ticks < 2:
            logger.debug(
                "OperatorSizer: allocation read empty after a prior grant -- "
                "holding evictions one tick in case it is transient."
            )
            return {}
        return self.compute_nodes_to_reclaim(reserved, self._profiles)

    @staticmethod
    def compute_nodes_to_reclaim(
        reserved: Dict[NodeIdStr, ExecutionResources],
        profiles: OpProfiles,
    ) -> Dict[NodeIdStr, ExecutionResources]:
        """Per-node excess of committed usage over the coordinator's slice,
        net of draining actors.

        Draining actors already leave without further help, and they are never
        eviction victims, so counting them would shed replacements for them on
        every tick until they finish draining. It would also cancel the actors
        a capacity transfer targets into the slots its donors are vacating.

        A node missing from ``reserved`` (dropped, or draining) is over by
        everything we hold on it that is not already draining.
        """
        zero = ExecutionResources.zero()
        draining = draining_by_node(profiles)
        reclaim: Dict[NodeIdStr, ExecutionResources] = {}
        for node_id, used in committed_by_node(profiles).items():
            leaving = draining.get(node_id, zero)
            over = (
                used.subtract(leaving).subtract(reserved.get(node_id, zero)).max(zero)
            )
            if not over.is_zero():
                reclaim[node_id] = over
        return reclaim

    # --- warmup latch ---------------------------------------------------------

    @staticmethod
    def _resolve_critical_ops(
        topology: "Topology",
    ) -> Tuple["ExperimentalActorPoolMapOperator", ...]:
        """The last map operator on each path back from the output.

        Warmup ends once every one of them has produced its first output --
        that is what "the pipeline is flowing end to end" means in practice.

        Walking back is necessary because only map operators report task
        outputs. Limit, union, mix, aggregate, the output splitter and the
        input buffer never increment ``num_task_outputs_generated``, so
        watching one of those would keep the sizer in warmup for the whole run.
        Write operators are skipped too: they are the sink, and the useful
        signal is one step earlier.

            read -> map -> limit -> split    watches: map
            read -> map -> write             watches: map

        There can be more than one, because union and mix take two or more
        inputs, so there is no single last operator:

            read_a -> map_a ---\
                                union -> split    watches: map_a and map_b
            read_b -> map_b ---/

        Waiting for both is deliberate. If warmup ended as soon as map_a
        produced, map_b would be sized from feedback while its queues were
        still empty -- it would read as idle and be drained back to min_size
        just as its data arrived.
        """

        def walk(
            allow_sinks: bool,
        ) -> Tuple["ExperimentalActorPoolMapOperator", ...]:
            found: List["ExperimentalActorPoolMapOperator"] = []

            def visit(op: "PhysicalOperator") -> None:
                if isinstance(op, ExperimentalActorPoolMapOperator) and (
                    allow_sinks or not is_sink_or_write(op)
                ):
                    found.append(op)
                    return
                for upstream in op.input_dependencies:
                    visit(upstream)

            visit(terminal_operator_from_topology(topology))
            return tuple(found)

        # A pipeline that is only a write has no non-sink map op; latch on the
        # sink itself rather than never leaving warmup.
        return walk(allow_sinks=False) or walk(allow_sinks=True)
