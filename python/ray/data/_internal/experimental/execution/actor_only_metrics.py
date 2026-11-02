"""Prometheus metrics specific to the actor-only backend.

Everything here is derived from state the standard metrics do not see: the
``ResourceBank``'s live and cumulative object-store accounting (per node and per
(operator, node)), per-actor output limits, and the escape hatch's upgrades.
``ExperimentalStreamingExecutor`` owns one ``ActorOnlyMetrics`` per dataset
execution and refreshes it on a fixed interval from the scheduling loop; the
operator and its actor pool only report events into it (output overshoots and
drain outcomes, through the structural ``_ActorOnlyOpMetrics`` seam), so they
carry no dependency on this module.
"""

from __future__ import annotations

import math
import time
from collections import Counter as cCounter, defaultdict
from enum import Enum
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Set, Tuple

from ray.data._internal.execution.interfaces.common import ActorStatus, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.util.metrics import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_bank import ResourceBankBase
    from ray.data._internal.execution.streaming_executor_state import Topology


class ObjStoreBreakdown(str, Enum):
    INPUTS = "inputs"
    OUTPUTS = "outputs"
    REMOTE = "remote"
    LOCAL = "local"
    FREED = "freed"


class ObjStoreUnit(str, Enum):
    BYTES = "bytes"
    BLOCKS = "blocks"


class Stat(str, Enum):
    MIN = "min"
    P50 = "p50"
    P90 = "p90"
    MAX = "max"
    MEAN = "mean"


class ActorOnlyTags(tuple, Enum):
    DATASET_NODE_UNIT_BREAKDOWN = ("dataset", "node", "unit", "breakdown")
    DATASET_NODE_STATUS = ("dataset", "node", "status")
    DATASET_NODE_OPERATOR_STATUS = ("dataset", "node", "operator", "status")
    DATASET_NODE_OPERATOR_UNIT_BREAKDOWN = (
        "dataset",
        "node",
        "operator",
        "unit",
        "breakdown",
    )
    DATASET_OPERATOR = ("dataset", "operator")
    DATASET_OPERATOR_UNIT_STAT = ("dataset", "operator", "unit", "stat")
    DATASET_OPERATOR_KIND = ("dataset", "operator", "kind")
    DATASET_OPERATOR_REASON = ("dataset", "operator", "reason")
    DATASET_OPERATOR_REASON_CAUSE = ("dataset", "operator", "reason", "cause")


# Buckets for actor-drain durations. Drains resolve on the scale of node grace
# periods and failure-detection timeouts (seconds to ~2 minutes); the generic
# task-duration ladder is too coarse there and pins quantiles to a boundary.
DRAIN_TIME_BUCKETS_S = [
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    30.0,
    45.0,
    60.0,
    90.0,
    120.0,
    180.0,
    300.0,
    600.0,
]


def _limit_stats(values: List[float]) -> Dict[Stat, float]:
    """min/p50/p90/max/mean over a small, fully materialized list (one entry
    per actor), computed exactly -- no sketch needed."""
    if not values:
        return {stat: 0.0 for stat in Stat}
    ordered = sorted(values)
    n = len(ordered)

    def _pct(q: float) -> float:
        # Nearest-rank percentile.
        rank = min(n - 1, max(0, math.ceil(q * n) - 1))
        return ordered[rank]

    return {
        Stat.MIN: ordered[0],
        Stat.P50: _pct(0.50),
        Stat.P90: _pct(0.90),
        Stat.MAX: ordered[-1],
        Stat.MEAN: sum(ordered) / n,
    }


class ActorOnlyRecorders:
    """The raw gauges / counters. Module-level singletons (Ray metrics are
    process-global), shared by every dataset in the process."""

    node_obj_store = Gauge(
        "data_actor_only_node_obj_store",
        description="Object store on a node",
        tag_keys=ActorOnlyTags.DATASET_NODE_UNIT_BREAKDOWN.value,
    )

    node_num_actors = Gauge(
        "data_actor_only_node_num_actors",
        description="Number of actors on a node by state (see ActorStatus)",
        tag_keys=ActorOnlyTags.DATASET_NODE_STATUS.value,
    )

    node_op_num_actors = Gauge(
        "data_actor_only_node_operator_num_actors",
        description="Number of actors on a node for an operator by state",
        tag_keys=ActorOnlyTags.DATASET_NODE_OPERATOR_STATUS.value,
    )

    node_op_input_locality = Gauge(
        "data_actor_only_node_operator_input_locality",
        description=(
            "Cumulative input bytes/blocks submitted to an operator's actors on "
            "a node, by locality. Summing over nodes gives the per-operator "
            "split; summing over operators gives the per-node split."
        ),
        tag_keys=ActorOnlyTags.DATASET_NODE_OPERATOR_UNIT_BREAKDOWN.value,
    )

    output_limit_upgrades = Counter(
        "data_actor_only_output_limit_upgrades",
        description=(
            "Times the output-limit escape hatch raised an operator's per-actor "
            "output limit to break a deadlock."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR.value,
    )

    op_output_limit = Gauge(
        "data_actor_only_operator_output_limit",
        description=(
            "Snapshot distribution of per-actor output limits for an operator "
            "(by stat)."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_UNIT_STAT.value,
    )

    op_output_backpressured_actors = Gauge(
        "data_actor_only_operator_output_backpressured_actors",
        description=(
            "Number of an operator's actors whose output pull budget is "
            "exhausted (output-backpressured)."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR.value,
    )

    op_output_overshoots = Counter(
        "data_actor_only_operator_output_overshoots",
        description=(
            "Cumulative count, per operator, of output-limit overshoots by "
            "kind: 'block_size' (a produced block exceeded 150% of its target "
            "block size) and 'output_bytes' (an actor's outstanding output "
            "exceeded 150% of its output-bytes limit)."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_KIND.value,
    )

    # Actor draining. A downscaled actor drains: it takes no new tasks and is
    # released only once it has no in-flight tasks and the ResourceBank counts
    # no outputs against it. A drain ends completed, interrupted (the actor
    # died still owing work) or cancelled (revived by a reclaim or a core
    # restart). ``reason`` is why the drain began (DrainReason in the actor
    # pool: resize | preemption | alloc_shrink | unknown) and never changes.
    num_draining_actors = Gauge(
        "data_num_draining_actors",
        description="Number of an operator's actors currently draining.",
        tag_keys=ActorOnlyTags.DATASET_OPERATOR.value,
    )
    actor_drains_started = Counter(
        "data_actor_drains_started",
        description="Actors that started draining, tagged by drain reason.",
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_REASON.value,
    )
    actor_drains_completed = Counter(
        "data_actor_drains_completed",
        description=(
            "Actors whose drain completed: released with no in-flight tasks "
            "and no outstanding outputs. Tagged by drain reason."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_REASON.value,
    )
    actor_drains_interrupted = Counter(
        "data_actor_drains_interrupted",
        description=(
            "Actors that died before their drain completed (e.g. the node was "
            "torn down under them), discarding whatever they still owed. "
            "Tagged by drain reason."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_REASON.value,
    )
    actor_drains_cancelled = Counter(
        "data_actor_drains_cancelled",
        description=(
            "Actors whose drain was cancelled and rejoined the pool. Tagged by "
            "drain reason and by cause: reclaimed (an upscale revived it) | "
            "core_restart (Ray core restarted an actor that still owes work)."
        ),
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_REASON_CAUSE.value,
    )
    actor_drain_completed_time = Histogram(
        "data_actor_drain_completed_time",
        description="Time from drain start to release, tagged by drain reason.",
        boundaries=DRAIN_TIME_BUCKETS_S,
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_REASON.value,
    )
    actor_drain_interrupted_time = Histogram(
        "data_actor_drain_interrupted_time",
        description=(
            "Time from drain start until the actor's death was detected. Under "
            "abrupt node loss this includes the failure-detection latency, so "
            "it is an upper bound on the real drain window. Tagged by drain "
            "reason."
        ),
        boundaries=DRAIN_TIME_BUCKETS_S,
        tag_keys=ActorOnlyTags.DATASET_OPERATOR_REASON.value,
    )


class _StaleTrackingGauge:
    """A Gauge whose series reset to 0 once they stop being written.

    A Gauge holds its last value forever, so a label set that stops being
    emitted (an actor leaves a node, an operator finishes, a node dies) would
    linger at a stale value. Callers ``set`` during a refresh cycle and then
    call ``flush_stale`` exactly once: every series written in the previous
    cycle but not this one is zeroed. Tracking is per ``ActorOnlyMetrics``
    instance (per dataset) even though the Gauge itself is process-global.
    """

    def __init__(self, gauge: Gauge):
        self._gauge = gauge
        self._emitted: Set[Tuple[Tuple[str, str], ...]] = set()
        self._current: Set[Tuple[Tuple[str, str], ...]] = set()

    def set(self, value: float, tags: Dict[str, str]) -> None:
        self._gauge.set(value, tags=tags)
        self._current.add(tuple(sorted(tags.items())))

    def flush_stale(self) -> None:
        for key in self._emitted - self._current:
            self._gauge.set(0, tags=dict(key))
        self._emitted = self._current
        self._current = set()


class ActorOnlyMetrics:
    """Per-dataset refresher for the actor-only gauges and counters."""

    def __init__(self, *, dataset_id: str, update_interval: float):
        self._dataset_id = dataset_id
        self._last_updated: float = 0.0
        self._update_interval: float = update_interval
        self.recorder = ActorOnlyRecorders()
        # Gauges get stale-series resetting; the counters are cumulative and
        # stay on ``recorder``.
        self._node_obj_store = _StaleTrackingGauge(self.recorder.node_obj_store)
        self._node_num_actors = _StaleTrackingGauge(self.recorder.node_num_actors)
        self._node_op_num_actors = _StaleTrackingGauge(self.recorder.node_op_num_actors)
        self._node_op_input_locality = _StaleTrackingGauge(
            self.recorder.node_op_input_locality
        )
        self._op_output_limit = _StaleTrackingGauge(self.recorder.op_output_limit)
        self._op_output_backpressured_actors = _StaleTrackingGauge(
            self.recorder.op_output_backpressured_actors
        )
        self._num_draining_actors = _StaleTrackingGauge(
            self.recorder.num_draining_actors
        )

    # --- counters, recorded at the event ---

    def record_output_limit_upgrade(self, op_tag: str) -> None:
        self.recorder.output_limit_upgrades.inc(
            1, tags={"dataset": self._dataset_id, "operator": op_tag}
        )

    def record_block_size_overshoot(self, op_tag: str) -> None:
        """A produced block exceeded 150% of its target size."""
        self._record_overshoot(op_tag, kind="block_size")

    def record_output_limit_overshoot(self, op_tag: str) -> None:
        """An actor's outstanding output exceeded 150% of its output-bytes
        limit."""
        self._record_overshoot(op_tag, kind="output_bytes")

    def _record_overshoot(self, op_tag: str, *, kind: str) -> None:
        self.recorder.op_output_overshoots.inc(
            1, tags={"dataset": self._dataset_id, "operator": op_tag, "kind": kind}
        )

    # --- drain events, recorded by the actor pool (see _ActorOnlyOpMetrics) ---

    def record_drain_started(self, op_tag: str, reason: str) -> None:
        self.recorder.actor_drains_started.inc(1, tags=self._drain_tags(op_tag, reason))

    def record_drain_completed(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        tags = self._drain_tags(op_tag, reason)
        self.recorder.actor_drains_completed.inc(1, tags=tags)
        self.recorder.actor_drain_completed_time.observe(duration_s, tags=tags)

    def record_drain_interrupted(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        tags = self._drain_tags(op_tag, reason)
        self.recorder.actor_drains_interrupted.inc(1, tags=tags)
        self.recorder.actor_drain_interrupted_time.observe(duration_s, tags=tags)

    def record_drain_cancelled(self, op_tag: str, reason: str, cause: str) -> None:
        self.recorder.actor_drains_cancelled.inc(
            1, tags={**self._drain_tags(op_tag, reason), "cause": cause}
        )

    def _drain_tags(self, op_tag: str, reason: str) -> Dict[str, str]:
        return {"dataset": self._dataset_id, "operator": op_tag, "reason": reason}

    # --- gauges, refreshed on an interval ---

    def maybe_update(
        self,
        *,
        topology: "Topology",
        resource_bank: "ResourceBankBase",
        force: bool = False,
    ) -> None:
        now = time.perf_counter()
        if (
            not force
            and self._last_updated > 0
            and (now - self._last_updated) < self._update_interval
        ):
            return
        self._last_updated = now
        self._update_node_object_store(resource_bank=resource_bank)
        self._update_actor_counts(topology=topology)
        self._update_operator_limits(topology=topology)
        self._update_node_operator_locality(
            topology=topology, resource_bank=resource_bank
        )
        self._update_output_backpressure(topology=topology)
        self._update_draining_actors(topology=topology)

    def _update_node_object_store(self, *, resource_bank: "ResourceBankBase") -> None:
        for node_id in resource_bank.node_ids():
            live = resource_bank.live_object_store(node_id=node_id)
            base_tags = {"dataset": self._dataset_id, "node": node_id}
            for unit, breakdown, value in (
                (
                    ObjStoreUnit.BYTES,
                    ObjStoreBreakdown.OUTPUTS,
                    live.get_output_bytes(),
                ),
                (
                    ObjStoreUnit.BLOCKS,
                    ObjStoreBreakdown.OUTPUTS,
                    live.get_output_blocks(),
                ),
                (ObjStoreUnit.BYTES, ObjStoreBreakdown.INPUTS, live.get_input_bytes()),
                (
                    ObjStoreUnit.BLOCKS,
                    ObjStoreBreakdown.INPUTS,
                    live.get_input_blocks(),
                ),
            ):
                self._node_obj_store.set(
                    value,
                    tags={
                        **base_tags,
                        "unit": unit.value,
                        "breakdown": breakdown.value,
                    },
                )

            cumulative = resource_bank.cumulative_object_store(node_id=node_id)
            # Freed = ever-produced minus still-live, outputs only: counting
            # inputs too would double-count same-node local transfers (the
            # output is withdrawn, then re-charged as the consumer's input).
            # The cumulative store counts an output once it is PULLED, while the
            # live store also holds prebuffered (not yet pulled) outputs, so the
            # prebuffered portion is excluded from the subtracted term or
            # ``freed`` could go negative. One ``output_bytes`` sample is added
            # per pulled block, so ``num_samples`` doubles as the block count.
            live_pulled_bytes = (
                live.get_output_bytes() - live.num_prebuffered_output_bytes
            )
            live_pulled_blocks = (
                live.get_output_blocks() - live.num_prebuffered_output_blocks
            )
            freed_bytes = cumulative.output_bytes.sum - live_pulled_bytes
            freed_blocks = cumulative.output_bytes.num_samples - live_pulled_blocks
            assert freed_bytes >= 0 and freed_blocks >= 0, (freed_bytes, freed_blocks)
            # Locality: only task INPUTS can be remote; every output is local to
            # the task that produced it.
            local_bytes = cumulative.input_bytes_local.sum + cumulative.output_bytes.sum
            remote_bytes = cumulative.input_bytes_remote.sum
            for unit, breakdown, value in (
                (ObjStoreUnit.BYTES, ObjStoreBreakdown.FREED, freed_bytes),
                (ObjStoreUnit.BLOCKS, ObjStoreBreakdown.FREED, freed_blocks),
                (ObjStoreUnit.BYTES, ObjStoreBreakdown.LOCAL, local_bytes),
                (ObjStoreUnit.BYTES, ObjStoreBreakdown.REMOTE, remote_bytes),
            ):
                self._node_obj_store.set(
                    value,
                    tags={
                        **base_tags,
                        "unit": unit.value,
                        "breakdown": breakdown.value,
                    },
                )

        self._node_obj_store.flush_stale()

    def _update_actor_counts(self, *, topology: "Topology") -> None:
        # Per (node, operator) is emitted; per node is aggregated here. The
        # per-operator view is left to Grafana (sum over nodes).
        node_totals: DefaultDict[NodeIdStr, cCounter[ActorStatus]] = defaultdict(
            cCounter
        )
        for op, op_state in topology.items():
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            op_tag = op_state.op_tag()
            for node_id, counts in op.actor_counts_by_node().items():
                for status, value in counts.items():
                    node_totals[node_id][status] += value
                    self._node_op_num_actors.set(
                        value,
                        tags={
                            "dataset": self._dataset_id,
                            "node": node_id,
                            "operator": op_tag,
                            "status": status.value,
                        },
                    )

        for node_id, counts in node_totals.items():
            for status, value in counts.items():
                self._node_num_actors.set(
                    value,
                    tags={
                        "dataset": self._dataset_id,
                        "node": node_id,
                        "status": status.value,
                    },
                )

        self._node_op_num_actors.flush_stale()
        self._node_num_actors.flush_stale()

    def _update_operator_limits(self, *, topology: "Topology") -> None:
        for op, op_state in topology.items():
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            op_tag = op_state.op_tag()
            # Distribution of the per-actor output limits across the pool.
            # Unbounded (infinite) limits are excluded so they don't skew it.
            pool = op.actor_pool
            limits_by_unit: Dict[ObjStoreUnit, List[float]] = {
                ObjStoreUnit.BYTES: [],
                ObjStoreUnit.BLOCKS: [],
            }
            for actor_id in pool.get_logical_ids():
                bytes_limit = pool.output_bytes_limit(actor_id)
                blocks_limit = pool.output_count_limit(actor_id)
                if math.isfinite(bytes_limit):
                    limits_by_unit[ObjStoreUnit.BYTES].append(bytes_limit)
                if math.isfinite(blocks_limit):
                    limits_by_unit[ObjStoreUnit.BLOCKS].append(blocks_limit)

            for unit, values in limits_by_unit.items():
                if not values:
                    # No live actors, or all unbounded: flush_stale resets any
                    # prior series.
                    continue
                for stat, value in _limit_stats(values).items():
                    self._op_output_limit.set(
                        value,
                        tags={
                            "dataset": self._dataset_id,
                            "operator": op_tag,
                            "unit": unit.value,
                            "stat": stat.value,
                        },
                    )

        self._op_output_limit.flush_stale()

    def _update_node_operator_locality(
        self, *, topology: "Topology", resource_bank: "ResourceBankBase"
    ) -> None:
        """Input-locality split joined on (node, operator). Grafana derives the
        per-operator and per-node views by summing over the other dimension, so
        all three groupings agree by construction."""
        op_tags: Dict[PhysicalOperator, str] = {
            op: op_state.op_tag()
            for op, op_state in topology.items()
            if isinstance(op, ExperimentalActorPoolMapOperator)
        }
        for op, node_id in resource_bank.op_node_ids():
            op_tag = op_tags.get(op)
            if op_tag is None:
                # Not (or no longer) an actor-pool operator in this topology;
                # flush_stale resets any prior series.
                continue
            cumulative = resource_bank.cumulative_object_store(op=op, node_id=node_id)
            local = cumulative.input_bytes_local
            remote = cumulative.input_bytes_remote
            base_tags = {
                "dataset": self._dataset_id,
                "node": node_id,
                "operator": op_tag,
            }
            for unit, breakdown, value in (
                (ObjStoreUnit.BYTES, ObjStoreBreakdown.LOCAL, local.sum),
                (ObjStoreUnit.BYTES, ObjStoreBreakdown.REMOTE, remote.sum),
                (ObjStoreUnit.BLOCKS, ObjStoreBreakdown.LOCAL, local.num_samples),
                (ObjStoreUnit.BLOCKS, ObjStoreBreakdown.REMOTE, remote.num_samples),
            ):
                self._node_op_input_locality.set(
                    value,
                    tags={
                        **base_tags,
                        "unit": unit.value,
                        "breakdown": breakdown.value,
                    },
                )

        self._node_op_input_locality.flush_stale()

    def _update_output_backpressure(self, *, topology: "Topology") -> None:
        for op, op_state in topology.items():
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            # Periodic observer: read the operator's cached count rather than
            # paying for the per-actor scan; the accessor recomputes only when
            # no exact-value caller has refreshed it recently.
            blocked, _running = op.cached_output_backpressured_actors()
            self._op_output_backpressured_actors.set(
                blocked,
                tags={"dataset": self._dataset_id, "operator": op_state.op_tag()},
            )

        self._op_output_backpressured_actors.flush_stale()

    def _update_draining_actors(self, *, topology: "Topology") -> None:
        for op, op_state in topology.items():
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            self._num_draining_actors.set(
                op.actor_pool.num_draining_actors(),
                tags={"dataset": self._dataset_id, "operator": op_state.op_tag()},
            )

        self._num_draining_actors.flush_stale()
