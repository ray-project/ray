"""Everything the sizer reports, and the state that keeps it quiet."""

from __future__ import annotations

import functools
import logging
import time
from collections import Counter
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set, TypeVar, cast

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.util import locality_string, memory_string
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    PlacedScalingRequest,
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer.common import (
    ExecutionResourcesByNode,
)
from ray.data._internal.experimental.execution.sizer.metrics import SizerMetrics
from ray.data._internal.experimental.execution.sizer.models import (
    ClusterStateSnapshot,
    PlacedRequests,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
    )
    from ray.data._internal.experimental.execution.sizer.op_status import OpStatus
    from ray.data._internal.experimental.execution.sizer.policy.states import (
        OpDecision,
        SizerPhase,
    )

logger = logging.getLogger(__name__)

_F = TypeVar("_F", bound=Callable[..., Any])


def timed(phase: str) -> Callable[[_F], _F]:
    """Method decorator: add each call's wall time to ``self._telemetry``
    under ``phase``. Calls add up until the next ``record_tick`` emits and
    clears them, so a method that runs on every scheduling-loop step reports
    its total since the last tick."""

    def decorate(fn: _F) -> _F:
        @functools.wraps(fn)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            t0 = time.perf_counter()
            try:
                return fn(self, *args, **kwargs)
            finally:
                self._telemetry.add_timing_phase(phase, time.perf_counter() - t0)

        return cast(_F, wrapper)

    return decorate


class SizerTelemetry:
    """The control loop runs every ``SIZER_CADENCE_S``, so unconditional
    logging would bury the interesting lines; each summary is logged only
    when it changes."""

    def __init__(self, dataset_id: str, *, metrics: Optional[SizerMetrics] = None):
        """``metrics`` defaults to a new ``SizerMetrics`` for the dataset; tests
        pass a recorder."""
        self._dataset_id = dataset_id
        self._metrics = metrics if metrics is not None else SizerMetrics(dataset_id)
        # Last summaries logged, to suppress repeats.
        self._last_placement_summary: Optional[str] = None
        self._last_capacity_summary: Optional[str] = None
        self._last_shortfall_summary: Optional[str] = None
        # Cumulative cross-node input bytes per op at the last tick, to log the
        # per-tick delta (see _log_locality).
        self._last_remote_bytes: Dict[ExperimentalActorPoolMapOperator, int] = {}
        # Wall time per phase since the last ``record_tick`` (see ``timed``).
        self._timings: Dict[str, float] = {}
        # Ops warned about as unplaceable, so each is warned about once until
        # it can be placed again (see ``warn_unplaceable_ops``).
        self._unplaceable_ops: Set[ExperimentalActorPoolMapOperator] = set()

    def add_timing_phase(self, phase: str, seconds: float) -> None:
        """Add ``seconds`` to ``phase``'s total for the current tick."""
        self._timings[phase] = self._timings.get(phase, 0.0) + seconds

    # --- how-many pass -------------------------------------------------------

    def log_decisions(
        self,
        *,
        phase: "SizerPhase",
        decisions: Dict[ExperimentalActorPoolMapOperator, "OpDecision"],
        statuses: Dict[ExperimentalActorPoolMapOperator, "OpStatus"],
    ) -> None:
        """Log, once per tick, why each op is sized the way it is: the
        phase, and each op's status, rule, delta and reason. The delta is
        what fits after the ledger; ``no room for n`` is the part that did
        not."""
        parts = []
        for op, op_decision in decisions.items():
            status = statuses[op].kind.name.lower()
            rule = op_decision.rule.name.lower()
            request = op_decision.request
            no_room = f" (no room for {op_decision.unmet})" if op_decision.unmet else ""
            if request is None:
                parts.append(f"{op.name}[{status}]: {rule}{no_room}")
                continue
            parts.append(
                f"{op.name}[{status}]: {rule} {request.delta:+d} "
                f"({request.reason}){no_room}"
            )
        logger.debug("OperatorSizer tick: phase=%s | %s", phase.value, "; ".join(parts))

    def warn_unplaceable_ops(
        self,
        *,
        topology: "Topology",
        snapshot: ClusterStateSnapshot,
    ) -> None:
        """Warn when an op has queued input and no actors, and no node in
        this dataset's slice can hold one of its actors. Without this, the
        pipeline stalls with no error: the sizer grants the op nothing, and
        nothing else reports why.

        No node can hold an actor when none matches the op's label selector,
        or when every matching node's slice is smaller than one actor. Ops
        placed through a placement group are skipped, since Ray core places
        them. Each op is warned about once until it can be placed again.
        """
        if not snapshot.reserved_by_node:
            # No slice yet (for example, at startup): every op looks
            # unplaceable, and nothing is wrong.
            return
        for op, nodes in snapshot.schedulable_nodes_by_op.items():
            constraint = op.placement_constraint()
            stuck = (
                not nodes
                and not constraint.is_pg_managed
                and not op.has_completed()
                and op.actor_pool.serving_size() == 0
                and topology[op].total_enqueued_input_blocks() > 0
            )
            if not stuck:
                self._unplaceable_ops.discard(op)
                continue
            if op in self._unplaceable_ops:
                continue
            self._unplaceable_ops.add(op)
            logger.warning(
                "OperatorSizer: %s has %d queued input block(s) and no actors, "
                "and none of the %d node(s) in this dataset's slice can hold "
                "one of its actors (label_selector=%s, per actor: %s). Check "
                "that the cluster's nodes carry the labels the op selects and "
                "have room for one actor; until then, the pipeline cannot make "
                "progress.",
                op.name,
                topology[op].total_enqueued_input_blocks(),
                len(snapshot.reserved_by_node),
                constraint.label_selector,
                op.actor_pool.per_actor_resource_usage(),
            )

    # --- scale_where ---------------------------------------------------------

    def log_placement_shortfall(
        self,
        *,
        op: ExperimentalActorPoolMapOperator,
        num_placed: int,
        num_new: int,
        num_reclaimed: int,
        free: ExecutionResourcesByNode,
        per_actor: ExecutionResources,
    ) -> None:
        total_free = ExecutionResources.zero()
        for resources in free.values():
            total_free = total_free.add(resources)
        summary = (
            f"{op.name}: placed {num_placed}/{num_new} new "
            f"(reclaimed {num_reclaimed}; free across {len(free)} nodes: "
            f"{total_free.cpu:.0f} cpu, {total_free.gpu:g} gpu, "
            f"{total_free.memory / 1e9:.0f} GiB mem; per-actor "
            f"{per_actor.cpu:g}cpu/{per_actor.gpu:g}gpu/"
            f"{per_actor.memory / 1e9:.1f}GiBmem)"
        )
        if summary != self._last_shortfall_summary:
            logger.debug(
                "OperatorSizer placement shortfall: %s -- remainder "
                "dropped, re-requested next tick.",
                summary,
            )
            self._last_shortfall_summary = summary

    def log_placement(
        self,
        alloc: ExecutionResourcesByNode,
        free: ExecutionResourcesByNode,
        placed: Dict[ExperimentalActorPoolMapOperator, "PlacedScalingRequest"],
        placement: "ActorPlacementStrategy",
    ) -> None:
        """What was placed where, and the capacity the strategy saw doing it."""
        if not alloc:
            if self._last_capacity_summary != "":
                logger.debug(
                    "OperatorSizer capacity: empty allocation -- no "
                    "placement-eligible nodes this tick."
                )
                self._last_capacity_summary = ""
            return
        if not placed:
            return
        breakdown = placement.upscale_breakdown()
        parts = []
        for op, req in placed.items():
            where = dict(Counter(req.nodes)) if req.nodes else "untargeted"
            if isinstance(req, ScaleUpRequest):
                via = breakdown.get(op.id)
                via_str = (
                    f" via={via.collocated}collocate/{via.spread}spread" if via else ""
                )
                reclaim_str = (
                    f" reclaim={req.num_to_reclaim}" if req.num_to_reclaim else ""
                )
                parts.append(f"{op.name}: +{req.delta}@{where}{reclaim_str}{via_str}")
            else:
                parts.append(f"{op.name}: {req.delta}@{where}")
        summary = "; ".join(parts)
        cap = self._capacity_map(alloc, free)
        if (
            summary != self._last_placement_summary
            or cap != self._last_capacity_summary
        ):
            logger.debug(
                "OperatorSizer placement: %s | capacity(%d nodes): %s",
                summary,
                len(alloc),
                cap,
            )
            self._last_placement_summary = summary
            self._last_capacity_summary = cap

    @staticmethod
    def _capacity_map(
        alloc: ExecutionResourcesByNode,
        free: ExecutionResourcesByNode,
    ) -> str:
        """Per-node free/allocated, compact enough for one log line."""
        parts = []
        for node_id in sorted(alloc):
            a, f = alloc[node_id], free.get(node_id)
            entry = f"{node_id[:8]}: {(f.cpu if f else 0) or 0:.0f}/{a.cpu or 0:.0f}cpu"
            if a.gpu:
                entry += f" {(f.gpu if f else 0) or 0:g}/{a.gpu:g}gpu"
            if a.memory:
                free_mem = (f.memory if f else 0) or 0
                entry += f" {memory_string(free_mem)}/{memory_string(a.memory)}mem"
            if a.object_store_memory:
                free_obj = (f.object_store_memory if f else 0) or 0
                entry += (
                    f" {memory_string(free_obj)}/"
                    f"{memory_string(a.object_store_memory)}obj"
                )
            parts.append(entry)
        return "; ".join(parts)

    # --- per-tick roll-up ----------------------------------------------------

    def record_tick(
        self,
        *,
        topology: "Topology",
        decisions: Dict[ExperimentalActorPoolMapOperator, "OpDecision"],
        where: PlacedRequests,
    ) -> None:
        """Close out one evaluation: emit and clear the phase timings
        collected since the last tick, then the actor deltas.

        The phase histograms observe every tick, including ones that sized
        nothing. The verbose roll-up is logged only when something actually
        changed.
        """
        timings, self._timings = self._timings, {}
        for phase, seconds in timings.items():
            self._metrics.record_tick_duration(phase, seconds)

        decided = {
            op: d.request.delta for op, d in decisions.items() if d.request is not None
        }
        placed = {op: req.delta for op, req in where.by_op.items()}
        if not decided and not placed:
            return

        for phase, deltas in (
            ("per_op_how_many", decided),
            ("per_op_where", placed),
        ):
            for op, delta in deltas.items():
                self._metrics.record_actor_delta(
                    operator=topology[op].op_tag(), phase=phase, delta=delta
                )

        up = down = 0
        per_op = []
        for op, delta in placed.items():
            if not delta:
                continue
            per_op.append(f"{op.name}:{delta:+d}")
            if delta > 0:
                up += delta
            else:
                down -= delta
        nodes = {
            node for req in where.by_op.values() if req.delta > 0 for node in req.nodes
        }
        logger.debug(
            "OperatorSizer tick: (%s ms) | "
            "scale_up=+%d scale_down=-%d across %d ops "
            "[%s]; placed on %d nodes",
            " ".join(
                f"{phase}={seconds * 1e3:.1f}" for phase, seconds in timings.items()
            ),
            up,
            down,
            len(where.by_op),
            ", ".join(per_op),
            len(nodes),
        )
        self._log_locality(topology)

    def _log_locality(self, topology: "Topology") -> None:
        """Per operator: local/remote block ratio and cross-node bytes moved.

        Only when the byte count moved -- otherwise this is one line per
        operator per tick saying nothing changed.
        """
        for op in topology:
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            total = op.metrics.bytes_remote_inputs_read
            delta = total - self._last_remote_bytes.get(op, 0)
            if delta == 0:
                continue
            self._last_remote_bytes[op] = total
            logger.debug(
                "OperatorSizer locality %s: %s +%s cross-node this tick "
                "(total %s, %d remote blocks)",
                op.name,
                locality_string(op._locality_hits, op._locality_misses),
                memory_string(delta),
                memory_string(total),
                op.metrics.num_remote_blocks_read,
            )
