"""Everything the sizer reports, and the state that keeps it quiet."""

from __future__ import annotations

import functools
import logging
import time
from collections import Counter
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, TypeVar, cast

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.execution.util import locality_string, memory_string
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    PlacedScalingRequest,
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer.metrics import SizerMetrics
from ray.data._internal.experimental.execution.sizer.models import (
    PerOpSizingRequest,
    PlacedSizingRequest,
)

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
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

    def __init__(self, dataset_id: str):
        self._dataset_id = dataset_id
        self._metrics = SizerMetrics(dataset_id)
        # Last pool size logged by the terminal-release branch, per op.
        self._release_logged_size: Dict[ExperimentalActorPoolMapOperator, int] = {}
        # Last summaries logged, to suppress repeats.
        self._last_request_summary: Optional[str] = None
        self._last_placement_summary: Optional[str] = None
        self._last_capacity_summary: Optional[str] = None
        self._last_shortfall_summary: Optional[str] = None
        # Cumulative cross-node input bytes per op at the last tick, to log the
        # per-tick delta (see _log_locality).
        self._last_remote_bytes: Dict[ExperimentalActorPoolMapOperator, int] = {}
        # Wall time per phase since the last ``record_tick`` (see ``timed``).
        self._timings: Dict[str, float] = {}

    def add_timing_phase(self, phase: str, seconds: float) -> None:
        """Add ``seconds`` to ``phase``'s total for the current tick."""
        self._timings[phase] = self._timings.get(phase, 0.0) + seconds

    # --- how-many pass -------------------------------------------------------

    def log_completed_release(
        self, op: ExperimentalActorPoolMapOperator, num_actors_to_release: int
    ) -> None:
        if self._release_logged_size.get(op) != num_actors_to_release:
            logger.debug(
                "OperatorSizer: releasing %d actors of %s (completed)",
                num_actors_to_release,
                op.name,
            )
            self._release_logged_size[op] = num_actors_to_release

    def log_request_summary(
        self, request: Dict[ExperimentalActorPoolMapOperator, "ActorPoolScalingRequest"]
    ) -> None:
        if not request:
            return
        summary = "; ".join(
            f"{op.name}: {req.delta:+d} ({req.reason})" for op, req in request.items()
        )
        if summary != self._last_request_summary:
            logger.debug("OperatorSizer: requests %s", summary)
            self._last_request_summary = summary

    # --- scale_where ---------------------------------------------------------

    def log_placement_shortfall(
        self,
        *,
        op: ExperimentalActorPoolMapOperator,
        num_placed: int,
        num_new: int,
        num_reclaimed: int,
        free: Dict[NodeIdStr, ExecutionResources],
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
        alloc: Dict[NodeIdStr, ExecutionResources],
        free: Dict[NodeIdStr, ExecutionResources],
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
                via_str = f" via={via[0]}collocate/{via[1]}spread" if via else ""
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
        alloc: Dict[NodeIdStr, ExecutionResources],
        free: Dict[NodeIdStr, ExecutionResources],
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
        topology: "Topology",
        how_many: PerOpSizingRequest,
        where: PlacedSizingRequest,
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

        if not how_many.request and not where.request:
            return

        for phase, placed in (
            ("per_op_how_many", how_many.request),
            ("per_op_where", where.request),
        ):
            for op, r in placed.items():
                self._metrics.record_actor_delta(
                    operator=topology[op].op_tag(), phase=phase, delta=r.delta
                )

        up = down = 0
        per_op = []
        for op, r in where.request.items():
            if not r.delta:
                continue
            per_op.append(f"{op.name}:{r.delta:+d}")
            if r.delta > 0:
                up += r.delta
            else:
                down -= r.delta
        nodes = {
            node for r in where.request.values() if r.delta > 0 for node in r.nodes
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
            len(where.request),
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
