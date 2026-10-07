import logging
import time
from collections import deque
from dataclasses import replace
from typing import Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from ray.train.health._internal.node_monitor_group import NodeMonitorGroup
from ray.train.health.check import Evaluator, HealthCheck
from ray.train.health.decision import Diagnose, Evict, HealthDecision, Noop, Reattempt
from ray.train.health.probe import (
    ControllerProbe,
    NodeIdStr,
    NodeProbe,
    Probe,
    ProbeResult,
    WorkerProbe,
)
from ray.train.health.state import HealthState, ResultKey

logger = logging.getLogger(__name__)

# {probe name: {world rank, node ID or key: reading}}
ProbeResults = Dict[str, Dict[ResultKey, ProbeResult]]

# The largest window_size a probe may ask for. At ~1.2 KB per reading with 10
# metrics, a WorkerProbe on 1,000 workers at this size holds ~120 MB.
MAX_WINDOW_SIZE = 100


def _dedup(values: Iterable) -> List:
    return list(dict.fromkeys(values))


def _without_evicted(
    decision: HealthDecision, evicted: Set[NodeIdStr]
) -> Optional[HealthDecision]:
    """The decision without already-evicted nodes, or ``None`` if it named only
    evicted nodes. A ``Diagnose`` with no ``target_nodes`` means all nodes, and
    an ``Evict`` with none has nothing to do."""
    if isinstance(decision, Evict) or (
        isinstance(decision, Diagnose) and decision.target_nodes
    ):
        nodes = [n for n in decision.target_nodes if n not in evicted]
        return replace(decision, target_nodes=nodes) if nodes else None
    return decision


def merge_decisions(
    decisions: Sequence[HealthDecision],
    *,
    evicted_nodes: Iterable[NodeIdStr] = (),
) -> List[HealthDecision]:
    """Merge the evaluators' decisions into the ones to carry out.

    Only the most severe kind is carried out. ``Reattempt``\\ s and ``Evict``\\ s
    are combined into one, because one restart covers all of them.
    ``Diagnose``\\ s are carried out one by one.

    Args:
        decisions: One decision per evaluator.
        evicted_nodes: Nodes already evicted from the run; decisions about
            them are dropped.

    Returns:
        The decisions to carry out. Empty if there is nothing to do.
    """
    evicted = set(evicted_nodes)
    live = [
        d
        for d in (_without_evicted(d, evicted) for d in decisions)
        if d is not None and not isinstance(d, Noop)
    ]
    if not live:
        return []

    top = max(d._severity for d in live)
    winners = [d for d in live if d._severity == top]
    if isinstance(winners[0], Diagnose):
        return winners

    reason = "; ".join(_dedup(d.reason for d in winners if d.reason))
    if isinstance(winners[0], Evict):
        nodes = _dedup(n for d in winners for n in d.target_nodes)
        return [Evict(reason=reason, target_nodes=nodes)]
    return [Reattempt(reason=reason)]


class HealthManager:
    """Runs mid-training health checks: keeps their probes and evaluators,
    samples ``NodeProbe``\\ s through a ``NodeMonitor`` on each node, polls
    ``ControllerProbe``\\ s, stores recent readings, and turns them into
    decisions.

    Args:
        checks: The mid-training health checks. Their creators are called
            here, once.

    Raises:
        ValueError: If a probe's ``window_size`` is not between 1 and
            ``MAX_WINDOW_SIZE``.
    """

    def __init__(self, checks: Sequence[HealthCheck]):
        self._probes: List[Probe] = []
        self._evaluators: List[Evaluator] = []
        for check in checks:
            if check.probe_creator:
                self._probes.extend(check.probe_creator())
            if check.evaluator_creator:
                self._evaluators.extend(check.evaluator_creator())

        self._window_sizes: Dict[str, int] = {}
        for probe in self._probes:
            if not 1 <= probe.window_size <= MAX_WINDOW_SIZE:
                raise ValueError(
                    f"{probe.probe_name()}.window_size must be between 1 and "
                    f"{MAX_WINDOW_SIZE}, got {probe.window_size}."
                )
            name = probe.probe_name()
            self._window_sizes[name] = max(
                self._window_sizes.get(name, 1), probe.window_size
            )

        self._node_monitors = NodeMonitorGroup(self.node_probes())
        # {probe name: {key: the latest readings, oldest first}}
        self._results: Dict[str, Dict[ResultKey, Deque[ProbeResult]]] = {}
        self._controller_probe_next_poll: Dict[int, float] = {}
        self._failing_probes: Set[str] = set()
        # Kept across restarts: an evicted node stays out for the whole run.
        self._evicted: Set[NodeIdStr] = set()
        self._disabled: Set[int] = set()

    @property
    def enabled(self) -> bool:
        return bool(self._probes or self._evaluators)

    def worker_probes(self) -> List[WorkerProbe]:
        return [p for p in self._probes if isinstance(p, WorkerProbe)]

    def node_probes(self) -> List[NodeProbe]:
        return [p for p in self._probes if isinstance(p, NodeProbe)]

    def controller_probes(self) -> List[ControllerProbe]:
        return [p for p in self._probes if isinstance(p, ControllerProbe)]

    @property
    def evicted_nodes(self) -> List[NodeIdStr]:
        return sorted(self._evicted)

    #####################################################################################
    # Lifecycle
    #####################################################################################

    def start(self, node_ids: List[NodeIdStr]) -> None:
        """Start sampling ``NodeProbe``\\ s on these nodes.

        Args:
            node_ids: The nodes to sample.
        """
        if self.node_probes():
            self._node_monitors.start(node_ids)

    def reset(self, node_ids: List[NodeIdStr]) -> None:
        """Prepare for a new attempt of the training run: drop the readings so
        far, call each evaluator's ``on_worker_group_start()``, and move the
        ``NodeMonitor``\\ s to the given nodes.

        Args:
            node_ids: The nodes the new attempt's workers run on.
        """
        self._results.clear()
        stale = [n for n in self._node_monitors.node_ids if n not in node_ids]
        self._node_monitors.shutdown(stale)
        self.start(node_ids)
        for evaluator in self._evaluators:
            try:
                evaluator.on_worker_group_start()
            except Exception:
                logger.warning(
                    "Evaluator %s raised in on_worker_group_start().",
                    type(evaluator).__name__,
                    exc_info=True,
                )

    def shutdown(self) -> None:
        """Stop every ``NodeMonitor``."""
        self._node_monitors.shutdown()

    #####################################################################################
    # Collecting readings
    #####################################################################################

    def poll_probes(self, timeout: float = 0.0) -> None:
        """Collect the latest readings of the probes that run outside the
        workers: poll each ``ControllerProbe`` that is due, and collect the
        ``NodeMonitor``\\ s' samples. A monitor that reports an error is
        restarted.

        Args:
            timeout: How long to wait for the ``NodeMonitor``\\ s to answer.
        """
        self._store(self._poll_controller_probes())
        self._store(self._poll_node_monitors(timeout))

    def store_worker_results(
        self, world_rank: int, results: Mapping[str, ProbeResult]
    ) -> None:
        """Store one worker's ``WorkerProbe`` readings.

        Args:
            world_rank: The worker's world rank.
            results: ``{probe name: ProbeResult}``.
        """
        self._store({name: {world_rank: r} for name, r in results.items()})

    def build_state(self) -> HealthState:
        return HealthState(
            probe_results={
                name: {key: list(window) for key, window in windows.items()}
                for name, windows in self._results.items()
            }
        )

    def _poll_controller_probes(self) -> ProbeResults:
        results: ProbeResults = {}
        now = time.monotonic()
        for probe in self.controller_probes():
            if now < self._controller_probe_next_poll.get(id(probe), 0.0):
                continue
            self._controller_probe_next_poll[id(probe)] = now + probe.poll_interval_s
            name = probe.probe_name()
            try:
                polled = probe.poll()
            except Exception:
                if name not in self._failing_probes:
                    self._failing_probes.add(name)
                    logger.warning("Controller probe %s failed.", name, exc_info=True)
                continue
            self._failing_probes.discard(name)
            results.setdefault(name, {}).update(polled)
        return results

    def _poll_node_monitors(self, timeout: float) -> ProbeResults:
        status = self._node_monitors.poll_status(timeout=timeout)
        results: ProbeResults = {}
        for node_id, node_monitor_status in status.node_monitor_statuses.items():
            if node_monitor_status.error is not None:
                logger.warning(
                    "The NodeMonitor on node %s failed; restarting it: %s",
                    node_id,
                    node_monitor_status.error,
                )
                self._node_monitors.shutdown([node_id])
                self._node_monitors.start([node_id])
                continue
            for name, result in node_monitor_status.probe_results.items():
                results.setdefault(name, {})[node_id] = result
        return results

    def _store(self, results: ProbeResults) -> None:
        """Append readings to their windows. A reading without a
        ``timestamp_s`` is stamped with the current time; one already stored,
        with the same ``timestamp_s``, is not stored again."""
        now = time.time()
        for name, per_key in results.items():
            windows = self._results.setdefault(name, {})
            size = self._window_sizes.get(name, 1)
            for key, result in per_key.items():
                if result.timestamp_s is None:
                    result = replace(result, timestamp_s=now)
                window = windows.setdefault(key, deque(maxlen=size))
                if window and window[-1].timestamp_s == result.timestamp_s:
                    continue
                window.append(result)

    #####################################################################################
    # Deciding
    #####################################################################################

    def poll_decisions(self) -> List[HealthDecision]:
        """Run every evaluator on the current readings and merge their
        decisions.

        Returns:
            The decisions to carry out. Empty if there is nothing to do.
        """
        decisions: List[HealthDecision] = []
        state = self.build_state()
        for evaluator in self._evaluators:
            if id(evaluator) in self._disabled:
                continue
            try:
                decision = evaluator.evaluate(state)
                if not hasattr(decision, "_severity"):
                    raise TypeError(
                        "evaluate() must return a Noop, Reattempt, Evict or "
                        f"Diagnose, not {decision!r}."
                    )
            except Exception:
                logger.exception(
                    "Evaluator %s failed; it will not be called again during "
                    "this run.",
                    type(evaluator).__name__,
                )
                self._disabled.add(id(evaluator))
                continue
            decisions.append(decision)

        merged = merge_decisions(decisions, evicted_nodes=self._evicted)
        for decision in merged:
            if isinstance(decision, Evict):
                self._evicted.update(decision.target_nodes)
        return merged
