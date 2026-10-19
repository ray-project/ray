import logging
import time
from dataclasses import dataclass, replace
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Set

from ray.train.health._internal.node_monitor import ScheduledProbe
from ray.train.health._internal.node_monitor_group import (
    DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S,
    NodeMonitorGroup,
)
from ray.train.health.check import PeriodicHealthCheck
from ray.train.health.decision import Diagnose, Evict, HealthDecision, Noop, Reattempt
from ray.train.health.evaluator import Evaluator
from ray.train.health.probe import (
    ControllerProbe,
    NodeIdStr,
    NodeProbe,
    Probe,
    ProbeResult,
)
from ray.train.health.state import HealthState

logger = logging.getLogger(__name__)

_SEVERITY = {Noop: 0, Diagnose: 1, Reattempt: 2, Evict: 3}


def _stamped(result: ProbeResult, now: float) -> ProbeResult:
    if result.timestamp_s is None:
        return replace(result, timestamp_s=now)
    return result


def merge_decisions(
    decisions: Sequence[HealthDecision],
    *,
    evicted_nodes: Iterable[NodeIdStr] = (),
) -> List[HealthDecision]:
    """Merge the evaluators' decisions into the ones to carry out.

    Only the most severe kind is kept. ``Reattempt``\\ s and ``Evict``\\ s are
    combined into one, and ``Diagnose``\\ s are kept one by one.

    Args:
        decisions: The evaluators' decisions.
        evicted_nodes: Nodes already evicted, which an ``Evict`` drops.

    Returns:
        The decisions to carry out. Empty if there is nothing to do.
    """
    evicted = set(evicted_nodes)
    live = []
    for decision in decisions:
        if isinstance(decision, Evict):
            nodes = [n for n in decision.target_nodes if n not in evicted]
            decision = replace(decision, target_nodes=nodes) if nodes else Noop()
        if not isinstance(decision, Noop):
            live.append(decision)
    if not live:
        return []

    kind = max((type(d) for d in live), key=lambda t: _SEVERITY[t])
    winners = [d for d in live if type(d) is kind]
    if kind is Diagnose:
        return winners
    reason = "; ".join(dict.fromkeys(d.reason for d in winners if d.reason))
    if kind is Evict:
        nodes = dict.fromkeys(n for d in winners for n in d.target_nodes)
        return [Evict(reason=reason, target_nodes=list(nodes))]
    return [Reattempt(reason=reason)]


@dataclass
class _Check:
    poll_interval_s: float
    evaluators: List[Evaluator]
    next_run: float = 0.0


class HealthManager:
    """Runs in-flight health checks: samples their ``NodeProbe``\\ s through a
    ``NodeMonitor`` on each node, polls their ``ControllerProbe``\\ s, and runs
    each check's evaluators every ``poll_interval_s`` of the check.

    A probe in several checks runs once, at the shortest of their intervals.

    Args:
        inflight_checks: The in-flight health checks. Their creators are called
            here, once.

    Raises:
        ValueError: If two different probe classes have the same name.
    """

    def __init__(self, inflight_checks: Sequence[PeriodicHealthCheck]):
        self._checks: List[_Check] = []
        self._probes: Dict[str, Probe] = {}
        self._poll_intervals_s: Dict[str, float] = {}
        for check in inflight_checks:
            self._checks.append(
                _Check(check.poll_interval_s, check.evaluator_creator())
            )
            for probe in check.probe_creator():
                name = probe.probe_name()
                existing = self._probes.setdefault(name, probe)
                if type(existing) is not type(probe):
                    raise ValueError(
                        f"Two different probes are named {name!r}: "
                        f"{type(existing).__name__} and {type(probe).__name__}."
                    )
                self._poll_intervals_s[name] = min(
                    self._poll_intervals_s.get(name, check.poll_interval_s),
                    check.poll_interval_s,
                )

        self._node_monitor_group = NodeMonitorGroup(
            [
                ScheduledProbe(probe, self._poll_intervals_s[name])
                for name, probe in self._probes.items()
                if isinstance(probe, NodeProbe)
            ]
        )
        # {probe name: {world rank, node ID or key: the latest ProbeResult}}
        self._results: Dict[str, Dict[Any, ProbeResult]] = {}
        # {controller probe name: when it is next due}
        self._next_poll: Dict[str, float] = {}
        # The probes and evaluator IDs whose current failure was logged.
        self._failing: Set[Any] = set()
        # The nodes evicted so far. reset() keeps them.
        self._evicted: Set[NodeIdStr] = set()

    @property
    def evicted_nodes(self) -> List[NodeIdStr]:
        return sorted(self._evicted)

    def reset(self, node_ids: List[NodeIdStr]) -> None:
        """Start an attempt of the training run: drop the results so far,
        restart the ``NodeMonitor``\\ s on the given nodes, and call each
        evaluator's ``on_worker_group_start()``.

        Args:
            node_ids: The nodes the attempt's workers run on.
        """
        self._results.clear()
        self._node_monitor_group.shutdown()
        if any(isinstance(p, NodeProbe) for p in self._probes.values()):
            self._node_monitor_group.start(node_ids)
        for check in self._checks:
            for evaluator in check.evaluators:
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
        self._node_monitor_group.shutdown()

    def store_worker_results(
        self, world_rank: int, results: Mapping[str, ProbeResult]
    ) -> None:
        """Store one worker's ``WorkerProbe`` results. A result without a
        ``timestamp_s`` is stamped with the time it arrives.

        Args:
            world_rank: The worker's world rank.
            results: ``{probe name: ProbeResult}``.
        """
        now = time.time()
        for name, result in results.items():
            self._results.setdefault(name, {})[world_rank] = _stamped(result, now)

    def poll(
        self, timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
    ) -> List[HealthDecision]:
        """Collect the ``NodeMonitor``\\ s' latest samples, poll the
        ``ControllerProbe``\\ s that are due, and run the evaluators of the
        checks that are due.

        Args:
            timeout: Timeout in seconds to wait for the ``NodeMonitor``\\ s.

        Returns:
            The decisions to carry out. Empty if there is nothing to do.
        """
        self._collect_node_samples(timeout)
        now = time.monotonic()
        self._poll_controller_probes(now)

        state = HealthState(
            probe_results={name: dict(r) for name, r in self._results.items()}
        )
        decisions = []
        for check in self._checks:
            if now >= check.next_run:
                check.next_run = now + check.poll_interval_s
                decisions += [self._evaluate(e, state) for e in check.evaluators]

        merged = merge_decisions(decisions, evicted_nodes=self._evicted)
        for decision in merged:
            if isinstance(decision, Evict):
                self._evicted.update(decision.target_nodes)
        return merged

    def _collect_node_samples(self, timeout: float) -> None:
        """Store the ``NodeMonitor``\\ s' latest samples, and restart each
        monitor that reports an error."""
        status = self._node_monitor_group.poll_status(timeout)
        for node_id, node_monitor_status in status.node_monitor_statuses.items():
            if node_monitor_status.monitor_error is not None:
                logger.warning(
                    "The NodeMonitor on node %s failed; restarting it: %s",
                    node_id,
                    node_monitor_status.monitor_error,
                )
                self._node_monitor_group.shutdown([node_id])
                self._node_monitor_group.start([node_id])
                continue
            for name, result in node_monitor_status.probe_results.items():
                self._results.setdefault(name, {})[node_id] = result
                if result.error is None:
                    self._failing.discard((name, node_id))
                else:
                    self._log_failure_once(
                        (name, node_id),
                        result.error,
                        "Node probe %s failed on node %s.",
                        name,
                        node_id,
                    )

    def _poll_controller_probes(self, now: float) -> None:
        """Poll each ``ControllerProbe`` that is due. A result without a
        ``timestamp_s`` is stamped with the time the poll returned."""
        for name, probe in self._probes.items():
            if not isinstance(probe, ControllerProbe):
                continue
            if now < self._next_poll.get(name, 0.0):
                continue
            self._next_poll[name] = now + self._poll_intervals_s[name]
            try:
                results = probe.poll()
            except Exception as e:
                self._log_failure_once(name, e, "Controller probe %s failed.", name)
                continue
            self._failing.discard(name)
            polled_at = time.time()
            self._results[name] = {
                key: _stamped(result, polled_at) for key, result in results.items()
            }

    def _evaluate(self, evaluator: Evaluator, state: HealthState) -> HealthDecision:
        """Run one evaluator. Return ``Noop()`` if it raised or did not return a
        ``Noop``, ``Reattempt``, ``Evict`` or ``Diagnose``."""
        try:
            decision = evaluator.evaluate(state)
            if type(decision) not in _SEVERITY:
                raise TypeError(
                    "evaluate() must return a Noop, Reattempt, Evict or "
                    f"Diagnose, not {decision!r}."
                )
        except Exception as e:
            self._log_failure_once(
                id(evaluator), e, "Evaluator %s failed.", type(evaluator).__name__
            )
            return Noop()
        self._failing.discard(id(evaluator))
        return decision

    def _log_failure_once(
        self, key: Any, error: Exception, message: str, *args: Any
    ) -> None:
        """Log ``error`` unless ``key`` is already failing."""
        if key not in self._failing:
            self._failing.add(key)
            logger.warning(message, *args, exc_info=error)
