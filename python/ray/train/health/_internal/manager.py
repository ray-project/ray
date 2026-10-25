import logging
from dataclasses import replace
from typing import Any, Dict, List, Optional, Sequence, Set

from ray.train.health._internal.constants import DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
from ray.train.health._internal.node_monitor_group import NodeMonitorGroup
from ray.train.health._internal.probe_utils import merge_probes
from ray.train.health.check import HealthCheck, PeriodicHealthCheck
from ray.train.health.decision import Diagnose, Evict, HealthDecision, Noop, Reattempt
from ray.train.health.evaluator import Evaluator
from ray.train.health.probe import NodeIdStr, NodeProbe, ProbeResult
from ray.train.health.state import HealthState

logger = logging.getLogger(__name__)

_DECISION_TYPES = (Noop, Reattempt, Evict, Diagnose)


def _join_reasons(decisions: Sequence[HealthDecision]) -> str:
    return "; ".join(dict.fromkeys(d.reason for d in decisions if d.reason))


def merge_decisions(decisions: Sequence[HealthDecision]) -> HealthDecision:
    """Merge the evaluators' decisions into the health decision to carry out.

    The most severe kind wins: ``Evict``, then ``Reattempt``, then
    ``Diagnose``. Decisions of that kind are combined into one.

    Args:
        decisions: The evaluators' decisions.

    Returns:
        The health decision to carry out. ``Noop()`` if there is nothing to do.

    Raises:
        TypeError: If a decision is not a ``Noop``, ``Reattempt``, ``Evict`` or
            ``Diagnose``.
    """
    # TODO: Add a field to Diagnose for checks that need the workers stopped,
    # such as an all-reduce test, and rank those above Reattempt.
    evicts, reattempts, diagnoses = [], [], []
    for decision in decisions:
        if isinstance(decision, Evict):
            evicts.append(decision)
        elif isinstance(decision, Reattempt):
            reattempts.append(decision)
        elif isinstance(decision, Diagnose):
            diagnoses.append(decision)
        elif not isinstance(decision, Noop):
            raise TypeError(f"Unknown health decision: {decision!r}.")

    if evicts:
        nodes = sorted({node for evict in evicts for node in evict.target_nodes})
        return Evict(reason=_join_reasons(evicts), target_nodes=nodes)
    if reattempts:
        return Reattempt(reason=_join_reasons(reattempts))
    if diagnoses:
        checks = [check for diagnose in diagnoses for check in diagnose.checks]
        return Diagnose(reason=_join_reasons(diagnoses), checks=checks)
    return Noop()


class HealthManager:
    """Runs a training run's health checks, and evaluates their results into
    health decisions.

    It runs two kinds of checks on the nodes of the attempt that ``reset()``
    starts:

    - In-flight checks run throughout training. Their probes are sampled
      continuously, and each ``evaluate_inflight_checks()`` evaluates their
      latest results.
    - On-demand checks, such as pre-flight checks and the checks of a
      ``Diagnose``, run once. ``start_on_demand_checks()`` starts them, and
      ``evaluate_on_demand_checks()`` evaluates their results once they are
      back. In-flight checks keep running meanwhile.

    Args:
        inflight_checks: The in-flight health checks.

    Raises:
        ValueError: If two different probe classes have the same name.
    """

    def __init__(self, inflight_checks: Sequence[PeriodicHealthCheck]):
        inflight_probes = merge_probes(inflight_checks)
        self._inflight_evaluators: List[Evaluator] = [
            evaluator
            for check in inflight_checks
            for evaluator in check.evaluator_creator()
        ]
        self._node_monitor_group = NodeMonitorGroup(
            [p for p in inflight_probes if isinstance(p.probe, NodeProbe)]
        )
        # {probe name: {node ID, world rank or ControllerProbe key: the latest
        # ProbeResult}}
        self._latest_results: Dict[str, Dict[Any, ProbeResult]] = {}
        # The evaluators of the on-demand checks that are running.
        self._on_demand_evaluators: List[Evaluator] = []
        # The IDs of the evaluators whose failure was logged.
        self._failed_evaluators: Set[int] = set()
        # The nodes evicted so far. reset() keeps them.
        self._evicted: Set[NodeIdStr] = set()

    @property
    def evicted_nodes(self) -> List[NodeIdStr]:
        return sorted(self._evicted)

    def reset(self, node_ids: List[NodeIdStr]) -> None:
        """Start monitoring a new attempt of the training run, and stop
        monitoring the previous one. Called at the start of each attempt, before
        its pre-flight checks and training.

        Args:
            node_ids: The nodes the attempt's workers run on.
        """
        self._latest_results.clear()
        self._on_demand_evaluators = []
        self._node_monitor_group.shutdown()
        self._node_monitor_group.start(node_ids)
        for evaluator in self._inflight_evaluators:
            try:
                evaluator.on_reset()
            except Exception:
                logger.warning(
                    "Evaluator %s raised in on_reset().",
                    type(evaluator).__name__,
                    exc_info=True,
                )

    def shutdown(self) -> None:
        """Stop monitoring."""
        self._node_monitor_group.shutdown()

    def evaluate_inflight_checks(
        self, timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
    ) -> HealthDecision:
        """Evaluate the in-flight checks' latest results.

        Args:
            timeout: Timeout in seconds to wait for the latest results.

        Returns:
            The health decision to carry out. ``Noop()`` if there is nothing to
            do.
        """
        self._poll_latest_results(timeout)
        state = HealthState(
            probe_results={name: dict(r) for name, r in self._latest_results.items()}
        )
        return self._decide(self._inflight_evaluators, state)

    def start_on_demand_checks(
        self, checks: Sequence[HealthCheck], timeout: float
    ) -> None:
        """Start running on-demand checks once on the monitored nodes.
        ``evaluate_on_demand_checks()`` evaluates their results.

        Args:
            checks: The checks, such as pre-flight checks or the checks of a
                ``Diagnose``.
            timeout: Timeout in seconds for the nodes to answer.
        """
        probes = {p.probe_name(): p for check in checks for p in check.probe_creator()}
        self._node_monitor_group.poll_probes_async(
            [p for p in probes.values() if isinstance(p, NodeProbe)], timeout_s=timeout
        )
        self._on_demand_evaluators = [
            evaluator for check in checks for evaluator in check.evaluator_creator()
        ]

    def evaluate_on_demand_checks(self) -> Optional[HealthDecision]:
        """Evaluate the results of the on-demand checks that
        ``start_on_demand_checks()`` started, once they are back.

        Returns:
            The health decision to carry out, or ``None`` while the checks run.
        """
        results = self._node_monitor_group.poll_probe_results()
        if results is None:
            return None
        evaluators, self._on_demand_evaluators = self._on_demand_evaluators, []
        return self._decide(evaluators, HealthState(probe_results=results))

    def _poll_latest_results(self, timeout: float) -> None:
        """Poll the latest probe results. A ``NodeMonitor`` that failed stops,
        with its results, until the next ``reset()``.

        Args:
            timeout: Timeout in seconds to wait for the ``NodeMonitor``\\ s.
        """
        status = self._node_monitor_group.poll_status(timeout)
        for node_id, node_monitor_status in status.node_monitor_statuses.items():
            if node_monitor_status.monitor_error is None:
                for name, result in node_monitor_status.probe_results.items():
                    self._latest_results.setdefault(name, {})[node_id] = result
                continue
            logger.warning(
                "Stopped monitoring node %s until the next attempt: %s",
                node_id,
                node_monitor_status.monitor_error,
            )
            self._node_monitor_group.shutdown([node_id])
            for results in self._latest_results.values():
                results.pop(node_id, None)

    def _decide(
        self, evaluators: List[Evaluator], state: HealthState
    ) -> HealthDecision:
        """Run the evaluators, and merge their decisions into the health
        decision to carry out. A node is evicted once.

        Args:
            evaluators: The evaluators to run.
            state: The results they evaluate.

        Returns:
            The health decision to carry out. ``Noop()`` if there is nothing to
            do.
        """
        decisions = []
        for evaluator in evaluators:
            decision = self._evaluate(evaluator, state)
            if isinstance(decision, Evict):
                nodes = [n for n in decision.target_nodes if n not in self._evicted]
                decision = replace(decision, target_nodes=nodes) if nodes else Noop()
            decisions.append(decision)
        decision = merge_decisions(decisions)
        if isinstance(decision, Evict):
            self._evicted.update(decision.target_nodes)
        return decision

    def _evaluate(self, evaluator: Evaluator, state: HealthState) -> HealthDecision:
        """Run one evaluator. One that raises, or returns something other than a
        ``Noop``, ``Reattempt``, ``Evict`` or ``Diagnose``, decides ``Noop()``,
        with a warning the first time.

        Args:
            evaluator: The evaluator to run.
            state: The results it evaluates.

        Returns:
            The evaluator's decision, or ``Noop()`` if it failed.
        """
        try:
            decision = evaluator.evaluate(state)
            error = None
            if not isinstance(decision, _DECISION_TYPES):
                error = TypeError(
                    f"evaluate() returned {decision!r}, not a Noop, Reattempt, "
                    "Evict or Diagnose."
                )
        except Exception as e:
            error = e
        if error is None:
            return decision
        if id(evaluator) not in self._failed_evaluators:
            self._failed_evaluators.add(id(evaluator))
            logger.warning(
                "Evaluator %s failed.", type(evaluator).__name__, exc_info=error
            )
        return Noop()
