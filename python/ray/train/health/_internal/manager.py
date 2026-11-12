import logging
import time
from dataclasses import replace
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Union,
)

import ray
from ray.train.health._internal.constants import (
    DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S,
    DEFAULT_ON_DEMAND_PROBE_POLL_INTERVAL_S,
    DEFAULT_ON_DEMAND_PROBE_TIMEOUT_S,
)
from ray.train.health._internal.node_monitor import NodeMonitorStatus
from ray.train.health._internal.node_monitor_group import (
    NodeMonitorGroup,
    NodeMonitorGroupPollStatus,
)
from ray.train.health._internal.probe_utils import ScheduledProbe, merge_probes
from ray.train.health.check import HealthCheck, PeriodicHealthCheck
from ray.train.health.decision import Diagnose, Evict, HealthDecision, Noop, Reattempt
from ray.train.health.evaluator import Evaluator
from ray.train.health.probe import (
    ControllerProbe,
    NodeIdStr,
    NodeProbe,
    Probe,
    ProbeResult,
    WorkerProbe,
)
from ray.train.health.state import HealthState

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.worker_group import WorkerStatus

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
    # TODO: Rank a Diagnose whose checks require idle workers above Reattempt.
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
    starts, and on the controller's node:

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
        self._inflight_probes = [p.probe for p in inflight_probes]
        self._worker_probes = [
            p for p in inflight_probes if isinstance(p.probe, WorkerProbe)
        ]
        self._node_monitor_group = NodeMonitorGroup(inflight_probes)
        # The last status of each NodeMonitor of the in-flight probes, merged
        # over the polls.
        self._last_node_monitor_group_status = NodeMonitorGroupPollStatus(
            node_monitor_statuses={}
        )
        # The last status of each worker, by world rank, merged over the polls.
        self._last_worker_statuses: Dict[int, "WorkerStatus"] = {}
        # The probes and evaluators of the on-demand checks that are running.
        self._on_demand_probes: List[Probe] = []
        self._on_demand_evaluators: List[Evaluator] = []
        # The IDs of the evaluators whose failure was logged.
        self._failed_evaluators: Set[int] = set()
        # The nodes evicted so far. reset() keeps them.
        self._evicted: Set[NodeIdStr] = set()

    @property
    def evicted_nodes(self) -> List[NodeIdStr]:
        return sorted(self._evicted)

    @property
    def worker_probes(self) -> List[ScheduledProbe]:
        """The in-flight checks' ``WorkerProbe``\\ s, for each worker to sample."""
        return list(self._worker_probes)

    def reset(self, node_ids: List[NodeIdStr]) -> None:
        """Start monitoring a new attempt of the training run, and stop
        monitoring the previous one. Called at the start of each attempt, before
        its pre-flight checks and training.

        Args:
            node_ids: The nodes the attempt's workers run on.
        """
        self._last_node_monitor_group_status = NodeMonitorGroupPollStatus(
            node_monitor_statuses={}
        )
        self._last_worker_statuses = {}
        self._on_demand_evaluators = []
        self._node_monitor_group.shutdown()
        self._node_monitor_group.start_worker_node_monitors(node_ids)
        self._node_monitor_group.start_controller_node_monitor(
            ray.get_runtime_context().get_node_id()
        )
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

    def evaluate(
        self, timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
    ) -> HealthDecision:
        """Evaluate the on-demand checks once they are back, and the in-flight
        checks. While on-demand checks run, an in-flight ``Diagnose`` is
        dropped rather than start more, unless a check requires idle workers:
        that ``Diagnose`` restarts the worker group instead.

        Args:
            timeout: Timeout in seconds to wait for the in-flight checks' latest
                results.

        Returns:
            The health decision to carry out. ``Noop()`` if there is nothing to
            do.
        """
        on_demand_decision = self.evaluate_on_demand_checks()
        inflight_decision = self.evaluate_inflight_checks(timeout=timeout)
        if on_demand_decision is None:
            on_demand_decision = Noop()
            if isinstance(inflight_decision, Diagnose) and not any(
                check.requires_idle_workers for check in inflight_decision.checks
            ):
                inflight_decision = Noop()
        return merge_decisions([on_demand_decision, inflight_decision])

    def evaluate_inflight_checks(
        self,
        worker_statuses: Optional[Mapping[int, "WorkerStatus"]] = None,
        timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S,
    ) -> HealthDecision:
        """Evaluate the in-flight checks' latest results.

        Args:
            worker_statuses: ``{world rank: WorkerStatus}`` from the worker
                group's latest poll, with each worker's ``WorkerProbe`` results
                and health report.
            timeout: Timeout in seconds to wait for the ``NodeMonitor``\\ s'
                latest results. The worker group polled ``worker_statuses``
                already.

        Returns:
            The health decision to carry out. ``Noop()`` if there is nothing to
            do.
        """
        self._poll_node_monitor_group_status(timeout)
        self._merge_worker_statuses(worker_statuses or {})
        state = _to_health_state(
            self._last_node_monitor_group_status,
            self._inflight_probes,
            self._last_worker_statuses,
        )
        return self._decide(self._inflight_evaluators, state)

    def start_on_demand_checks(
        self,
        checks: Sequence[HealthCheck],
        timeout: float = DEFAULT_ON_DEMAND_PROBE_TIMEOUT_S,
    ) -> None:
        """Start running on-demand checks once on the monitored nodes.
        ``evaluate_on_demand_checks()`` evaluates their results.

        Args:
            checks: The checks, such as pre-flight checks or the checks of a
                ``Diagnose``.
            timeout: Timeout in seconds for the nodes to answer.
        """
        probes = {p.probe_name(): p for check in checks for p in check.probe_creator()}
        self._on_demand_probes = list(probes.values())
        self._node_monitor_group.poll_probes_async(
            self._on_demand_probes, timeout_s=timeout
        )
        self._on_demand_evaluators = [
            evaluator for check in checks for evaluator in check.evaluator_creator()
        ]

    def evaluate_on_demand_checks(self) -> Optional[HealthDecision]:
        """Evaluate the results of the on-demand checks that
        ``start_on_demand_checks()`` started, once they are back.

        Returns:
            The health decision to carry out, or ``None`` while the checks run.
            ``Noop()`` if no on-demand check is running.
        """
        if not self._on_demand_evaluators:
            return Noop()
        status = self._node_monitor_group.poll_probe_results()
        if status is None:
            return None
        evaluators, self._on_demand_evaluators = self._on_demand_evaluators, []
        decision = self._decide(
            evaluators, _to_health_state(status, self._on_demand_probes)
        )
        # The controller logs every other decision when it acts on it. A Noop
        # has no action, so log here that the checks, such as a Diagnose's,
        # are done and found nothing.
        if isinstance(decision, Noop):
            logger.info("The on-demand health checks found nothing to act on.")
        return decision

    def start_and_evaluate_on_demand_checks(
        self,
        checks: Sequence[HealthCheck],
        timeout: float = DEFAULT_ON_DEMAND_PROBE_TIMEOUT_S,
    ) -> HealthDecision:
        """Run on-demand checks once on the monitored nodes, and wait for their
        decision: until their results are back, or the timeout passed.

        Args:
            checks: The checks, such as the pre-flight checks that stop the
                workers.
            timeout: Timeout in seconds for the nodes to answer.

        Returns:
            The health decision to carry out. ``Noop()`` if there is nothing to
            do.
        """
        self.start_on_demand_checks(checks, timeout)
        decision = self.evaluate_on_demand_checks()
        while decision is None:
            time.sleep(DEFAULT_ON_DEMAND_PROBE_POLL_INTERVAL_S)
            decision = self.evaluate_on_demand_checks()
        return decision

    def _merge_worker_statuses(
        self, worker_statuses: Mapping[int, "WorkerStatus"]
    ) -> None:
        """Merge each worker's new status into its last one. A worker keeps its
        last ``WorkerProbe`` results and health report while its poll hangs.

        Args:
            worker_statuses: ``{world rank: WorkerStatus}`` from the worker
                group's latest poll.
        """
        for rank, new_status in worker_statuses.items():
            last_status = self._last_worker_statuses.get(rank)
            if last_status is not None:
                new_status = replace(
                    new_status,
                    probe_results={
                        **last_status.probe_results,
                        **new_status.probe_results,
                    },
                    health_report=(
                        new_status.health_report
                        if new_status.health_report is not None
                        else last_status.health_report
                    ),
                )
            self._last_worker_statuses[rank] = new_status

    def _poll_node_monitor_group_status(self, timeout: float) -> None:
        """Poll each ``NodeMonitor``, and merge its new status into its last
        one. A monitor keeps its last results while its poll hangs. A worker
        node's monitor that failed stops, with its results, until the next
        ``reset()``.

        Args:
            timeout: Timeout in seconds to wait for the ``NodeMonitor``\\ s.
        """
        new_status = self._node_monitor_group.poll_status(timeout)
        last_status = self._last_node_monitor_group_status
        node_monitor_statuses: Dict[NodeIdStr, NodeMonitorStatus] = {}
        for node_id, node_monitor_status in new_status.node_monitor_statuses.items():
            if node_monitor_status.monitor_error is None:
                node_monitor_statuses[node_id] = _merge(
                    last_status.node_monitor_statuses.get(node_id, NodeMonitorStatus()),
                    node_monitor_status,
                )
                continue
            logger.warning(
                "Stopped monitoring node %s until the next attempt: %s",
                node_id,
                node_monitor_status.monitor_error,
            )
            self._node_monitor_group.shutdown_worker_node_monitors([node_id])
        self._last_node_monitor_group_status = NodeMonitorGroupPollStatus(
            node_monitor_statuses=node_monitor_statuses,
            controller_node_monitor_status=_merge(
                last_status.controller_node_monitor_status,
                new_status.controller_node_monitor_status,
            ),
        )

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


def _merge(last: NodeMonitorStatus, new: NodeMonitorStatus) -> NodeMonitorStatus:
    """Merge a monitor's new status into its last one, which keeps the
    results of the probes the new one has none for, such as while a poll
    hangs. The error is the new status's: a monitor that fails reports its
    error on every poll, and one that answers again has none."""
    return NodeMonitorStatus(
        probe_results={**last.probe_results, **new.probe_results},
        monitor_error=new.monitor_error,
    )


def _to_health_state(
    status: NodeMonitorGroupPollStatus,
    probes: Sequence[Probe],
    worker_statuses: Optional[Mapping[int, "WorkerStatus"]] = None,
) -> HealthState:
    """Build the ``HealthState`` of some probes from the statuses of their
    monitors and workers. A monitor that failed gives each of its probes a
    result with its error, and a ``ControllerProbe`` whose poll failed has its
    error in ``probe_errors``.

    Args:
        status: The statuses of the monitors.
        probes: The probes the monitors poll.
        worker_statuses: ``{world rank: WorkerStatus}``, with each worker's
            ``WorkerProbe`` results and health report.

    Returns:
        The ``HealthState``.
    """
    node_probe_names = [p.probe_name() for p in probes if isinstance(p, NodeProbe)]
    # {node ID: {probe name: result}} -> {probe name: {node ID: result}}
    probe_results: Dict[str, Dict[Any, ProbeResult]] = {}
    for node_id, node_monitor_status in status.node_monitor_statuses.items():
        node_results = _probe_results(node_monitor_status, node_probe_names)
        for name, result in node_results.items():
            probe_results.setdefault(name, {})[node_id] = result

    controller_probe_names = [
        p.probe_name() for p in probes if isinstance(p, ControllerProbe)
    ]
    probe_errors: Dict[str, Exception] = {}
    controller_results = _probe_results(
        status.controller_node_monitor_status, controller_probe_names
    )
    for name, result in controller_results.items():
        if isinstance(result, ProbeResult):
            probe_errors[name] = result.error
        else:
            probe_results[name] = dict(result)

    health_reports: Dict[int, ProbeResult] = {}
    for rank, worker_status in (worker_statuses or {}).items():
        for name, result in worker_status.probe_results.items():
            probe_results.setdefault(name, {})[rank] = result
        if worker_status.health_report is not None:
            health_reports[rank] = worker_status.health_report
    return HealthState(
        probe_results=probe_results,
        probe_errors=probe_errors,
        health_reports=health_reports,
    )


def _probe_results(
    status: NodeMonitorStatus, probe_names: List[str]
) -> Dict[str, Union[ProbeResult, Dict[Any, ProbeResult]]]:
    """Get a monitor's probe results, or if the monitor failed, a result with
    its error for each probe."""
    if status.monitor_error is None:
        return status.probe_results
    return {
        name: ProbeResult(timestamp_s=time.time(), error=status.monitor_error)
        for name in probe_names
    }
