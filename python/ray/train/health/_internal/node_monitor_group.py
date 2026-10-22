import time
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional, Sequence

import ray
from ray.actor import ActorHandle
from ray.train.health._internal.constants import (
    DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S,
)
from ray.train.health._internal.node_monitor import NodeMonitor, NodeMonitorStatus
from ray.train.health._internal.probe_utils import ScheduledProbe
from ray.train.health.probe import (
    ControllerProbe,
    NodeIdStr,
    NodeProbe,
    Probe,
    ProbeResult,
)
from ray.train.health.state import HealthState
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class _OngoingPoll(NamedTuple):
    start_time: float
    task: ObjectRef


class _OnDemandPoll(NamedTuple):
    node_id: NodeIdStr
    probes: List[Probe]
    task: ObjectRef


@dataclass(frozen=True)
class NodeMonitorGroupPollStatus:
    node_monitor_statuses: Dict[NodeIdStr, NodeMonitorStatus]
    controller_node_monitor_status: Optional[NodeMonitorStatus] = None

    @property
    def errors(self) -> Dict[NodeIdStr, Exception]:
        return {
            node_id: node_monitor_status.monitor_error
            for node_id, node_monitor_status in self.node_monitor_statuses.items()
            if node_monitor_status.monitor_error is not None
        }


class NodeMonitorGroup:
    """``NodeMonitor`` actors: one on each worker node, for the
    ``NodeProbe``\\ s, and one on the controller's node, for the
    ``ControllerProbe``\\ s.

    For the latest samples, use ``poll_status()``. For on-demand probes, use
    ``poll_probes_async()`` and ``poll_probe_results()``.

    Args:
        probes: The ``ScheduledProbe``\\ s the monitors sample.
        health_check_timeout_s: The time in seconds a monitor's
            ``poll_status()`` may stay unanswered before it is reported as an
            error.
    """

    def __init__(
        self,
        probes: List[ScheduledProbe],
        health_check_timeout_s: float = DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S,
    ):
        self._node_probes = [p for p in probes if isinstance(p.probe, NodeProbe)]
        self._controller_probes = [
            p for p in probes if isinstance(p.probe, ControllerProbe)
        ]
        self._health_check_timeout_s = health_check_timeout_s
        self._monitor_cls = ray.remote(concurrency_groups={"on_demand": 1})(NodeMonitor)
        self._worker_node_monitors: Dict[NodeIdStr, ActorHandle] = {}
        self._controller_node_id: Optional[NodeIdStr] = None
        self._controller_node_monitor: Optional[ActorHandle] = None
        # {monitor: its poll_status() call that has yet to answer}
        self._ongoing_polls: Dict[ActorHandle, _OngoingPoll] = {}
        # The time the on-demand probes' results are due, and their calls.
        self._on_demand_deadline = 0.0
        self._on_demand_polls: List[_OnDemandPoll] = []
        self._on_demand_controller_poll: Optional[_OnDemandPoll] = None

    # Worker node monitors.

    @property
    def worker_node_ids(self) -> List[NodeIdStr]:
        return list(self._worker_node_monitors)

    def start_worker_node_monitors(self, node_ids: List[NodeIdStr]) -> None:
        """Start a monitor on each of these nodes that does not have one.

        Args:
            node_ids: The worker nodes to monitor.
        """
        for node_id in node_ids:
            if node_id not in self._worker_node_monitors:
                self._worker_node_monitors[node_id] = self._start_monitor(
                    node_id, self._node_probes
                )

    def shutdown_worker_node_monitors(
        self, node_ids: Optional[List[NodeIdStr]] = None
    ) -> None:
        """Stop the monitors on some worker nodes, or on every one.

        Args:
            node_ids: The worker nodes to stop monitoring. ``None`` means all.
        """
        if node_ids is None:
            node_ids = self.worker_node_ids
        for node_id in node_ids:
            self._kill_monitor(self._worker_node_monitors.pop(node_id, None))

    # The controller node monitor.

    def start_controller_node_monitor(self) -> None:
        """Start a monitor on this node, the controller's, if there is none."""
        if self._controller_node_monitor is None:
            self._controller_node_id = ray.get_runtime_context().get_node_id()
            self._controller_node_monitor = self._start_monitor(
                self._controller_node_id, self._controller_probes
            )

    def shutdown_controller_node_monitor(self) -> None:
        """Stop the monitor on the controller's node."""
        self._kill_monitor(self._controller_node_monitor)
        self._controller_node_monitor = None

    def shutdown(self) -> None:
        """Stop every monitor."""
        self.shutdown_worker_node_monitors()
        self.shutdown_controller_node_monitor()

    # In-flight probes.

    def poll_status(
        self, timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
    ) -> NodeMonitorGroupPollStatus:
        """Poll the latest samples of every monitor. A monitor that has not
        answered within ``timeout`` has an empty status, until it has not
        answered for longer than the health-check timeout.

        Args:
            timeout: The maximum time to wait for the monitors to answer.

        Returns:
            Each monitor's status.
        """
        statuses = self._poll_status(timeout)

        def status(monitor: ActorHandle, probes: List[ScheduledProbe]):
            return _fail_probes(
                statuses.get(monitor, NodeMonitorStatus()), [p.probe for p in probes]
            )

        controller_status = None
        if self._controller_node_monitor is not None:
            controller_status = status(
                self._controller_node_monitor, self._controller_probes
            )
        return NodeMonitorGroupPollStatus(
            node_monitor_statuses={
                node_id: status(monitor, self._node_probes)
                for node_id, monitor in self._worker_node_monitors.items()
            },
            controller_node_monitor_status=controller_status,
        )

    def _poll_status(self, timeout: float) -> Dict[ActorHandle, NodeMonitorStatus]:
        """Call ``poll_status()`` on every monitor, or keep waiting on its call
        that has yet to answer from an earlier round.

        Args:
            timeout: The maximum time to wait for the monitors to answer.

        Returns:
            ``{monitor: its status}``, without the monitors that have yet to
            answer and have not passed the health-check timeout.
        """
        monitors = list(self._worker_node_monitors.items())
        if self._controller_node_monitor is not None:
            monitors.append((self._controller_node_id, self._controller_node_monitor))
        start_time = time.monotonic()
        for _, monitor in monitors:
            if monitor not in self._ongoing_polls:
                self._ongoing_polls[monitor] = _OngoingPoll(
                    start_time=start_time, task=monitor.poll_status.remote()
                )
        tasks = [self._ongoing_polls[monitor].task for _, monitor in monitors]
        if not tasks:
            return {}
        done = set(ray.wait(tasks, num_returns=len(tasks), timeout=timeout)[0])

        statuses: Dict[ActorHandle, NodeMonitorStatus] = {}
        for node_id, monitor in monitors:
            poll = self._ongoing_polls[monitor]
            elapsed_s = time.monotonic() - poll.start_time
            if poll.task in done:
                del self._ongoing_polls[monitor]
                statuses[monitor] = _get(poll.task)
            elif elapsed_s > self._health_check_timeout_s:
                statuses[monitor] = NodeMonitorStatus(
                    monitor_error=TimeoutError(
                        f"The NodeMonitor on node {node_id} has not answered a "
                        f"poll for {elapsed_s:.1f} seconds."
                    )
                )
        return statuses

    # On-demand probes.

    def poll_probes_async(
        self,
        probes: List[Probe],
        timeout_s: float,
        node_ids: Optional[List[NodeIdStr]] = None,
    ) -> None:
        """Start polling probes once: the ``NodeProbe``\\ s on worker nodes, and
        the ``ControllerProbe``\\ s on the controller's node. Get their results
        with ``poll_probe_results()``.

        Args:
            probes: The probes to poll.
            timeout_s: Timeout in seconds for the monitors to answer.
            node_ids: The worker nodes to poll the ``NodeProbe``\\ s on. ``None``
                means every one.

        Raises:
            ValueError: If a worker node has no monitor.
        """
        node_ids = self.worker_node_ids if node_ids is None else node_ids
        missing = [n for n in node_ids if n not in self._worker_node_monitors]
        if missing:
            raise ValueError(f"No NodeMonitor on nodes {missing}.")
        node_probes = [p for p in probes if isinstance(p, NodeProbe)]
        controller_probes = [p for p in probes if isinstance(p, ControllerProbe)]

        self._on_demand_deadline = time.monotonic() + timeout_s
        self._on_demand_polls = [
            _poll_probes_async(
                node_id, self._worker_node_monitors[node_id], node_probes
            )
            for node_id in (node_ids if node_probes else [])
        ]
        self._on_demand_controller_poll = None
        if controller_probes:
            self._on_demand_controller_poll = _poll_probes_async(
                self._controller_node_id,
                self._controller_node_monitor,
                controller_probes,
            )

    def poll_probe_results(self) -> Optional[HealthState]:
        """Get the results of the probes that the last ``poll_probes_async()``
        polls, once every monitor answered or the timeout passed. A monitor that
        did not answer, or that failed, gives each of its probes the error.

        Returns:
            The results of every probe, or ``None`` while a monitor has yet to
            answer and the timeout has not passed.
        """
        controller_poll = self._on_demand_controller_poll
        polls = self._on_demand_polls + ([controller_poll] if controller_poll else [])
        tasks = [poll.task for poll in polls]
        done = (
            set(ray.wait(tasks, num_returns=len(tasks), timeout=0)[0])
            if tasks
            else set()
        )
        if len(done) < len(tasks) and time.monotonic() < self._on_demand_deadline:
            return None

        def results(poll: _OnDemandPoll) -> Dict[str, Any]:
            if poll.task in done:
                status = _get(poll.task)
            else:
                status = NodeMonitorStatus(
                    monitor_error=TimeoutError(
                        f"The NodeMonitor on node {poll.node_id} did not answer "
                        "in time."
                    )
                )
            return _fail_probes(status, poll.probes).probe_results

        probe_results: Dict[str, Dict[Any, ProbeResult]] = {}
        for poll in self._on_demand_polls:
            for name, result in results(poll).items():
                probe_results.setdefault(name, {})[poll.node_id] = result
        return HealthState(
            probe_results=probe_results,
            controller_probe_results=results(controller_poll)
            if controller_poll
            else {},
        )

    # Shared by both kinds of monitor.

    def _start_monitor(
        self, node_id: NodeIdStr, probes: List[ScheduledProbe]
    ) -> ActorHandle:
        """Start a monitor, which takes no CPUs, on a node."""
        return self._monitor_cls.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=False
            ),
        ).remote(probes)

    def _kill_monitor(self, monitor: Optional[ActorHandle]) -> None:
        if monitor is not None:
            ray.kill(monitor)
            self._ongoing_polls.pop(monitor, None)


def _poll_probes_async(
    node_id: NodeIdStr, monitor: ActorHandle, probes: List[Probe]
) -> _OnDemandPoll:
    return _OnDemandPoll(node_id, probes, monitor.poll_probes.remote(probes))


def _get(task: ObjectRef) -> NodeMonitorStatus:
    try:
        return ray.get(task)
    except Exception as e:
        return NodeMonitorStatus(monitor_error=e)


def _fail_probes(
    status: NodeMonitorStatus, probes: Sequence[Probe]
) -> NodeMonitorStatus:
    """Give each probe of a monitor that failed a result with its error."""
    if status.monitor_error is not None:
        now = time.time()
        status.probe_results = {
            p.probe_name(): ProbeResult(timestamp_s=now, error=status.monitor_error)
            for p in probes
        }
    return status
