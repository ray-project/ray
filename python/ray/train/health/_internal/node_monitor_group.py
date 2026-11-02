import time
from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional

import ray
from ray.actor import ActorHandle
from ray.train.health._internal.constants import (
    DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S,
)
from ray.train.health._internal.node_monitor import NodeMonitor, NodeMonitorStatus
from ray.train.health._internal.probe_utils import ScheduledProbe
from ray.train.health.probe import ControllerProbe, NodeIdStr, NodeProbe, Probe
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class _OngoingPoll(NamedTuple):
    start_time: float
    task: ObjectRef


@dataclass(frozen=True)
class NodeMonitorGroupPollStatus:
    node_monitor_statuses: Dict[NodeIdStr, NodeMonitorStatus]
    controller_node_monitor_status: NodeMonitorStatus = field(
        default_factory=NodeMonitorStatus
    )

    @property
    def errors(self) -> Dict[NodeIdStr, Exception]:
        return {
            node_id: node_monitor_status.monitor_error
            for node_id, node_monitor_status in self.node_monitor_statuses.items()
            if node_monitor_status.monitor_error is not None
        }


class NodeMonitorGroup:
    """One ``NodeMonitor`` actor on each of a set of nodes. Each monitor samples
    the scheduled probes it starts with. For on-demand probes, use
    ``poll_probes_async()`` and ``poll_probe_results()``.

    Args:
        probes: The ``ScheduledProbe``\\ s every monitor samples.
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
        self._controller_node_monitor: Optional[ActorHandle] = None
        # Keyed by monitor, since the controller's node can be a worker node.
        self._ongoing_polls: Dict[ActorHandle, _OngoingPoll] = {}
        # The on-demand probes that poll_probes_async() polls: the time their
        # results are due, and the ObjectRefs of the monitors'
        # NodeMonitorStatuses.
        self._on_demand_deadline = 0.0
        self._on_demand_polls: Dict[NodeIdStr, ObjectRef] = {}
        self._on_demand_controller_poll: Optional[ObjectRef] = None

    @property
    def worker_node_ids(self) -> List[NodeIdStr]:
        return sorted(self._worker_node_monitors)

    def start_worker_node_monitors(self, node_ids: List[NodeIdStr]) -> None:
        """Start a ``NodeMonitor`` of the ``NodeProbe``\\ s on each of these
        nodes that does not have one.

        Args:
            node_ids: The nodes to monitor.
        """
        for node_id in node_ids:
            if node_id not in self._worker_node_monitors:
                self._worker_node_monitors[node_id] = self._start_monitor(
                    node_id, self._node_probes
                )

    def start_controller_node_monitor(self, node_id: NodeIdStr) -> None:
        """Start the ``NodeMonitor`` of the ``ControllerProbe``\\ s on the
        controller's node, in place of any earlier one.

        Args:
            node_id: The controller's node.
        """
        self._kill_monitor(self._controller_node_monitor)
        self._controller_node_monitor = self._start_monitor(
            node_id, self._controller_probes
        )

    def shutdown_worker_node_monitors(
        self, node_ids: Optional[List[NodeIdStr]] = None
    ) -> None:
        """Stop the monitors on some worker nodes, or on every one.

        Args:
            node_ids: The nodes to stop monitoring. ``None`` means all.
        """
        if node_ids is None:
            node_ids = list(self._worker_node_monitors)
        for node_id in node_ids:
            self._kill_monitor(self._worker_node_monitors.pop(node_id, None))

    def shutdown(self) -> None:
        """Stop every monitor."""
        self.shutdown_worker_node_monitors()
        self._kill_monitor(self._controller_node_monitor)

    def poll_status(
        self, timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
    ) -> NodeMonitorGroupPollStatus:
        """Poll the latest samples of every monitor.

        Args:
            timeout: The maximum time to wait for the poll tasks to complete.

        Returns:
            A ``NodeMonitorGroupPollStatus`` with each monitor's status.
        """
        statuses = self._poll_monitors_and_collect_errors(timeout)
        return NodeMonitorGroupPollStatus(
            node_monitor_statuses={
                node_id: statuses[monitor]
                for node_id, monitor in self._worker_node_monitors.items()
            },
            controller_node_monitor_status=statuses[self._controller_node_monitor],
        )

    def _poll_monitors_and_collect_errors(
        self, timeout: float
    ) -> Dict[ActorHandle, NodeMonitorStatus]:
        """Launch a poll task on each monitor and collect the results.

        Args:
            timeout: The maximum time to wait for the poll tasks to complete.

        Returns:
            ``{monitor: NodeMonitorStatus}`` for every monitor.
        """
        start_time = time.monotonic()
        poll_task_to_monitor = self._get_poll_tasks()
        done_polls, hanging_polls = ray.wait(
            list(poll_task_to_monitor),
            num_returns=len(poll_task_to_monitor),
            timeout=timeout,
        )

        node_monitor_statuses: Dict[ActorHandle, NodeMonitorStatus] = {}
        for hanging_poll in hanging_polls:
            monitor = poll_task_to_monitor[hanging_poll]
            # Keep the hanging poll task, and wait on it in the next round.
            ongoing_poll = self._ongoing_polls.setdefault(
                monitor, _OngoingPoll(start_time=start_time, task=hanging_poll)
            )
            error = None
            elapsed_s = time.monotonic() - ongoing_poll.start_time
            if elapsed_s > self._health_check_timeout_s:
                error = TimeoutError(
                    f"The NodeMonitor has not answered a poll for {elapsed_s:.1f} "
                    "seconds."
                )
            node_monitor_statuses[monitor] = NodeMonitorStatus(monitor_error=error)

        for done_poll in done_polls:
            monitor = poll_task_to_monitor[done_poll]
            self._ongoing_polls.pop(monitor, None)
            node_monitor_statuses[monitor] = _get_status(done_poll)

        return node_monitor_statuses

    def _get_poll_tasks(self) -> Dict[ObjectRef, ActorHandle]:
        """Get a poll task for each monitor: the one still waiting from an
        earlier round, or a new one.

        Returns:
            ``{poll task: monitor}`` for every monitor.
        """
        monitors = [*self._worker_node_monitors.values(), self._controller_node_monitor]
        poll_task_to_monitor: Dict[ObjectRef, ActorHandle] = {}
        for monitor in monitors:
            ongoing_poll = self._ongoing_polls.get(monitor)
            if ongoing_poll is not None:
                poll_task_to_monitor[ongoing_poll.task] = monitor
            else:
                poll_task_to_monitor[monitor.poll_status.remote()] = monitor
        return poll_task_to_monitor

    def poll_probes_async(
        self,
        probes: List[Probe],
        timeout_s: float,
        node_ids: Optional[List[NodeIdStr]] = None,
    ) -> None:
        """Start polling some probes once: the ``NodeProbe``\\ s on some worker
        nodes, and the ``ControllerProbe``\\ s on the controller's node. Get
        their results with ``poll_probe_results()``.

        Args:
            probes: The probes to poll.
            timeout_s: Timeout in seconds for the nodes to answer.
            node_ids: The worker nodes to poll the ``NodeProbe``\\ s on. ``None``
                means every worker node with a monitor.

        Raises:
            ValueError: If a node has no monitor.
        """
        node_ids = list(self._worker_node_monitors) if node_ids is None else node_ids
        missing = [n for n in node_ids if n not in self._worker_node_monitors]
        if missing:
            raise ValueError(f"No NodeMonitor on nodes {missing}.")
        node_probes = [p for p in probes if isinstance(p, NodeProbe)]
        controller_probes = [p for p in probes if isinstance(p, ControllerProbe)]
        self._on_demand_deadline = time.monotonic() + timeout_s
        self._on_demand_polls = {
            node_id: self._worker_node_monitors[node_id].poll_probes.remote(node_probes)
            for node_id in node_ids
        }
        self._on_demand_controller_poll = (
            self._controller_node_monitor.poll_probes.remote(controller_probes)
        )

    def poll_probe_results(self) -> Optional[NodeMonitorGroupPollStatus]:
        """Get the results of the probes that ``poll_probes_async()`` polls, once
        every node answered or the timeout passed. A node that did not answer
        has a ``TimeoutError`` as its ``monitor_error``.

        Returns:
            A ``NodeMonitorGroupPollStatus`` with the status of each polled
            monitor, or ``None`` while a node has yet to answer and the timeout
            has not passed.
        """
        polls = [*self._on_demand_polls.values(), self._on_demand_controller_poll]
        done_polls, hanging_polls = ray.wait(polls, num_returns=len(polls), timeout=0)
        if hanging_polls and time.monotonic() < self._on_demand_deadline:
            return None

        statuses = {poll: _get_status(poll) for poll in done_polls}
        for hanging_poll in hanging_polls:
            statuses[hanging_poll] = NodeMonitorStatus(
                monitor_error=TimeoutError("The NodeMonitor did not answer in time.")
            )
        return NodeMonitorGroupPollStatus(
            node_monitor_statuses={
                node_id: statuses[poll]
                for node_id, poll in self._on_demand_polls.items()
            },
            controller_node_monitor_status=statuses[self._on_demand_controller_poll],
        )

    def _start_monitor(
        self, node_id: NodeIdStr, probes: List[ScheduledProbe]
    ) -> ActorHandle:
        """Start a ``NodeMonitor``, which takes no CPUs, on a node."""
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


def _get_status(poll: ObjectRef) -> NodeMonitorStatus:
    """Get the ``NodeMonitorStatus`` that a finished poll returned, or its
    error."""
    try:
        return ray.get(poll)
    except Exception as e:
        return NodeMonitorStatus(monitor_error=e)
