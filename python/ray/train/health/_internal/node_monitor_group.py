import logging
import time
from dataclasses import dataclass
from typing import Dict, List, NamedTuple, Optional

import ray
from ray.actor import ActorHandle
from ray.train.health._internal.node_monitor import (
    NodeMonitor,
    NodeMonitorStatus,
    ScheduledProbe,
)
from ray.train.health.probe import NodeIdStr, NodeProbe
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

# The time in seconds a NodeMonitor may leave a poll unanswered before it is
# reported as failed.
DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S = 60.0

# Timeout in seconds for poll_status() to wait for the monitors.
DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S = 1.0


class _OngoingPoll(NamedTuple):
    start_time: float
    task: ObjectRef


@dataclass(frozen=True)
class NodeMonitorGroupPollStatus:
    node_monitor_statuses: Dict[NodeIdStr, NodeMonitorStatus]

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
    ``poll_probes_async()``.

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
        self._probes = list(probes)
        self._health_check_timeout_s = health_check_timeout_s
        self._monitor_cls = ray.remote(NodeMonitor)
        self._monitors: Dict[NodeIdStr, ActorHandle] = {}
        self._ongoing_polls: Dict[NodeIdStr, _OngoingPoll] = {}

    @property
    def node_ids(self) -> List[NodeIdStr]:
        return sorted(self._monitors)

    def start(self, node_ids: List[NodeIdStr]) -> None:
        """Start a ``NodeMonitor``, which takes no CPUs, on each of these nodes
        that does not have one.

        Args:
            node_ids: The nodes to monitor.
        """
        for node_id in node_ids:
            if node_id in self._monitors:
                continue
            self._monitors[node_id] = self._monitor_cls.options(
                num_cpus=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_id, soft=False
                ),
            ).remote(self._probes)

    def shutdown(self, node_ids: Optional[List[NodeIdStr]] = None) -> None:
        """Stop the monitors on some nodes, or on every node.

        Args:
            node_ids: The nodes to stop monitoring. ``None`` means all.
        """
        for node_id in list(self._monitors) if node_ids is None else node_ids:
            monitor = self._monitors.pop(node_id, None)
            if monitor is not None:
                ray.kill(monitor)
            self._ongoing_polls.pop(node_id, None)

    def poll_status(
        self, timeout: float = DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S
    ) -> NodeMonitorGroupPollStatus:
        """Poll the latest samples of every monitor.

        Args:
            timeout: The maximum time to wait for the poll tasks to complete.

        Returns:
            A ``NodeMonitorGroupPollStatus`` with each monitor's status.
        """
        return NodeMonitorGroupPollStatus(
            node_monitor_statuses=self._poll_monitors_and_collect_errors(timeout)
        )

    def _poll_monitors_and_collect_errors(
        self, timeout: float
    ) -> Dict[NodeIdStr, NodeMonitorStatus]:
        """Launch a poll task on each monitor and collect the results.

        Args:
            timeout: The maximum time to wait for the poll tasks to complete.

        Returns:
            ``{node ID: NodeMonitorStatus}`` for every monitor.
        """
        start_time = time.monotonic()
        poll_task_to_node = self._get_poll_tasks()
        if not poll_task_to_node:
            return {}
        done_polls, hanging_polls = ray.wait(
            list(poll_task_to_node), num_returns=len(poll_task_to_node), timeout=timeout
        )

        node_monitor_statuses: Dict[NodeIdStr, NodeMonitorStatus] = {}
        for hanging_poll in hanging_polls:
            node_id = poll_task_to_node[hanging_poll]
            # Keep the hanging poll task, and wait on it in the next round.
            ongoing_poll = self._ongoing_polls.setdefault(
                node_id, _OngoingPoll(start_time=start_time, task=hanging_poll)
            )
            error = None
            elapsed_s = time.monotonic() - ongoing_poll.start_time
            if elapsed_s > self._health_check_timeout_s:
                error = TimeoutError(
                    f"The NodeMonitor on node {node_id} has not answered a poll "
                    f"for {elapsed_s:.1f} seconds."
                )
            node_monitor_statuses[node_id] = NodeMonitorStatus(monitor_error=error)

        for done_poll in done_polls:
            node_id = poll_task_to_node[done_poll]
            self._ongoing_polls.pop(node_id, None)
            try:
                node_monitor_statuses[node_id] = ray.get(done_poll)
            except Exception as e:
                logger.warning("Polling the NodeMonitor on node %s failed.", node_id)
                node_monitor_statuses[node_id] = NodeMonitorStatus(monitor_error=e)

        return node_monitor_statuses

    def _get_poll_tasks(self) -> Dict[ObjectRef, NodeIdStr]:
        """Get a poll task for each monitor: the one still waiting from an
        earlier round, or a new one."""
        poll_task_to_node: Dict[ObjectRef, NodeIdStr] = {}
        for node_id, monitor in self._monitors.items():
            ongoing_poll = self._ongoing_polls.get(node_id)
            if ongoing_poll is not None:
                poll_task_to_node[ongoing_poll.task] = node_id
            else:
                poll_task_to_node[monitor.poll_status.remote()] = node_id
        return poll_task_to_node

    def poll_probes_async(
        self, probes: List[NodeProbe], node_ids: Optional[List[NodeIdStr]] = None
    ) -> Dict[NodeIdStr, ObjectRef]:
        """Poll some probes once on some monitors' nodes.

        Args:
            probes: The probes to poll.
            node_ids: The nodes to poll them on. ``None`` means every node with a
                monitor.

        Returns:
            ``{node ID: ObjectRef}`` for each node's ``NodeMonitorStatus``.

        Raises:
            ValueError: If a node has no monitor.
        """
        node_ids = list(self._monitors) if node_ids is None else node_ids
        missing = [node_id for node_id in node_ids if node_id not in self._monitors]
        if missing:
            raise ValueError(f"No NodeMonitor on nodes {missing}.")
        return {
            node_id: self._monitors[node_id].poll_probes.remote(probes)
            for node_id in node_ids
        }
