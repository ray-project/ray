import logging
import time
from dataclasses import dataclass
from typing import Dict, List, NamedTuple, Optional

import ray
from ray.actor import ActorHandle
from ray.train.health._internal.node_monitor import NodeMonitor, NodeMonitorStatus
from ray.train.health.probe import NodeIdStr, NodeProbe
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S = 60.0

# How long poll_status() waits for the monitors by default.
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
            node_id: node_monitor_status.error
            for node_id, node_monitor_status in self.node_monitor_statuses.items()
            if node_monitor_status.error is not None
        }


class NodeMonitorGroup:
    """One ``NodeMonitor`` actor on each of a set of nodes.

    Args:
        probes: The ``NodeProbe``\\ s every monitor samples.
        health_check_timeout_s: How long a monitor's ``poll_status()`` may stay
            unanswered before it is reported as an error.
    """

    def __init__(
        self,
        probes: List[NodeProbe],
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
        """Get the latest samples from every monitor.

        A monitor that does not answer within ``timeout`` is not polled again
        until it answers. Once it has been unanswered for longer than
        ``health_check_timeout_s``, its status carries a ``TimeoutError``. A
        monitor whose poll failed, for example because it died, carries the
        failure.

        Args:
            timeout: The maximum time to wait for the monitors to answer.

        Returns:
            The status of every monitor.
        """
        start_time = time.monotonic()
        for node_id, monitor in self._monitors.items():
            if node_id not in self._ongoing_polls:
                self._ongoing_polls[node_id] = _OngoingPoll(
                    start_time, monitor.poll_status.remote()
                )
        if not self._ongoing_polls:
            return NodeMonitorGroupPollStatus(node_monitor_statuses={})

        task_to_node = {poll.task: node for node, poll in self._ongoing_polls.items()}
        done, hanging = ray.wait(
            list(task_to_node), num_returns=len(task_to_node), timeout=timeout
        )

        node_monitor_statuses: Dict[NodeIdStr, NodeMonitorStatus] = {}
        for task in hanging:
            node_id = task_to_node[task]
            elapsed_s = time.monotonic() - self._ongoing_polls[node_id].start_time
            error = None
            if elapsed_s > self._health_check_timeout_s:
                error = TimeoutError(
                    f"The NodeMonitor on node {node_id} has not answered a poll "
                    f"for {elapsed_s:.1f} seconds."
                )
            node_monitor_statuses[node_id] = NodeMonitorStatus(error=error)

        for task in done:
            node_id = task_to_node[task]
            del self._ongoing_polls[node_id]
            try:
                node_monitor_statuses[node_id] = ray.get(task)
            except Exception as e:
                logger.warning("Polling the NodeMonitor on node %s failed.", node_id)
                node_monitor_statuses[node_id] = NodeMonitorStatus(error=e)

        return NodeMonitorGroupPollStatus(node_monitor_statuses=node_monitor_statuses)
