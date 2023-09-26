import copy
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from google.protobuf.json_format import MessageToDict

from ray.autoscaler._private.resource_demand_scheduler import UtilizationScore
from ray.autoscaler._private.util import NodeType
from ray.core.generated.autoscaler_pb2 import ResourceRequest
from ray.core.generated.instance_manager_pb2 import (
    NodeTypeConfig,
    ResourceScheduleConfig,
    ScheduleResourceBundlesReply,
    ScheduleResourceBundlesRequest,
)


class SchedulingNodeStatus(Enum):
    """
    The status of a scheduling node (`SchedulingNode`)
    """

    # The node is to be launched.
    TO_LAUNCH = "TO_LAUNCH"
    # The node is pending to be launched (a pending request, or a pending instance)
    PENDING = "PENDING"
    # The node is running.
    RUNNING = "RUNNING"


@dataclass
class SchedulingNode:
    """
    A abstraction of a node that can be scheduled on by the resource scheduler.

    A scheduling node is expected to be used as:

        node  = SchedulingNode.from_node_config(node_config)
        infeasible = node.try_schedule(requests)
        score = node.compute_score()

        .... do something with the score ....

    NOTE:
        One could also extend the scheduling behavior by overriding:
            1. try_schedule()
            2. compute_score()

    """

    # Class level node id counter.
    _next_node_id = 0

    # A unique id simply for differentiating nodes. Not the same as a normal ray node
    # id.
    node_id: int
    # Node type name.
    node_type: NodeType
    # Requests committed to be placed on this node.
    sched_requests: List[ResourceRequest] = field(default_factory=list)
    # The node's current resource capacity.
    total_resources: Dict[str, float] = field(default_factory=dict)
    # The node's available resources.
    available_resources: Dict[str, float] = field(default_factory=dict)
    # Node's labels, including static or dynamic labels.
    labels: Dict[str, str] = field(default_factory=dict)
    # Status
    status: SchedulingNodeStatus = SchedulingNodeStatus.TO_LAUNCH
    # Observability descriptive message for why the node was launched in the
    # first place.
    # TODO
    launch_reason: Optional[str] = None

    @classmethod
    def from_node_config(
        cls,
        node_config: NodeTypeConfig,
        status: SchedulingNodeStatus = SchedulingNodeStatus.TO_LAUNCH,
    ) -> "SchedulingNode":
        """
        Create a scheduling node from a node config.
        """
        return cls(
            node_id=cls.next_node_id(),
            node_type=node_config.name,
            total_resources=dict(node_config.resources),
            available_resources=dict(node_config.resources),
            labels=dict(node_config.labels),
            status=status,
        )

    def try_schedule(self, requests: List[ResourceRequest]) -> List[ResourceRequest]:
        """
        Try to schedule the resource requests on this node.

        This modifies the node's available resources if the requests are schedulable.
        When iterating through the requests, the requests are sorted by the
        `_sort_resource_request` function. The requests are scheduled one by one in
        the sorted order, and no backtracking is done.

        Args:
            requests: The resource requests to be scheduled.

        Returns:
            A list of infeasible requests that cannot be scheduled on this node.
        """
        pass

    def compute_score(self) -> UtilizationScore:
        """
        Compute the utilization score for this node with respect to the current resource
        request being scheduled.

        A "higher" score means that this node is more suitable for scheduling the
        current scheduled resource requests.

        The score is a tuple of 4 values:
            1. Whether this node is a GPU node and the current resource request has
                GPU requirements:
                    0: if this node is a GPU node and the current resource request
                    placed onto the node has no GPU requirements.
                    1: if this node is not a GPU node or the current resource request
                    placed onto the node has GPU requirements.
            2. The number of resource types being scheduled.
            3. The minimum utilization rate across all resource types.
            4. The average utilization rate across all resource types.

        NOTE:
            This function is adapted from  _resource_based_utilization_scorer from
            autoscaler v1.

        TODO(rickyx,jjyao):  We should also consider node labels for
            scoring. For example, if a node has a label that matches the affinity
            label of the resource request, we should give it a higher score.

        TODO(rickyx): add pluggable scoring functions here.

        Returns:
            A utilization score for this node.
        """
        pass

    @classmethod
    def next_node_id(cls) -> int:
        """
        Return the next scheduling node id.
        """
        cls._next_node_id += 1
        return cls._next_node_id

    def __repr__(self) -> str:
        return (
            "SchedulingNode(id={node_id},node_type={node_type}, "
            "status={status}, "
            "total_resources={total_resources}, "
            "available_resources={available_resources}, "
            "labels={labels}, launch_reason={launch_reason}), "
            "sched_requests={sched_requests})"
        ).format(
            node_id=self.node_id,
            node_type=self.node_type,
            status=self.status,
            total_resources=self.total_resources,
            available_resources=self.available_resources,
            labels=self.labels,
            launch_reason=self.launch_reason,
            sched_requests="|".join(str(MessageToDict(r)) for r in self.sched_requests),
        )


# TODO(rickyx): Move interface to a separate file.
class IResourceScheduler(metaclass=ABCMeta):
    """
    Interface for a resource scheduler.

    Implements the `instance_manager.proto ResourceSchedulerService` interface.
    """

    @abstractmethod
    def schedule_resource_bundles(
        self, request: ScheduleResourceBundlesRequest
    ) -> ScheduleResourceBundlesReply:
        pass


@dataclass
class ScheduleContext:
    """
    A context object that holds the current scheduling context.
    It contains:
        1. existing nodes (including pending nodes and pending requests).
        2. the number of nodes by node types available for launching based on the max
            number of workers in the config. This takes into account any pending/running
            nodes.
        3. the resource schedule config.

    The context should be updated during the scheduling process:
    ```
        ctx.update(new_nodes)
    ```

    Most of the getters return a copy of the internal state to prevent accidental
    modification of the internal state.

    """

    # The current resource schedule config.
    config_: ResourceScheduleConfig
    # The current schedulable nodes (including pending nodes and pending requests).
    nodes_: List[SchedulingNode] = field(default_factory=list)
    # The number of nodes by node types available for launching based on the max
    # number of workers in the config. This takes into account any pending/running
    # nodes.
    node_type_available_: Dict[NodeType, int] = field(default_factory=dict)

    def __init__(
        self,
        nodes: List[SchedulingNode],
        node_type_available: Dict[NodeType, int],
        config: ResourceScheduleConfig,
    ):
        self._nodes = nodes
        self._node_type_available = node_type_available
        self._config = config

    @classmethod
    def from_schedule_request(
        cls, req: ScheduleResourceBundlesRequest
    ) -> "ScheduleContext":
        """
        Create a schedule context from a schedule request.

        It will populate the context with the existing nodes and the available node
        types from the config.
        """
        pass

    def get_nodes(self) -> List[SchedulingNode]:
        # NOTE: We do the deep copy here since the nodes are usually being updated
        # during the scheduling process. To prevent accidental modification of the
        # nodes, we return a copy, and callers should modify the context explicitly
        # with `ctx.update()`
        return copy.deepcopy(self._nodes)

    def get_node_config(self, node_type: NodeType) -> Optional[NodeTypeConfig]:
        c = self._config.node_type_configs.get(node_type, None)
        if c:
            return copy.deepcopy(c)
        return None

    def get_sched_config(self) -> ResourceScheduleConfig:
        return copy.deepcopy(self._config)

    def update(self, new_nodes: List[SchedulingNode]) -> None:
        """
        Update the context with the new nodes.
        """
        pass

    def get_node_type_available(self) -> Dict[NodeType, int]:
        return copy.deepcopy(self._node_type_available)

    def get_cluster_shape(self) -> Dict[NodeType, int]:
        """
        Return the current cluster shape.

        The cluster shape is a dict of node types and the number of nodes of that type.
        """
        cluster_shape = defaultdict(int)
        for node in self._nodes:
            cluster_shape[node.node_type] += 1
        return cluster_shape

    def get_max_num_nodes(self) -> Optional[int]:
        """
        Return the max number of nodes allowed in the cluster.
        """
        return (
            self._config.max_num_nodes
            if self._config.HasField("max_num_nodes")
            else None
        )


class SimpleResourceScheduler(IResourceScheduler):
    """
    A "simple" resource scheduler that schedules resource requests based on the
    following rules:
        1. Enforce the minimal count of nodes for each worker node type.
        2. Enforce the cluster resource constraints.
        3. Schedule the gang resource requests.
        4. Schedule the tasks/actor resource requests

    """

    def schedule_resource_bundles(
        self, request: ScheduleResourceBundlesRequest
    ) -> ScheduleResourceBundlesReply:
        pass
