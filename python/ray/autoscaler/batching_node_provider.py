from collections import (
    dataclass,
    defaultdict
)
from copy import copy
from enum import Enum
import logging
from typing import Any, Dict, List, Optional

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_USER_NODE_TYPE,
    TAG_RAY_NODE_STATUS,
)
from ray.autoscaler._private.util import (
    NodeID,
    NodeIP,
    NodeType,
)

logger = logging.getLogger(__name__)

NodeStatus = str


@dataclass
class ScaleRequest:
    """Stores desired scale computed by the autoscaler.

    Attributes:
        desired_num_workers: Map of worker NodeType to desired number of workers of
            that type.
        workers_to_delete: List of ids of nodes that should be removed.
    """
    desired_num_workers: Dict[NodeType, int] = {}
    workers_to_delete: List[NodeID] = []


class NodeKind(Enum):
    HEAD = "head"
    WORKER = "worker"


@dataclass
class NodeData:
    """Stores all data about a Ray node needed by the autoscaler.

    Attributes:
        kind (NodeKind): Whether the node is the head or a worker.
        type (str): The user-defined type of the node.
        ip (Optional[str]): Cluster-internal ip of the node. ip can be None if the ip
            has not yet been assigned.
        status (str): The status of the node. You must adhere to the following semantics
            for status:
            * The value for "tag-ray-node-status" must be "up-to-date" if and only if
                the node is running.
            * The value for "tag-ray-node-status" must be "update-failed" if and only if
                the node is in an unknown or failed state.
            * If the node is in a pending (starting-up) state, the value should be
                a brief user-facing description of why the node is pending.
    """
    kind: NodeKind
    type: NodeType
    ip: Optional[NodeIP]
    status: NodeStatus


class BatchingNodeProvider(NodeProvider):
    """Abstract subclass of NodeProvider meant for use with external cluster managers.

    Implementing a concrete subclass of BatchingNodeProvider requires overriding two
    methods:

        get_node_data(): Queries cluster manager for node info. Returns a mapping from
            node id to NodeData.

        submit_scale_request(): Submits a request to create and delete nodes.

    In addition, the following optional method is available:

        safe_to_scale(): Whether we should submit the scale request. If False is
            returned, we will not submit a scale request for this autoscaler iteration.

    See the method docstrings for more information.
    """
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.node_data_dict: Dict[NodeID, NodeData] = {}
        self.scale_request = ScaleRequest()

    def get_node_data(self) -> Dict[NodeID, NodeData]:
        """Queries cluster manager for node info. Returns a mapping from node id to
        NodeData.

        Each NodeData value must adhere to the semantics of the NodeData docstring.
        (Note in particular the requirements for NodeData.status.)

        Consistency requirement:
        If a node id was present in ScaleRequest.workers_to_delete of a previously
        submitted scale request, it should no longer be present as key in get_node_data.
        (Node termination must be registered immediately when submit_scale_request
        returns.)
        """
        raise NotImplementedError

    def submit_scale_request(self, scale_request: ScaleRequest) -> None:
        """Tells the cluster manager which nodes to delete and how many nodes of
        each node type to maintain.

        Consistency requirement:
        If a node id was present in ScaleRequest.workers_to_delete of a previously
        submitted scale request, it should no longer be present as key in get_node_data.
        (Node termination must be registered immediately when submit_scale_request
        returns.)
        """
        raise NotImplementedError

    def safe_to_scale() -> bool:
        """Optional condition to determine if it's safe to submit a scale request.
        Can be used to wait for convergence of state managed by an external cluster
        manager.

        If False is returned, the scale request will not be submitted during the current
        autoscaler update iteration.
        """
        return True

    def post_process(self) -> None:
        """Submit a scale request if it is safe to do so.
        """
        if self.safe_to_scale():
            self.submit_scale_request(self.scale_request)

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        self.node_data_dict = self.get_node_data()
        self.scale_request = ScaleRequest(
            desired_num_workers=self.get_num_workers(self.node_data_dict),
            workers_to_delete=[]
        )
        return list(self.node_data_dict.keys())

    def get_num_workers(self, node_data_dict: Dict[str, Any]):
        num_workers_dict = defaultdict(int)
        for node_data in node_data_dict.values():
            node_type = node_data.node_tags[TAG_RAY_USER_NODE_TYPE]
            num_workers_dict[node_type] += 1
        return num_workers_dict

    def node_tags(self, node_id: str) -> Dict[str, str]:
        node_data = self.node_data_dict[node_id]
        tags = copy(node_data.tags)
        tags[TAG_RAY_NODE_STATUS] = node_data.status
        return tags

    def internal_ip(self, node_id: str) -> str:
        return self.node_data_dict[node_id].internal_ip

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        self.scale_request.desired_num_workers[node_type] += count

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """
        node_type = self.node_tags(node_id)[TAG_RAY_USER_NODE_TYPE]
        self.scale_request.desired_num_workers[node_type] -= 1
        self.scale_request.workers_to_terminate.append(node_id)
