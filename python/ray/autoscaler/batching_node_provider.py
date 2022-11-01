from collections import (
    dataclass,
    defaultdict
)
from copy import copy
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
    desired_num_workers: Dict[NodeType, int] = {}
    workers_to_delete: List[NodeID] = []


@dataclass
class NodeData:
    node_id: NodeID
    node_tags: Dict[str, str]
    internal_ip: Optional[NodeIP]
    external_ip: Optional[NodeIP]
    node_status: NodeStatus


class BatchingNodeProvider(NodeProvider):
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.node_data_dict = {}
        self.scale_request = ScaleRequest()

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        self.node_data_dict = self.get_node_data()
        self.scale_request = ScaleRequest(
            desired_num_workers=self.get_num_workers(self.node_data_dict),
            workers_to_delete=[]
        )
        return list(self.node_data_dict.keys())

    def get_node_data(self):
        raise NotImplementedError

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

    def external_ip(self, node_id: str) -> str:
        return self.node_data_dict[node_id].external_ip

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

    def post_process(self) -> None:
        if self.safe_to_scale():
            self.submit_scale_request(self.scale_request)

    def safe_to_scale() -> bool:
        """Optional condition to determine if it's safe to submit a scale request.
        Can be used to wait for convergence of state managed by an external cluster manager.

        If False is returned, the scale request will not be submitted during the current autoscaler
        update iteration.
        """
        return True

    def submit_scale_request(self, scale_request: ScaleRequest) -> None:
        raise NotImplementedError
