import logging
from types import ModuleType
from typing import Any, Dict, List, Optional

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_USER_NODE_TYPE,
    TAG_RAY_NODE_STATUS
)
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

class ScaleRequest:
    pass

class NodeData:
    pass

@DeveloperAPI
class BatchingNodeProvider(NodeProvider):
    """Interface for getting and returning nodes from a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom node providers. It is not allowed to call into
    NodeProvider methods from any Ray package outside the autoscaler, only to
    define new implementations of NodeProvider for use with the "external" node
    provider option.

    NodeProviders are namespaced by the `cluster_name` parameter; they only
    operate on nodes within that namespace.

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.node_data = {}
        self.scale_request = ScaleRequest()

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        self.node_data = self.get_node_data()
        self.scale_request = ScaleRequest(
            desired_num_workers=self.get_cur_num_workers(node_data),
            workers_to_delete=[]
        )
        return list(node_data.keys())

    def get_cur_num_workers(self, self.node_data):
        for node_data in self.node_data.values():
            pass

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        node_data = self.node_data[node_id]
        return self.node_data.node_tags + {TAG_RAY_NODE_STATUS: node_data.status}

    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        return self.node_data[node_id].external_ip

    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        return self.node_data[node_id].internal_ip

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.

        Optionally returns a mapping from created node ids to node metadata.

        Optionally may throw a
        ray.autoscaler.node_launch_exception.NodeLaunchException which the
        autoscaler may use to provide additional functionality such as
        observability.

        """
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
        pass
