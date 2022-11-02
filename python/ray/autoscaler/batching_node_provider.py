import logging
from collections import dataclass, defaultdict
from typing import Any, Dict, List, Optional

from ray.autoscaler._private.constants import (
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
)
from ray.autoscaler._private.util import NodeID, NodeIP, NodeKind, NodeType
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
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


@dataclass
class NodeData:
    """Stores all data about a Ray node needed by the autoscaler.

    Attributes:
        kind: Whether the node is the head or a worker.
        type: The user-defined type of the node.
        ip: Cluster-internal ip of the node. ip can be None if the ip
            has not yet been assigned.
        status: The status of the node. You must adhere to the following semantics
            for status:
            * The status must be "up-to-date" if and only if the node is running.
            * The status must be "update-failed" if and only if the node is in an
                unknown or failed state.
            * If the node is in a pending (starting-up) state, the status should be
                a brief user-facing description of why the node is pending.
    """

    kind: NodeKind
    type: NodeType
    ip: Optional[NodeIP]
    status: NodeStatus


class BatchingNodeProvider(NodeProvider):
    """Abstract subclass of NodeProvider meant for use with external cluster managers.

    Batches reads of cluster state into a single method, get_node_data, called at the
    start of an autoscaling update.

    Batches modifications to cluster state into a single method, submit_scale_request,
    called at the end of an autoscaling update.

    Implementing a concrete subclass of BatchingNodeProvider only requires overriding
    get_node_data() and submit_scale_request().

    The scale request for an autoscaling update may be conditionally
    cancelled using the optional method safe_to_scale().

    See the method docstrings for more information.
    """

    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ) -> None:
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.node_data_dict: Dict[NodeID, NodeData] = {}

        # Disallow multiple node providers, unless explicitly allowed for testing.
        global provider_exists
        if not _allow_multiple:
            assert (
                not provider_exists
            ), "Only one BatchingNodeProvider allowed per process."

        # These flags enforce correct behavior for single-threaded node providers
        # which interact with external cluster managers:
        assert (
            provider_config.get(DISABLE_NODE_UPDATERS_KEY, False) is True
        ), f"To use BatchingNodeProvider, must set `{DISABLE_NODE_UPDATERS_KEY}:True`."
        assert provider_config.get(DISABLE_LAUNCH_CONFIG_CHECK_KEY, False) is True, (
            "To use BatchingNodeProvider, must set "
            f"`{DISABLE_LAUNCH_CONFIG_CHECK_KEY}:True`."
        )
        assert (
            provider_config.get(FOREGROUND_NODE_LAUNCH_KEY, False) is True
        ), f"To use BatchingNodeProvider, must set `{FOREGROUND_NODE_LAUNCH_KEY}:True`."

        # self.scale_change_needed tracks whether we need to update scale.
        # set to True in create_node and terminate_nodes calls
        # reset to False in non_terminated_nodes, which occurs at the start of the
        #   autoscaling update. For good measure, also set to false in post_process.
        self.scale_change_needed = False

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
        """Submit a scale request if it is safe to do so."""
        if self.scale_change_needed and self.safe_to_scale():
            self.submit_scale_request(self.scale_request)
        self.scale_change_needed = False

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        self.scale_change_needed = False
        self.node_data_dict = self.get_node_data()

        # Initialize ScaleRequest
        self.scale_request = ScaleRequest(
            desired_num_workers=self.cur_num_workers,  # Current scale
            workers_to_delete=[],  # No workers to delete yet
        )
        return list(self.node_data_dict.keys())

    @property
    def cur_num_workers(self, node_data_dict: Dict[str, Any]):
        num_workers_dict = defaultdict(int)
        for node_data in node_data_dict.values():
            if node_data.kind == NodeKind.HEAD:
                # Only track workers.
                continue
            num_workers_dict[node_data.type] += 1
        return num_workers_dict

    def node_tags(self, node_id: str) -> Dict[str, str]:
        node_data = self.node_data_dict[node_id]
        return {
            TAG_RAY_NODE_KIND: node_data.kind,
            TAG_RAY_NODE_STATUS: node_data.status,
            TAG_RAY_USER_NODE_TYPE: node_data.type,
        }

    def internal_ip(self, node_id: str) -> str:
        return self.node_data_dict[node_id].ip

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        self.scale_request.desired_num_workers[node_type] += count
        self.scale_change_needed = True

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """
        node_type = self.node_tags(node_id)[TAG_RAY_USER_NODE_TYPE]
        self.scale_request.desired_num_workers[node_type] -= 1
        if self.scale_request.desired_num_workers[node_type] < 0:
            raise ValueError(
                "NodeProvider attempted to request less than 0 workers of type "
                f"{node_type}."
            )
        self.scale_request.workers_to_terminate.append(node_id)
        self.scale_change_needed = True
