import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.schema import NodeType

logger = logging.getLogger(__name__)

# Type Alias. This is a **unique identifier** for a cloud node in the cluster.
# The node provider should guarantee that this id is unique across the cluster,
# such that:
#   - When a cloud node is created and running, no other cloud node in the
#     cluster has the same id.
#   - When a cloud node is terminated, no other cloud node in the cluster should
#     be assigned the same id later.
CloudInstanceId = str


@dataclass
class CloudInstance:
    # The cloud instance id.
    cloud_instance_id: CloudInstanceId
    # The node type of the cloud instance.
    node_type: NodeType
    # The ip address of the cloud instance.
    tags: Dict[str, str] = field(default_factory=dict)
    # Internal ip of the cloud instance.
    internal_ip: str = ""
    # External ip of the cloud instance.
    external_ip: str = ""


@dataclass
class UpdateCloudNodeProviderRequest:
    # Nodes to launch.
    to_launch: Dict[NodeType, int] = field(default_factory=dict)
    # Nodes to terminate.
    to_terminate: List[CloudInstanceId] = field(default_factory=list)


@dataclass
class LaunchFailureCause:
    # Launch failure exception.
    exception: Optional[Exception] = None
    # details
    details: Optional[str] = None


@dataclass
class UpdateCloudNodeProviderReply:
    # Nodes that are launching.
    # When a node is included in this list, it means that the node is allocated and
    # launched successfully, and a call to get_allocated_nodes()
    # should include this node if it wasn't terminated/crashed.
    # For a node type key, the value is:
    #   - A list of cloud node ids, where the nodes are successfully launched.
    #   - An empty list:
    #       - if no nodes could be launched, in this case, the
    #       launch failures should include failure info.
    #       - if node provider launches nodes asynchronously, no cloud ids are
    #       available.
    launching: Dict[NodeType, List[CloudInstanceId]] = field(default_factory=dict)

    # Nodes launch failures grouped by each node type.
    launch_failures: Dict[NodeType, LaunchFailureCause] = field(default_factory=dict)

    # Nodes that are terminating.
    # When a node is included in this list, it means that the node is terminated
    # successfully, and a call to get_allocated_nodes() should not include
    # this node anymore.
    terminating: List[CloudInstanceId] = field(default_factory=list)


class ICloudNodeProvider(ABC):
    @abstractmethod
    def running_nodes(
        self,
    ) -> List[CloudInstance]:
        """Get cloud nodes in the cluster that are running.

        Running cloud nodes are nodes that are launched and ready to have ray
        processes installed and initialized.

        The API is expected to be eventually consistent with the update() API,
        where a call to update() may not immediately reflect in the result of
        this API.

        Returns:
            List of cloud instances, uniquely identifying the cloud nodes in the
            cluster.
        """
        pass

    @abstractmethod
    def update(
        self, request: UpdateCloudNodeProviderRequest
    ) -> UpdateCloudNodeProviderReply:
        """Update the cluster.

        This API is expected to be eventually consistent with the running_nodes().

        Args:
            request: the request to update the cluster.

        Returns:
            reply to the update request.
        """
        pass


class NodeProviderAdapter(ICloudNodeProvider):
    """
    Warps a NodeProviderV1 to a NodeProvider.

    TODO(rickyx):
    The current adapter right now consists of two sets of APIs:
    - v1: the old APIs that are used by the autoscaler, where
    we forward the calls to the NodeProviderV1.
    - v2: the new APIs that are used by the autoscaler v2, this is
    defined in the ICloudNodeProvider interface.

    We should eventually remove the v1 APIs and only use the v2 APIs.
    It's currently left as a TODO since changing the v1 APIs would
    requires a lot of changes in the cluster launcher codebase.
    """

    def __init__(
        self,
        provider: NodeProviderV1,
        node_launcher: BaseNodeLauncher,
        instance_config_provider: NodeProviderConfig,
    ) -> None:
        super().__init__()
        self._provider = provider
        self._node_launcher = node_launcher
        self._config = instance_config_provider

    def running_nodes(self) -> List[CloudInstance]:
        """
        Get cloud nodes in the cluster that are running.

        Overrides ICloudNodeProvider.running_nodes()
        """
        # TODO: make node provider returns running nodes directly
        # instead of pending/terminating nodes.
        cloud_instance_ids = self._v1_non_terminated_nodes({})
        # Filter out nodes that are not running.
        # This is efficient since the provider is expected to cache the
        # running status of the nodes.
        cloud_instances = []
        for cloud_instance_id in cloud_instance_ids:
            if not self._v1_is_running(cloud_instance_id):
                continue

            node_tags = self._v1_node_tags(cloud_instance_id)
            cloud_instances.append(
                CloudInstance(
                    cloud_instance_id=cloud_instance_id,
                    node_type=node_tags.get(TAG_RAY_USER_NODE_TYPE, None),
                    tags=node_tags,
                    internal_ip=self._v1_internal_ip(cloud_instance_id),
                    external_ip=self._v1_external_ip(cloud_instance_id),
                )
            )
        return cloud_instances

    def update(
        self, request: UpdateCloudNodeProviderRequest
    ) -> UpdateCloudNodeProviderReply:
        """
        Update the cluster with new nodes/terminating nodes.

        Overrides ICloudNodeProvider.update()
        """
        reply = UpdateCloudNodeProviderReply()

        # Launch nodes
        self._launch_nodes(request, reply)

        # Terminated nodes.
        self._terminate_nodes(request, reply)

        # Finalize the updates for async node providers.
        self._v1_post_process()

        return reply

    ###########################################
    # Private APIs
    ###########################################

    def _launch_nodes(
        self, req: UpdateCloudNodeProviderRequest, reply: UpdateCloudNodeProviderReply
    ) -> None:
        """
        Launch nodes.

        Args:
            req: the request to launch nodes.
            reply: the reply to the request. It will be populated with the launched
            nodes and launch failures.
        """

        # Default to synchronous update.
        for node_type, count in req.to_launch.items():
            try:
                created_nodes = self._node_launcher.launch_node(
                    self._config.get_raw_config_mutable(),
                    count,
                    node_type,
                    raise_exception=True,
                )
                reply.launching[node_type] = (
                    list(created_nodes.keys()) if created_nodes else []
                )
            except Exception as e:
                # TODO(rickyx):
                # This currently assumes that the node launcher will always
                # launch all nodes successfully or none of them.
                # We should eventually make the node launcher return
                # the nodes that are successfully launched when errors happen.
                logger.error(f"Failed to launch {count} nodes of {node_type}: {e}")
                reply.launch_failures[node_type] = LaunchFailureCause(exception=e)

    def _terminate_nodes(
        self, req: UpdateCloudNodeProviderRequest, reply: UpdateCloudNodeProviderReply
    ) -> None:
        """
        Terminate nodes.

        Args:
            req: the request to terminate nodes.
            reply: the reply to the request. It will be populated with the terminating
            nodes.
        """
        for cloud_instance_id in req.to_terminate:
            try:
                self._v1_terminate_node(cloud_instance_id)
                # We disregard the result here since when:
                # 1. a result is returned to indicate successful termination request
                # 2. a result is not returned since the node provider async terminates
                # node.
                # Both of these cases would indicate a successful termination request
                reply.terminating.append(cloud_instance_id)
            except Exception as e:
                logger.error(f"Failed to terminate node {cloud_instance_id}: {e}")

    ###########################################
    # V1 Legacy APIs for backward compatibility
    ###########################################
    """
    Below are the necessary legacy APIs from the V1 node provider.
    These are needed as of now to provide the needed features
    for V2 node provider.
    The goal is to eventually remove these APIs and only use the
    V2 APIs by modifying the individual node provider to inherit
    from ICloudNodeProvider.
    """

    def _v1_terminate_node(self, node_id: CloudInstanceId) -> Optional[Dict[str, Any]]:
        return self._provider.terminate_node(node_id)

    def _v1_non_terminated_nodes(
        self, tag_filters: Dict[str, str]
    ) -> List[CloudInstanceId]:
        return self._provider.non_terminated_nodes(tag_filters)

    def _v1_is_running(self, node_id: CloudInstanceId) -> bool:
        return self._provider.is_running(node_id)

    def _v1_post_process(self) -> None:
        self._provider.post_process()

    def _v1_node_tags(self, node_id: CloudInstanceId) -> Dict[str, str]:
        return self._provider.node_tags(node_id)

    def _v1_internal_ip(self, node_id: CloudInstanceId) -> str:
        return self._provider.internal_ip(node_id)

    def _v1_external_ip(self, node_id: CloudInstanceId) -> str:
        return self._provider.external_ip(node_id)
