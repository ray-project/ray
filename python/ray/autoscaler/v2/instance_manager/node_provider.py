import logging
from abc import ABC, abstractmethod
from collections import defaultdict
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


@dataclass
class UpdateCloudNodeProviderRequest:
    # Nodes to launch.
    to_launch: Dict[NodeType, int] = field(default_factory=dict)
    # Nodes to terminate.
    to_terminate: List[CloudInstanceId] = field(default_factory=list)


@dataclass
class UpdateCloudNodeProviderException:
    # Launch failure exception.
    exception: Optional[Exception] = None
    # details
    details: Optional[str] = None


@dataclass
class UpdateCloudNodeProviderReply:
    # Number of nodes that are launching.
    # We are not providing more information than the count because
    # some node providers will not return and  will not be able to
    # return the cloud instance id of the launching nodes.
    # A successfully launched node should be discovered by the
    # get_running_nodes() API.
    num_launching: Dict[NodeType, int] = field(default_factory=lambda: defaultdict(int))

    # Nodes launch failures grouped by each node type.
    launch_failures: Dict[NodeType, UpdateCloudNodeProviderException] = field(
        default_factory=dict
    )

    # Nodes that are terminating.
    # When a node is included in this list, it means that the node is being
    # terminated. When node provider eventually terminate the node
    # and a call to get_running_nodes() should not include
    # this node anymore.
    # A successfully terminated node should be discovered by being absent from
    # get_running_nodes() API.
    terminating: List[CloudInstanceId] = field(default_factory=list)

    # Nodes that failed to terminate and the reason.
    terminate_failures: Dict[CloudInstanceId, UpdateCloudNodeProviderException] = field(
        default_factory=dict
    )


class ICloudNodeProvider(ABC):
    """
    The interface for a cloud node provider.

    This interface is a minimal interface that should be implemented by the
    various cloud node providers (e.g. AWS, and etc).

    The cloud node provider is responsible for managing the cloud nodes in the
    cluster. It provides the following main functionalities:
        - Launch new cloud nodes.
        - Terminate cloud nodes.
        - Get the running cloud nodes in the cluster.

    Below properties of the cloud node provider are assumed with this interface:

    1. Eventually consistent
    The cloud node provider is expected to be eventually consistent with the
    cluster state. For example, when a node is request to be terminated/launched,
    the node  provider may not immediately reflect the change in the result
    of get_running_nodes().

    2. Asynchronous
    The node provider could also be asynchronous, where the termination/launch
    request may not immediately return the result of the request.
    # TODO(rickyx): we might able to have shortcuts for cloud node providers
    impl that synchronously return results.

    3. Unique cloud node ids
    Cloud node ids are expected to be unique across the cluster.

    """

    @abstractmethod
    def get_running_nodes(
        self,
    ) -> List[CloudInstance]:
        """Get cloud nodes in the cluster that are running.

        Running cloud nodes are nodes that are launched and ready to have ray
        processes be installed and initialized.

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

        Args:
            request: the request to update the cluster.

        Returns:
            reply to the update request.
        """
        pass


class NodeProviderAdapter(ICloudNodeProvider):
    """
    Warps a NodeProviderV1 to a ICloudNodeProvider.

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

    def get_running_nodes(self) -> List[CloudInstance]:
        """
        Get cloud nodes in the cluster that are running.

        Overrides ICloudNodeProvider.get_running_nodes()
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
                self._node_launcher.launch_node(
                    self._config.get_raw_config_mutable(),
                    count,
                    node_type,
                    raise_exception=True,
                )
                reply.num_launching[node_type] += count
            except Exception as e:
                # TODO(rickyx):
                # This currently assumes that the node launcher will always
                # launch all nodes successfully or none of them.
                # We should eventually make the node launcher return
                # the nodes that are successfully launched when errors happen.
                logger.error(f"Failed to launch {count} nodes of {node_type}: {e}")
                reply.num_launching[node_type] = 0
                reply.launch_failures[node_type] = UpdateCloudNodeProviderException(
                    exception=e
                )

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
                reply.terminate_failures[
                    cloud_instance_id
                ] = UpdateCloudNodeProviderException(exception=e)

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
