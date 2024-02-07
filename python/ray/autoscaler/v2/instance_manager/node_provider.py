import logging
from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


# Type Alias. This is a **unique identifier** for a cloud instance in the cluster.
# The provider should guarantee that this id is unique across the cluster,
# such that:
#   - When a cloud instance is created and running, no other cloud instance in the
#     cluster has the same id.
#   - When a cloud instance is terminated, no other cloud instance in the cluster will
#     be assigned the same id later.
CloudInstanceId = str


@dataclass
class CloudInstance:
    """
    A class that represents a cloud instance in the cluster, with necessary metadata
    of the cloud instance.
    """

    # The cloud instance id.
    cloud_instance_id: CloudInstanceId
    # The node type of the cloud instance.
    node_type: NodeType
    # Update request id from which the cloud instance is launched.
    request_id: str
    # If the cloud instance is running.
    is_running: bool


@dataclass
class CloudInstanceProviderError:
    """
    An base error class that represents an error that happened in the cloud instance
    provider.
    """

    # The exception that caused the error.
    exception: Optional[Exception]
    # The details of the error.
    details: Optional[str]
    # The timestamp of the error occurred in nanoseconds.
    timestamp_ns: int


@dataclass
class LaunchNodeError(CloudInstanceProviderError):
    # The node type that failed to launch.
    node_type: NodeType
    # Number of nodes that failed to launch.
    count: int
    # A unique id that identifies from which update request the error originates.
    request_id: str


@dataclass
class TerminateNodeError(CloudInstanceProviderError):
    # The cloud instance id of the node that failed to terminate.
    cloud_instance_id: CloudInstanceId
    # From which update request the error originates.
    request_id: str


class ICloudInstanceProvider(ABC):
    """
    The interface for a cloud instance provider.

    This interface is a minimal interface that should be implemented by the
    various cloud instance providers (e.g. AWS, and etc).

    The cloud instance provider is responsible for managing the cloud instances in the
    cluster. It provides the following main functionalities:
        - Launch new cloud instances.
        - Terminate existing running instances.
        - Get the non-terminated cloud instances in the cluster.
        - Poll the errors that happened for the updates to the cloud instance provider.

    Below properties of the cloud instance provider are assumed with this interface:

    1. Eventually consistent
    The cloud instance provider is expected to be eventually consistent with the
    cluster state. For example, when a cloud instance is request to be terminated
    or launched, the provider may not immediately reflect the change in its state.
    However, the provider is expected to eventually reflect the change in its state.

    2. Asynchronous
    The provider could also be asynchronous, where the termination/launch
    request may not immediately return the result of the request.

    3. Unique cloud instance ids
    Cloud instance ids are expected to be unique across the cluster.

    4. Idempotent updates
    For the update APIs (e.g. ensure_min_nodes, terminate), the provider may use the
    request ids to provide idempotency.

    Usage:
        ```
            provider: ICloudInstanceProvider = ...

            # Update the cluster with a desired shape.
            provider.launch(
                shape={
                    "worker_nodes": 10,
                    "ray_head": 1,
                },
                request_id="1",
            )

            # Get the non-terminated nodes of the cloud instance provider.
            running = provider.get_non_terminated()

            # Poll the errors
            errors = provider.poll_errors()

            # Terminate nodes.
            provider.terminate(
                ids=["cloud_instance_id_1", "cloud_instance_id_2"],
                request_id="2",
            )

            # Process the state of the provider.
            ...
        ```
    """

    @abstractmethod
    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        """Get the non-terminated cloud instances in the cluster.

        Returns:
            A dictionary of the non-terminated cloud instances in the cluster.
            The key is the cloud instance id, and the value is the cloud instance.
        """
        pass

    @abstractmethod
    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        """
        Terminate the cloud instances asynchronously.

        Args:
            ids: the cloud instance ids to terminate.
            request_id: a unique id that identifies the request.
        """
        pass

    @abstractmethod
    def launch(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
    ) -> None:
        """Launch the cloud instances asynchronously.

        Args:
            shape: A map from node type to number of nodes to launch.
            request_id: a unique id that identifies the update request.
        """
        pass

    @abstractmethod
    def poll_errors(self) -> List[CloudInstanceProviderError]:
        """
        Poll the errors that happened since the last poll.

        This method would also clear the errors that happened since the last poll.

        Returns:
            The errors that happened since the last poll.
        """
        pass


class NodeProvider(metaclass=ABCMeta):
    """NodeProvider defines the interface for
    interacting with cloud provider, such as AWS, GCP, Azure, etc.
    """

    @abstractmethod
    def create_nodes(self, instance_type_name: str, count: int) -> List[str]:
        """Create new nodes synchronously, returns all non-terminated nodes in the cluster.
        Note that create_nodes could fail partially.
        """
        pass

    @abstractmethod
    def terminate_node(self, cloud_instance_id: str) -> None:
        """
        Terminate nodes asynchronously, returns immediately."""
        pass

    @abstractmethod
    def get_non_terminated_nodes(
        self,
    ) -> Dict[str, Instance]:
        """Get all non-terminated nodes in the cluster"""
        pass

    @abstractmethod
    def get_nodes_by_cloud_instance_id(
        self,
        cloud_instance_ids: List[str],
    ) -> Dict[str, Instance]:
        """Get nodes by node ids, including terminated nodes"""
        pass

    @abstractmethod
    def is_readonly(self) -> bool:
        return False


# TODO:
#  We will make the NodeProviderAdaptor inherits from ICloudInstanceProvider
# to make V1 providers work with the new interface.
class NodeProviderAdapter(NodeProvider):
    """
    Warps a NodeProviderV1 to a NodeProvider.
    """

    def __init__(
        self,
        provider: NodeProviderV1,
        node_launcher: BaseNodeLauncher,
        autoscaling_config: AutoscalingConfig,
    ) -> None:
        super().__init__()
        self._provider = provider
        self._node_launcher = node_launcher
        self._config = autoscaling_config

    def _filter_instances(
        self,
        instances: Dict[str, Instance],
        instance_ids_filter: Set[str],
        instance_states_filter: Set[int],
    ) -> Dict[str, Instance]:
        filtered = {}
        for instance_id, instance in instances.items():
            if instance_ids_filter and instance_id not in instance_ids_filter:
                continue
            if instance_states_filter and instance.state not in instance_states_filter:
                continue
            filtered[instance_id] = instance
        return filtered

    def create_nodes(self, instance_type_name: str, count: int) -> List[Instance]:
        created_nodes = self._node_launcher.launch_node(
            self._config.get_raw_config_mutable(),
            count,
            instance_type_name,
        )
        # TODO: we should handle failures where the instance type is
        # not available
        if created_nodes:
            return [
                self._get_instance(cloud_instance_id)
                for cloud_instance_id in created_nodes.keys()
            ]
        return []

    def terminate_node(self, clould_instance_id: str) -> None:
        self._provider.terminate_node(clould_instance_id)

    def is_readonly(self) -> bool:
        return self._provider.is_readonly()

    def get_non_terminated_nodes(self):
        clould_instance_ids = self._provider.non_terminated_nodes({})
        return self.get_nodes_by_cloud_instance_id(clould_instance_ids)

    def get_nodes_by_cloud_instance_id(
        self,
        cloud_instance_ids: List[str],
    ) -> Dict[str, Instance]:
        instances = {}
        for cloud_instance_id in cloud_instance_ids:
            instances[cloud_instance_id] = self._get_instance(cloud_instance_id)
        return instances

    def _get_instance(self, cloud_instance_id: str) -> Instance:
        instance = Instance()
        instance.cloud_instance_id = cloud_instance_id
        if self._provider.is_running(cloud_instance_id):
            instance.status = Instance.ALLOCATED
        elif self._provider.is_terminated(cloud_instance_id):
            instance.status = Instance.TERMINATED
        else:
            instance.status = Instance.UNKNOWN
        instance.internal_ip = self._provider.internal_ip(cloud_instance_id)
        instance.external_ip = self._provider.external_ip(cloud_instance_id)
        instance.instance_type = self._provider.node_tags(cloud_instance_id)[
            TAG_RAY_USER_NODE_TYPE
        ]
        return instance
