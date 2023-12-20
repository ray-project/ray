import logging
from abc import ABC, ABCMeta, abstractmethod
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field

from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.instance_manager_pb2 import Instance

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
    # Update request id.
    id: str 
    # Target cluster shape (number of running nodes by type).
    target_running_nodes: Dict[NodeType, int] = field(default_factory=dict)
    # Nodes to terminate.
    to_terminate: List[CloudInstanceId] = field(default_factory=list)


@dataclass
class GetNodeProviderStateRequest:
    # Errors that have happened since the given timestamp in nanoseconds.
    errors_since_ns: int

@dataclass
class CloudNodeProviderError:
    """
    An error class that represents an error that happened in the cloud node provider.
    """
    # The exception that caused the error.
    exception: Optional[Exception] = None
    # The details of the error.
    details: Optional[str] = None
    # The timestamp of the error in nanoseconds.
    timestamp_ns: int

@dataclass
class LaunchNodeError(CloudNodeProviderError):
    # The node type that failed to launch.
    node_type: NodeType
    # Number of nodes that failed to launch.
    count:int
    # From which update request the error originates.
    update_id: str

@dataclass
class TerminateNodeError(CloudNodeProviderError):
    # The cloud instance id of the node that failed to terminate.
    cloud_instance_id: CloudInstanceId
    # From which update request the error originates.
    update_id: str

@dataclass
class CloudNodeProviderState:
    """
    The state of a cloud node provider.
    """

    # The terminated cloud nodes in the cluster. 
    terminated: List[CloudInstanceId] = field(default_factory=list)
    # The cloud nodes that are currently running.
    running: Dict[CloudInstanceId, CloudInstance] = field(default_factory=dict)
    # Errors that have happened when launching nodes.
    launch_errors: List[LaunchNodeError] = field(default_factory=list)
    # Errors that have happened when terminating nodes.
    termination_errors: List[TerminateNodeError] = field(default_factory=list)


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
    the node provider may not immediately reflect the change in its state.

    2. Asynchronous
    The node provider could also be asynchronous, where the termination/launch
    request may not immediately return the result of the request.

    3. Unique cloud node ids
    Cloud node ids are expected to be unique across the cluster.

    """

    @abstractmethod
    def get_state(self) -> CloudNodeProviderState:
        """Get the current state of the cloud node provider.

        Returns:
            The current state of the cloud node provider.
        """
        pass

    @abstractmethod
    def update(
        self, request: UpdateCloudNodeProviderRequest
    ) -> None:
        """Update the cloud node provider state by launching 
         or terminating cloud nodes.

        Args:
            request: the request to update the cluster.

        Returns:
            reply to the update request.
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


class NodeProviderAdapter(NodeProvider):
    """
    Warps a NodeProviderV1 to a NodeProvider.
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
            instance.status = Instance.STOPPED
        else:
            instance.status = Instance.UNKNOWN
        instance.internal_ip = self._provider.internal_ip(cloud_instance_id)
        instance.external_ip = self._provider.external_ip(cloud_instance_id)
        instance.instance_type = self._provider.node_tags(cloud_instance_id)[
            TAG_RAY_USER_NODE_TYPE
        ]
        return instance
