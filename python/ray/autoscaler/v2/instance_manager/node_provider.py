from collections import defaultdict
from dataclasses import dataclass
import logging
from abc import ABCMeta, abstractmethod
import time
from typing import Dict, List, Optional, Set
from ray.autoscaler.v2.schema import CloudInstanceId
from ray.autoscaler._private.constants import (
    DISABLE_NODE_UPDATERS_KEY,
)

from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import (
    TAG_RAY_NODE_INSTANCE_ID,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.core.generated.instance_manager_pb2 import Instance


logger = logging.getLogger(__name__)


# TODO: should we make the cloud instance id into a strongly type?
# so that it could be easily differentiate from instance id.
@dataclass
class CloudProviderNode:
    cloud_instance_id: str
    internal_ip: str
    external_ip: str
    instance_type: str
    node_tags: Dict[str, str]


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
    ) -> List[CloudInstanceId]:
        """Get all non-terminated nodes in the cluster"""
        pass

    @abstractmethod
    def is_readonly(self) -> bool:
        return False

    @abstractmethod
    def disable_ray_installer(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_node_info_by_id(self, cloud_instance_id: str) -> CloudProviderNode:
        """Get a node by its cloud instance id"""
        pass


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

    def create_nodes(self, instance_type_name: str, count: int) -> List[Instance]:
        logger.info(
            f"create_nodes: creating {count} nodes of type {instance_type_name}"
        )
        try:
            self._node_launcher.pending.inc(instance_type_name, count)
            self._node_launcher.launch_node(
                self._config.get_raw_config_mutable(),
                count,
                instance_type_name,
            )
        except Exception as e:
            # TODO: error handling
            logger.error(f"create_nodes: failed to launch nodes: {e}")
            raise e

    def terminate_node(self, cloud_instance_id: str) -> None:
        self._provider.terminate_node(cloud_instance_id)

    def is_readonly(self) -> bool:
        return self._provider.is_readonly()

    def get_non_terminated_nodes(self) -> Set[CloudInstanceId]:
        return self._provider.non_terminated_nodes({})

    def get_node_info_by_id(self, cloud_instance_id: str) -> CloudProviderNode:
        return CloudProviderNode(
            cloud_instance_id=cloud_instance_id,
            internal_ip=self._provider.internal_ip(cloud_instance_id),
            external_ip=self._provider.external_ip(cloud_instance_id),
            instance_type=self._provider.node_tags(cloud_instance_id)[
                TAG_RAY_USER_NODE_TYPE
            ],
            node_tags=self._provider.node_tags(cloud_instance_id),
        )

    def disable_ray_installer(self) -> bool:
        return self._config.provider_config.get(DISABLE_NODE_UPDATERS_KEY, False)
