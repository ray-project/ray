import logging
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional, Set, override

from ray.autoscaler._private import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.v2.instance_manager.node_config_provider import NodeConfigProvider
from ray.core.generated.instance_manager_pb2 import Instance, InstanceType

logger = logging.getLogger(__name__)


class NodeProvider(metaclass=ABCMeta):
    """NodeProvider defines the interface for
    interacting with cloud provider, such as AWS, GCP, Azure, etc.
    """

    @abstractmethod
    def create_nodes(
        self, instance_type: InstanceType, count: int
    ) -> Dict[str, Instance]:
        """Create new nodes synchronously, returns all non-terminated nodes in the cluster.
        Note that create_nodes could fail partially.
        """
        pass

    @abstractmethod
    def async_terminate_nodes(self, instance_ids: List[str]) -> None:
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
    def get_nodes_by_id(
        self,
        node_ids: List[str],
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
        node_config_provider: NodeConfigProvider,
    ) -> None:
        super().__init__()
        self._provider = provider
        self._node_launcher = node_launcher
        self._registry = node_config_provider

    def _filter_instances(self, instances, instance_ids_filter, instance_states_filter):
        filtered = {}
        for instance_id, instance in instances.items():
            if instance_ids_filter and instance_id not in instance_ids_filter:
                continue
            if instance_states_filter and instance.state not in instance_states_filter:
                continue
            filtered[instance_id] = instance
        return filtered

    @override
    def create_nodes(self, instance_type: InstanceType, count: int):
        self._node_launcher.launch_node(
            self._registry.get_node_config(instance_type.name),
            count,
            instance_type.name,
        )
        return self._get_none_terminated_instances()

    @override
    def async_terminate_nodes(self, instance_ids: List[str]) -> None:
        self._provider.terminate_node(instance_ids)

    @override
    def is_readonly(self) -> bool:
        return self._provider.is_readonly()

    @override
    def get_non_terminated_nodes(self):
        instance_ids = self._provider.non_terminated_nodes()
        return self.get_nodes_by_id(instance_ids)

    @abstractmethod
    def get_nodes_by_id(
        self,
        instance_ids: List[str],
    ) -> Dict[str, Instance]:
        instances = {}
        for instance_id in instance_ids:
            instances[instance_id] = self._get_instance(instance_id)
        return instances

    def _get_instance(self, instance_id: str):
        instance = Instance()
        instance.cloud_instance_id = instance_id
        if self._provider.is_running(instance_id):
            instance.state = Instance.RESOURCES_ALLOCATED
        elif self._provider.is_terminated(instance_id):
            instance.state = Instance.DEAD
        else:
            instance.state = Instance.REQUESTED
        instance.interal_ip = self._provider.internal_ip(instance_id)
        instance.external_ip = self._provider.internal_ip(instance_id)
        instance.instance_type = self._provider.node_tags(instance_id)[
            TAG_RAY_USER_NODE_TYPE
        ]
        return instance
