import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, override

from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.core.generated.instance_manager_pb2 import InstanceType

logger = logging.getLogger(__name__)


class NodeProvider(metaclass=ABCMeta):
    """NodeProvider defines the interface for
    interacting with cloud provider, such as AWS, GCP, Azure, etc.
    """

    @abstractmethod
    def create_nodes(self, instance_type: InstanceType, count: int):
        pass

    @abstractmethod
    def terminate_nodes(self, instance_ids: List[str]):
        pass

    @abstractmethod
    def get_nodes(self, instance_ids: List[str]):
        pass

    @abstractmethod
    def is_readonly(self) -> bool:
        return False


class NodeTypeRegistry(metaclass=ABCMeta):
    @abstractmethod
    def get_node_config(self, instance_type: InstanceType) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_resources(self, instance_type: InstanceType) -> Dict[str, float]:
        pass


class NodeProviderAdapter(NodeProvider):
    def __init__(
        self, provider: NodeProviderV1, node_type_registry: NodeTypeRegistry
    ) -> None:
        super().__init__()
        self._provider = provider
        self._registry = node_type_registry

    def _get_tags(self, instance_type: InstanceType):
        # TODO
        return None

    @override
    def create_nodes(self, instance_type: InstanceType, count: int):
        self._provider.create_node_with_resources(
            node_config=self._registry.get_node_config(instance_type),
            tags=self._get_tags(instance_type),
            count=count,
            resources=self._registry.get_resources(instance_type),
        )

    @override
    def terminate_nodes(self, instance_ids: List[str]) -> None:
        self._provider.terminate_node(instance_ids)

    @override
    def get_nodes(self, instance_ids: List[str]):
        pass

    @override
    def is_readonly(self) -> bool:
        return self._provider.is_readonly()
