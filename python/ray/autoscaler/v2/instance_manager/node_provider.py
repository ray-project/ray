import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, override

from ray.autoscaler._private import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import (  # TAG_RAY_FILE_MOUNTS_CONTENTS,  -> this is set after startup
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_RUNTIME_CONFIG,
    TAG_RAY_USER_NODE_TYPE,
)
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
        pass

    @abstractmethod
    def terminate_nodes(self, instance_ids: List[str]):
        pass

    @abstractmethod
    def get_nodes(self, instance_ids: List[str]) -> Dict[str, Instance]:
        pass

    @abstractmethod
    def is_readonly(self) -> bool:
        return False


class NodeConfigRegistry(object):
    def __init__(self, node_configs: Dict[str, Any]) -> None:
        self._node_configs = node_configs

    def get_node_config(self, instance_type: InstanceType) -> Dict[str, Any]:
        return self._node_configs["available_node_types"][instance_type.name][
            "node_config"
        ]


class NodeProviderAdapter(NodeProvider):
    def __init__(
        self,
        provider: NodeProviderV1,
        node_launcher: BaseNodeLauncher,
        node_config_registry: NodeConfigRegistry,
    ) -> None:
        super().__init__()
        self._provider = provider
        self._node_launcher = node_launcher
        self._registry = node_config_registry

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
            self._registry.get_node_config(instance_type), count, instance_type.name
        )
        return self._get_none_terminated_instances()

    @override
    def terminate_nodes(self, instance_ids: List[str]) -> None:
        self._provider.terminate_node(instance_ids)

    @override
    def get_nodes(
        self,
        instance_ids_filter: Optional[Set[str]],
        instance_states_filter: Optional[Set[int]],
    ):
        # TODO: more efficient implementation.
        instances = self._provider.non_terminated_nodes()
        return self._filter_instances(
            instances, instance_ids_filter, instance_states_filter
        )

    @override
    def is_readonly(self) -> bool:
        return self._provider.is_readonly()

    def _get_none_terminated_instances(self):
        instance_ids = self._provider.non_terminated_nodes()
        instances = {}
        for instance_id in instance_ids:
            instances[instance_id] = self._get_instance(instance_id)
        return instances

    def _get_instance(self, node_id: str):
        instance = Instance()
        instance.external_instance_id = node_id
        if self._provider.is_running(node_id):
            instance.state = Instance.ALIVE
        elif self._provider.is_terminated(node_id):
            instance.state = Instance.DEAD
        else:
            instance.state = Instance.REQUESTED
        instance.interal_ip = self._provider.internal_ip(node_id)
        instance.external_ip = self._provider.internal_ip(node_id)
        instance.instance_type = self._provider.node_tags(node_id)[
            TAG_RAY_USER_NODE_TYPE
        ]
        return instance
