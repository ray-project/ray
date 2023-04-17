import copy
import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Set, override

from ray.autoscaler._private import BaseNodeLauncher
from ray.autoscaler._private.updater import NodeUpdater
from ray.autoscaler._private.util import hash_runtime_conf, with_head_node_ip
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import (  # TAG_RAY_FILE_MOUNTS_CONTENTS,  -> this is set after startup
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


class NodeConfigProvider(object):
    def __init__(self, node_configs: Dict[str, Any]) -> None:
        self._sync_continuously = False
        self._node_configs = node_configs
        self._calculate_hashes()
        self._sync_continuously = self._node_configs.get(
            "generate_file_mounts_contents_hash", True
        )

    def _calculate_hashes(self) -> None:
        self._runtime_hash, self._file_mounts_contents_hash = hash_runtime_conf(
            self._node_configs["file_mounts"],
            self._node_configs["cluster_synced_files"],
            [
                self._node_configs["worker_setup_commands"],
                self._node_configs["worker_start_ray_commands"],
            ],
            generate_file_mounts_contents_hash=self._node_configs.get(
                "generate_file_mounts_contents_hash", True
            ),
        )

    def get_node_config(self, instance_type_name: str) -> Dict[str, Any]:
        return self._node_configs["available_node_types"][instance_type_name][
            "node_config"
        ]

    def get_docker_config(self, instance_type_name: str) -> Dict[str, Any]:
        if "docker" not in self._node_configs:
            return {}
        docker_config = copy.deepcopy(self._node_configs.get("docker", {}))
        node_specific_docker_config = self._node_configs["available_node_types"][
            instance_type_name
        ].get("docker", {})
        docker_config.update(node_specific_docker_config)
        return docker_config

    def get_worker_start_ray_commands(self, instance: Instance) -> List[str]:
        if (
            instance.num_successful_updates > 0
            and not self._node_config_provider.restart_only
        ):
            return []
        return self._node_configs["worker_start_ray_commands"]

    def get_worker_setup_commands(self, instance: Instance) -> List[str]:
        if (
            instance.num_successful_updates > 0
            and self._node_config_provider.restart_only
        ):
            return []

        return self._node_configs["available_node_types"][instance.name][
            "worker_setup_commands"
        ]

    def get_node_type_specific_config(
        self, instance_type_name: str, config_name: str
    ) -> Any:
        config = self._node_config_provider.get_config(config_name)
        node_specific_config = self._node_configs["available_node_types"][
            instance_type_name
        ]
        if config_name in node_specific_config:
            config = node_specific_config[config_name]
        return config

    def get_config(self, config_name, default=None) -> Any:
        return self._node_configs.get(config_name, default)

    @property
    def restart_only(self) -> bool:
        return self._node_configs.get("restart_only", False)

    @property
    def no_restart(self) -> bool:
        return self._node_configs.get("no_restart", False)

    @property
    def runtime_hash(self) -> str:
        return self._runtime_hash

    @property
    def file_mounts_contents_hash(self) -> str:
        return self._file_mounts_contents_hash


class NodeProviderAdapter(NodeProvider):
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


class RayInstaller(object):
    """
    RayInstaller is responsible for installing ray on the target instance.
    """

    def __init__(
        self,
        provider: NodeProviderV1,
        node_config_provider: NodeConfigProvider,
    ) -> None:
        self._provider = provider
        self._node_config_provider = node_config_provider

    def install_ray(self, instance: Instance, head_node_ip: str) -> bool:
        setup_commands = self._node_config_provider.get_worker_setup_commands(instance)
        ray_start_commands = self._node_config_provider.get_worker_start_ray_commands(
            instance
        )
        docker_config = self._node_config_provider.get_docker_config(instance)

        logger.info(
            f"Creating new (spawn_updater) updater thread for node"
            f" {instance.cloud_instance_id}."
        )
        updater = NodeUpdater(
            node_id=instance.instance_id,
            provider_config=self._node_config_provider.get_config("provider"),
            provider=self._provider,
            auth_config=self._node_config_provider.get_config("auth"),
            cluster_name=self._node_config_provider.get_config("cluster_name"),
            file_mounts=self._node_config_provider.get_config("file_mounts"),
            initialization_commands=with_head_node_ip(
                self.get_node_type_specific_config(
                    instance.instance_id, "initialization_commands"
                ),
                head_node_ip,
            ),
            setup_commands=with_head_node_ip(setup_commands, head_node_ip),
            ray_start_commands=with_head_node_ip(ray_start_commands, head_node_ip),
            runtime_hash=self._node_config_provider.runtime_hash,
            file_mounts_contents_hash=self._node_config_provider.file_mounts_contents_hash,
            is_head_node=False,
            cluster_synced_files=self._node_config_provider.get_config(
                "cluster_synced_files"
            ),
            rsync_options={
                "rsync_exclude": self._node_config_provider.get_config("rsync_exclude"),
                "rsync_filter": self._node_config_provider.get_config("rsync_filter"),
            },
            use_internal_ip=True,
            docker_config=docker_config,
            node_resources=instance.node_resources,
        )
        updater.run()
