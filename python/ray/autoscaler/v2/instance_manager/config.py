import copy
from enum import Enum
from typing import Any, Dict, List

from ray.autoscaler._private.util import hash_runtime_conf, prepare_config


class Provider(Enum):
    UNKNOWN = 0
    ALIYUN = 1
    AWS = 2
    AZURE = 3
    GCP = 4
    KUBERAY = 5
    LOCAL = 6


class NodeProviderConfig(object):
    """
    NodeProviderConfig is the helper class to provide instance
    related configs.
    """

    def __init__(
        self, node_configs: Dict[str, Any], skip_content_hash: bool = False
    ) -> None:
        """
        Args:
            node_configs : The node configs.
            skip_content_hash :
                Whether to skip file mounts/ray command hash calculation.
        """
        self._sync_continuously = False
        self.update_configs(node_configs, skip_content_hash)

    def update_configs(
        self, node_configs: Dict[str, Any], skip_content_hash: bool
    ) -> None:
        self._node_configs = prepare_config(node_configs)
        if skip_content_hash:
            return
        self._calculate_hashes()
        self._sync_continuously = self._node_configs.get(
            "generate_file_mounts_contents_hash", True
        )

    def _calculate_hashes(self) -> None:
        self._runtime_hash, self._file_mounts_contents_hash = hash_runtime_conf(
            self._node_configs.get("file_mounts", {}),
            self._node_configs.get("cluster_synced_files", []),
            [
                self._node_configs.get("worker_setup_commands", []),
                self._node_configs.get("worker_start_ray_commands", []),
            ],
            generate_file_mounts_contents_hash=self._node_configs.get(
                "generate_file_mounts_contents_hash", True
            ),
        )

    def get_node_config(self, instance_type_name: str) -> Dict[str, Any]:
        return copy.deepcopy(
            self._node_configs["available_node_types"][instance_type_name][
                "node_config"
            ]
        )

    def get_docker_config(self, instance_type_name: str) -> Dict[str, Any]:
        docker_config = copy.deepcopy(self._node_configs.get("docker", {}))
        node_specific_docker_config = self._node_configs["available_node_types"][
            instance_type_name
        ].get("docker", {})
        docker_config.update(node_specific_docker_config)
        return docker_config

    def get_worker_start_ray_commands(
        self, num_successful_updates: int = 0
    ) -> List[str]:
        if num_successful_updates > 0 and not self._node_config_provider.restart_only:
            return []
        return self._node_configs.get("worker_start_ray_commands", [])

    def get_head_setup_commands(self) -> List[str]:
        return self._node_configs.get("head_setup_commands", [])

    def get_head_start_ray_commands(self) -> List[str]:
        return self._node_configs.get("head_start_ray_commands", [])

    def get_worker_setup_commands(
        self, instance_type_name: str, num_successful_updates: int = 0
    ) -> List[str]:
        if num_successful_updates > 0 and self._node_config_provider.restart_only:
            return []
        return self.get_node_type_specific_config(
            instance_type_name, "worker_setup_commands"
        )

    def get_node_type_specific_config(
        self, instance_type_name: str, config_name: str
    ) -> Any:
        config = self.get_config(config_name)
        node_specific_config = self._node_configs["available_node_types"].get(
            instance_type_name, {}
        )
        if config_name in node_specific_config:
            config = node_specific_config[config_name]
        return config

    def get_node_resources(self, instance_type_name: str) -> Dict[str, float]:
        return copy.deepcopy(
            self._node_configs["available_node_types"][instance_type_name].get(
                "resources", {}
            )
        )

    def get_node_labels(self, instance_type_name: str) -> Dict[str, str]:
        return copy.deepcopy(
            self._node_configs["available_node_types"][instance_type_name].get(
                "labels", {}
            )
        )

    def get_config(self, config_name, default=None) -> Any:
        return self._node_configs.get(config_name, default)

    def get_raw_config_mutable(self) -> Dict[str, Any]:
        return self._node_configs

    def get_provider_instance_type(self, instance_type_name: str) -> str:
        provider = self.provider
        node_config = self.get_node_type_specific_config(
            instance_type_name, "node_config"
        )
        if provider in [Provider.AWS, Provider.ALIYUN]:
            return node_config.get("InstanceType", "")
        elif provider == Provider.AZURE:
            return node_config.get("azure_arm_parameters", {}).get("vmSize", "")
        elif provider == Provider.GCP:
            return node_config.get("machineType", "")
        elif provider in [Provider.KUBERAY, Provider.LOCAL, Provider.UNKNOWN]:
            return ""
        else:
            raise ValueError(f"Unknown provider {provider}")

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

    @property
    def provider(self) -> Provider:
        provider_str = self._node_configs.get("provider", {}).get("type", "")
        if provider_str == "local":
            return Provider.LOCAL
        elif provider_str == "aws":
            return Provider.AWS
        elif provider_str == "azure":
            return Provider.AZURE
        elif provider_str == "gcp":
            return Provider.GCP
        elif provider_str == "aliyun":
            return Provider.ALIYUN
        elif provider_str == "kuberay":
            return Provider.KUBERAY
        else:
            return Provider.UNKNOWN
