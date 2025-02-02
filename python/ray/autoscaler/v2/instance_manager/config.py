import copy
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from ray._private.ray_constants import env_integer
from ray._private.utils import binary_to_hex
from ray._raylet import GcsClient
from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    DEFAULT_UPSCALING_SPEED,
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
)
from ray.autoscaler._private.kuberay.autoscaling_config import AutoscalingConfigProducer
from ray.autoscaler._private.monitor import BASE_READONLY_CONFIG
from ray.autoscaler._private.util import (
    format_readonly_node_type,
    hash_launch_conf,
    hash_runtime_conf,
    prepare_config,
    validate_config,
)
from ray.autoscaler.v2.schema import NodeType
from ray.autoscaler.v2.sdk import get_cluster_resource_state
from ray.autoscaler.v2.utils import is_head_node

logger = logging.getLogger(__name__)


class Provider(Enum):
    UNKNOWN = 0
    ALIYUN = 1
    AWS = 2
    AZURE = 3
    GCP = 4
    KUBERAY = 5
    LOCAL = 6
    READ_ONLY = 7


class IConfigReader(ABC):
    """An interface for reading Autoscaling config.

    A utility class that reads autoscaling configs from various sources:
        - File
        - In-memory dict
        - Remote config service (e.g. KubeRay's config)

    Example:
        reader = FileConfigReader("path/to/config.yaml")
        # Get the recently cached config.
        config = reader.get_cached_autoscaling_config()

        ...
        # Refresh the cached config.
        reader.refresh_cached_autoscaling_config()
        config = reader.get_cached_autoscaling_config()

    """

    @abstractmethod
    def get_cached_autoscaling_config(self) -> "AutoscalingConfig":
        """Returns the recently read autoscaling config.

        Returns:
            AutoscalingConfig: The recently read autoscaling config.
        """
        pass

    @abstractmethod
    def refresh_cached_autoscaling_config(self):
        """Read the config from the source."""
        pass


@dataclass(frozen=True)
class InstanceReconcileConfig:
    # The timeout for waiting for a REQUESTED instance to be ALLOCATED.
    request_status_timeout_s: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_REQUEST_STATUS_TIMEOUT_S", 10 * 60
    )
    # The timeout for waiting for a ALLOCATED instance to be RAY_RUNNING.
    allocate_status_timeout_s: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_ALLOCATE_STATUS_TIMEOUT_S", 300
    )
    # The timeout for waiting for a RAY_INSTALLING instance to be RAY_RUNNING.
    ray_install_status_timeout_s: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_RAY_INSTALL_STATUS_TIMEOUT_S", 30 * 60
    )
    # The timeout for waiting for a TERMINATING instance to be TERMINATED.
    terminating_status_timeout_s: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_TERMINATING_STATUS_TIMEOUT_S", 300
    )
    # The timeout for waiting for a RAY_STOP_REQUESTED instance
    # to be RAY_STOPPING or RAY_STOPPED.
    ray_stop_requested_status_timeout_s: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_RAY_STOP_REQUESTED_STATUS_TIMEOUT_S", 300
    )
    # The interval for raise a warning when an instance in transient status
    # is not updated for a long time.
    transient_status_warn_interval_s: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_TRANSIENT_STATUS_WARN_INTERVAL_S", 90
    )
    # The number of times to retry requesting to allocate an instance.
    max_num_retry_request_to_allocate: int = env_integer(
        "RAY_AUTOSCALER_RECONCILE_MAX_NUM_RETRY_REQUEST_TO_ALLOCATE", 3
    )


@dataclass
class NodeTypeConfig:
    """
    NodeTypeConfig is the helper class to provide node type specific configs.
    This maps to subset of the `available_node_types` field in the
    autoscaling config.
    """

    # Node type name
    name: NodeType
    # The minimal number of worker nodes to be launched for this node type.
    min_worker_nodes: int
    # The maximal number of worker nodes can be launched for this node type.
    max_worker_nodes: int
    # Idle timeout seconds for worker nodes of this node type.
    idle_timeout_s: Optional[float] = None
    # The total resources on the node.
    resources: Dict[str, float] = field(default_factory=dict)
    # The labels on the node.
    labels: Dict[str, str] = field(default_factory=dict)
    # The node config's launch config hash. It's calculated from the auth
    # config, and the node's config in the `AutoscalingConfig` for the node
    # type when launching the node. It's used to detect config changes.
    launch_config_hash: str = ""

    def __post_init__(self):
        assert self.min_worker_nodes <= self.max_worker_nodes
        assert self.min_worker_nodes >= 0


class AutoscalingConfig:
    """
    AutoscalingConfig is the helper class to provide autoscaling
    related configs.

    # TODO(rickyx):
        1. Move the config validation logic here.
        2. Deprecate the ray-schema.json for validation because it's
        static thus not possible to validate the config with interdependency
        of each other.
    """

    def __init__(
        self,
        configs: Dict[str, Any],
        skip_content_hash: bool = False,
    ) -> None:
        """
        Args:
            configs : The raw configs dict.
            skip_content_hash :
                Whether to skip file mounts/ray command hash calculation.
        """
        self._sync_continuously = False
        self.update_configs(configs, skip_content_hash)

    def update_configs(self, configs: Dict[str, Any], skip_content_hash: bool) -> None:
        self._configs = prepare_config(configs)
        validate_config(self._configs)
        if skip_content_hash:
            return
        self._calculate_hashes()
        self._sync_continuously = self._configs.get(
            "generate_file_mounts_contents_hash", True
        )

    def _calculate_hashes(self) -> None:
        logger.info("Calculating hashes for file mounts and ray commands.")
        self._runtime_hash, self._file_mounts_contents_hash = hash_runtime_conf(
            self._configs.get("file_mounts", {}),
            self._configs.get("cluster_synced_files", []),
            [
                self._configs.get("worker_setup_commands", []),
                self._configs.get("worker_start_ray_commands", []),
            ],
            generate_file_mounts_contents_hash=self._configs.get(
                "generate_file_mounts_contents_hash", True
            ),
        )

    def get_cloud_node_config(self, ray_node_type: NodeType) -> Dict[str, Any]:
        return copy.deepcopy(
            self.get_node_type_specific_config(ray_node_type, "node_config") or {}
        )

    def get_docker_config(self, ray_node_type: NodeType) -> Dict[str, Any]:
        """
        Return the docker config for the specified node type.
            If it's a head node, the image will be chosen in the following order:
                1. Node specific docker image.
                2. The 'docker' config's 'head_image' field.
                3. The 'docker' config's 'image' field.
            If it's a worker node, the image will be chosen in the following order:
                1. Node specific docker image.
                2. The 'docker' config's 'worker_image' field.
                3. The 'docker' config's 'image' field.
        """
        # TODO(rickyx): It's unfortunate we have multiple fields in ray-schema.json
        #  that can specify docker images. We should consolidate them.
        docker_config = copy.deepcopy(self._configs.get("docker", {}))
        node_specific_docker_config = self._configs["available_node_types"][
            ray_node_type
        ].get("docker", {})
        # Override the global docker config with node specific docker config.
        docker_config.update(node_specific_docker_config)

        if self._configs.get("head_node_type") == ray_node_type:
            if "head_image" in docker_config:
                logger.info(
                    "Overwriting image={} by head_image({}) for head node docker.".format(  # noqa: E501
                        docker_config["image"], docker_config["head_image"]
                    )
                )
                docker_config["image"] = docker_config["head_image"]
        else:
            if "worker_image" in docker_config:
                logger.info(
                    "Overwriting image={} by worker_image({}) for worker node docker.".format(  # noqa: E501
                        docker_config["image"], docker_config["worker_image"]
                    )
                )
                docker_config["image"] = docker_config["worker_image"]

        # These fields should be merged.
        docker_config.pop("head_image", None)
        docker_config.pop("worker_image", None)
        return docker_config

    def get_worker_start_ray_commands(self) -> List[str]:
        return self._configs.get("worker_start_ray_commands", [])

    def get_head_setup_commands(self) -> List[str]:
        return self._configs.get("head_setup_commands", [])

    def get_head_start_ray_commands(self) -> List[str]:
        return self._configs.get("head_start_ray_commands", [])

    def get_worker_setup_commands(self, ray_node_type: NodeType) -> List[str]:
        """
        Return the worker setup commands for the specified node type.

        If the node type specific worker setup commands are not specified,
        return the global worker setup commands.
        """
        worker_setup_command = self.get_node_type_specific_config(
            ray_node_type, "worker_setup_commands"
        )
        if worker_setup_command is None:
            # Return global worker setup commands if node type specific
            # worker setup commands are not specified.
            logger.info(
                "Using global worker setup commands for {}".format(ray_node_type)
            )
            return self._configs.get("worker_setup_commands", [])
        return worker_setup_command

    def get_initialization_commands(self, ray_node_type: NodeType) -> List[str]:
        """
        Return the initialization commands for the specified node type.

        If the node type specific initialization commands are not specified,
        return the global initialization commands.
        """
        initialization_command = self.get_node_type_specific_config(
            ray_node_type, "initialization_commands"
        )
        if initialization_command is None:
            logger.info(
                "Using global initialization commands for {}".format(ray_node_type)
            )
            return self._configs.get("initialization_commands", [])
        return initialization_command

    def get_node_type_specific_config(
        self, ray_node_type: NodeType, config_name: str
    ) -> Optional[Any]:
        node_specific_config = self._configs["available_node_types"].get(
            ray_node_type, {}
        )
        return node_specific_config.get(config_name, None)

    def get_node_resources(self, ray_node_type: NodeType) -> Dict[str, float]:
        return copy.deepcopy(
            self.get_node_type_specific_config(ray_node_type, "resources") or {}
        )

    def get_node_labels(self, ray_node_type: NodeType) -> Dict[str, str]:
        return copy.deepcopy(
            self.get_node_type_specific_config(ray_node_type, "labels") or {}
        )

    def get_config(self, config_name, default=None) -> Any:
        return self._configs.get(config_name, default)

    def get_provider_instance_type(self, ray_node_type: NodeType) -> str:
        provider = self.provider
        node_config = self.get_node_type_specific_config(ray_node_type, "node_config")
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

    def get_node_type_configs(self) -> Dict[NodeType, NodeTypeConfig]:
        """
        Returns the node type configs from the `available_node_types` field.

        Returns:
            Dict[NodeType, NodeTypeConfig]: The node type configs.
        """
        available_node_types = self._configs.get("available_node_types", {})
        if not available_node_types:
            return None
        node_type_configs = {}
        auth_config = self._configs.get("auth", {})
        head_node_type = self.get_head_node_type()
        assert head_node_type
        for node_type, node_config in available_node_types.items():
            launch_config_hash = hash_launch_conf(
                node_config.get("node_config", {}), auth_config
            )
            max_workers_nodes = node_config.get("max_workers", 0)
            if head_node_type == node_type:
                max_workers_nodes += 1

            node_type_configs[node_type] = NodeTypeConfig(
                name=node_type,
                min_worker_nodes=node_config.get("min_workers", 0),
                max_worker_nodes=max_workers_nodes,
                idle_timeout_s=node_config.get("idle_timeout_s", None),
                resources=node_config.get("resources", {}),
                labels=node_config.get("labels", {}),
                launch_config_hash=launch_config_hash,
            )
        return node_type_configs

    def get_head_node_type(self) -> NodeType:
        """
        Returns the head node type.

        If there is only one node type, return the only node type as the head
        node type.
        If there are multiple node types, return the head node type specified
        in the config.
        """
        available_node_types = self._configs.get("available_node_types", {})
        if len(available_node_types) == 1:
            return list(available_node_types.keys())[0]
        return self._configs.get("head_node_type")

    def get_max_num_worker_nodes(self) -> Optional[int]:
        return self.get_config("max_workers", None)

    def get_max_num_nodes(self) -> Optional[int]:
        max_num_workers = self.get_max_num_worker_nodes()
        if max_num_workers is not None:
            return max_num_workers + 1  # For head node
        return None

    def get_raw_config_mutable(self) -> Dict[str, Any]:
        return self._configs

    def get_upscaling_speed(self) -> float:
        return self.get_config("upscaling_speed", DEFAULT_UPSCALING_SPEED)

    def get_max_concurrent_launches(self) -> int:
        return AUTOSCALER_MAX_CONCURRENT_LAUNCHES

    def disable_node_updaters(self) -> bool:
        provider_config = self._configs.get("provider", {})
        return provider_config.get(DISABLE_NODE_UPDATERS_KEY, True)

    def get_idle_timeout_s(self) -> Optional[float]:
        """
        Returns the idle timeout in seconds if present in config, otherwise None.
        """
        idle_timeout_s = self.get_config("idle_timeout_minutes", None)
        return idle_timeout_s * 60 if idle_timeout_s is not None else None

    def disable_launch_config_check(self) -> bool:
        provider_config = self.get_provider_config()
        return provider_config.get(DISABLE_LAUNCH_CONFIG_CHECK_KEY, True)

    def get_instance_reconcile_config(self) -> InstanceReconcileConfig:
        # TODO(rickyx): we need a way to customize these configs,
        # either extending the current ray-schema.json, or just use another
        # schema validation paths.
        return InstanceReconcileConfig()

    def get_provider_config(self) -> Dict[str, Any]:
        return self._configs.get("provider", {})

    def dump(self) -> str:
        return yaml.safe_dump(self._configs)

    @property
    def provider(self) -> Provider:
        provider_str = self._configs.get("provider", {}).get("type", "")
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
        elif provider_str == "readonly":
            return Provider.READ_ONLY
        else:
            return Provider.UNKNOWN

    @property
    def runtime_hash(self) -> str:
        return self._runtime_hash

    @property
    def file_mounts_contents_hash(self) -> str:
        return self._file_mounts_contents_hash


class FileConfigReader(IConfigReader):
    """A class that reads cluster config from a yaml file."""

    def __init__(self, config_file: str, skip_content_hash: bool = True) -> None:
        """
        Args:
            config_file: The path to the config file.
            skip_content_hash:  Whether to skip file mounts/ray command
                hash calculation. Default to True.
        """
        self._config_file_path = Path(config_file).resolve()
        self._skip_content_hash = skip_content_hash
        self._cached_config = self._read()

    def _read(self) -> AutoscalingConfig:
        with open(self._config_file_path) as f:
            config = yaml.safe_load(f.read())
            return AutoscalingConfig(config, skip_content_hash=self._skip_content_hash)

    def get_cached_autoscaling_config(self) -> AutoscalingConfig:
        """
        Returns:
            AutoscalingConfig: The autoscaling config.
        """

        return self._cached_config

    def refresh_cached_autoscaling_config(self):
        self._cached_config = self._read()


class KubeRayConfigReader(IConfigReader):
    """A class that reads cluster config from a K8s RayCluster CR."""

    def __init__(self, config_producer: AutoscalingConfigProducer):
        self._config_producer = config_producer
        self._cached_config = self._generate_configs_from_k8s()

    def _generate_configs_from_k8s(self) -> AutoscalingConfig:
        return AutoscalingConfig(self._config_producer())

    def get_cached_autoscaling_config(self) -> AutoscalingConfig:
        """
        Returns:
            AutoscalingConfig: The autoscaling config.
        """
        return self._cached_config

    def refresh_cached_autoscaling_config(self):
        """
        Reads the configs from the K8s RayCluster CR.

        This reads from the K8s API server every time to pick up changes.
        """
        self._cached_config = self._generate_configs_from_k8s()


class ReadOnlyProviderConfigReader(IConfigReader):
    """A class that reads cluster config for a read-only provider.

    This is used for laptop mode / manual cluster setup modes, in order to
    provide status reporting in the same way for users."""

    def __init__(self, gcs_address: str):
        self._configs = BASE_READONLY_CONFIG
        self._gcs_client = GcsClient(address=gcs_address)

    def refresh_cached_autoscaling_config(self) -> AutoscalingConfig:
        # Update the config with node types from GCS.
        ray_cluster_resource_state = get_cluster_resource_state(self._gcs_client)

        # Format each node type's config from the running nodes.
        available_node_types = {}

        head_node_type = None
        for node_state in ray_cluster_resource_state.node_states:
            node_type = format_readonly_node_type(binary_to_hex(node_state.node_id))
            if is_head_node(node_state):
                head_node_type = node_type

            available_node_types[node_type] = {
                "resources": dict(node_state.total_resources),
                "min_workers": 0,
                "max_workers": 0 if is_head_node(node_state) else 1,
                "node_config": {},
            }
        if available_node_types:
            self._configs["available_node_types"].update(available_node_types)
            self._configs["max_workers"] = len(available_node_types)
            assert head_node_type, "Head node type should be found."
            self._configs["head_node_type"] = head_node_type

        # Don't idle terminated nodes in read-only mode.
        self._configs.pop("idle_timeout_minutes", None)

    def get_cached_autoscaling_config(self) -> AutoscalingConfig:
        return AutoscalingConfig(self._configs, skip_content_hash=True)
