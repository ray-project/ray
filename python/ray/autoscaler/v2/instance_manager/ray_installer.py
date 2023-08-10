import logging
import subprocess

from ray.autoscaler._private.updater import NodeUpdater
from ray.autoscaler._private.util import with_envs, with_head_node_ip
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


class RayInstaller(object):
    """
    RayInstaller is responsible for installing ray on the target instance.
    """

    def __init__(
        self,
        provider: NodeProviderV1,
        config: NodeProviderConfig,
        process_runner=subprocess,
    ) -> None:
        self._provider = provider
        self._config = config
        self._process_runner = process_runner

    def install_ray(self, instance: Instance, head_node_ip: str) -> bool:
        """
        Install ray on the target instance synchronously.
        """

        setup_commands = self._config.get_worker_setup_commands(instance.instance_type)
        ray_start_commands = self._config.get_worker_start_ray_commands()
        docker_config = self._config.get_docker_config(instance.instance_type)

        logger.info(
            f"Creating new (spawn_updater) updater thread for node"
            f" {instance.cloud_instance_id}."
        )
        provider_instance_type_name = self._config.get_provider_instance_type(
            instance.instance_type
        )
        updater = NodeUpdater(
            node_id=instance.instance_id,
            provider_config=self._config.get_config("provider"),
            provider=self._provider,
            auth_config=self._config.get_config("auth"),
            cluster_name=self._config.get_config("cluster_name"),
            file_mounts=self._config.get_config("file_mounts"),
            initialization_commands=with_head_node_ip(
                self._config.get_node_type_specific_config(
                    instance.instance_type, "initialization_commands"
                ),
                head_node_ip,
            ),
            setup_commands=with_head_node_ip(setup_commands, head_node_ip),
            # This will prepend envs to the begin of the ray start commands, e.g.
            # `RAY_HEAD_IP=<head_node_ip> \
            #  RAY_CLOUD_INSTANCE_ID=<instance_id> \
            #  ray start --head ...`
            #  See src/ray/common/constants.h for ENV name definitions.
            ray_start_commands=with_envs(
                ray_start_commands,
                {
                    "RAY_HEAD_IP": head_node_ip,
                    "RAY_CLOUD_INSTANCE_ID": instance.instance_id,
                    "RAY_NODE_TYPE_NAME": instance.instance_type,
                    "RAY_CLOUD_INSTANCE_TYPE_NAME": provider_instance_type_name,
                },
            ),
            runtime_hash=self._config.runtime_hash,
            file_mounts_contents_hash=self._config.file_mounts_contents_hash,
            is_head_node=False,
            cluster_synced_files=self._config.get_config("cluster_synced_files"),
            rsync_options={
                "rsync_exclude": self._config.get_config("rsync_exclude"),
                "rsync_filter": self._config.get_config("rsync_filter"),
            },
            use_internal_ip=True,
            docker_config=docker_config,
            node_resources=self._config.get_node_resources(instance.instance_type),
            node_labels=self._config.get_node_labels(instance.instance_type),
            process_runner=self._process_runner,
        )
        try:
            updater.run()
        except Exception:
            # Errors has already been handled.
            return False
        return True
