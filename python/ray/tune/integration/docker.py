import os
from typing import Optional, Tuple

from ray import services
from ray.autoscaler.sdk import rsync, configure_logging
from ray.tune.syncer import NodeSyncer
from ray.tune.sync_client import SyncClient
from ray.tune.utils import env_integer


class DockerSyncer(NodeSyncer):
    """DockerSyncer used for synchronization between Docker containers.
    This syncer extends the node syncer, but is usually instantiated
    without a custom sync client. The sync client defaults to
    ``DockerSyncClient`` instead.

    Set the env var `TUNE_SYNCER_VERBOSITY` to increase verbosity
    of syncing operations (0, 1, 2, 3). Defaults to 0.

    .. note::
        This syncer only works with the Ray cluster launcher.
        If you use your own Docker setup, make sure the nodes can connect
        to each other via SSH, and try the regular SSH-based syncer instead.

    Example:

    .. code-block:: python

        from ray.tune.integration.docker import DockerSyncer
        tune.run(train,
                 sync_config=tune.SyncConfig(
                     sync_to_driver=DockerSyncer))

    """

    _cluster_config_file = os.path.expanduser("~/ray_bootstrap_config.yaml")

    def __init__(self,
                 local_dir: str,
                 remote_dir: str,
                 sync_client: Optional[SyncClient] = None):
        configure_logging(
            log_style="record",
            verbosity=env_integer("TUNE_SYNCER_VERBOSITY", 0))
        self.local_ip = services.get_node_ip_address()
        self.worker_ip = None

        sync_client = sync_client or DockerSyncClient()
        sync_client.configure(self._cluster_config_file)

        super(NodeSyncer, self).__init__(local_dir, remote_dir, sync_client)

    def set_worker_ip(self, worker_ip: str):
        self.worker_ip = worker_ip

    @property
    def _remote_path(self) -> Tuple[str, str]:
        return (self.worker_ip, self._remote_dir)


class DockerSyncClient(SyncClient):
    """DockerSyncClient to be used by DockerSyncer.
    This client takes care of executing the synchronization
    commands for Docker nodes. In its ``sync_down`` and
    ``sync_up`` commands, it expects tuples for the source
    and target, respectively, for compatibility with docker.
    """

    def __init__(self):
        self._command_runners = {}
        self._cluster_config = None

    def configure(self, cluster_config_file: str):
        self._cluster_config_file = cluster_config_file

    def sync_up(self, source: str, target: Tuple[str, str]) -> bool:
        """Here target is a tuple (target_node, target_dir)"""
        target_node, target_dir = target

        # Add trailing slashes for rsync
        source = os.path.join(source, "")
        target_dir = os.path.join(target_dir, "")

        rsync(
            cluster_config=self._cluster_config_file,
            source=source,
            target=target_dir,
            down=False,
            ip_address=target_node,
            use_internal_ip=True)

        return True

    def sync_down(self, source: Tuple[str, str], target: str) -> bool:
        """Here source is a tuple (source_node, source_dir)"""
        source_node, source_dir = source

        # Add trailing slashes for rsync
        source_dir = os.path.join(source_dir, "")
        target = os.path.join(target, "")

        rsync(
            cluster_config=self._cluster_config_file,
            source=source_dir,
            target=target,
            down=True,
            ip_address=source_node,
            use_internal_ip=True)

        return True

    def delete(self, target: str) -> bool:
        raise NotImplementedError
