import logging
import os
from typing import Optional, Tuple, List

from ray.autoscaler.sdk import rsync, configure_logging
from ray.util import get_node_ip_address
from ray.util.debug import log_once
from ray.tune.syncer import NodeSyncer
from ray.tune.sync_client import SyncClient
from ray.ray_constants import env_integer

logger = logging.getLogger(__name__)


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
                     syncer=DockerSyncer))

    """

    _cluster_config_file = os.path.expanduser("~/ray_bootstrap_config.yaml")

    def __init__(
        self, local_dir: str, remote_dir: str, sync_client: Optional[SyncClient] = None
    ):
        configure_logging(
            log_style="record", verbosity=env_integer("TUNE_SYNCER_VERBOSITY", 0)
        )
        self.local_ip = get_node_ip_address()
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

    Args:
        should_bootstrap: Whether to bootstrap the autoscaler
            cofiguration. This may be useful when you are
            running into authentication problems; i.e.:
            https://github.com/ray-project/ray/issues/17756.

    """

    def __init__(self, should_bootstrap: bool = True):
        self._command_runners = {}
        self._cluster_config = None
        if os.environ.get("TUNE_SYNC_DISABLE_BOOTSTRAP") == "1":
            should_bootstrap = False
            logger.debug("Skipping bootstrap for docker sync client.")
        self._should_bootstrap = should_bootstrap

    def configure(self, cluster_config_file: str):
        self._cluster_config_file = cluster_config_file

    def sync_up(
        self, source: str, target: Tuple[str, str], exclude: Optional[List] = None
    ) -> bool:
        """Here target is a tuple (target_node, target_dir)"""
        target_node, target_dir = target

        # Add trailing slashes for rsync
        source = os.path.join(source, "")
        target_dir = os.path.join(target_dir, "")
        import click

        try:
            rsync(
                cluster_config=self._cluster_config_file,
                source=source,
                target=target_dir,
                down=False,
                ip_address=target_node,
                should_bootstrap=self._should_bootstrap,
                use_internal_ip=True,
            )
        except click.ClickException:
            if log_once("docker_rsync_up_fail"):
                logger.warning(
                    "Rsync-up failed. Consider using a durable trainable "
                    "or setting the `TUNE_SYNC_DISABLE_BOOTSTRAP=1` env var."
                )
            raise

        return True

    def sync_down(
        self, source: Tuple[str, str], target: str, exclude: Optional[List] = None
    ) -> bool:
        """Here source is a tuple (source_node, source_dir)"""
        source_node, source_dir = source

        # Add trailing slashes for rsync
        source_dir = os.path.join(source_dir, "")
        target = os.path.join(target, "")
        import click

        try:
            rsync(
                cluster_config=self._cluster_config_file,
                source=source_dir,
                target=target,
                down=True,
                ip_address=source_node,
                should_bootstrap=self._should_bootstrap,
                use_internal_ip=True,
            )
        except click.ClickException:
            if log_once("docker_rsync_down_fail"):
                logger.warning(
                    "Rsync-down failed. Consider using a durable trainable "
                    "or setting the `TUNE_SYNC_DISABLE_BOOTSTRAP=1` env var."
                )
            raise

        return True

    def delete(self, target: str) -> bool:
        raise NotImplementedError
