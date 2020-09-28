import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

import yaml
from ray import services
from ray.autoscaler._private.command_runner import DockerCommandRunner
from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_UNMANAGED, \
    NODE_KIND_WORKER, TAG_RAY_NODE_KIND
from ray.tune.syncer import NodeSyncer
from ray.tune.sync_client import SyncClient


class _WrappedProvider:
    def __init__(self, config_path: str):
        from ray.autoscaler._private.util import validate_config
        from ray.autoscaler.node_provider import _get_node_provider

        with open(config_path) as f:
            new_config = yaml.safe_load(f.read())
        validate_config(new_config)
        self._config = new_config

        self.provider = _get_node_provider(self.config["provider"],
                                           self.config["cluster_name"])

        self._ip_cache = {}
        self._node_id_cache = {}

    @property
    def config(self) -> Dict:
        return self._config

    def all_workers(self) -> List[str]:
        return self.head() + self.workers() + self.unmanaged_workers()

    def head(self) -> List[str]:
        return self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD})

    def workers(self) -> List[str]:
        return self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

    def unmanaged_workers(self) -> List[str]:
        return self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_UNMANAGED})

    def get_ip_to_node_id(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        for node_id in self.all_workers():
            if node_id not in self._ip_cache:
                internal_ip = self.provider.internal_ip(node_id)
                self._ip_cache[internal_ip] = node_id
                self._node_id_cache[node_id] = internal_ip
        return self._node_id_cache, self._ip_cache

    def internal_ip(self, node_id: str) -> str:
        return self.provider.internal_ip(node_id)

    def external_ip(self, node_id: str) -> str:
        return self.provider.external_ip(node_id)


_provider = None


class DockerSyncer(NodeSyncer):
    """DockerSyncer used for synchronization between Docker containers.
    This syncer extends the node syncer, but is usually instantiated
    without a custom sync client. The sync client defaults to
    ``DockerSyncClient`` instead.

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

    _cluster_path = os.path.expanduser("~/ray_bootstrap_config.yaml")

    def __init__(self,
                 local_dir: str,
                 remote_dir: str,
                 sync_client: Optional[SyncClient] = None):
        self.local_ip = services.get_node_ip_address()
        self.worker_ip = None
        self.worker_node_id = None

        global _provider
        if _provider is None:
            _provider = _WrappedProvider(self._cluster_path)

        sync_client = sync_client or DockerSyncClient()
        sync_client.configure(_provider, _provider.config)

        self._provider = _provider

        super(NodeSyncer, self).__init__(local_dir, remote_dir, sync_client)

    def set_worker_ip(self, worker_ip: str):
        self.worker_ip = worker_ip
        self.worker_node_id = self._provider.get_ip_to_node_id()[1][worker_ip]

    @property
    def _remote_path(self) -> Tuple[str, str]:
        return (self.worker_node_id, self._remote_dir)


class DockerSyncClient(SyncClient):
    """DockerSyncClient to be used by DockerSyncer.
    This client takes care of executing the synchronization
    commands for Docker nodes. In its ``sync_down`` and
    ``sync_up`` commands, it expects tuples for the source
    and target, respectively, for compatibility with the
    DockerCommandRunner.
    """

    def __init__(self, process_runner: Any = subprocess):
        self._process_runner = process_runner
        self._command_runners = {}
        self.provider = None

    def configure(self, provider: _WrappedProvider, cluster_config: Dict):
        self.provider = provider
        self.cluster_config = cluster_config

    def _create_command_runner(self, node_id: str) -> DockerCommandRunner:
        """Create a command runner for one Docker node"""
        args = {
            "log_prefix": "DockerSync: ",
            "node_id": node_id,
            "provider": self.provider,
            "auth_config": self.cluster_config["auth"],
            "cluster_name": self.cluster_config["cluster_name"],
            "process_runner": self._process_runner,
            "use_internal_ip": True,
            "docker_config": self.cluster_config["docker"],
        }
        return DockerCommandRunner(**args)

    def _get_command_runner(self, node_id: str) -> DockerCommandRunner:
        """Create command runner if it doesn't exist"""
        # Todo(krfricke): These cached runners are currently
        # never cleaned up. They are cheap so this shouldn't
        # cause much problems, but should be addressed if
        # the SyncClient is used more extensively in the future.
        if node_id not in self._command_runners:
            command_runner = self._create_command_runner(node_id)
            self._command_runners[node_id] = command_runner
        return self._command_runners[node_id]

    def sync_up(self, source: str, target: Tuple[str, str]) -> bool:
        """Here target is a tuple (target_node, target_dir)"""
        target_node, target_dir = target

        # Add trailing slashes for rsync
        source = os.path.join(source, "")
        target_dir = os.path.join(target_dir, "")

        command_runner = self._get_command_runner(target_node)
        command_runner.run_rsync_up(source, target_dir)
        return True

    def sync_down(self, source: Tuple[str, str], target: str) -> bool:
        """Here source is a tuple (source_node, source_dir)"""
        source_node, source_dir = source

        # Add trailing slashes for rsync
        source_dir = os.path.join(source_dir, "")
        target = os.path.join(target, "")

        command_runner = self._get_command_runner(source_node)
        command_runner.run_rsync_down(source_dir, target)
        return True

    def delete(self, target: str) -> bool:
        raise NotImplementedError
