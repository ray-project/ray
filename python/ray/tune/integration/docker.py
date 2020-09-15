import subprocess

from ray import services, logger
from ray.autoscaler.command_runner import DockerCommandRunner
from ray.tune.syncer import NodeSyncer
from ray.tune.sync_client import SyncClient


class DockerSyncClient(SyncClient):
    """DockerSyncClient to be used by KubernetesSyncer.

    This client takes care of executing the synchronization
    commands for Docker nodes. In its ``sync_down`` and
    ``sync_up`` commands, it expects tuples for the source
    and target, respectively, for compatibility with the
    DockerCommandRunner.

    Args:
        namespace (str): Namespace in which the pods live.
        process_runner: How commands should be called.
            Defaults to ``subprocess``.

    """

    def __init__(self, namespace, process_runner=subprocess):
        self.namespace = namespace
        self._process_runner = process_runner
        self.args = {
            "log_prefix": "prefix",
            "node_id": 0,
            "provider": provider,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": False,
        }
        self._command_runners = {}

    def _create_command_runner(self, node_id):
        """Create a command runner for one Kubernetes node"""
        return DockerCommandRunner(docker_config, **self.args)

    def _get_command_runner(self, node_id):
        """Create command runner if it doesn't exist"""
        # Todo(krfricke): These cached runners are currently
        # never cleaned up. They are cheap so this shouldn't
        # cause much problems, but should be addressed if
        # the SyncClient is used more extensively in the future.
        if node_id not in self._command_runners:
            command_runner = self._create_command_runner(node_id)
            self._command_runners[node_id] = command_runner
        return self._command_runners[node_id]

    def sync_up(self, source, target):
        """Here target is a tuple (target_node, target_dir)"""
        target_node, target_dir = target

        # Add trailing slashes for rsync
        source += "/" if not source.endswith("/") else ""
        target_dir += "/" if not target_dir.endswith("/") else ""

        command_runner = self._get_command_runner(target_node)
        command_runner.run_rsync_up(source, target_dir)
        return True

    def sync_down(self, source, target):
        """Here source is a tuple (source_node, source_dir)"""
        source_node, source_dir = source

        # Add trailing slashes for rsync
        source_dir += "/" if not source_dir.endswith("/") else ""
        target += "/" if not target.endswith("/") else ""

        command_runner = self._get_command_runner(source_node)
        command_runner.run_rsync_down(source_dir, target)
        return True

    def delete(self, target):
        """No delete function because it is only used by
        the KubernetesSyncer, which doesn't call delete."""
        return True
