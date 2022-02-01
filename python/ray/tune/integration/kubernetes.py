import os
from typing import Any, Optional, Tuple, List
import subprocess

from ray import logger
from ray.autoscaler._private.command_runner import KubernetesCommandRunner
from ray.tune.syncer import NodeSyncer
from ray.tune.sync_client import SyncClient
from ray.util import get_node_ip_address


def try_import_kubernetes():
    try:
        import kubernetes
    except ImportError:
        kubernetes = None
    return kubernetes


kubernetes = try_import_kubernetes()


def NamespacedKubernetesSyncer(namespace):
    """Wrapper to return a ``KubernetesSyncer`` for a Kubernetes namespace.

    Args:
        namespace (str): Kubernetes namespace.

    Returns:
        A ``KubernetesSyncer`` class to be passed to ``tune.run()``.

    Example:

    .. code-block:: python

        from ray.tune.integration.kubernetes import NamespacedKubernetesSyncer
        tune.run(train,
                 sync_config=tune.SyncConfig(
                     syncer=NamespacedKubernetesSyncer("ray")))

    """

    class _NamespacedKubernetesSyncer(KubernetesSyncer):
        _namespace = namespace

    return _NamespacedKubernetesSyncer


class KubernetesSyncer(NodeSyncer):
    """KubernetesSyncer used for synchronization between Kubernetes pods.

    This syncer extends the node syncer, but is usually instantiated
    without a custom sync client. The sync client defaults to
    ``KubernetesSyncClient`` instead.

    KubernetesSyncer uses the default namespace ``ray``. You should
    probably use ``NamespacedKubernetesSyncer`` to return a class
    with a custom namespace instead.
    """

    _namespace = "ray"

    def __init__(
        self, local_dir: str, remote_dir: str, sync_client: Optional[SyncClient] = None
    ):
        if not kubernetes:
            raise ImportError(
                "kubernetes is not installed on this machine/container. "
                "Try: pip install kubernetes"
            )
        self.local_ip = get_node_ip_address()
        self.local_node = self._get_kubernetes_node_by_ip(self.local_ip)
        self.worker_ip = None
        self.worker_node = None

        sync_client = sync_client or KubernetesSyncClient(
            namespace=self.__class__._namespace
        )

        super(NodeSyncer, self).__init__(local_dir, remote_dir, sync_client)

    def set_worker_ip(self, worker_ip: str):
        self.worker_ip = worker_ip
        self.worker_node = self._get_kubernetes_node_by_ip(worker_ip)

    def _get_kubernetes_node_by_ip(self, node_ip: str) -> Optional[str]:
        """Return node name by internal or external IP"""
        kubernetes.config.load_incluster_config()
        api = kubernetes.client.CoreV1Api()
        pods = api.list_namespaced_pod(self._namespace)
        for pod in pods.items:
            if pod.status.host_ip == node_ip or pod.status.pod_ip == node_ip:
                return pod.metadata.name

        logger.error("Could not find Kubernetes pod name for IP {}".format(node_ip))
        return None

    @property
    def _remote_path(self) -> Tuple[str, str]:
        return self.worker_node, self._remote_dir


class KubernetesSyncClient(SyncClient):
    """KubernetesSyncClient to be used by KubernetesSyncer.

    This client takes care of executing the synchronization
    commands for Kubernetes clients. In its ``sync_down`` and
    ``sync_up`` commands, it expects tuples for the source
    and target, respectively, for compatibility with the
    KubernetesCommandRunner.

    Args:
        namespace (str): Namespace in which the pods live.
        process_runner: How commands should be called.
            Defaults to ``subprocess``.

    """

    def __init__(self, namespace: str, process_runner: Any = subprocess):
        self.namespace = namespace
        self._process_runner = process_runner
        self._command_runners = {}

    def _create_command_runner(self, node_id: str) -> KubernetesCommandRunner:
        """Create a command runner for one Kubernetes node"""
        return KubernetesCommandRunner(
            log_prefix="KubernetesSyncClient: {}:".format(node_id),
            namespace=self.namespace,
            node_id=node_id,
            auth_config=None,
            process_runner=self._process_runner,
        )

    def _get_command_runner(self, node_id: str) -> KubernetesCommandRunner:
        """Create command runner if it doesn't exist"""
        # Todo(krfricke): These cached runners are currently
        # never cleaned up. They are cheap so this shouldn't
        # cause much problems, but should be addressed if
        # the SyncClient is used more extensively in the future.
        if node_id not in self._command_runners:
            command_runner = self._create_command_runner(node_id)
            self._command_runners[node_id] = command_runner
        return self._command_runners[node_id]

    def sync_up(
        self, source: str, target: Tuple[str, str], exclude: Optional[List] = None
    ) -> bool:
        """Here target is a tuple (target_node, target_dir)"""
        target_node, target_dir = target

        # Add trailing slashes for rsync
        source = os.path.join(source, "")
        target_dir = os.path.join(target_dir, "")

        command_runner = self._get_command_runner(target_node)
        command_runner.run_rsync_up(source, target_dir)
        return True

    def sync_down(
        self, source: Tuple[str, str], target: str, exclude: Optional[List] = None
    ) -> bool:
        """Here source is a tuple (source_node, source_dir)"""
        source_node, source_dir = source

        # Add trailing slashes for rsync
        source_dir = os.path.join(source_dir, "")
        target = os.path.join(target, "")

        command_runner = self._get_command_runner(source_node)
        command_runner.run_rsync_down(source_dir, target)
        return True

    def delete(self, target: str) -> bool:
        """No delete function because it is only used by
        the KubernetesSyncer, which doesn't call delete."""
        return True
