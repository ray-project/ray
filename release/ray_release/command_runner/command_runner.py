import abc
from typing import Dict, Any

from ray_release.cluster_manager.cluster_manager import ClusterManager


class CommandRunner(abc.ABC):
    def __init__(self, session_manager: ClusterManager):
        self.session_manager = session_manager

    def prepare_local_env(self):
        """Prepare local environment, e.g. install dependencies."""

    def prepare_remote_env(self):
        """Prepare remote environment, e.g. upload files."""

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        """Wait for cluster nodes to be up.

        Args:
            num_nodes (int): Number of nodes to wait for.
            timeout (float): Timeout in seconds to wait for nodes before
             raising a ``PrepareCommandTimeoutError``.

        Returns:
            None

        Raises:
            PrepareCommandTimeoutError
        """
        raise NotImplementedError

    def run_command(self, command: str):
        """Run command."""
        raise NotImplementedError

    def fetch_results(self) -> Dict[str, Any]:
        raise NotImplementedError
