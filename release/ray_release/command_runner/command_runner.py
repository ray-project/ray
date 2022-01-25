import abc
from typing import Dict, Any

from ray_release.session_manager.session_manager import SessionManager


class CommandRunner(abc.ABC):
    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager

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
