import abc
from typing import Any, Dict, Optional

from click.exceptions import ClickException

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.logger import logger
from ray_release.util import exponential_backoff_retry


class CommandRunner(abc.ABC):
    """This is run on Buildkite runners."""

    def __init__(
        self,
        cluster_manager: ClusterManager,
        working_dir: str,
        artifact_path: Optional[str] = None,
    ):
        self.cluster_manager = cluster_manager
        self.working_dir = working_dir

    def prepare_remote_env(self):
        """Prepare remote environment, e.g. upload files."""
        raise NotImplementedError

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900.0):
        """Wait for cluster nodes to be up.

        Args:
            num_nodes: Number of nodes to wait for.
            timeout: Timeout in seconds to wait for nodes before
             raising a ``PrepareCommandTimeoutError``.

        Returns:
            None

        Raises:
            PrepareCommandTimeoutError
        """
        raise NotImplementedError

    def run_command(
        self,
        command: str,
        env: Optional[Dict] = None,
        timeout: float = 3600.0,
        raise_on_timeout: bool = True,
    ) -> float:
        """Run command."""
        raise NotImplementedError

    def run_prepare_command(
        self, command: str, env: Optional[Dict] = None, timeout: float = 3600.0
    ):
        """Run prepare command.

        Command runners may choose to run this differently than the
        test command.
        """
        return exponential_backoff_retry(
            lambda: self.run_command(command, env, timeout),
            ClickException,
            initial_retry_delay_s=5,
            max_retries=3,
        )

    def get_last_logs(self) -> Optional[str]:
        try:
            return self.get_last_logs_ex()
        except Exception as e:
            logger.exception(f"Error fetching logs: {e}")
            return None

    def get_last_logs_ex(self):
        raise NotImplementedError

    def fetch_results(self) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_metrics(self) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_artifact(self) -> None:
        raise NotImplementedError
