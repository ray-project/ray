import abc
from typing import Dict, Any, Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.file_manager.file_manager import FileManager
from ray_release.util import exponential_backoff_retry
from click.exceptions import ClickException

class CommandRunner(abc.ABC):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
    ):
        self.cluster_manager = cluster_manager
        self.file_manager = file_manager
        self.working_dir = working_dir

        self.result_output_json = "/tmp/release_test_out.json"
        self.metrics_output_json = "/tmp/metrics_test_out.json"

    @property
    def command_env(self):
        return {
            "TEST_OUTPUT_JSON": self.result_output_json,
            "METRICS_OUTPUT_JSON": self.metrics_output_json,
        }

    def get_full_command_env(self, env: Optional[Dict] = None):
        full_env = self.command_env.copy()

        if env:
            full_env.update(env)

        return full_env

    def prepare_local_env(self, ray_wheels_url: Optional[str] = None):
        """Prepare local environment, e.g. install dependencies."""
        raise NotImplementedError

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

    def save_metrics(self, start_time: float, timeout: float = 900.0):
        """Obtains Prometheus metrics from head node and saves them
        to ``self.metrics_output_json``.

        Args:
            start_time: From which UNIX timestamp to start the query.
            timeout: Timeout in seconds.

        Returns:
            None
        """
        raise NotImplementedError

    def run_command(
        self, command: str, env: Optional[Dict] = None, timeout: float = 3600.0
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
            initial_retry_delay_s=5, max_retries=3)

    def get_last_logs(self):
        raise NotImplementedError

    def fetch_results(self) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_metrics(self) -> Dict[str, Any]:
        raise NotImplementedError
