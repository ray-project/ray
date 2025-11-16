import abc
import os
from typing import Any, Dict, List, Optional

from click.exceptions import ClickException

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger
from ray_release.reporter.artifacts import DEFAULT_ARTIFACTS_DIR
from ray_release.util import exponential_backoff_retry


class CommandRunner(abc.ABC):
    """This is run on Buildkite runners."""

    # the directory for runners to dump files to (on buildkite runner instances).
    # Write to this directory. run_release_tests.sh will ensure that the content
    # shows up under buildkite job's "Artifacts" UI tab.
    _DEFAULT_ARTIFACTS_DIR = DEFAULT_ARTIFACTS_DIR

    # the artifact file name put under s3 bucket root.
    # AnyscalejobWrapper will upload user generated artifact to this path
    # and AnyscaleJobRunner will then download from there.
    _USER_GENERATED_ARTIFACT = "user_generated_artifact"

    # the path where result json will be written to on both head node
    # as well as the relative path where result json will be uploaded to on s3.
    _RESULT_OUTPUT_JSON = "/tmp/release_test_out.json"

    # the path where output json will be written to on both head node
    # as well as the relative path where metrics json will be uploaded to on s3.
    _METRICS_OUTPUT_JSON = "/tmp/metrics_test_out.json"

    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
        artifact_path: Optional[str] = None,
    ):
        self.cluster_manager = cluster_manager
        self.file_manager = file_manager
        self.working_dir = working_dir

    @property
    def command_env(self):
        return {
            "TEST_OUTPUT_JSON": self._RESULT_OUTPUT_JSON,
            "METRICS_OUTPUT_JSON": self._METRICS_OUTPUT_JSON,
            "USER_GENERATED_ARTIFACT": self._USER_GENERATED_ARTIFACT,
            "BUILDKITE_BRANCH": os.environ.get("BUILDKITE_BRANCH", ""),
        }

    def get_full_command_env(self, env: Optional[Dict] = None):
        full_env = self.command_env.copy()

        if env:
            full_env.update(env)

        return full_env

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
        self,
        command: str,
        env: Optional[Dict] = None,
        timeout: float = 3600.0,
        raise_on_timeout: bool = True,
        pip: Optional[List[str]] = None,
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

    def fetch_artifact(self, artifact_path):
        raise NotImplementedError
