import json
import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.command_runner.job_runner import JobRunner
from ray_release.exception import (
    ClusterNodesWaitTimeout,
    CommandError,
    JobBrokenError,
    RemoteEnvSetupError,
    LogsError,
    ResultsError,
)
from ray_release.file_manager.file_manager import FileManager
from ray_release.job_manager import JobManager, AnyscaleJobManager
from ray_release.logger import logger
from ray_release.util import format_link, get_anyscale_sdk
from ray_release.wheels import install_matching_ray_locally

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class AnyscaleJobRunner(JobRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
        sdk: Optional["AnyscaleSDK"] = None,
    ):
        super().__init__(
            cluster_manager=cluster_manager,
            file_manager=file_manager,
            working_dir=working_dir,
        )
        self.sdk = sdk or get_anyscale_sdk()
        self.job_manager = AnyscaleJobManager(cluster_manager)

        self.last_command_scd_id = None

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        # Handled by Anyscale
        return 

    def save_metrics(self, start_time: float, timeout: float = 900):
        return

    def run_command(
        self, command: str, env: Optional[Dict] = None, timeout: float = 3600.0
    ) -> float:
        full_env = self.get_full_command_env(env)

        if full_env:
            env_str = " ".join(f"{k}={v}" for k, v in full_env.items()) + " "
        else:
            env_str = ""

        full_command = f"{env_str}{command}"

        status_code, time_taken, error = self.job_manager.run_and_wait(
            full_command, full_env, working_dir=".", upload_path=self.file_manager.uri, timeout=int(timeout)
        )

        if status_code == -2:
            raise JobBrokenError(f"Job state is 'BROKEN' with error:\n{error}\n")

        if status_code != 0:
            raise CommandError(f"Command returned non-success status: {status_code} with error:\n{error}\n")

        return time_taken

    def get_last_logs(self, scd_id: Optional[str] = None):
        try:
            return self.job_manager.get_last_logs()
        except Exception as e:
            raise LogsError(f"Could not get last logs: {e}") from e

    def _fetch_json(self, path: str) -> Dict[str, Any]:
        try:
            tmpfile = tempfile.mkstemp(suffix=".json")[1]
            logger.info(tmpfile)
            self.file_manager.download(path, tmpfile)

            with open(tmpfile, "rt") as f:
                data = json.load(f)

            os.unlink(tmpfile)
            return data
        except Exception as e:
            raise ResultsError(f"Could not fetch results from session: {e}") from e

    def fetch_results(self) -> Dict[str, Any]:
        return self._fetch_json(self.result_output_json)

    def fetch_metrics(self) -> Dict[str, Any]:
        return self._fetch_json(self.metrics_output_json)
