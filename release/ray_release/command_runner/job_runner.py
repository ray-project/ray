import json
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.exception import (
    ClusterNodesWaitTimeout,
    CommandError,
    CommandTimeout,
    LogsError,
    FetchResultError,
)
from ray_release.file_manager.file_manager import FileManager
from ray_release.job_manager import JobManager
from ray_release.logger import logger
from ray_release.util import format_link, get_anyscale_sdk

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class JobRunner(CommandRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
        sdk: Optional["AnyscaleSDK"] = None,
        artifact_path: Optional[str] = None,
    ):
        super(JobRunner, self).__init__(
            cluster_manager=cluster_manager,
            file_manager=file_manager,
            working_dir=working_dir,
        )
        self.sdk = sdk or get_anyscale_sdk()
        self.job_manager = JobManager(cluster_manager)

        self.last_command_scd_id = None

    def _copy_script_to_working_dir(self, script_name):
        script = os.path.join(os.path.dirname(__file__), f"_{script_name}")
        shutil.copy(script, script_name)

    def prepare_remote_env(self):
        self._copy_script_to_working_dir("wait_cluster.py")
        self._copy_script_to_working_dir("prometheus_metrics.py")

        # Do not upload the files here. Instead, we use the job runtime environment
        # to automatically upload the local working dir.

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        # Wait script should be uploaded already. Kick off command
        try:
            # Give 30 seconds more to acount for communication
            self.run_prepare_command(
                f"python wait_cluster.py {num_nodes} {timeout}", timeout=timeout + 30
            )
        except (CommandError, CommandTimeout) as e:
            raise ClusterNodesWaitTimeout(
                f"Not all {num_nodes} nodes came up within {timeout} seconds."
            ) from e

    def save_metrics(self, start_time: float, timeout: float = 900):
        self.run_prepare_command(
            f"python prometheus_metrics.py {start_time}", timeout=timeout
        )

    def run_command(
        self,
        command: str,
        env: Optional[Dict] = None,
        timeout: float = 3600.0,
        raise_on_timeout: bool = True,
    ) -> float:
        full_env = self.get_full_command_env(env)

        if full_env:
            env_str = " ".join(f"{k}={v}" for k, v in full_env.items()) + " "
        else:
            env_str = ""

        full_command = f"{env_str}{command}"
        logger.info(
            f"Running command in cluster {self.cluster_manager.cluster_name}: "
            f"{full_command}"
        )

        logger.info(
            f"Link to cluster: "
            f"{format_link(self.cluster_manager.get_cluster_url())}"
        )

        status_code, time_taken = self.job_manager.run_and_wait(
            full_command, full_env, working_dir=".", timeout=int(timeout)
        )

        if status_code != 0:
            raise CommandError(f"Command returned non-success status: {status_code}")

        return time_taken

    def get_last_logs_ex(self, scd_id: Optional[str] = None):
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
            raise FetchResultError(f"Could not fetch results from session: {e}") from e

    def fetch_results(self) -> Dict[str, Any]:
        return self._fetch_json(self._RESULT_OUTPUT_JSON)

    def fetch_metrics(self) -> Dict[str, Any]:
        return self._fetch_json(self._METRICS_OUTPUT_JSON)

    def fetch_artifact(self):
        raise NotImplementedError
