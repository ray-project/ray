import json
import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.job_runner import JobRunner
from ray_release.exception import (
    CommandError,
    JobBrokenError,
    LogsError,
    ResultsError,
)
from ray_release.file_manager.job_file_manager import JobFileManager
from ray_release.job_manager import AnyscaleJobManager
from ray_release.logger import logger
from ray_release.util import get_anyscale_sdk, generate_tmp_s3_path, join_s3_paths

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class AnyscaleJobRunner(JobRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: JobFileManager,
        working_dir: str,
        sdk: Optional["AnyscaleSDK"] = None,
    ):
        assert isinstance(file_manager, JobFileManager)
        super().__init__(
            cluster_manager=cluster_manager,
            file_manager=file_manager,
            working_dir=working_dir,
        )
        self.sdk = sdk or get_anyscale_sdk()
        self.job_manager = AnyscaleJobManager(cluster_manager)

        self.last_command_scd_id = None
        self.path_in_bucket = join_s3_paths(
            "working_dirs",
            self.cluster_manager.test_name.replace(" ", "_"),
            generate_tmp_s3_path(),
        )
        self.upload_path = join_s3_paths(
            f"s3://{self.file_manager.bucket}", self.path_in_bucket
        )

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

        results_file_uri = join_s3_paths(self.upload_path, self.result_output_json)
        full_command = (
            f"{env_str}{command}; "
            "pip install -q awscli && "
            f"aws s3 cp {self.result_output_json} {results_file_uri} "
            "--acl bucket-owner-full-control"
        )

        status_code, time_taken, error = self.job_manager.run_and_wait(
            full_command,
            full_env,
            working_dir=".",
            upload_path=self.upload_path,
            timeout=int(timeout),
        )

        if status_code == -2:
            raise JobBrokenError(f"Job state is 'BROKEN' with error:\n{error}\n")

        if status_code != 0:
            raise CommandError(
                f"Command returned non-success status: {status_code} with error:\n"
                f"{error}\n"
            )

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
            self.file_manager.download_from_s3(
                path, tmpfile, delete_after_download=True
            )

            with open(tmpfile, "rt") as f:
                data = json.load(f)

            os.unlink(tmpfile)
            return data
        except Exception as e:
            raise ResultsError(f"Could not fetch results from session: {e}") from e

    def fetch_results(self) -> Dict[str, Any]:
        return self._fetch_json(
            join_s3_paths(self.path_in_bucket, self.result_output_json)
        )

    def fetch_metrics(self) -> Dict[str, Any]:
        return self._fetch_json(self.metrics_output_json)
