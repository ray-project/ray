import json
import os
import tempfile
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.anyscale_util import LAST_LOGS_LENGTH
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.exception import (
    ClusterNodesWaitTimeout,
    CommandError,
    CommandTimeout,
    LogsError,
    RemoteEnvSetupError,
    ResultsError,
)
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger
from ray_release.util import (
    exponential_backoff_retry,
    format_link,
    get_anyscale_sdk,
    ANYSCALE_HOST,
)

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class SDKRunner(CommandRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
        sdk: Optional["AnyscaleSDK"] = None,
    ):
        super(SDKRunner, self).__init__(
            cluster_manager=cluster_manager,
            file_manager=file_manager,
            working_dir=working_dir,
        )
        self.sdk = sdk or get_anyscale_sdk()

        self.last_command_scd_id = None

    def prepare_local_env(self, ray_wheels_url: Optional[str] = None):
        pass

    def prepare_remote_env(self):
        # Copy wait script to working dir
        wait_script = os.path.join(os.path.dirname(__file__), "_wait_cluster.py")
        # Copy wait script to working dir
        if os.path.exists("wait_cluster.py"):
            os.unlink("wait_cluster.py")
        os.link(wait_script, "wait_cluster.py")

        # Copy prometheus metrics script to working dir
        metrics_script = os.path.join(
            os.path.dirname(__file__), "_prometheus_metrics.py"
        )
        # Copy wait script to working dir
        if os.path.exists("prometheus_metrics.py"):
            os.unlink("prometheus_metrics.py")
        os.link(metrics_script, "prometheus_metrics.py")

        try:
            self.file_manager.upload()
        except Exception as e:
            raise RemoteEnvSetupError(
                f"Error setting up remote environment: {e}"
            ) from e

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
        is_long_running: bool = False,
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

        result = self.sdk.create_session_command(
            dict(session_id=self.cluster_manager.cluster_id, shell_command=full_command)
        )

        scd_id = result.result.id
        self.last_command_scd_id = scd_id

        completed = result.result.finished_at is not None

        start_time = time.monotonic()
        timeout_at = start_time + timeout
        next_status = start_time + 30

        while not completed:
            now = time.monotonic()
            if now >= timeout_at:
                raise CommandTimeout(
                    f"Cluster command timed out after {timeout} seconds."
                )

            if now >= next_status:
                logger.info(
                    f"... command still running ..."
                    f"({int(now - start_time)} seconds) ..."
                )
                next_status += 30

            # Sleep 1 sec before next check.
            time.sleep(1)

            result = exponential_backoff_retry(
                lambda: self.sdk.get_session_command(session_command_id=scd_id),
                retry_exceptions=Exception,
                initial_retry_delay_s=10,
                max_retries=3,
            )
            completed = result.result.finished_at

        status_code = result.result.status_code
        time_taken = time.monotonic() - start_time

        if status_code != 0:
            raise CommandError(f"Command returned non-success status: {status_code}")

        return time_taken

    def get_last_logs(self, scd_id: Optional[str] = None):
        scd_id = scd_id or self.last_command_scd_id
        if not scd_id:
            raise LogsError(
                "Must specify scd_id to fetch command logs. Did "
                "you already kick off a command?"
            )

        # Todo: It would be nice to get an actual SDK API here
        result, _, _ = self.sdk.api_client.call_api(
            "/api/v2/session_commands/{session_command_id}/execution_logs",
            "GET",
            path_params={"session_command_id": scd_id},
            query_params={"start_line": -LAST_LOGS_LENGTH, "end_line": 0},
            header_params={},
            response_type=object,
            _host=str(ANYSCALE_HOST),
            _preload_content=True,
            _return_http_data_only=False,
        )
        return result["result"]["lines"]

    def _fetch_json(self, path: str) -> Dict[str, Any]:
        try:
            tmpfile = tempfile.mktemp()
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
