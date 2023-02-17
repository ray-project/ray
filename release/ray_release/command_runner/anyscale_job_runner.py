import json
import os
import re
import tempfile
import shlex
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.job_runner import JobRunner
from ray_release.exception import (
    TestCommandTimeout,
    TestCommandError,
    JobStartupFailed,
    PrepareCommandError,
    PrepareCommandTimeout,
    JobBrokenError,
    FetchResultError,
    JobTerminatedBeforeStartError,
    JobNoLogsError,
    JobTerminatedError,
)
from ray_release.file_manager.job_file_manager import JobFileManager
from ray_release.job_manager import AnyscaleJobManager
from ray_release.logger import logger
from ray_release.util import get_anyscale_sdk, generate_tmp_s3_path, join_s3_paths

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

TIMEOUT_RETURN_CODE = 124


def _get_env_str(env: Dict[str, str]) -> str:
    if env:
        env_str = " ".join(f"{k}={v}" for k, v in env.items()) + " "
    else:
        env_str = ""
    return env_str


class AnyscaleJobRunner(JobRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: JobFileManager,
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
        self.path_in_bucket = join_s3_paths(
            "working_dirs",
            self.cluster_manager.test_name.replace(" ", "_"),
            generate_tmp_s3_path(),
        )
        self.upload_path = join_s3_paths(
            f"s3://{self.file_manager.bucket}", self.path_in_bucket
        )
        self.output_json = "/tmp/output.json"
        self.prepare_commands = []
        self._wait_for_nodes_timeout = 0

    def prepare_remote_env(self):
        # Copy anyscale job script to working dir
        job_script = os.path.join(os.path.dirname(__file__), "_anyscale_job_wrapper.py")
        if os.path.exists("anyscale_job_wrapper.py"):
            os.unlink("anyscale_job_wrapper.py")
        os.link(job_script, "anyscale_job_wrapper.py")

        super().prepare_remote_env()

    def run_prepare_command(
        self, command: str, env: Optional[Dict] = None, timeout: float = 3600.0
    ):
        self.prepare_commands.append((command, env, timeout))

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        self._wait_for_nodes_timeout = timeout
        self.job_manager.cluster_startup_timeout += timeout
        super().wait_for_nodes(num_nodes, timeout)

    def save_metrics(self, start_time: float, timeout: float = 900):
        # Handled in run_command
        return

    def _handle_command_output(
        self, job_status_code: int, error: str, raise_on_timeout: bool = True
    ):
        if job_status_code == -2:
            raise JobBrokenError(f"Job state is 'BROKEN' with error:\n{error}\n")

        if job_status_code == -3:
            raise JobTerminatedError(
                "Job entered 'TERMINATED' state (it was terminated "
                "manually or Ray was stopped):"
                f"\n{error}\n"
            )

        if job_status_code == -4:
            raise JobTerminatedBeforeStartError(
                "Job entered 'TERMINATED' state before it started "
                "(most likely due to inability to provision required nodes; "
                "otherwise it was terminated manually or Ray was stopped):"
                f"\n{error}\n"
            )

        # First try to obtain the output.json from S3.
        # If that fails, try logs.
        try:
            output_json = self.fetch_output()
        except Exception:
            logger.exception("Exception when obtaining output from S3.")
            try:
                logs = self.get_last_logs()
                output_json = re.search(r"### JSON \|([^\|]*)\| ###", logs)
                output_json = json.loads(output_json.group(1))
            except Exception:
                output_json = None

        workload_status_code = None
        if output_json:
            logger.info(f"Output: {output_json}")
            workload_status_code = output_json["return_code"]
            workload_time_taken = output_json["workload_time_taken"]
            prepare_return_codes = output_json["prepare_return_codes"]
            last_prepare_time_taken = output_json["last_prepare_time_taken"]

            if prepare_return_codes and prepare_return_codes[-1] != 0:
                if prepare_return_codes[-1] == TIMEOUT_RETURN_CODE:
                    raise PrepareCommandTimeout(
                        "Prepare command timed out after "
                        f"{last_prepare_time_taken} seconds."
                    )
                raise PrepareCommandError(
                    f"Prepare command '{self.prepare_commands[-1]}' returned "
                    f"non-success status: {prepare_return_codes[-1]} with error:"
                    f"\n{error}\n"
                )
        else:
            raise JobNoLogsError("Could not obtain logs for the job.")

        if workload_status_code == TIMEOUT_RETURN_CODE:
            if not raise_on_timeout:
                # Expected - treat as success.
                return

            raise TestCommandTimeout(
                f"Command timed out after {workload_time_taken} seconds."
            )

        if workload_status_code is not None and workload_status_code != 0:
            raise TestCommandError(
                f"Command returned non-success status: {workload_status_code} with "
                f"error:\n{error}\n"
            )

        if job_status_code == -1:
            raise JobStartupFailed(
                "Job returned non-success state: 'OUT_OF_RETRIES' "
                "(command has not been ran or no logs could have been obtained) "
                f"with error:\n{error}\n"
            )

    @property
    def command_env(self):
        env = super().command_env
        # Make sure we don't buffer stdout so we don't lose any logs.
        env["PYTHONUNBUFFERED"] = "1"
        return env

    def run_command(
        self,
        command: str,
        env: Optional[Dict] = None,
        timeout: float = 3600.0,
        raise_on_timeout: bool = True,
    ) -> float:
        prepare_command_strs = []
        prepare_command_timeouts = []
        # Convert the prepare commands, envs and timeouts into shell-compliant
        # strings that can be passed to the wrapper script
        for prepare_command, prepare_env, prepare_timeout in self.prepare_commands:
            prepare_env = self.get_full_command_env(prepare_env)
            env_str = _get_env_str(prepare_env)
            prepare_command_strs.append(f"{env_str} {prepare_command}")
            prepare_command_timeouts.append(prepare_timeout)

        prepare_commands_shell = " ".join(
            shlex.quote(str(x)) for x in prepare_command_strs
        )
        prepare_commands_timeouts_shell = " ".join(
            shlex.quote(str(x)) for x in prepare_command_timeouts
        )

        full_env = self.get_full_command_env(env)
        env_str = _get_env_str(full_env)

        no_raise_on_timeout_str = (
            " --test-no-raise-on-timeout" if not raise_on_timeout else ""
        )
        full_command = (
            f"{env_str}python anyscale_job_wrapper.py '{command}' "
            f"--test-workload-timeout {timeout}{no_raise_on_timeout_str} "
            "--results-s3-uri "
            f"'{join_s3_paths(self.upload_path, self.result_output_json)}' "
            "--metrics-s3-uri "
            f"'{join_s3_paths(self.upload_path, self.metrics_output_json)}' "
            "--output-s3-uri "
            f"'{join_s3_paths(self.upload_path, self.output_json)}' "
            f"--prepare-commands {prepare_commands_shell} "
            f"--prepare-commands-timeouts {prepare_commands_timeouts_shell}"
        )
        job_status_code, time_taken = self.job_manager.run_and_wait(
            full_command,
            full_env,
            working_dir=".",
            upload_path=self.upload_path,
            # The timeout set here is just for the prepare commands + test workload
            # WITHOUT wait for nodes time included, as that is set separately.
            # Since wait for nodes is a part of prepare_commands, we manually
            # subtract the timeout for it here.
            timeout=int(timeout)
            + sum(prepare_command_timeouts)
            - self._wait_for_nodes_timeout,
        )
        try:
            error = self.job_manager.last_job_result.state.error
        except AttributeError:
            error = None

        self._handle_command_output(
            job_status_code, error, raise_on_timeout=raise_on_timeout
        )

        return time_taken

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
            raise FetchResultError(f"Could not fetch results from session: {e}") from e

    def fetch_results(self) -> Dict[str, Any]:
        return self._fetch_json(
            join_s3_paths(self.path_in_bucket, self.result_output_json)
        )

    def fetch_metrics(self) -> Dict[str, Any]:
        return self._fetch_json(
            join_s3_paths(self.path_in_bucket, self.metrics_output_json)
        )

    def fetch_output(self) -> Dict[str, Any]:
        return self._fetch_json(join_s3_paths(self.path_in_bucket, self.output_json))

    def cleanup(self):
        try:
            self.file_manager.delete(self.path_in_bucket, recursive=True)
        except Exception:
            # No big deal if we don't clean up, the bucket
            # is set to automatically expire objects anyway
            pass
