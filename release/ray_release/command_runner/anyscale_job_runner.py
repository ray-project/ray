import json
import os
import re
import shlex
import tempfile
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray_release.cloud_util import (
    convert_abfss_uri_to_https,
    generate_tmp_cloud_storage_path,
    upload_working_dir_to_azure,
)
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.job_runner import JobRunner
from ray_release.exception import (
    FetchResultError,
    JobBrokenError,
    JobNoLogsError,
    JobOutOfRetriesError,
    JobTerminatedBeforeStartError,
    JobTerminatedError,
    PrepareCommandError,
    PrepareCommandTimeout,
    TestCommandError,
    TestCommandTimeout,
)
from ray_release.file_manager.job_file_manager import JobFileManager
from ray_release.job_manager import AnyscaleJobManager
from ray_release.logger import logger
from ray_release.util import (
    AZURE_CLOUD_STORAGE,
    AZURE_STORAGE_CONTAINER,
    S3_CLOUD_STORAGE,
    get_anyscale_sdk,
)

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

TIMEOUT_RETURN_CODE = 124


def _join_cloud_storage_paths(*paths: str):
    paths = list(paths)
    if len(paths) > 1:
        for i in range(1, len(paths)):
            while paths[i][0] == "/":
                paths[i] = paths[i][1:]
    joined_path = os.path.join(*paths)
    while joined_path[-1] == "/":
        joined_path = joined_path[:-1]
    return joined_path


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
        artifact_path: Optional[str] = None,
    ):
        super().__init__(
            cluster_manager=cluster_manager,
            file_manager=file_manager,
            working_dir=working_dir,
        )
        self.sdk = sdk or get_anyscale_sdk()
        self.job_manager = AnyscaleJobManager(cluster_manager)

        self.last_command_scd_id = None
        self.path_in_bucket = _join_cloud_storage_paths(
            "working_dirs",
            self.cluster_manager.test.get_name().replace(" ", "_"),
            generate_tmp_cloud_storage_path(),
        )
        # The root cloud storage bucket path. result, metric, artifact files
        # will be uploaded to under it on cloud storage.
        cloud_storage_provider = os.environ.get(
            "ANYSCALE_CLOUD_STORAGE_PROVIDER",
            S3_CLOUD_STORAGE,
        )

        if cloud_storage_provider == AZURE_CLOUD_STORAGE:
            # Azure ABFSS involves container and account name in the path
            # and in a specific format/order.
            self.upload_path = _join_cloud_storage_paths(
                f"{AZURE_CLOUD_STORAGE}://{AZURE_STORAGE_CONTAINER}@{self.file_manager.bucket}.dfs.core.windows.net",
                self.path_in_bucket,
            )
        else:
            self.upload_path = _join_cloud_storage_paths(
                f"{cloud_storage_provider}://{self.file_manager.bucket}",
                self.path_in_bucket,
            )
        self.output_json = "/tmp/output.json"
        self.prepare_commands = []
        self._wait_for_nodes_timeout = 0

        self._results_uploaded = True
        self._metrics_uploaded = True

        # artifact related
        # user provided path to where they write the artifact to.
        self._artifact_path = artifact_path
        self._artifact_uploaded = artifact_path is not None

    def prepare_remote_env(self):
        self._copy_script_to_working_dir("anyscale_job_wrapper.py")
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

            # If we know results/metrics were not uploaded, we can fail fast
            # fetching later.
            self._results_uploaded = output_json["uploaded_results"]
            self._metrics_uploaded = output_json["uploaded_metrics"]
            self._artifact_uploaded = output_json["uploaded_artifact"]

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
            raise JobOutOfRetriesError(
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
        pip: Optional[List[str]] = None,
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

        no_raise_on_timeout_str = (
            " --test-no-raise-on-timeout" if not raise_on_timeout else ""
        )
        results_cloud_storage_uri = _join_cloud_storage_paths(
            self.upload_path, self._RESULT_OUTPUT_JSON
        )
        metrics_cloud_storage_uri = _join_cloud_storage_paths(
            self.upload_path, self._METRICS_OUTPUT_JSON
        )
        output_cloud_storage_uri = _join_cloud_storage_paths(
            self.upload_path, self.output_json
        )
        upload_cloud_storage_uri = self.upload_path
        # Convert ABFSS URI to HTTPS URI for Azure
        # since azcopy doesn't support ABFSS.
        # azcopy is used to fetch these artifacts on Buildkite
        # after job is done.
        if self.upload_path.startswith(AZURE_CLOUD_STORAGE):
            results_cloud_storage_uri = convert_abfss_uri_to_https(
                results_cloud_storage_uri
            )
            metrics_cloud_storage_uri = convert_abfss_uri_to_https(
                metrics_cloud_storage_uri
            )
            output_cloud_storage_uri = convert_abfss_uri_to_https(
                output_cloud_storage_uri
            )
            upload_cloud_storage_uri = convert_abfss_uri_to_https(
                upload_cloud_storage_uri
            )
        full_command = (
            f"python anyscale_job_wrapper.py '{command}' "
            f"--test-workload-timeout {timeout}{no_raise_on_timeout_str} "
            "--results-cloud-storage-uri "
            f"'{results_cloud_storage_uri}' "
            "--metrics-cloud-storage-uri "
            f"'"
            f"{metrics_cloud_storage_uri}' "
            "--output-cloud-storage-uri "
            f"'{output_cloud_storage_uri}' "
            f"--upload-cloud-storage-uri '{upload_cloud_storage_uri}' "
            f"--prepare-commands {prepare_commands_shell} "
            f"--prepare-commands-timeouts {prepare_commands_timeouts_shell} "
        )
        if self._artifact_path:
            full_command += f"--artifact-path '{self._artifact_path}' "

        timeout = min(
            (self.cluster_manager.maximum_uptime_minutes - 1) * 60,
            # The timeout set here is just for the prepare commands + test workload
            # WITHOUT wait for nodes time included, as that is set separately.
            # Since wait for nodes is a part of prepare_commands, we manually
            # subtract the timeout for it here.
            # We also add 15 mins for upload & metrics collection.
            timeout
            + sum(prepare_command_timeouts)
            - self._wait_for_nodes_timeout
            + 900,
        )
        working_dir = "."
        # If running on Azure, upload working dir to Azure blob storage first
        if self.upload_path.startswith(AZURE_CLOUD_STORAGE):
            azure_file_path = upload_working_dir_to_azure(
                working_dir=os.getcwd(), azure_directory_uri=self.upload_path
            )
            working_dir = azure_file_path
            logger.info(f"Working dir uploaded to {working_dir}")

        job_status_code, time_taken = self.job_manager.run_and_wait(
            full_command,
            full_env,
            working_dir=working_dir,
            upload_path=self.upload_path,
            timeout=int(timeout),
            pip=pip,
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
            self.file_manager.download_from_cloud(
                path, tmpfile, delete_after_download=True
            )

            with open(tmpfile, "rt") as f:
                content = f.read()

            try:
                data = json.loads(content)
            except json.JSONDecodeError as e:
                logger.info(f"Result content = {content}")
                raise e

            os.unlink(tmpfile)
            return data
        except Exception as e:
            raise FetchResultError(f"Could not fetch results from session: {e}") from e

    def fetch_results(self) -> Dict[str, Any]:
        if not self._results_uploaded:
            raise FetchResultError(
                "Could not fetch results from session as they were not uploaded."
            )
        return self._fetch_json(
            _join_cloud_storage_paths(self.path_in_bucket, self._RESULT_OUTPUT_JSON)
        )

    def fetch_metrics(self) -> Dict[str, Any]:
        if not self._metrics_uploaded:
            raise FetchResultError(
                "Could not fetch metrics from session as they were not uploaded."
            )
        return self._fetch_json(
            _join_cloud_storage_paths(self.path_in_bucket, self._METRICS_OUTPUT_JSON)
        )

    def fetch_artifact(self):
        """Fetch artifact (file) from `self._artifact_path` on Anyscale cluster
        head node.

        Note, an implementation detail here is that by the time this function is called,
        the artifact file is already present in s3 bucket by the name of
        `self._USER_GENERATED_ARTIFACT`. This is because, the uploading to s3 portion is
        done by `_anyscale_job_wrapper`.

        The fetched artifact will be placed under `self._DEFAULT_ARTIFACTS_DIR`,
        which will ultimately show up in buildkite Artifacts UI tab.
        The fetched file will have the same filename and extension as the one
        on Anyscale cluster head node (same as `self._artifact_path`).
        """
        if not self._artifact_uploaded:
            raise FetchResultError(
                "Could not fetch artifact from session as they "
                "were either not generated or not uploaded."
            )
        # first make sure that `self._DEFAULT_ARTIFACTS_DIR` exists.
        if not os.path.exists(self._DEFAULT_ARTIFACTS_DIR):
            os.makedirs(self._DEFAULT_ARTIFACTS_DIR, 0o755)

        # we use the same artifact file name and extension specified by user
        # and put it under `self._DEFAULT_ARTIFACTS_DIR`.
        artifact_file_name = os.path.basename(self._artifact_path)
        self.file_manager.download_from_cloud(
            _join_cloud_storage_paths(
                self.path_in_bucket, self._USER_GENERATED_ARTIFACT
            ),
            os.path.join(self._DEFAULT_ARTIFACTS_DIR, artifact_file_name),
        )

    def fetch_output(self) -> Dict[str, Any]:
        return self._fetch_json(
            _join_cloud_storage_paths(self.path_in_bucket, self.output_json),
        )

    def cleanup(self):
        # We piggy back on s3 retention policy for clean up instead of doing this
        # ourselves. We find many cases where users want the data to be available
        # for a short-while for debugging purpose.
        pass
