import os
import time
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple


from anyscale.sdk.anyscale_client.models import (
    CreateProductionJob,
    HaJobStates,
)

from ray_release.anyscale_util import get_cluster_name
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import CommandTimeout
from ray_release.logger import logger
from ray_release.util import (
    ANYSCALE_HOST,
    exponential_backoff_retry,
    anyscale_job_url,
    format_link,
)

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient  # noqa: F401


class JobManager:
    def __init__(self, cluster_manager: ClusterManager):
        self.job_id_pool: Dict[int, str] = dict()
        self.start_time: Dict[int, float] = dict()
        self.counter = 0
        self.cluster_manager = cluster_manager
        self.job_client = None
        self.last_job_id = None

    def _get_job_client(self) -> "JobSubmissionClient":
        from ray.job_submission import JobSubmissionClient  # noqa: F811

        if not self.job_client:
            self.job_client = JobSubmissionClient(
                self.cluster_manager.get_cluster_address()
            )
        return self.job_client

    def _run_job(
        self,
        cmd_to_run: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
    ) -> int:
        self.counter += 1
        command_id = self.counter
        env = os.environ.copy()
        env["RAY_ADDRESS"] = self.cluster_manager.get_cluster_address()
        env.setdefault("ANYSCALE_HOST", str(ANYSCALE_HOST))

        full_cmd = " ".join(f"{k}={v}" for k, v in env_vars.items()) + " " + cmd_to_run
        logger.info(f"Executing {cmd_to_run} with {env_vars} via ray job submit")

        job_client = self._get_job_client()

        runtime_env = None
        if working_dir:
            runtime_env = {"working_dir": working_dir}

        job_id = job_client.submit_job(
            # Entrypoint shell command to execute
            entrypoint=full_cmd,
            runtime_env=runtime_env,
        )
        self.last_job_id = job_id
        self.job_id_pool[command_id] = job_id
        self.start_time[command_id] = time.time()
        return command_id

    def _get_job_status_with_retry(self, command_id):
        job_client = self._get_job_client()
        return exponential_backoff_retry(
            lambda: job_client.get_job_status(self.job_id_pool[command_id]),
            retry_exceptions=Exception,
            initial_retry_delay_s=1,
            max_retries=3,
        )

    def _wait_job(self, command_id: int, timeout: int):
        from ray.job_submission import JobStatus  # noqa: F811

        start_time = time.monotonic()
        timeout_at = start_time + timeout
        next_status = start_time + 30

        while True:
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
            status = self._get_job_status_with_retry(command_id)
            if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
                break
            time.sleep(1)
        status = self._get_job_status_with_retry(command_id)
        # TODO(sang): Propagate JobInfo.error_type
        if status == JobStatus.SUCCEEDED:
            retcode = 0
        else:
            retcode = -1
        duration = time.time() - self.start_time[command_id]
        return retcode, duration

    def run_and_wait(
        self,
        cmd_to_run,
        env_vars,
        working_dir: Optional[str] = None,
        timeout: int = 120,
    ) -> Tuple[int, float]:
        cid = self._run_job(cmd_to_run, env_vars, working_dir=working_dir)
        return self._wait_job(cid, timeout)

    def get_last_logs(self):
        # return None
        job_client = self._get_job_client()
        return job_client.get_job_logs(self.last_job_id)


class AnyscaleJobManager:
    def __init__(self, cluster_manager: ClusterManager):
        self.start_time = None
        self.counter = 0
        self.cluster_manager = cluster_manager

    def _run_job(
        self,
        cmd_to_run: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
        upload_path: Optional[str] = None,
    ) -> None:
        env = os.environ.copy()
        # env["RAY_ADDRESS"] = self.cluster_manager.get_cluster_address()
        env.setdefault("ANYSCALE_HOST", str(ANYSCALE_HOST))

        full_cmd = " ".join(f"{k}={v}" for k, v in env_vars.items()) + " " + cmd_to_run
        logger.info(f"Executing {cmd_to_run} with {env_vars} via Anyscale job submit")

        anyscale_client = self.sdk

        runtime_env = None
        if working_dir:
            runtime_env = {"working_dir": working_dir}
            if upload_path:
                runtime_env["upload_path"] = upload_path

        job_response = anyscale_client.create_job(
            CreateProductionJob(
                name=self.cluster_manager.cluster_name,
                description=f"Smoke test: {self.cluster_manager.smoke_test}",
                project_id=self.cluster_manager.project_id,
                config=dict(
                    entrypoint=full_cmd,
                    runtime_env=runtime_env,
                    build_id=self.cluster_manager.cluster_env_build_id,
                    compute_config_id=self.cluster_manager.cluster_compute_id,
                    max_retries=0,
                ),
            ),
        )
        self.last_job_result = job_response.result
        self.cluster_manager.cluster_id = self.last_job_result.state.cluster_id
        self.start_time = time.time()

        logger.info(
            f"Link to job: " f"{format_link(anyscale_job_url(self.last_job_result.id))}"
        )

        return

    @property
    def job_id(self):
        return self.last_job_result.id

    @property
    def sdk(self):
        return self.cluster_manager.sdk

    @property
    def last_job_result(self):
        return self._last_job_result

    @last_job_result.setter
    def last_job_result(self, value):
        self._last_job_result = value
        cluster_id = value.state.cluster_id
        if cluster_id:
            self.cluster_manager.cluster_id = value.state.cluster_id
            self.cluster_manager.cluster_name = get_cluster_name(
                value.state.cluster_id, self.sdk
            )

    @property
    def last_job_status(self):
        return self._last_job_result.state.current_state

    def _get_job_status_with_retry(self):
        anyscale_client = self.cluster_manager.sdk
        return exponential_backoff_retry(
            lambda: anyscale_client.get_production_job(self.job_id),
            retry_exceptions=Exception,
            initial_retry_delay_s=1,
            max_retries=3,
        ).result

    def __del__(self):
        self._terminate_job()

    def _terminate_job(self, raise_exceptions: bool = False):
        try:
            self.sdk.terminate_job(self.job_id)
        except Exception:
            if raise_exceptions:
                raise

    def _wait_job(self, timeout: int):
        start_time = time.monotonic()
        timeout_at = start_time + timeout
        next_status = start_time + 30

        while True:
            now = time.monotonic()
            if now >= timeout_at:
                self._terminate_job()
                raise CommandTimeout(f"Job timed out after {timeout} seconds.")

            if now >= next_status:
                logger.info(
                    f"... job still running ..."
                    f"({int(now - start_time)} seconds) ..."
                )
                next_status += 30
            result = self._get_job_status_with_retry()
            self.last_job_result = result
            status = self.last_job_status
            if status in {
                HaJobStates.SUCCESS,
                HaJobStates.TERMINATED,
                HaJobStates.BROKEN,
                HaJobStates.OUT_OF_RETRIES,
            }:
                break
            time.sleep(1)
        result = self._get_job_status_with_retry()
        self.last_job_result = result
        status = self.last_job_status
        if status == HaJobStates.SUCCESS:
            retcode = 0
        elif status == HaJobStates.BROKEN:
            retcode = -2
        else:
            retcode = -1
        error = result.state.error
        duration = time.time() - self.start_time
        return retcode, duration, error

    def run_and_wait(
        self,
        cmd_to_run,
        env_vars,
        working_dir: Optional[str] = None,
        timeout: int = 120,
        upload_path: Optional[str] = None,
    ) -> Tuple[int, float]:
        self._run_job(
            cmd_to_run, env_vars, working_dir=working_dir, upload_path=upload_path
        )
        return self._wait_job(timeout)

    def get_last_logs(self):
        return None
