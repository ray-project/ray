import os
import time
from typing import Any, Dict, Optional, Tuple


from anyscale.sdk.anyscale_client.models import (
    CreateProductionJob,
    HaJobStates,
)

from ray_release.anyscale_util import LAST_LOGS_LENGTH, get_cluster_name
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import CommandTimeout
from ray_release.logger import logger
from ray_release.util import (
    ANYSCALE_HOST,
    exponential_backoff_retry,
    anyscale_job_url,
    format_link,
)


class AnyscaleJobManager:
    def __init__(self, cluster_manager: ClusterManager):
        self.start_time = None
        self.counter = 0
        self.cluster_manager = cluster_manager
        self._last_logs = None
        self.wait_for_nodes_timeout = 0

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
        if self.wait_for_nodes_timeout > 0:
            timeout_at = start_time + self.wait_for_nodes_timeout
        else:
            timeout_at = start_time + timeout
        next_status = start_time + 30
        job_running = False

        while True:
            now = time.monotonic()
            if now >= timeout_at:
                self._get_last_logs()
                self._terminate_job()
                raise CommandTimeout(f"Job timed out after {timeout} seconds.")

            if now >= next_status:
                if job_running:
                    msg = "... job still running ..."
                else:
                    msg = "... job not yet running ..."
                logger.info(f"{msg}({int(now - start_time)} seconds) ...")
                next_status += 30

            result = self._get_job_status_with_retry()
            self.last_job_result = result
            status = self.last_job_status

            if not job_running and status in {
                HaJobStates.RUNNING,
                HaJobStates.ERRORED,
            }:
                logger.info(f"... job started ...({int(now - start_time)} seconds) ...")
                job_running = True
                if self.wait_for_nodes_timeout > 0:
                    timeout_at = now + timeout

            if status == HaJobStates.ERRORED:
                self._get_last_logs()

            if status in {
                HaJobStates.SUCCESS,
                HaJobStates.TERMINATED,
                HaJobStates.BROKEN,
                HaJobStates.OUT_OF_RETRIES,
            }:
                self._get_last_logs()
                break
            time.sleep(1)

        result = self._get_job_status_with_retry()
        self.last_job_result = result
        status = self.last_job_status
        if status == HaJobStates.SUCCESS:
            retcode = 0
        elif status == HaJobStates.BROKEN:
            retcode = -2
        elif status == HaJobStates.TERMINATED:
            retcode = -3
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

    def _get_last_logs(self):
        try:
            self._last_logs = "\n".join(
                self.sdk.fetch_production_job_logs(job_id=self.job_id).split("\n")[
                    -LAST_LOGS_LENGTH:
                ]
            )
        except Exception as e:
            if not self._last_logs:
                self._last_logs = e
        return self._last_logs

    def get_last_logs(self):
        return self._last_logs
