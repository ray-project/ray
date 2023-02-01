import io
import os
import time
from contextlib import redirect_stdout, redirect_stderr
from typing import Any, Dict, Optional, Tuple


from anyscale.sdk.anyscale_client.models import (
    CreateProductionJob,
    HaJobStates,
)
from anyscale.controllers.job_controller import JobController, terminal_state

from ray_release.anyscale_util import LAST_LOGS_LENGTH, get_cluster_name
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import (
    CommandTimeout,
    JobStartupTimeout,
    JobStartupFailed,
)
from ray_release.logger import logger
from ray_release.util import (
    ANYSCALE_HOST,
    exponential_backoff_retry,
    anyscale_job_url,
    format_link,
)

job_status_to_return_code = {
    HaJobStates.SUCCESS: 0,
    HaJobStates.OUT_OF_RETRIES: -1,
    HaJobStates.BROKEN: -2,
    HaJobStates.TERMINATED: -3,
}


class AnyscaleJobManager:
    def __init__(self, cluster_manager: ClusterManager):
        self.start_time = None
        self.counter = 0
        self.cluster_manager = cluster_manager
        self._last_job_result = None
        self._last_logs = None
        self.cluster_startup_timeout = 600

    def _run_job(
        self,
        cmd_to_run: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
        upload_path: Optional[str] = None,
    ) -> None:
        env = os.environ.copy()
        env.setdefault("ANYSCALE_HOST", str(ANYSCALE_HOST))

        full_cmd = " ".join(f"{k}={v}" for k, v in env_vars.items()) + " " + cmd_to_run
        logger.info(f"Executing {cmd_to_run} with {env_vars} via Anyscale job submit")

        anyscale_client = self.sdk

        runtime_env = None
        if working_dir:
            runtime_env = {"working_dir": working_dir}
            if upload_path:
                runtime_env["upload_path"] = upload_path

        try:
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
        except Exception as e:
            raise JobStartupFailed(
                "Error starting job with name "
                f"{self.cluster_manager.cluster_name}: "
                f"{e}"
            ) from e

        self.last_job_result = job_response.result
        self.cluster_manager.cluster_id = self.last_job_result.state.cluster_id
        self.start_time = time.time()

        logger.info(
            f"Link to job: " f"{format_link(anyscale_job_url(self.last_job_result.id))}"
        )

        return

    @property
    def job_id(self):
        if not self.last_job_result:
            return None
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
        if not self.last_job_result:
            return
        try:
            self.sdk.terminate_job(self.job_id)
        except Exception:
            if raise_exceptions:
                raise

    def _wait_job(self, timeout: int):
        start_time = time.monotonic()
        # Waiting for cluster needs to be a part of the whole
        # run.
        timeout_at = start_time + self.cluster_startup_timeout
        next_status = start_time + 30
        job_running = False

        while True:
            now = time.monotonic()
            if now >= timeout_at:
                self._terminate_job()
                if not job_running:
                    raise JobStartupTimeout(
                        "Cluster did not start within "
                        f"{self.cluster_startup_timeout} seconds."
                    )
                raise CommandTimeout(f"Job timed out after {timeout} seconds.")

            if now >= next_status:
                if job_running:
                    msg = "... job still running ..."
                else:
                    msg = "... job not yet running ..."
                logger.info(
                    f"{msg}({int(now - start_time)} seconds, "
                    f"{int(timeout_at - now)} seconds to timeout) ..."
                )
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
                # If job has started, we switch from waiting for cluster
                # to the actual command (incl. prepare commands) timeout.
                timeout_at = now + timeout

            if status in terminal_state:
                logger.info(f"Job entered terminal state {status}.")
                break
            time.sleep(1)

        result = self._get_job_status_with_retry()
        self.last_job_result = result
        status = self.last_job_status
        assert status in terminal_state
        retcode = job_status_to_return_code[status]
        duration = time.time() - self.start_time
        self._last_logs = None
        return retcode, duration

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
        if not self.job_id:
            raise RuntimeError(
                "Job has not been started, therefore there are no logs to obtain."
            )

        if self._last_logs:
            return self._last_logs

        # TODO: replace with an actual API call.
        def _get_logs():
            job_controller = JobController()
            buf = io.StringIO()
            with redirect_stdout(buf), redirect_stderr(None):
                job_controller.logs(
                    job_id=self.job_id,
                    should_follow=False,
                )
                print("", flush=True)
            output = buf.getvalue().strip()
            assert "### Starting ###" in output, "No logs fetched"
            return "\n".join(output.splitlines()[-LAST_LOGS_LENGTH * 3 :])

        ret = exponential_backoff_retry(
            _get_logs,
            retry_exceptions=Exception,
            initial_retry_delay_s=30,
            max_retries=5,
        )
        if ret:
            self._last_logs = ret
        return ret
