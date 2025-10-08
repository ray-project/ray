from typing import Dict, Any, Optional
import time
from ray_release.logger import logger
from ray_release.exception import (
    JobStartupFailed,
    JobStartupTimeout,
    CommandTimeout,
)
from ray_release.util import exponential_backoff_retry
from anyscale.job.models import JobConfig, JobState, ComputeConfig
import anyscale

METRICS_COLLECTION_TIMEOUT = 15 * 60  # 15 mins for uploading metrics to cloud
TERMINAL_STATES = [
    JobState.FAILED,
    JobState.SUCCEEDED,
    JobState.UNKNOWN,
]
job_state_to_return_code = {
    JobState.SUCCEEDED: 0,
    JobState.UNKNOWN: -2,
    JobState.FAILED: -3,
}


class NewAnyscaleJobManager:
    def __init__(
        self,
        start_time: float,
        project_name: str,
        cloud_name: str,
        image: str,
        compute_config: ComputeConfig,
        log_streaming_limit: int,
        command_timeout: int,
        prepare_command_timeout: int,
    ):
        self.start_time = start_time
        self.timeout = command_timeout
        self.project_name = project_name
        self.cloud_name = cloud_name
        self.image = image
        self.compute_config = None
        self.log_streaming_limit = None
        self.prepare_command_timeout = prepare_command_timeout

        self.job_id = None
        self.duration = None
        self.last_job_result = None
        self.last_job_status = None
        self.last_logs = None

    @property
    def in_progress(self):
        return self.last_job_result and self.last_job_status not in TERMINAL_STATES

    @property
    def last_job_status(self) -> Optional[JobState]:
        if not self.last_job_result:
            return None
        return self.last_job_result.state

    def run_job(
        self,
        entrypoint: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
    ):
        try:
            anyscale_job_config = JobConfig(
                cloud=self.cloud_name,
                project=self.project_name,
                image_uri=self.image,
                compute_config=self.compute_config_name,
                entrypoint=entrypoint,
                env_vars=env_vars,
                working_dir=working_dir,
            )
            self.job_id = anyscale.job.submit(anyscale_job_config)
        except Exception as e:
            raise JobStartupFailed(f"Error starting job: {e}")
        self.last_job_result = anyscale.job.status(id=self.job_id)
        self.start_time = time.time()
        return

    def terminate_job(self, raise_exceptions: bool = False):
        if not self.in_progress:
            return
        logger.info(f"Terminating job {self.job_id}")
        try:
            terminated_job_id = anyscale.job.terminate(id=self.job_id)
            if terminated_job_id:
                logger.info(f"Job {self.job_id} terminated successfully")
        except Exception as e:
            msg = f"Error terminating job {self.job_id}: {e}"
            if raise_exceptions:
                logger.error(msg)
                raise
            logger.exception(msg)

    def get_job_status_with_retry(self):
        return exponential_backoff_retry(
            lambda: anyscale.job.status(id=self.job_id),
            retry_exceptions=Exception,
            initial_retry_delay_s=1,
            max_retries=3,
        )

    def wait_job(self, timeout: int):
        assert self.job_id, "Job must have been started"
        start_time = time.monotonic()

        timeout_at = start_time + timeout
        next_status_check_time = start_time + 30
        job_running = False

        while True:
            now = time.monotonic()
            if now >= timeout_at:
                self.terminate_job()
                if not job_running:
                    raise JobStartupTimeout(f"Job didn't start in {timeout} seconds")
                raise CommandTimeout(f"Job timed out after {timeout} seconds")

            if now >= next_status_check_time:
                if job_running:
                    msg = "... job still running ..."
                else:
                    msg = "... job not yet running ..."
                logger.info(
                    f"{msg}{int(now - start_time)} seconds, "
                    f"{int(timeout_at - now)} seconds to job timeout) ..."
                )
                next_status_check_time += 30

            result = self.get_job_status_with_retry()
            self.last_job_result = result
            job_status = self.last_job_status
            if not job_running and job_status == JobState.RUNNING:
                logger.info(f"Job {self.job_id} started")
                job_running = True
                timeout_at = now + timeout

            if job_status in TERMINAL_STATES:
                logger.info(f"Job entered terminal state {job_status}")
                break
            time.sleep(1)

        result = self.get_job_status_with_retry()
        self.last_job_result = result
        job_status = self.last_job_status
        assert job_status in TERMINAL_STATES
        if job_status in [JobState.FAILED, JobState.UNKNOWN] and not job_running:
            retcode = -4
        else:
            retcode = job_state_to_return_code[job_status]
        self.duration = time.time() - self.start_time
        return retcode, self.duration

    def run_and_wait(
        self,
        entrypoint: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
    ):
        timeout = self.timeout + self.prepare_command_timeout
        self.run_job(entrypoint, env_vars, working_dir)
        return self.wait_job(timeout)

    def get_ray_logs(self) -> str:
        if not self.log_streaming_limit:
            return anyscale.job.get_logs(id=self.job_id)
        return anyscale.job.get_logs(id=self.job_id, max_lines=self.log_streaming_limit)

    def get_last_logs(self) -> str:
        if not self.job_id:
            raise RuntimeError("Job not started")
        if self.last_logs:
            return self.last_logs

        # Skip loading logs when the job ran for too long and collected too much logs.
        if self.duration is not None and self.duration > 4 * 3600:
            return None

        ret = exponential_backoff_retry(
            self.get_ray_logs,
            retry_exceptions=Exception,
            initial_retry_delay_s=30,
            max_retries=3,
        )
        if ret and not self.in_progress:
            self.last_logs = ret
        return ret
