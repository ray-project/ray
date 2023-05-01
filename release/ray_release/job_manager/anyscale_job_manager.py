import os
import time
import subprocess
import tempfile
from collections import deque
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple


from anyscale.sdk.anyscale_client.models import (
    CreateProductionJob,
    HaJobStates,
)
from ray_release.anyscale_util import LAST_LOGS_LENGTH, get_cluster_name
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import (
    CommandTimeout,
    JobStartupTimeout,
    JobStartupFailed,
)
from ray_release.logger import logger
from ray_release.signal_handling import register_handler, unregister_handler
from ray_release.util import (
    ANYSCALE_HOST,
    ERROR_LOG_PATTERNS,
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
terminal_state = set(job_status_to_return_code.keys())


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
        self.start_time = time.time()

        logger.info(f"Link to job: " f"{format_link(self.job_url)}")
        return

    @property
    def sdk(self):
        return self.cluster_manager.sdk

    @property
    def last_job_result(self):
        return self._last_job_result

    @last_job_result.setter
    def last_job_result(self, value):
        cluster_id = value.state.cluster_id
        # Set this only once.
        if self.cluster_manager.cluster_id is None and cluster_id:
            self.cluster_manager.cluster_id = value.state.cluster_id
            self.cluster_manager.cluster_name = get_cluster_name(
                value.state.cluster_id, self.sdk
            )
        self._last_job_result = value

    @property
    def job_id(self) -> Optional[str]:
        if not self.last_job_result:
            return None
        return self.last_job_result.id

    @property
    def job_url(self) -> Optional[str]:
        if not self.job_id:
            return None
        return anyscale_job_url(self.job_id)

    @property
    def last_job_status(self) -> Optional[HaJobStates]:
        if not self.last_job_result:
            return None
        return self.last_job_result.state.current_state

    @property
    def in_progress(self) -> bool:
        return self.last_job_result and self.last_job_status not in terminal_state

    def _get_job_status_with_retry(self):
        anyscale_client = self.cluster_manager.sdk
        return exponential_backoff_retry(
            lambda: anyscale_client.get_production_job(self.job_id),
            retry_exceptions=Exception,
            initial_retry_delay_s=1,
            max_retries=3,
        ).result

    def _terminate_job(self, raise_exceptions: bool = False):
        if not self.in_progress:
            return
        logger.info(f"Terminating job {self.job_id}...")
        try:
            self.sdk.terminate_job(self.job_id)
            logger.info(f"Job {self.job_id} terminated!")
        except Exception:
            msg = f"Couldn't terminate job {self.job_id}!"
            if raise_exceptions:
                logger.error(msg)
                raise
            else:
                logger.exception(msg)

    @contextmanager
    def _terminate_job_context(self):
        """
        Context to ensure the job is terminated.

        Aside from running _terminate_job at exit, it also registers
        a signal handler to terminate the job if the program is interrupted
        or terminated. It restores the original handlers on exit.
        """

        def terminate_handler(signum, frame):
            self._terminate_job()

        register_handler(terminate_handler)

        yield

        self._terminate_job()
        unregister_handler(terminate_handler)

    def _wait_job(self, timeout: int):
        # The context ensures the job always either finishes normally
        # or is terminated.
        with self._terminate_job_context():
            assert self.job_id, "Job must have been started"

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
                        f"{int(timeout_at - now)} seconds to job timeout) ..."
                    )
                    next_status += 30

                result = self._get_job_status_with_retry()
                self.last_job_result = result
                status = self.last_job_status

                if not job_running and status in {
                    HaJobStates.RUNNING,
                    HaJobStates.ERRORED,
                }:
                    logger.info(
                        f"... job started ...({int(now - start_time)} seconds) ..."
                    )
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
        if status == HaJobStates.TERMINATED and not job_running:
            # Soft infra error
            retcode = -4
        else:
            retcode = job_status_to_return_code[status]
        duration = time.time() - self.start_time
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

    def _get_ray_logs(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Obtain any ray logs that contain keywords that indicate a crash, such as
        ERROR or Traceback
        """
        tmpdir = tempfile.mktemp()
        try:
            subprocess.check_output(
                [
                    "anyscale",
                    "logs",
                    "cluster",
                    "--id",
                    self.cluster_manager.cluster_id,
                    "--head-only",
                    "--download",
                    "--download-dir",
                    tmpdir,
                ]
            )
        except Exception as e:
            logger.log(f"Failed to download logs from anyscale {e}")
            return None
        return AnyscaleJobManager._find_job_driver_and_ray_error_logs(tmpdir)

    @staticmethod
    def _find_job_driver_and_ray_error_logs(
        tmpdir: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        # Ignored some ray files that do not crash ray despite having exceptions
        ignored_ray_files = [
            "monitor.log",
            "event_AUTOSCALER.log",
            "event_JOBS.log",
        ]
        error_output = None
        job_driver_output = None
        matched_pattern_count = 0
        for root, _, files in os.walk(tmpdir):
            for file in files:
                if file in ignored_ray_files:
                    continue
                with open(os.path.join(root, file)) as lines:
                    output = "".join(deque(lines, maxlen=3 * LAST_LOGS_LENGTH))
                    # job-driver logs
                    if file.startswith("job-driver-"):
                        job_driver_output = output
                        continue
                    # ray error logs, favor those that match with the most number of
                    # error patterns
                    if (
                        len([error for error in ERROR_LOG_PATTERNS if error in output])
                        > matched_pattern_count
                    ):
                        error_output = output
        return job_driver_output, error_output

    def get_last_logs(self):
        if not self.job_id:
            raise RuntimeError(
                "Job has not been started, therefore there are no logs to obtain."
            )

        if self._last_logs:
            return self._last_logs

        def _get_logs():
            job_driver_log, ray_error_log = self._get_ray_logs()
            assert job_driver_log or ray_error_log, "No logs fetched"
            if job_driver_log:
                return job_driver_log
            else:
                return ray_error_log

        ret = exponential_backoff_retry(
            _get_logs,
            retry_exceptions=Exception,
            initial_retry_delay_s=30,
            max_retries=3,
        )
        if ret and not self.in_progress:
            self._last_logs = ret
        return ret
