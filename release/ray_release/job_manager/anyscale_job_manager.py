import time
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple, List


import anyscale
from anyscale.sdk.anyscale_client.models import (
    CreateProductionJob,
    HaJobStates,
)
from ray_release.anyscale_util import get_cluster_name
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import (
    CommandTimeout,
    JobStartupTimeout,
    JobStartupFailed,
)
from ray_release.logger import logger
from ray_release.signal_handling import register_handler, unregister_handler
from ray_release.util import (
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
        self._duration = None

    def _run_job(
        self,
        cmd_to_run: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
        upload_path: Optional[str] = None,
        pip: Optional[List[str]] = None,
    ) -> None:
        env_vars_for_job = env_vars.copy()
        env_vars_for_job[
            "ANYSCALE_JOB_CLUSTER_ENV_NAME"
        ] = self.cluster_manager.cluster_env_name

        logger.info(
            f"Executing {cmd_to_run} with {env_vars_for_job} via Anyscale job submit"
        )

        anyscale_client = self.sdk

        runtime_env = {
            "env_vars": env_vars_for_job,
            "pip": pip or [],
        }
        if working_dir:
            runtime_env["working_dir"] = working_dir
            if upload_path:
                runtime_env["upload_path"] = upload_path

        try:
            job_response = anyscale_client.create_job(
                CreateProductionJob(
                    name=self.cluster_manager.cluster_name,
                    description=f"Smoke test: {self.cluster_manager.smoke_test}",
                    project_id=self.cluster_manager.project_id,
                    config=dict(
                        entrypoint=cmd_to_run,
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
        self._duration = time.time() - self.start_time
        return retcode, self._duration

    def run_and_wait(
        self,
        cmd_to_run,
        env_vars,
        working_dir: Optional[str] = None,
        timeout: int = 120,
        upload_path: Optional[str] = None,
        pip: Optional[List[str]] = None,
    ) -> Tuple[int, float]:
        self._run_job(
            cmd_to_run,
            env_vars,
            working_dir=working_dir,
            upload_path=upload_path,
            pip=pip,
        )
        return self._wait_job(timeout)

    def _get_ray_logs(self) -> str:
        """
        Obtain the last few logs
        """
        if self.cluster_manager.log_streaming_limit == -1:
            return anyscale.job.get_logs(id=self.job_id)
        return anyscale.job.get_logs(
            id=self.job_id, max_lines=self.cluster_manager.log_streaming_limit
        )

    def get_last_logs(self):
        if not self.job_id:
            raise RuntimeError(
                "Job has not been started, therefore there are no logs to obtain."
            )

        if self._last_logs:
            return self._last_logs

        # Skip loading logs when the job ran for too long and collected too much logs.
        if self._duration is not None and self._duration > 4 * 3_600:
            return None

        ret = exponential_backoff_retry(
            self._get_ray_logs,
            retry_exceptions=Exception,
            initial_retry_delay_s=30,
            max_retries=3,
        )
        if ret and not self.in_progress:
            self._last_logs = ret
        return ret
