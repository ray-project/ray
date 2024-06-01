import asyncio
import logging
import os
import random
import string
from typing import Any, Dict, Iterator, Optional, Union

from ray._private.gcs_utils import GcsAioClient
from ray.actor import ActorHandle
from ray.dashboard.consts import (
    RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
    DEFAULT_JOB_START_TIMEOUT_SECONDS,
)
from ray.dashboard.modules.job.common import (
    JobInfo,
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.dashboard.modules.job.job_supervisor import JobSupervisor
from ray.job_submission import JobStatus

logger = logging.getLogger(__name__)


def generate_job_id() -> str:
    """Returns a job_id of the form 'raysubmit_XYZ'.

    Prefixed with 'raysubmit' to avoid confusion with Ray JobID (driver ID).
    """
    rand = random.SystemRandom()
    possible_characters = list(
        set(string.ascii_letters + string.digits)
        - {"I", "l", "o", "O", "0"}  # No confusing characters
    )
    id_part = "".join(rand.choices(possible_characters, k=16))
    return f"raysubmit_{id_part}"


class JobManager:
    """Job Manager is a component managing full lifecycle of Ray jobs
    from creation to completion providing avenues for monitoring & observability.

    NOTE: Job Manager is a stateful component: for every running Ray job it holds
          corresponding `JobSupervisor` that provides for
            - Monitoring & updating job's status in GCS
            - Managing of job's lifecycle
    """

    WAIT_FOR_ACTOR_DEATH_TIMEOUT_S = 0.1

    def __init__(self, gcs_aio_client: GcsAioClient, logs_dir: str):
        self._gcs_aio_client = gcs_aio_client
        self._job_info_client = JobInfoStorageClient(gcs_aio_client)
        self._gcs_address = gcs_aio_client.address

        self._log_client = JobLogStorageClient()
        self._logs_dir = logs_dir

        self._job_supervisors: Dict[str, JobSupervisor] = {}

        # TODO add job supervisors recovery on restart

    async def submit_job(
        self,
        *,
        entrypoint: str,
        submission_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_memory: Optional[int] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
        _start_signal_actor: Optional[ActorHandle] = None,
    ) -> str:
        """
        Submits Ray Job for execution on a Ray cluster.

        Ray Job execution workflow looks like following:

        JobManager
            |--> JobSupervisor
                    |--> JobExecutor (Actor on Head/Worker-node)
                            |--> (subprocess) Job's driver entrypoint

            1. JobManager instantiates JobSupervisor actor (guaranteed to be running on a Head-node) orchestrating
                execution (and monitoring)
            2. JobSupervisor upon request launches JobExecutor and "joins" it (similar to `Thread.join`)
                awaiting on the results returned from the job's driver
            3. JobExecutor launches job's driver's entrypoint (as subprocess), managing
                its lifecycle and reporting its status.

        While, job execution process is asynchronous, we want to provide reasonable guarantees to the
        caller about the status of the job execution process by the time job submission completes.

        We provide following guarantees upon successful return of the Job Submission API:

        1. Job's supervising actor (`JobSupervisor`) is launched
        2. Job's execution (asynchronous) control loop is active (inside `JobSupervisor`)
        3. Job's monitoring loop is active (inside `JobSupervisor`)

        Args:
            entrypoint: Driver command to execute in subprocess shell.
                Represents the entrypoint to start user application.
            runtime_env: Runtime environment used to execute driver command,
                which could contain its own ray.init() to configure runtime
                env at ray cluster, task and actor level.
            metadata: Support passing arbitrary data to driver command in
                case needed.
            entrypoint_num_cpus: The quantity of CPU cores to reserve for the execution
                of the entrypoint command, separately from any tasks or actors launched
                by it. Defaults to 0.
            entrypoint_num_gpus: The quantity of GPUs to reserve for
                the entrypoint command, separately from any tasks or actors launched
                by it. Defaults to 0.
            entrypoint_memory: The amount of total available memory for workers
                requesting memory the entrypoint command, separately from any tasks
                or actors launched by it. Defaults to 0.
            entrypoint_resources: The quantity of various custom resources
                to reserve for the entrypoint command, separately from any tasks or
                actors launched by it.
            _start_signal_actor: Used in testing only to capture state
                transitions between PENDING -> RUNNING. Regular user shouldn't
                need this.

        Returns:
            job_id: Generated uuid for further job management. Only valid
                within the same ray cluster.
        """
        if entrypoint_num_cpus is None:
            entrypoint_num_cpus = 0
        if entrypoint_num_gpus is None:
            entrypoint_num_gpus = 0
        if entrypoint_memory is None:
            entrypoint_memory = 0

        if submission_id is None:
            submission_id = generate_job_id()

        try:
            startup_timeout_s = _get_job_startup_timeout_s()

            logger.info(
                f"Submitting job {submission_id} with entrypoint `{entrypoint}` (startup timeout set at {startup_timeout_s}s)"
            )

            if submission_id in self._job_supervisors:
                raise ValueError(f"Job with submission id {submission_id} already exists")

            supervisor = JobSupervisor(
                job_id=submission_id,
                entrypoint=entrypoint,
                gcs_address=self._gcs_address,
                logs_dir=self._logs_dir,
                startup_timeout_s=startup_timeout_s,
            )

            self._job_supervisors[submission_id] = supervisor

            # Job execution process is async, however we await on the
            # `launch` method here to propagate right away any failures
            # raised during job's launching sequence
            await supervisor.launch(
                runtime_env=runtime_env,
                metadata=metadata,
                entrypoint_num_cpus=entrypoint_num_cpus,
                entrypoint_num_gpus=entrypoint_num_gpus,
                entrypoint_memory=entrypoint_memory,
                entrypoint_resources=entrypoint_resources,
                _start_signal_actor=_start_signal_actor,
            )

        except Exception as e:
            logger.error(
                f"Failed to start Job Supervisor and launch driver for job {submission_id}",
                exc_info=e,
            )

            raise e

        return submission_id

    async def stop_job(self, job_id) -> bool:
        """Request a job to exit, fire and forget.

        Returns boolean flag signaling whether `stop` method was invoked on the supervisor.

        In case of supervisor not being found, job could be presumed dead.
        """
        supervisor = self._job_supervisors.get(job_id)
        if supervisor is not None:
            # Actor is still alive, signal it to stop the driver, fire and
            # forget
            await supervisor.stop()
            return True
        else:
            return False

    async def delete_job(self, job_id):
        """Delete a job's info and metadata from the cluster."""
        job_status = await self._job_info_client.get_status(job_id)

        if job_status is None or not job_status.is_terminal():
            raise RuntimeError(
                f"Attempted to delete job '{job_id}', "
                f"but it is in a non-terminal state {job_status}."
            )

        await self._job_info_client.delete_info(job_id)
        return True

    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get latest status of a job."""
        return await self._job_info_client.get_status(job_id)

    async def get_job_info(self, job_id: str) -> Optional[JobInfo]:
        """Get latest info of a job."""
        return await self._job_info_client.get_info(job_id)

    async def list_jobs(self) -> Dict[str, JobInfo]:
        """Get info for all jobs."""
        return await self._job_info_client.get_all_jobs()

    def get_job_logs(self, job_id: str) -> str:
        """Get all logs produced by a job."""
        return self._log_client.get_logs(job_id)

    async def _tail_logs(self, job_id: str) -> Iterator[str]:
        for lines in self._log_client.tail_logs(job_id):
            if lines is not None:
                yield "".join(lines)
            else:
                return


def _get_job_startup_timeout_s() -> float:
    return float(
        os.environ.get(
            RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
            DEFAULT_JOB_START_TIMEOUT_SECONDS,
        )
    )
