import asyncio
import logging
import os
import random
import string
from typing import Any, Dict, Iterator, Optional, Union

import ray
import ray._private.ray_constants as ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray.actor import ActorHandle
from ray.dashboard.consts import (
    RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
    DEFAULT_JOB_START_TIMEOUT_SECONDS,
)
from ray.dashboard.modules.job.common import (
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobInfo,
    JobInfoStorageClient,
    _get_supervisor_actor_for_job,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.dashboard.modules.job.job_supervisor import JobSupervisor
from ray.job_submission import JobStatus
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
)

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
    """Provide python APIs for job submission and management.

    It does not provide persistence, all info will be lost if the cluster
    goes down.
    """

    # Time that we will sleep while tailing logs if no new log line is
    # available.
    LOG_TAIL_SLEEP_S = 1
    WAIT_FOR_ACTOR_DEATH_TIMEOUT_S = 0.1

    def __init__(self, gcs_aio_client: GcsAioClient, logs_dir: str):
        self._gcs_aio_client = gcs_aio_client
        self._job_info_client = JobInfoStorageClient(gcs_aio_client)
        self._gcs_address = gcs_aio_client.address

        self._log_client = JobLogStorageClient()
        self._logs_dir = logs_dir

        self._supervisor_actor_cls = ray.remote(JobSupervisor)

    async def _get_head_node_scheduling_strategy(
        self,
    ) -> Optional[NodeAffinitySchedulingStrategy]:
        head_node_id_bytes = await self._gcs_aio_client.internal_kv_get(
            "head_node_id".encode(),
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=30,
        )

        if head_node_id_bytes is None:
            logger.error(
                "Head node ID not found in GCS. Using Ray's default actor "
                "scheduling strategy for the job driver instead of running "
                "it on the head node."
            )
            return None

        head_node_id = head_node_id_bytes.decode()

        return NodeAffinitySchedulingStrategy(node_id=head_node_id, soft=False)

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
            |--> JobSupervisor (Actor on Head-node)
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

        supervisor: Optional[ActorHandle] = None

        try:
            startup_timeout_s = _get_job_startup_timeout_s()

            logger.info(
                f"Submitting job {submission_id} with entrypoint `{entrypoint}` (startup timeout set at {startup_timeout_s}s)"
            )

            # NOTE: JobSupervisor is *always* scheduled onto the head-node
            head_node_scheduling_strategy = (
                await self._get_head_node_scheduling_strategy()
            )

            supervisor_actor_name = JOB_ACTOR_NAME_TEMPLATE.format(job_id=submission_id)

            supervisor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=supervisor_actor_name,
                num_cpus=0,
                scheduling_strategy=head_node_scheduling_strategy,
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            ).remote(
                job_id=submission_id,
                entrypoint=entrypoint,
                gcs_address=self._gcs_address,
                logs_dir=self._logs_dir,
                startup_timeout_s=startup_timeout_s,
            )

            # Job execution process is async, however we await on the
            # `launch` method here to propagate right away any failures
            # raised during job's launching sequence
            await supervisor.launch.remote(
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

            if supervisor is not None:
                ray.kill(supervisor, no_restart=True)

            if isinstance(
                e, ValueError
            ) and f"The name {supervisor_actor_name} (namespace={SUPERVISOR_ACTOR_RAY_NAMESPACE}) is already taken" in str(
                e
            ):
                raise ValueError(
                    f"Job with submission id {submission_id} already exists"
                )

            raise e

        return submission_id

    def stop_job(self, job_id) -> bool:
        """Request a job to exit, fire and forget.

        Returns whether or not the job was running.
        """
        job_supervisor_actor = _get_supervisor_actor_for_job(job_id)
        if job_supervisor_actor is not None:
            # Actor is still alive, signal it to stop the driver, fire and
            # forget
            job_supervisor_actor.stop.remote()
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

    def job_info_client(self) -> JobInfoStorageClient:
        return self._job_info_client

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

    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        """Return an iterator following the logs of a job."""
        if await self.get_job_status(job_id) is None:
            raise RuntimeError(f"Job '{job_id}' does not exist.")

        for lines in self._log_client.tail_logs(job_id):
            if lines is None:
                # Return if the job has exited and there are no new log lines.
                status = await self.get_job_status(job_id)
                if status.is_terminal():
                    return

                await asyncio.sleep(self.LOG_TAIL_SLEEP_S)
            else:
                yield "".join(lines)


def _get_job_startup_timeout_s() -> float:
    return float(
        os.environ.get(
            RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
            DEFAULT_JOB_START_TIMEOUT_SECONDS,
        )
    )
