import asyncio
import logging
import os
import random
import string
import time
import traceback
from typing import Any, Dict, Iterator, Optional, Union

import ray
import ray._private.ray_constants as ray_constants
from ray._private.event.event_logger import get_event_logger
from ray._private.gcs_utils import GcsAioClient
from ray._private.utils import run_background_task
from ray.actor import ActorHandle
from ray.core.generated.event_pb2 import Event
from ray.dashboard.consts import (
    DEFAULT_JOB_START_TIMEOUT_SECONDS,
    RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
)
from ray.dashboard.modules.job.common import (
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobInfo,
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.dashboard.modules.job.job_supervisor import JobSupervisor
from ray.exceptions import ActorUnschedulableError, RuntimeEnvSetupError
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
        self._supervisor_actor_cls = ray.remote(JobSupervisor)
        self.monitored_jobs = set()
        try:
            self.event_logger = get_event_logger(Event.SourceType.JOBS, logs_dir)
        except Exception:
            self.event_logger = None

        self._recover_running_jobs_event = asyncio.Event()

        run_background_task(self._recover_running_jobs())

    def _get_job_driver_logger(self, job_id: str) -> logging.Logger:
        """Return job driver logger to log messages to the job driver log file.

        If this function is called for the first time, configure the logger.
        """
        job_driver_logger = logging.getLogger(f"{__name__}.driver-{job_id}")

        # Configure the logger if it's not already configured.
        if not job_driver_logger.handlers:
            job_driver_log_path = self._log_client.get_log_file_path(job_id)
            job_driver_handler = logging.FileHandler(job_driver_log_path)
            job_driver_formatter = logging.Formatter(ray_constants.LOGGER_FORMAT)
            job_driver_handler.setFormatter(job_driver_formatter)
            job_driver_logger.addHandler(job_driver_handler)

        return job_driver_logger

    async def _recover_running_jobs(self):
        """Recovers all running jobs from the status client.

        For each job, we will spawn a coroutine to monitor it.
        Each will be added to self._running_jobs and reconciled.
        """
        try:
            # TODO right now upon (re)starting every JM will monitor every job
            all_jobs = await self._job_info_client.get_all_jobs()

            logger.info(f"Recovered jobs from GCS: {','.join(list(all_jobs.keys()))}")

            for job_id, job_info in all_jobs.items():
                if not job_info.status.is_terminal():
                    run_background_task(self._monitor_job(job_id))
        finally:
            # This event is awaited in `submit_job` to avoid race conditions between
            # recovery and new job submission, so it must always get set even if there
            # are exceptions.
            self._recover_running_jobs_event.set()

    async def _monitor_job(
        self, job_id: str, job_supervisor: Optional[ActorHandle] = None
    ):
        """Monitors the specified job until it enters a terminal state.

        This is necessary because we need to handle the case where the
        JobSupervisor dies unexpectedly.
        """
        if job_id in self.monitored_jobs:
            logger.debug(f"Job {job_id} is already being monitored.")
            return

        self.monitored_jobs.add(job_id)
        try:
            await self._monitor_job_internal(job_id, job_supervisor)
        finally:
            self.monitored_jobs.remove(job_id)

    def _handle_supervisor_startup(self, job_id: str, result: Optional[Exception]):
        """Handle the result of starting a job supervisor actor.

        If started successfully, result should be None. Otherwise it should be
        an Exception.

        On failure, the job will be marked failed with a relevant error
        message.
        """
        if result is None:
            return

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

        logger.info(
            "Head node ID found in GCS; scheduling job driver on "
            f"head node {head_node_id}"
        )

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
        Job execution happens asynchronously.

        1) Generate a new unique id for this job submission, each call of this
            method assumes they're independent submission with its own new
            ID, job supervisor actor, and child process.
        2) Create new detached actor with same runtime_env as job spec

        Actual setting up runtime_env, subprocess group, driver command
        execution, subprocess cleaning up and running status update to GCS
        is all handled by job supervisor actor.

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

        # Wait for `_recover_running_jobs` to run before accepting submissions to
        # avoid duplicate monitoring of the same job.
        await self._recover_running_jobs_event.wait()

        # Wait for the actor to start up asynchronously so this call always
        # returns immediately and we can catch errors with the actor starting
        # up.
        try:
            # NOTE: JobSupervisor is *always* scheduled onto the head-node
            head_node_scheduling_strategy = (
                await self._get_head_node_scheduling_strategy()
            )

            supervisor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=JOB_ACTOR_NAME_TEMPLATE.format(job_id=submission_id),
                num_cpus=0,
                scheduling_strategy=head_node_scheduling_strategy,
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            ).remote(submission_id, entrypoint, self._gcs_address)

            # NOTE: Job execution process is async, however we await on the
            #       `start` method to propagate any failures arising during
            #       job driver's initiating sequence
            await supervisor.start.remote(
                runtime_env=runtime_env,
                metadata=metadata,
                entrypoint_num_cpus=entrypoint_num_cpus,
                entrypoint_num_gpus=entrypoint_num_gpus,
                entrypoint_memory=entrypoint_memory,
                entrypoint_resources=entrypoint_resources,
            )

        except Exception as e:
            tb_str = traceback.format_exc()

            logger.warning(
                f"Failed to start supervisor actor for job {submission_id}: '{e}'"
                f". Full traceback:\n{tb_str}"
            )

            # TODO move to JS
            await self._job_info_client.put_status(
                submission_id,
                JobStatus.FAILED,
                message=(
                    f"Failed to start supervisor actor {submission_id}: '{e}'"
                    f". Full traceback:\n{tb_str}"
                ),
            )

        return submission_id

    def stop_job(self, job_id) -> bool:
        """Request a job to exit, fire and forget.

        Returns whether or not the job was running.
        """
        job_supervisor_actor = _get_actor_for_job(job_id)
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


def _get_actor_for_job(job_id: str) -> Optional[ActorHandle]:
    """Fetches JobSupervisor actor for job identified by Ray Job (submission) id"""
    try:
        return ray.get_actor(
            JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_id),
            namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
        )
    except ValueError as ve:  # Ray returns ValueError for nonexistent actor.
        logger.warning(f"Job supervisor for job {job_id} not found: {str(ve)}")
        return None
