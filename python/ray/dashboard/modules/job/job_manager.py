import asyncio
import copy
import logging
import os
import random
import string
import time
import traceback
from typing import Any, AsyncIterator, Dict, Optional, Union

import ray
import ray._private.ray_constants as ray_constants
from ray._common.utils import Timer, run_background_task
from ray._private.accelerators.npu import NOSET_ASCEND_RT_VISIBLE_DEVICES_ENV_VAR
from ray._private.accelerators.nvidia_gpu import NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR
from ray._private.event.event_logger import get_event_logger
from ray._raylet import GcsClient
from ray.actor import ActorHandle
from ray.core.generated.event_pb2 import Event
from ray.dashboard.consts import (
    DEFAULT_JOB_START_TIMEOUT_SECONDS,
    RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR,
    RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
    RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR,
)
from ray.dashboard.modules.job.common import (
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobInfo,
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.dashboard.modules.job.job_supervisor import JobSupervisor
from ray.dashboard.modules.job.utils import get_head_node_id
from ray.dashboard.utils import close_logger_file_descriptor
from ray.exceptions import ActorDiedError, ActorUnschedulableError, RuntimeEnvSetupError
from ray.job_submission import JobErrorType, JobStatus
from ray.runtime_env import RuntimeEnvConfig
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    SchedulingStrategyT,
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
    JOB_MONITOR_LOOP_PERIOD_S = 1
    WAIT_FOR_ACTOR_DEATH_TIMEOUT_S = 0.1

    def __init__(
        self, gcs_client: GcsClient, logs_dir: str, timeout_check_timer: Timer = None
    ):
        self._gcs_client = gcs_client
        self._logs_dir = logs_dir
        self._job_info_client = JobInfoStorageClient(gcs_client, logs_dir)
        self._gcs_address = gcs_client.address
        self._cluster_id_hex = gcs_client.cluster_id.hex()
        self._log_client = JobLogStorageClient()
        self._supervisor_actor_cls = ray.remote(JobSupervisor)
        self._timeout_check_timer = timeout_check_timer or Timer()
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
            all_jobs = await self._job_info_client.get_all_jobs()
            for job_id, job_info in all_jobs.items():
                if not job_info.status.is_terminal():
                    run_background_task(self._monitor_job(job_id))
        finally:
            # This event is awaited in `submit_job` to avoid race conditions between
            # recovery and new job submission, so it must always get set even if there
            # are exceptions.
            self._recover_running_jobs_event.set()

    def _get_actor_for_job(self, job_id: str) -> Optional[ActorHandle]:
        try:
            return ray.get_actor(
                JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_id),
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            )
        except ValueError:  # Ray returns ValueError for nonexistent actor.
            return None

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
        except Exception as e:
            logger.error("Unhandled exception in job monitoring!", exc_info=e)
            raise e
        finally:
            self.monitored_jobs.remove(job_id)

    async def _monitor_job_internal(
        self, job_id: str, job_supervisor: Optional[ActorHandle] = None
    ):
        timeout = float(
            os.environ.get(
                RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
                DEFAULT_JOB_START_TIMEOUT_SECONDS,
            )
        )

        job_status = None
        job_info = None
        ping_obj_ref = None

        while True:
            try:
                # NOTE: Job monitoring loop sleeps before proceeding with monitoring
                #       sequence to consolidate the control-flow of the pacing
                #       in a single place, rather than having it spread across
                #       many branches
                await asyncio.sleep(self.JOB_MONITOR_LOOP_PERIOD_S)

                job_status = await self._job_info_client.get_status(
                    job_id, timeout=None
                )
                if job_status == JobStatus.PENDING:
                    # Compare the current time with the job start time.
                    # If the job is still pending, we will set the status
                    # to FAILED.
                    if job_info is None:
                        job_info = await self._job_info_client.get_info(
                            job_id, timeout=None
                        )

                    if (
                        self._timeout_check_timer.time() - job_info.start_time / 1000
                        > timeout
                    ):
                        err_msg = (
                            "Job supervisor actor failed to start within "
                            f"{timeout} seconds. This timeout can be "
                            f"configured by setting the environment "
                            f"variable {RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR}."
                        )
                        resources_specified = (
                            (
                                job_info.entrypoint_num_cpus is not None
                                and job_info.entrypoint_num_cpus > 0
                            )
                            or (
                                job_info.entrypoint_num_gpus is not None
                                and job_info.entrypoint_num_gpus > 0
                            )
                            or (
                                job_info.entrypoint_memory is not None
                                and job_info.entrypoint_memory > 0
                            )
                            or (
                                job_info.entrypoint_resources is not None
                                and len(job_info.entrypoint_resources) > 0
                            )
                        )
                        if resources_specified:
                            err_msg += (
                                " This may be because the job entrypoint's specified "
                                "resources (entrypoint_num_cpus, entrypoint_num_gpus, "
                                "entrypoint_resources, entrypoint_memory)"
                                "aren't available on the cluster."
                                " Try checking the cluster's available resources with "
                                "`ray status` and specifying fewer resources for the "
                                "job entrypoint."
                            )
                        await self._job_info_client.put_status(
                            job_id,
                            JobStatus.FAILED,
                            message=err_msg,
                            error_type=JobErrorType.JOB_SUPERVISOR_ACTOR_START_TIMEOUT,
                            timeout=None,
                        )
                        logger.error(err_msg)
                        break

                if job_supervisor is None:
                    job_supervisor = self._get_actor_for_job(job_id)

                if job_supervisor is None:
                    if job_status == JobStatus.PENDING:
                        # Maybe the job supervisor actor is not created yet.
                        # We will wait for the next loop.
                        continue
                    else:
                        # The job supervisor actor is not created, but the job
                        # status is not PENDING. This means the job supervisor
                        # actor is not created due to some unexpected errors.
                        # We will set the job status to FAILED.
                        logger.error(f"Failed to get job supervisor for job {job_id}.")
                        await self._job_info_client.put_status(
                            job_id,
                            JobStatus.FAILED,
                            message=(
                                "Unexpected error occurred: "
                                "failed to get job supervisor."
                            ),
                            error_type=JobErrorType.JOB_SUPERVISOR_ACTOR_START_FAILURE,
                            timeout=None,
                        )
                        break

                # Check to see if `JobSupervisor` is alive and reachable
                if ping_obj_ref is None:
                    ping_obj_ref = job_supervisor.ping.options(
                        max_task_retries=-1
                    ).remote()
                ready, _ = ray.wait([ping_obj_ref], timeout=0)
                if ready:
                    ray.get(ping_obj_ref)
                    ping_obj_ref = None
                else:
                    continue

            except Exception as e:
                job_status = await self._job_info_client.get_status(
                    job_id, timeout=None
                )
                target_job_error_message = ""
                target_job_error_type: Optional[JobErrorType] = None
                if job_status is not None and job_status.is_terminal():
                    # If the job is already in a terminal state, then the actor
                    # exiting is expected.
                    pass
                else:
                    if isinstance(e, RuntimeEnvSetupError):
                        logger.error(f"Failed to set up runtime_env for job {job_id}.")

                        target_job_error_message = f"runtime_env setup failed: {e}"
                        target_job_error_type = JobErrorType.RUNTIME_ENV_SETUP_FAILURE

                    elif isinstance(e, ActorUnschedulableError):
                        logger.error(
                            f"Failed to schedule job {job_id} because the supervisor "
                            f"actor could not be scheduled: {e}"
                        )

                        target_job_error_message = (
                            f"Job supervisor actor could not be scheduled: {e}"
                        )
                        target_job_error_type = (
                            JobErrorType.JOB_SUPERVISOR_ACTOR_UNSCHEDULABLE
                        )

                    elif isinstance(e, ActorDiedError):
                        logger.error(f"Job supervisor actor for {job_id} died: {e}")
                        target_job_error_message = f"Job supervisor actor died: {e}"
                        target_job_error_type = JobErrorType.JOB_SUPERVISOR_ACTOR_DIED

                    else:
                        logger.error(
                            f"Job monitoring for job {job_id} failed "
                            f"unexpectedly: {e}.",
                            exc_info=e,
                        )

                        target_job_error_message = f"Unexpected error occurred: {e}"
                        target_job_error_type = (
                            JobErrorType.JOB_SUPERVISOR_ACTOR_UNKNOWN_FAILURE
                        )

                    job_status = JobStatus.FAILED
                    await self._job_info_client.put_status(
                        job_id,
                        job_status,
                        message=target_job_error_message,
                        error_type=target_job_error_type
                        or JobErrorType.JOB_SUPERVISOR_ACTOR_UNKNOWN_FAILURE,
                        timeout=None,
                    )

                # Log error message to the job driver file for easy access.
                if target_job_error_message:
                    log_path = self._log_client.get_log_file_path(job_id)
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    with open(log_path, "a") as log_file:
                        log_file.write(target_job_error_message)

                # Log events
                if self.event_logger:
                    event_log = (
                        f"Completed a ray job {job_id} with a status {job_status}."
                    )
                    if target_job_error_message:
                        event_log += f" {target_job_error_message}"
                        self.event_logger.error(event_log, submission_id=job_id)
                    else:
                        self.event_logger.info(event_log, submission_id=job_id)

                break

        # Kill the actor defensively to avoid leaking actors in unexpected error cases.
        if job_supervisor is None:
            job_supervisor = self._get_actor_for_job(job_id)
        if job_supervisor is not None:
            ray.kill(job_supervisor, no_restart=True)

    def _handle_supervisor_startup(self, job_id: str, result: Optional[Exception]):
        """Handle the result of starting a job supervisor actor.

        If started successfully, result should be None. Otherwise it should be
        an Exception.

        On failure, the job will be marked failed with a relevant error
        message.
        """
        if result is None:
            return

    def _get_supervisor_runtime_env(
        self,
        user_runtime_env: Dict[str, Any],
        submission_id: str,
        resources_specified: bool = False,
    ) -> Dict[str, Any]:
        """Configure and return the runtime_env for the supervisor actor.

        Args:
            user_runtime_env: The runtime_env specified by the user.
            resources_specified: Whether the user specified resources in the
                submit_job() call. If so, we will skip the workaround introduced
                in #24546 for GPU detection and just use the user's resource
                requests, so that the behavior matches that of the user specifying
                resources for any other actor.

        Returns:
            The runtime_env for the supervisor actor.
        """
        # Make a copy to avoid mutating passed runtime_env.
        runtime_env = (
            copy.deepcopy(user_runtime_env) if user_runtime_env is not None else {}
        )

        # NOTE(edoakes): Can't use .get(, {}) here because we need to handle the case
        # where env_vars is explicitly set to `None`.
        env_vars = runtime_env.get("env_vars")
        if env_vars is None:
            env_vars = {}

        env_vars[ray_constants.RAY_WORKER_NICENESS] = "0"

        if not resources_specified:
            # Don't set CUDA_VISIBLE_DEVICES for the supervisor actor so the
            # driver can use GPUs if it wants to. This will be removed from
            # the driver's runtime_env so it isn't inherited by tasks & actors.
            env_vars[NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR] = "1"
            env_vars[NOSET_ASCEND_RT_VISIBLE_DEVICES_ENV_VAR] = "1"

        runtime_env["env_vars"] = env_vars

        if os.getenv(RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR, "0") == "1":
            config = runtime_env.get("config")
            # Empty fields may be set to None, so we need to check for None explicitly.
            if config is None:
                config = RuntimeEnvConfig()
            config["log_files"] = [self._log_client.get_log_file_path(submission_id)]
            runtime_env["config"] = config
        return runtime_env

    async def _get_scheduling_strategy(
        self, resources_specified: bool
    ) -> SchedulingStrategyT:
        """Get the scheduling strategy for the job.

        If resources_specified is true, or if the environment variable is set to
        allow the job to run on worker nodes, we will use Ray's default actor
        placement strategy. Otherwise, we will force the job to use the head node.

        Args:
            resources_specified: Whether the job specified any resources
                (CPUs, GPUs, or custom resources).

        Returns:
            The scheduling strategy to use for the job.
        """
        if resources_specified:
            return "DEFAULT"

        if os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "0") == "1":
            logger.info(
                f"{RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR} was set to 1. "
                "Using Ray's default actor scheduling strategy for the job "
                "driver instead of running it on the head node."
            )
            return "DEFAULT"

        # If the user did not specify any resources or set the driver on worker nodes
        # env var, we will run the driver on the head node.

        head_node_id = await get_head_node_id(self._gcs_client)
        if head_node_id is None:
            logger.info(
                "Head node ID not found in GCS. Using Ray's default actor "
                "scheduling strategy for the job driver instead of running "
                "it on the head node."
            )
            scheduling_strategy = "DEFAULT"
        else:
            logger.info(
                "Head node ID found in GCS; scheduling job driver on "
                f"head node {head_node_id}"
            )
            scheduling_strategy = NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        return scheduling_strategy

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

        logger.info(f"Starting job with submission_id: {submission_id}")
        job_info = JobInfo(
            entrypoint=entrypoint,
            status=JobStatus.PENDING,
            start_time=int(time.time() * 1000),
            metadata=metadata,
            runtime_env=runtime_env,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_memory=entrypoint_memory,
            entrypoint_resources=entrypoint_resources,
        )
        new_key_added = await self._job_info_client.put_info(
            submission_id, job_info, overwrite=False
        )
        if not new_key_added:
            raise ValueError(
                f"Job with submission_id {submission_id} already exists. "
                "Please use a different submission_id."
            )

        driver_logger = self._get_job_driver_logger(submission_id)
        # Wait for the actor to start up asynchronously so this call always
        # returns immediately and we can catch errors with the actor starting
        # up.
        try:
            resources_specified = any(
                [
                    entrypoint_num_cpus is not None and entrypoint_num_cpus > 0,
                    entrypoint_num_gpus is not None and entrypoint_num_gpus > 0,
                    entrypoint_memory is not None and entrypoint_memory > 0,
                    entrypoint_resources not in [None, {}],
                ]
            )
            scheduling_strategy = await self._get_scheduling_strategy(
                resources_specified
            )
            if self.event_logger:
                self.event_logger.info(
                    f"Started a ray job {submission_id}.", submission_id=submission_id
                )

            driver_logger.info("Runtime env is setting up.")
            supervisor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=JOB_ACTOR_NAME_TEMPLATE.format(job_id=submission_id),
                num_cpus=entrypoint_num_cpus,
                num_gpus=entrypoint_num_gpus,
                memory=entrypoint_memory,
                resources=entrypoint_resources,
                scheduling_strategy=scheduling_strategy,
                runtime_env=self._get_supervisor_runtime_env(
                    runtime_env, submission_id, resources_specified
                ),
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
                # Don't pollute task events with system actor tasks that users don't
                # know about.
                enable_task_events=False,
            ).remote(
                submission_id,
                entrypoint,
                metadata or {},
                self._gcs_address,
                self._cluster_id_hex,
                self._logs_dir,
            )
            supervisor.run.remote(
                _start_signal_actor=_start_signal_actor,
                resources_specified=resources_specified,
            )

            # Monitor the job in the background so we can detect errors without
            # requiring a client to poll.
            run_background_task(
                self._monitor_job(submission_id, job_supervisor=supervisor)
            )
        except Exception as e:
            tb_str = traceback.format_exc()
            driver_logger.warning(
                f"Failed to start supervisor actor for job {submission_id}: '{e}'"
                f". Full traceback:\n{tb_str}"
            )
            await self._job_info_client.put_status(
                submission_id,
                JobStatus.FAILED,
                message=(
                    f"Failed to start supervisor actor {submission_id}: '{e}'"
                    f". Full traceback:\n{tb_str}"
                ),
                error_type=JobErrorType.JOB_SUPERVISOR_ACTOR_START_FAILURE,
            )
        finally:
            close_logger_file_descriptor(driver_logger)

        return submission_id

    def stop_job(self, job_id) -> bool:
        """Request a job to exit, fire and forget.

        Returns whether or not the job was running.
        """
        job_supervisor_actor = self._get_actor_for_job(job_id)
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

    async def tail_job_logs(self, job_id: str) -> AsyncIterator[str]:
        """Return an iterator following the logs of a job."""
        if await self.get_job_status(job_id) is None:
            raise RuntimeError(f"Job '{job_id}' does not exist.")

        job_finished = False
        async for lines in self._log_client.tail_logs(job_id):
            if lines is None:
                if job_finished:
                    # Job has already finished and we have read EOF afterwards,
                    # it's guaranteed that we won't get any more logs.
                    return
                else:
                    status = await self.get_job_status(job_id)
                    if status.is_terminal():
                        job_finished = True
                        # Continue tailing logs generated between the
                        # last EOF read and the finish of the job.

                await asyncio.sleep(self.LOG_TAIL_SLEEP_S)
            else:
                yield "".join(lines)
