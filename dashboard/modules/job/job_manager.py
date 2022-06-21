import asyncio
import copy
import json
import logging
import os
import random
import string
import subprocess
import time
import traceback
from asyncio.tasks import FIRST_COMPLETED
from collections import deque
from typing import Any, Dict, Iterator, Optional, Tuple

import ray
import ray._private.ray_constants as ray_constants
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
from ray.actor import ActorHandle
from ray.dashboard.modules.job.common import (
    JOB_ID_METADATA_KEY,
    JOB_NAME_METADATA_KEY,
    JobInfo,
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.utils import file_tail_iterator
from ray.exceptions import RuntimeEnvSetupError
from ray.job_submission import JobStatus

logger = logging.getLogger(__name__)

# asyncio python version compatibility
try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future


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


class JobLogStorageClient:
    """
    Disk storage for stdout / stderr of driver script logs.
    """

    JOB_LOGS_PATH = "job-driver-{job_id}.log"
    # Number of last N lines to put in job message upon failure.
    NUM_LOG_LINES_ON_ERROR = 10

    def get_logs(self, job_id: str) -> str:
        try:
            with open(self.get_log_file_path(job_id), "r") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def tail_logs(self, job_id: str) -> Iterator[str]:
        return file_tail_iterator(self.get_log_file_path(job_id))

    def get_last_n_log_lines(
        self, job_id: str, num_log_lines=NUM_LOG_LINES_ON_ERROR
    ) -> str:
        log_tail_iter = self.tail_logs(job_id)
        log_tail_deque = deque(maxlen=num_log_lines)
        for line in log_tail_iter:
            if line is None:
                break
            else:
                log_tail_deque.append(line)
        return "".join(log_tail_deque)

    def get_log_file_path(self, job_id: str) -> Tuple[str, str]:
        """
        Get the file path to the logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.log
        """
        return os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            self.JOB_LOGS_PATH.format(job_id=job_id),
        )


class JobSupervisor:
    """
    Ray actor created by JobManager for each submitted job, responsible to
    setup runtime_env, execute given shell command in subprocess, update job
    status, persist job logs and manage subprocess group cleaning.

    One job supervisor actor maps to one subprocess, for one job_id.
    Job supervisor actor should fate share with subprocess it created.
    """

    SUBPROCESS_POLL_PERIOD_S = 0.1

    def __init__(self, job_id: str, entrypoint: str, user_metadata: Dict[str, str]):
        self._job_id = job_id
        self._job_info_client = JobInfoStorageClient()
        self._log_client = JobLogStorageClient()
        self._driver_runtime_env = self._get_driver_runtime_env()
        self._entrypoint = entrypoint

        # Default metadata if not passed by the user.
        self._metadata = {JOB_ID_METADATA_KEY: job_id, JOB_NAME_METADATA_KEY: job_id}
        self._metadata.update(user_metadata)

        # fire and forget call from outer job manager to this actor
        self._stop_event = asyncio.Event()

    def _get_driver_runtime_env(self) -> Dict[str, Any]:
        # Get the runtime_env set for the supervisor actor.
        curr_runtime_env = dict(ray.get_runtime_context().runtime_env)
        # Allow CUDA_VISIBLE_DEVICES to be set normally for the driver's tasks
        # & actors.
        env_vars = curr_runtime_env.get("env_vars", {})
        env_vars.pop(ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR)
        curr_runtime_env["env_vars"] = env_vars
        return curr_runtime_env

    def ping(self):
        """Used to check the health of the actor."""
        pass

    def _exec_entrypoint(self, logs_path: str) -> subprocess.Popen:
        """
        Runs the entrypoint command as a child process, streaming stderr &
        stdout to given log files.

        Meanwhile we start a demon process and group driver
        subprocess in same pgid, such that if job actor dies, entire process
        group also fate share with it.

        Args:
            logs_path: File path on head node's local disk to store driver
                command's stdout & stderr.
        Returns:
            child_process: Child process that runs the driver command. Can be
                terminated or killed upon user calling stop().
        """
        with open(logs_path, "w") as logs_file:
            child_process = subprocess.Popen(
                self._entrypoint,
                shell=True,
                start_new_session=True,
                stdout=logs_file,
                stderr=subprocess.STDOUT,
            )
            parent_pid = os.getpid()
            # Create new pgid with new subprocess to execute driver command
            child_pid = child_process.pid
            child_pgid = os.getpgid(child_pid)

            # Open a new subprocess to kill the child process when the parent
            # process dies kill -s 0 parent_pid will succeed if the parent is
            # alive. If it fails, SIGKILL the child process group and exit
            subprocess.Popen(
                f"while kill -s 0 {parent_pid}; do sleep 1; done; kill -9 -{child_pgid}",  # noqa: E501
                shell=True,
                # Suppress output
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return child_process

    def _get_driver_env_vars(self) -> Dict[str, str]:
        """Returns environment variables that should be set in the driver."""
        ray_addr = ray._private.services.find_bootstrap_address().pop()
        return {
            # Set JobConfig for the child process (runtime_env, metadata).
            RAY_JOB_CONFIG_JSON_ENV_VAR: json.dumps(
                {
                    "runtime_env": self._driver_runtime_env,
                    "metadata": self._metadata,
                }
            ),
            # Always set RAY_ADDRESS as find_bootstrap_address address for
            # job submission. In case of local development, prevent user from
            # re-using http://{address}:{dashboard_port} to interact with
            # jobs SDK.
            # TODO:(mwtian) Check why "auto" does not work in entrypoint script
            ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE: ray_addr,
            # Set PYTHONUNBUFFERED=1 to stream logs during the job instead of
            # only streaming them upon completion of the job.
            "PYTHONUNBUFFERED": "1",
        }

    async def _polling(self, child_process) -> int:
        try:
            while child_process is not None:
                return_code = child_process.poll()
                if return_code is not None:
                    # subprocess finished with return code
                    return return_code
                else:
                    # still running, yield control, 0.1s by default
                    await asyncio.sleep(self.SUBPROCESS_POLL_PERIOD_S)
        except Exception:
            if child_process:
                # TODO (jiaodong): Improve this with SIGTERM then SIGKILL
                child_process.kill()
            return 1

    async def run(
        self,
        # Signal actor used in testing to capture PENDING -> RUNNING cases
        _start_signal_actor: Optional[ActorHandle] = None,
    ):
        """
        Stop and start both happen asynchrously, coordinated by asyncio event
        and coroutine, respectively.

        1) Sets job status as running
        2) Pass runtime env and metadata to subprocess as serialized env
            variables.
        3) Handle concurrent events of driver execution and
        """
        curr_status = self._job_info_client.get_status(self._job_id)
        assert curr_status == JobStatus.PENDING, "Run should only be called once."

        if _start_signal_actor:
            # Block in PENDING state until start signal received.
            await _start_signal_actor.wait.remote()

        self._job_info_client.put_status(self._job_id, JobStatus.RUNNING)

        try:
            # Configure environment variables for the child process. These
            # will *not* be set in the runtime_env, so they apply to the driver
            # only, not its tasks & actors.
            os.environ.update(self._get_driver_env_vars())
            logger.info(
                "Submitting job with RAY_ADDRESS = "
                f"{os.environ[ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE]}"
            )
            log_path = self._log_client.get_log_file_path(self._job_id)
            child_process = self._exec_entrypoint(log_path)

            polling_task = create_task(self._polling(child_process))
            finished, _ = await asyncio.wait(
                [polling_task, self._stop_event.wait()], return_when=FIRST_COMPLETED
            )

            if self._stop_event.is_set():
                polling_task.cancel()
                # TODO (jiaodong): Improve this with SIGTERM then SIGKILL
                child_process.kill()
                self._job_info_client.put_status(self._job_id, JobStatus.STOPPED)
            else:
                # Child process finished execution and no stop event is set
                # at the same time
                assert len(finished) == 1, "Should have only one coroutine done"
                [child_process_task] = finished
                return_code = child_process_task.result()
                if return_code == 0:
                    self._job_info_client.put_status(self._job_id, JobStatus.SUCCEEDED)
                else:
                    log_tail = self._log_client.get_last_n_log_lines(self._job_id)
                    if log_tail is not None and log_tail != "":
                        message = (
                            "Job failed due to an application error, "
                            "last available logs:\n" + log_tail
                        )
                    else:
                        message = None
                    self._job_info_client.put_status(
                        self._job_id, JobStatus.FAILED, message=message
                    )
        except Exception:
            logger.error(
                "Got unexpected exception while trying to execute driver "
                f"command. {traceback.format_exc()}"
            )
        finally:
            # clean up actor after tasks are finished
            ray.actor.exit_actor()

    def stop(self):
        """Set step_event and let run() handle the rest in its asyncio.wait()."""
        self._stop_event.set()


class JobManager:
    """Provide python APIs for job submission and management.

    It does not provide persistence, all info will be lost if the cluster
    goes down.
    """

    JOB_ACTOR_NAME = "_ray_internal_job_actor_{job_id}"
    # Time that we will sleep while tailing logs if no new log line is
    # available.
    LOG_TAIL_SLEEP_S = 1
    JOB_MONITOR_LOOP_PERIOD_S = 1

    def __init__(self):
        self._job_info_client = JobInfoStorageClient()
        self._log_client = JobLogStorageClient()
        self._supervisor_actor_cls = ray.remote(JobSupervisor)

        self._recover_running_jobs()

    def _recover_running_jobs(self):
        """Recovers all running jobs from the status client.

        For each job, we will spawn a coroutine to monitor it.
        Each will be added to self._running_jobs and reconciled.
        """
        all_jobs = self._job_info_client.get_all_jobs()
        for job_id, job_info in all_jobs.items():
            if not job_info.status.is_terminal():
                create_task(self._monitor_job(job_id))

    def _get_actor_for_job(self, job_id: str) -> Optional[ActorHandle]:
        try:
            return ray.get_actor(self.JOB_ACTOR_NAME.format(job_id=job_id))
        except ValueError:  # Ray returns ValueError for nonexistent actor.
            return None

    async def _monitor_job(
        self, job_id: str, job_supervisor: Optional[ActorHandle] = None
    ):
        """Monitors the specified job until it enters a terminal state.

        This is necessary because we need to handle the case where the
        JobSupervisor dies unexpectedly.
        """
        is_alive = True
        if job_supervisor is None:
            job_supervisor = self._get_actor_for_job(job_id)

            if job_supervisor is None:
                logger.error(f"Failed to get job supervisor for job {job_id}.")
                self._job_info_client.put_status(
                    job_id,
                    JobStatus.FAILED,
                    message="Unexpected error occurred: Failed to get job supervisor.",
                )
                is_alive = False

        while is_alive:
            try:
                await job_supervisor.ping.remote()
                await asyncio.sleep(self.JOB_MONITOR_LOOP_PERIOD_S)
            except Exception as e:
                is_alive = False
                if self._job_info_client.get_status(job_id).is_terminal():
                    # If the job is already in a terminal state, then the actor
                    # exiting is expected.
                    pass
                elif isinstance(e, RuntimeEnvSetupError):
                    logger.info(f"Failed to set up runtime_env for job {job_id}.")
                    self._job_info_client.put_status(
                        job_id,
                        JobStatus.FAILED,
                        message=f"runtime_env setup failed: {e}",
                    )
                else:
                    logger.warning(
                        f"Job supervisor for job {job_id} failed unexpectedly: {e}."
                    )
                    self._job_info_client.put_status(
                        job_id,
                        JobStatus.FAILED,
                        message=f"Unexpected error occurred: {e}",
                    )

        # Kill the actor defensively to avoid leaking actors in unexpected error cases.
        if job_supervisor is not None:
            ray.kill(job_supervisor, no_restart=True)

    def _get_current_node_resource_key(self) -> str:
        """Get the Ray resource key for current node.

        It can be used for actor placement.
        """
        current_node_id = ray.get_runtime_context().node_id.hex()
        for node in ray.nodes():
            if node["NodeID"] == current_node_id:
                # Found the node.
                for key in node["Resources"].keys():
                    if key.startswith("node:"):
                        return key
        else:
            raise ValueError("Cannot find the node dictionary for current node.")

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
        self, user_runtime_env: Dict[str, Any]
    ) -> Dict[str, Any]:

        """Configure and return the runtime_env for the supervisor actor."""

        # Make a copy to avoid mutating passed runtime_env.
        runtime_env = (
            copy.deepcopy(user_runtime_env) if user_runtime_env is not None else {}
        )

        # NOTE(edoakes): Can't use .get(, {}) here because we need to handle the case
        # where env_vars is explicitly set to `None`.
        env_vars = runtime_env.get("env_vars")
        if env_vars is None:
            env_vars = {}

        # Don't set CUDA_VISIBLE_DEVICES for the supervisor actor so the
        # driver can use GPUs if it wants to. This will be removed from
        # the driver's runtime_env so it isn't inherited by tasks & actors.
        env_vars[ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR] = "1"
        runtime_env["env_vars"] = env_vars
        return runtime_env

    def submit_job(
        self,
        *,
        entrypoint: str,
        job_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
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
            _start_signal_actor: Used in testing only to capture state
                transitions between PENDING -> RUNNING. Regular user shouldn't
                need this.

        Returns:
            job_id: Generated uuid for further job management. Only valid
                within the same ray cluster.
        """
        if job_id is None:
            job_id = generate_job_id()
        elif self._job_info_client.get_status(job_id) is not None:
            raise RuntimeError(f"Job {job_id} already exists.")

        logger.info(f"Starting job with job_id: {job_id}")
        job_info = JobInfo(
            entrypoint=entrypoint,
            status=JobStatus.PENDING,
            start_time=int(time.time() * 1000),
            metadata=metadata,
            runtime_env=runtime_env,
        )
        self._job_info_client.put_info(job_id, job_info)

        # Wait for the actor to start up asynchronously so this call always
        # returns immediately and we can catch errors with the actor starting
        # up.
        try:
            supervisor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=self.JOB_ACTOR_NAME.format(job_id=job_id),
                num_cpus=0,
                # Currently we assume JobManager is created by dashboard server
                # running on headnode, same for job supervisor actors scheduled
                resources={
                    self._get_current_node_resource_key(): 0.001,
                },
                runtime_env=self._get_supervisor_runtime_env(runtime_env),
            ).remote(job_id, entrypoint, metadata or {})
            supervisor.run.remote(_start_signal_actor=_start_signal_actor)

            # Monitor the job in the background so we can detect errors without
            # requiring a client to poll.
            create_task(self._monitor_job(job_id, job_supervisor=supervisor))
        except Exception as e:
            self._job_info_client.put_status(
                job_id,
                JobStatus.FAILED,
                message=f"Failed to start job supervisor: {e}.",
            )

        return job_id

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

    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get latest status of a job."""
        return self._job_info_client.get_status(job_id)

    def get_job_info(self, job_id: str) -> Optional[JobInfo]:
        """Get latest info of a job."""
        return self._job_info_client.get_info(job_id)

    def list_jobs(self) -> Dict[str, JobInfo]:
        """Get info for all jobs."""
        return self._job_info_client.get_all_jobs()

    def get_job_logs(self, job_id: str) -> str:
        """Get all logs produced by a job."""
        return self._log_client.get_logs(job_id)

    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        """Return an iterator following the logs of a job."""
        if self.get_job_status(job_id) is None:
            raise RuntimeError(f"Job '{job_id}' does not exist.")

        for line in self._log_client.tail_logs(job_id):
            if line is None:
                # Return if the job has exited and there are no new log lines.
                status = self.get_job_status(job_id)
                if status not in {JobStatus.PENDING, JobStatus.RUNNING}:
                    return

                await asyncio.sleep(self.LOG_TAIL_SLEEP_S)
            else:
                yield line
