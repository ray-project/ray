import asyncio
from asyncio.tasks import FIRST_COMPLETED
import os
import json
import logging
import traceback
import random
import subprocess
import string
from collections import deque
from typing import Any, Dict, Iterator, Tuple, Optional

import ray
from ray.exceptions import RuntimeEnvSetupError
import ray.ray_constants as ray_constants
from ray.actor import ActorHandle
from ray.dashboard.modules.job.common import (
    JobStatus, JobStatusInfo, JobStatusStorageClient, JOB_ID_METADATA_KEY,
    JOB_NAME_METADATA_KEY)
from ray.dashboard.modules.job.utils import file_tail_iterator
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR

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
        set(string.ascii_letters + string.digits) -
        {"I", "l", "o", "O", "0"}  # No confusing characters
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

    def get_last_n_log_lines(self,
                             job_id: str,
                             num_log_lines=NUM_LOG_LINES_ON_ERROR) -> str:
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
            ray.worker._global_node.get_logs_dir_path(),
            self.JOB_LOGS_PATH.format(job_id=job_id))


class JobSupervisor:
    """
    Ray actor created by JobManager for each submitted job, responsible to
    setup runtime_env, execute given shell command in subprocess, update job
    status, persist job logs and manage subprocess group cleaning.

    One job supervisor actor maps to one subprocess, for one job_id.
    Job supervisor actor should fate share with subprocess it created.
    """

    SUBPROCESS_POLL_PERIOD_S = 0.1

    def __init__(self, job_id: str, entrypoint: str,
                 user_metadata: Dict[str, str]):
        self._job_id = job_id
        self._status_client = JobStatusStorageClient()
        self._log_client = JobLogStorageClient()
        self._runtime_env = ray.get_runtime_context().runtime_env
        self._entrypoint = entrypoint

        # Default metadata if not passed by the user.
        self._metadata = {
            JOB_ID_METADATA_KEY: job_id,
            JOB_NAME_METADATA_KEY: job_id
        }
        self._metadata.update(user_metadata)

        # fire and forget call from outer job manager to this actor
        self._stop_event = asyncio.Event()

    def ready(self):
        """Dummy object ref. Return of this function represents job supervisor
        actor stated successfully with runtime_env configured, and is ready to
        move on to running state.
        """
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
                stderr=subprocess.STDOUT)
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
            _start_signal_actor: Optional[ActorHandle] = None):
        """
        Stop and start both happen asynchrously, coordinated by asyncio event
        and coroutine, respectively.

        1) Sets job status as running
        2) Pass runtime env and metadata to subprocess as serialized env
            variables.
        3) Handle concurrent events of driver execution and
        """
        cur_status = self._get_status()
        assert cur_status.status == JobStatus.PENDING, (
            "Run should only be called once.")

        if _start_signal_actor:
            # Block in PENDING state until start signal received.
            await _start_signal_actor.wait.remote()

        self._status_client.put_status(self._job_id,
                                       JobStatusInfo(JobStatus.RUNNING))

        try:
            # Set JobConfig for the child process (runtime_env, metadata).
            os.environ[RAY_JOB_CONFIG_JSON_ENV_VAR] = json.dumps({
                "runtime_env": self._runtime_env,
                "metadata": self._metadata,
            })
            # Set RAY_ADDRESS to local Ray address, if it is not set.
            os.environ[
                ray_constants.RAY_ADDRESS_ENVIRONMENT_VARIABLE] = \
                ray._private.services.get_ray_address_from_environment()
            # Set PYTHONUNBUFFERED=1 to stream logs during the job instead of
            # only streaming them upon completion of the job.
            os.environ["PYTHONUNBUFFERED"] = "1"
            log_path = self._log_client.get_log_file_path(self._job_id)
            child_process = self._exec_entrypoint(log_path)

            polling_task = create_task(self._polling(child_process))
            finished, _ = await asyncio.wait(
                [polling_task, self._stop_event.wait()],
                return_when=FIRST_COMPLETED)

            if self._stop_event.is_set():
                polling_task.cancel()
                # TODO (jiaodong): Improve this with SIGTERM then SIGKILL
                child_process.kill()
                self._status_client.put_status(self._job_id, JobStatus.STOPPED)
            else:
                # Child process finished execution and no stop event is set
                # at the same time
                assert len(
                    finished) == 1, "Should have only one coroutine done"
                [child_process_task] = finished
                return_code = child_process_task.result()
                if return_code == 0:
                    self._status_client.put_status(self._job_id,
                                                   JobStatus.SUCCEEDED)
                else:
                    log_tail = self._log_client.get_last_n_log_lines(
                        self._job_id)
                    if log_tail is not None and log_tail != "":
                        message = ("Job failed due to an application error, "
                                   "last available logs:\n" + log_tail)
                    else:
                        message = None
                    self._status_client.put_status(
                        self._job_id,
                        JobStatusInfo(
                            status=JobStatus.FAILED, message=message))
        except Exception:
            logger.error(
                "Got unexpected exception while trying to execute driver "
                f"command. {traceback.format_exc()}")
        finally:
            # clean up actor after tasks are finished
            ray.actor.exit_actor()

    def _get_status(self) -> Optional[JobStatusInfo]:
        return self._status_client.get_status(self._job_id)

    def stop(self):
        """Set step_event and let run() handle the rest in its asyncio.wait().
        """
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

    def __init__(self):
        self._status_client = JobStatusStorageClient()
        self._log_client = JobLogStorageClient()
        self._supervisor_actor_cls = ray.remote(JobSupervisor)

    def _get_actor_for_job(self, job_id: str) -> Optional[ActorHandle]:
        try:
            return ray.get_actor(self.JOB_ACTOR_NAME.format(job_id=job_id))
        except ValueError:  # Ray returns ValueError for nonexistent actor.
            return None

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
            raise ValueError(
                "Cannot find the node dictionary for current node.")

    def _handle_supervisor_startup(self, job_id: str,
                                   result: Optional[Exception]):
        """Handle the result of starting a job supervisor actor.

        If started successfully, result should be None. Otherwise it should be
        an Exception.

        On failure, the job will be marked failed with a relevant error
        message.
        """
        if result is None:
            return
        elif isinstance(result, RuntimeEnvSetupError):
            logger.info(f"Failed to set up runtime_env for job {job_id}.")
            self._status_client.put_status(
                job_id,
                JobStatusInfo(
                    status=JobStatus.FAILED,
                    message=(f"runtime_env setup failed: {result}")))
        elif isinstance(result, Exception):
            logger.error(
                f"Failed to start supervisor for job {job_id}: {result}.")
            self._status_client.put_status(
                job_id,
                JobStatusInfo(
                    status=JobStatus.FAILED,
                    message=f"Error occurred while starting the job: {result}")
            )
        else:
            assert False, "This should not be reached."

    def submit_job(self,
                   *,
                   entrypoint: str,
                   job_id: Optional[str] = None,
                   runtime_env: Optional[Dict[str, Any]] = None,
                   metadata: Optional[Dict[str, str]] = None,
                   _start_signal_actor: Optional[ActorHandle] = None) -> str:
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
        elif self._status_client.get_status(job_id) is not None:
            raise RuntimeError(f"Job {job_id} already exists.")

        logger.info(f"Starting job with job_id: {job_id}")
        self._status_client.put_status(job_id, JobStatus.PENDING)

        # Wait for the actor to start up asynchronously so this call always
        # returns immediately and we can catch errors with the actor starting
        # up. We may want to put this in an actor instead in the future.
        try:
            actor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=self.JOB_ACTOR_NAME.format(job_id=job_id),
                num_cpus=0,
                # Currently we assume JobManager is created by dashboard server
                # running on headnode, same for job supervisor actors scheduled
                resources={
                    self._get_current_node_resource_key(): 0.001,
                },
                runtime_env=runtime_env).remote(job_id, entrypoint, metadata
                                                or {})
            actor.run.remote(_start_signal_actor=_start_signal_actor)

            def callback(result: Optional[Exception]):
                return self._handle_supervisor_startup(job_id, result)

            actor.ready.remote()._on_completed(callback)
        except Exception as e:
            self._handle_supervisor_startup(job_id, e)

        return job_id

    def stop_job(self, job_id) -> bool:
        """Request job to exit, fire and forget.

        Args:
            job_id: ID of the job.
        Returns:
            stopped:
                True if there's running job
                False if no running job found
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
        """Get latest status of a job. If job supervisor actor is no longer
        alive, it will also attempt to make adjustments needed to bring job
        to correct terminiation state.

        All job status is stored and read only from GCS.

        Args:
            job_id: ID of the job.
        Returns:
            job_status: Latest known job status
        """
        job_supervisor_actor = self._get_actor_for_job(job_id)
        if job_supervisor_actor is None:
            # Job actor either exited or failed, we need to ensure never
            # left job in non-terminal status in case actor failed without
            # updating GCS with latest status.
            last_status = self._status_client.get_status(job_id)
            if last_status and last_status.status in {
                    JobStatus.PENDING, JobStatus.RUNNING
            }:
                self._status_client.put_status(job_id, JobStatus.FAILED)

        return self._status_client.get_status(job_id)

    def get_job_logs(self, job_id: str) -> str:
        return self._log_client.get_logs(job_id)

    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        if self.get_job_status(job_id) is None:
            raise RuntimeError(f"Job '{job_id}' does not exist.")

        for line in self._log_client.tail_logs(job_id):
            if line is None:
                # Return if the job has exited and there are no new log lines.
                status = self.get_job_status(job_id)
                if status.status not in {JobStatus.PENDING, JobStatus.RUNNING}:
                    return

                await asyncio.sleep(self.LOG_TAIL_SLEEP_S)
            else:
                yield line
