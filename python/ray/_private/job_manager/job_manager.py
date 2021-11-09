import asyncio
from asyncio.tasks import FIRST_COMPLETED
import pickle
import os
import json
import logging
import traceback
import subprocess

from typing import Any, Dict, Tuple, Optional
from uuid import uuid4

import ray
import ray.ray_constants as ray_constants
from ray.actor import ActorHandle
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
    _internal_kv_get,
    _internal_kv_put,
)
from ray.dashboard.modules.job.data_types import JobStatus
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
# Used in testing only to cover job status under concurrency
from ray._private.test_utils import SignalActor

logger = logging.getLogger(__name__)

# asyncio python version compatibility
try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

JOB_ID_METADATA_KEY = "job_submission_id"


class JobLogStorageClient:
    """
    Disk storage for stdout / stderr of driver script logs.
    """
    JOB_LOGS_STDOUT_KEY = "job-driver-{job_id}.out"
    JOB_LOGS_STDERR_KEY = "job-driver-{job_id}.err"

    def get_stdout(self, job_id: str):
        stdout_file, _ = self.get_log_file_paths(job_id)
        try:
            with open(stdout_file, "rb") as f:
                return f.read().rstrip()
        except FileNotFoundError:
            return b"No stdout log available yet."

    def get_stderr(self, job_id: str):
        _, stderr_file = self.get_log_file_paths(job_id)
        try:
            with open(stderr_file, "rb") as f:
                return f.read().rstrip()
        except FileNotFoundError:
            return b"No stderr log available yet."

    def get_log_file_paths(self, job_id: str) -> Tuple[str, str]:
        """
        Get file paths to logs of given job. Example:

        stdout:
            /tmp/ray/session_date/logs/jobs/_ray_internal_job_logs_{job_id}.out
        stderr:
            /tmp/ray/session_date/logs/jobs/_ray_internal_job_logs_{job_id}.err
        """
        session_dir = ray.worker._global_node.get_session_dir_path()
        jobs_log_dir = os.path.join(session_dir + "/logs/jobs")
        if not os.path.exists(jobs_log_dir):
            os.mkdir(jobs_log_dir)

        stdout_file_name = f"{self.JOB_LOGS_STDOUT_KEY.format(job_id=job_id)}"
        stderr_file_name = f"{self.JOB_LOGS_STDERR_KEY.format(job_id=job_id)}"

        return (os.path.join(jobs_log_dir, stdout_file_name),
                os.path.join(jobs_log_dir, stderr_file_name))


class JobStatusStorageClient:
    """
    Handles formatting of status storage key given job id.
    """
    JOB_STATUS_KEY = "_ray_internal_job_status_{job_id}"

    def __init__(self):
        assert _internal_kv_initialized()

    def put_status(self, job_id: str, status: JobStatus):
        assert isinstance(status, JobStatus)
        _internal_kv_put(
            self.JOB_STATUS_KEY.format(job_id=job_id), pickle.dumps(status))

    def get_status(self, job_id: str) -> JobStatus:
        pickled_status = _internal_kv_get(
            self.JOB_STATUS_KEY.format(job_id=job_id))
        assert pickled_status is not None, f"Status not found for {job_id}"
        return pickle.loads(pickled_status)


class JobSupervisor:
    """
    Ray actor created by JobManager for each submitted job, responsible to
    setup runtime_env, execute given shell command in subprocess, update job
    status, persist job logs and manage subprocess group cleaning.

    One job supervisor actor maps to one subprocess, for one job_id.
    Job supervisor actor should fate share with subprocess it created.
    """

    SUBPROCESS_POLL_PERIOD_S = 0.1

    def __init__(self, job_id: str, metadata: Dict[str, str]):
        self._job_id = job_id
        self._status_client = JobStatusStorageClient()
        self._log_client = JobLogStorageClient()
        self._runtime_env = ray.get_runtime_context().runtime_env

        self._metadata = metadata
        self._metadata[JOB_ID_METADATA_KEY] = job_id

        # fire and forget call from outer job manager to this actor
        self._stop_event = asyncio.Event()

    async def ready(self):
        """Dummy object ref. Return of this function represents job supervisor
        actor stated successfully with runtime_env configured, and is ready to
        move on to running state.
        """
        pass

    async def _exec_entrypoint_cmd(self, entrypoint_cmd: str, stdout_path: str,
                                   stderr_path: str) -> subprocess.Popen:
        """
        Runs a command as a child process, streaming stderr & stdout to given
        log files.

        Meanwhile we start a demon process and group driver
        subprocess in same pgid, such that if job actor dies, entire process
        group also fate share with it.

        Args:
            entrypoint_cmd: Driver command to execute in subprocess.
            stdout_path: File path on head node's local disk to store driver
                command's stdout.
            stderr_path: File path on head node's local disk to store driver
                command's stderr.
        Returns:
            child_process: Child process that runs the driver command. Can be
                terminated or killed upon user calling stop().
        """
        with open(stdout_path, "a+") as stdout, open(stderr_path,
                                                     "a+") as stderr:
            child_process = subprocess.Popen(
                entrypoint_cmd,
                shell=True,
                start_new_session=True,
                stdout=stdout,
                stderr=stderr)
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
            entrypoint_cmd: str,
            # Signal actor used in testing to capture PENDING -> RUNNING cases
            _start_signal_actor: Optional[SignalActor] = None):
        """
        Stop and start both happen asynchrously, coordinated by asyncio event
        and coroutine, respectively.

        1) Sets job status as running
        2) Pass runtime env and metadata to subprocess as serialized env
            variables.
        3) Handle concurrent events of driver execution and
        """
        cur_status = self._get_status()
        assert cur_status == JobStatus.PENDING, (
            "Run should only be called once.")

        if _start_signal_actor:
            # Block in PENDING state until start signal received.
            await _start_signal_actor.wait.remote()

        self._status_client.put_status(self._job_id, JobStatus.RUNNING)

        try:
            # Set JobConfig for the child process (runtime_env, metadata).
            os.environ[RAY_JOB_CONFIG_JSON_ENV_VAR] = json.dumps({
                "runtime_env": self._runtime_env,
                "metadata": self._metadata,
            })
            ray_redis_address = ray._private.services.find_redis_address_or_die(  # noqa: E501
            )
            os.environ[ray_constants.
                       RAY_ADDRESS_ENVIRONMENT_VARIABLE] = ray_redis_address

            stdout_path, stderr_path = self._log_client.get_log_file_paths(
                self._job_id)
            child_process = await self._exec_entrypoint_cmd(
                entrypoint_cmd, stdout_path, stderr_path)

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
                    self._status_client.put_status(self._job_id,
                                                   JobStatus.FAILED)
        except Exception:
            logger.error(
                "Got unexpected exception while trying to execute driver "
                f"command. {traceback.format_exc()}")
        finally:
            # clean up actor after tasks are finished
            ray.actor.exit_actor()

    def _get_status(self) -> JobStatus:
        return self._status_client.get_status(self._job_id)

    def stop(self):
        """Set step_event and let run() handle the rest in its asyncio.wait().
        """
        self._stop_event.set()


class JobManager:
    """
    Provide python APIs for job submission and management. It does not provide
    job id generation or persistence, where all runtime data should be expected
    as lost once the ray cluster running job manager instance is down.
    """
    JOB_ACTOR_NAME = "_ray_internal_job_actor_{job_id}"

    def __init__(self):
        self._status_client = JobStatusStorageClient()
        self._log_client = JobLogStorageClient()
        self._supervisor_actor_cls = ray.remote(JobSupervisor)

        assert _internal_kv_initialized()

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
                "Cannot found the node dictionary for current node.")

    def submit_job(self,
                   entrypoint: str,
                   runtime_env: Optional[Dict[str, Any]] = None,
                   metadata: Optional[Dict[str, str]] = None,
                   _start_signal_actor: Optional[SignalActor] = None) -> str:
        """
        Job execution happens asynchronously.

        1) Generate a new unique id for this job submission, each call of this
            method assumes they're independent submission with its own new
            uuid, job supervisor actor and child process.
        2) Create new detached actor with same runtime_env as job spec

        Actual setting up runtime_env, subprocess group, driver command
        execution, subprocess cleaning up and running status update to GCS
        is all handled by job supervisor actor.

        Args:
            entrypoint: Driver command to execute in subprocess shell.
                Represents the entrypoint to start user application.
            runtime_env: Runtime environment used to execute driver command,
                which could contain its own ray.init() to configure runtime
                env at ray cluster, task and actor level. For now, we
                assume same runtime_env used for job supervisor actor and
                driver command.
            metadata: Support passing arbitrary data to driver command in
                case needed.
            _start_signal_actor: Used in testing only to capture state
                transitions between PENDING -> RUNNING. Regular user shouldn't
                need this.

        Returns:
            job_id: Generated uuid for further job management. Only valid
                within the same ray cluster.
        """
        job_id = str(uuid4())
        self._status_client.put_status(job_id, JobStatus.PENDING)
        supervisor = None

        try:
            logger.debug(
                f"Submitting job with generated internal job_id: {job_id}")
            supervisor = self._supervisor_actor_cls.options(
                lifetime="detached",
                name=self.JOB_ACTOR_NAME.format(job_id=job_id),
                num_cpus=0,
                # Currently we assume JobManager is created by dashboard server
                # running on headnode, same for job supervisor actors scheduled
                resources={
                    self._get_current_node_resource_key(): 0.001,
                },
                # For now we assume supervisor actor and driver script have
                # same runtime_env.
                runtime_env=runtime_env).remote(job_id, metadata or {})
            ray.get(supervisor.ready.remote())
        except Exception as e:
            if supervisor:
                ray.kill(supervisor, no_restart=True)
            self._status_client.put_status(job_id, JobStatus.FAILED)
            raise RuntimeError(
                f"Failed to start actor for job {job_id}. This could be "
                "runtime_env configuration failure or invalid runtime_env."
                f"Exception message: {str(e)}")

        # Kick off the job to run in the background.
        supervisor.run.remote(entrypoint, _start_signal_actor)

        return job_id

    def stop_job(self, job_id) -> bool:
        """Request job to exit, fire and forget.

        Args:
            job_id: Generated uuid from submit_job. Only valid in same ray
                cluster.
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

    def get_job_status(self, job_id: str) -> JobStatus:
        """Get latest status of a job. If job supervisor actor is no longer
        alive, it will also attempt to make adjustments needed to bring job
        to correct terminiation state.

        All job status is stored and read only from GCS.

        Args:
            job_id: Generated uuid from submit_job. Only valid in same ray
                cluster.
        Returns:
            job_status: Latest known job status
        """
        job_supervisor_actor = self._get_actor_for_job(job_id)
        if job_supervisor_actor is None:
            # Job actor either exited or failed, we need to ensure never
            # left job in non-terminal status in case actor failed without
            # updating GCS with latest status.
            last_status = self._status_client.get_status(job_id)
            if last_status in {JobStatus.PENDING, JobStatus.RUNNING}:
                self._status_client.put_status(job_id, JobStatus.FAILED)

        return self._status_client.get_status(job_id)

    def get_job_stdout(self, job_id: str) -> bytes:
        return self._log_client.get_stdout(job_id)

    def get_job_stderr(self, job_id: str) -> bytes:
        return self._log_client.get_stderr(job_id)
