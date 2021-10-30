import asyncio
import pickle
import os
import json
import psutil

from typing import Any, Dict, Tuple, Optional
from uuid import uuid4

import ray
import ray.ray_constants as ray_constants
from ray.actor import ActorHandle
from ray.serve.utils import get_current_node_resource_key
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
    _internal_kv_get,
    _internal_kv_put,
)
from ray.dashboard.modules.job.data_types import JobStatus
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR
from ray.serve.utils import logger


class JobLogStorageClient:
    """
    Disk storage for stdout / stderr of driver script logs.
    """
    JOB_LOGS_STDOUT_KEY = "_ray_internal_job_logs_{job_id}.out"
    JOB_LOGS_STDERR_KEY = "_ray_internal_job_logs_{job_id}.err"

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
    JOB_CHILD_PROCESS_PID_KEY = "_ray_internal_job_child_process_pid_{job_id}"

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

    def put_child_process_pid(self, job_id: str, pid):
        _internal_kv_put(
            self.JOB_CHILD_PROCESS_PID_KEY.format(job_id=job_id),
            pickle.dumps(pid))

    def get_child_process_pid(self, job_id: str):
        pickled_pid = _internal_kv_get(
            self.JOB_CHILD_PROCESS_PID_KEY.format(job_id=job_id))
        return pickle.loads(pickled_pid)


class JobSupervisor:
    """
    Ray actor created by JobManager for each submitted job, responsible to
    setup runtime_env, execute given shell command in subprocess, update job
    status and persist job logs.
    One job supervisor actor maps to one subprocess, for one job_id.
    Job supervisor actor should fate share with subprocess it created.
    """

    def __init__(self, job_id: str, metadata: Dict[str, str]):
        self._job_id = job_id
        self._status_client = JobStatusStorageClient()
        self._log_client = JobLogStorageClient()
        self._runtime_env = ray.get_runtime_context().runtime_env
        self._metadata = metadata

    async def ready(self):
        pass

    async def _exec_cmd(self, cmd: str, stdout_path: str, stderr_path: str):
        """
        Runs a command as a child process, streaming stderr & stdout to given
        log files.
        """

        with open(stdout_path, "a+") as stdout, open(stderr_path,
                                                     "a+") as stderr:
            child = await asyncio.create_subprocess_shell(
                cmd, stdout=stdout, stderr=stderr)
            logger.debug(f"Started driver command subprocess PID: {child.pid}")
            self._status_client.put_child_process_pid(self._job_id, child.pid)
            self._task_coro = asyncio.create_task(child.wait())
            exit_code = await self._task_coro
            return exit_code

    async def run(self, cmd: str):
        """Run the command, then exit afterwards.
        Should update state and logs.
        """
        cur_status = self.get_status()
        assert cur_status == JobStatus.PENDING, (
            "Run should only be called once.")
        self._status_client.put_status(self._job_id, JobStatus.RUNNING)
        exit_code = None

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

            exit_code = await self._exec_cmd(cmd, stdout_path, stderr_path)
        except asyncio.CancelledError:
            logger.debug("Driver command coroutine canceled.")
        except Exception as e:
            logger.debug(e)
        finally:
            logger.debug(
                f"Driver command coroutine returned, exit code: {exit_code}")
            # 3) Once command finishes, update status to SUCCEEDED or FAILED.
            # No action if command is stopped by user
            cur_status = self.get_status()
            if cur_status != JobStatus.STOPPED and exit_code is not None:
                # Update terminal status based on subprocess return code.
                if exit_code == 0:
                    self._status_client.put_status(self._job_id,
                                                   JobStatus.SUCCEEDED)
                else:
                    self._status_client.put_status(self._job_id,
                                                   JobStatus.FAILED)

            if exit_code is not None:
                # Exit code as None indicates driver command is terminated
                # by calling stop() which cancels task coroutine. In this case
                # we want to ensure stop() finishes with returned status and
                # offload supervisor actor exit at job manager level to avoid
                # race condition where supervisor actor died before
                # job_supervisor_actor.stop.remote() returns.
                logger.debug(
                    "Gracefully self-exiting job supervisor actor after "
                    "driver command finished execution.")
                ray.actor.exit_actor()

    def get_status(self) -> JobStatus:
        return self._status_client.get_status(self._job_id)

    def stop(self):
        def on_terminate(proc):
            logger.debug(
                f"process {proc} terminated with exit code {proc.returncode}")

        if self._task_coro is None:
            logger.debug("No running task to cancel.")
            return False
        else:
            logger.debug(
                f"Stopping task coroutine for job {self._job_id} ....")

            self._task_coro.cancel()
            pid = self._status_client.get_child_process_pid(self._job_id)
            if psutil.pid_exists(pid):
                child = psutil.Process(pid)
                child.terminate()
                gone, alive = psutil.wait_procs(
                    [child], timeout=3, callback=on_terminate)
                for child in alive:
                    child.kill()

            self._status_client.put_status(self._job_id, JobStatus.STOPPED)

            return True


class JobManager:
    """
    Provide python APIs for job submission and management. It does not provide
    job id generation or persistence, where all runtime data should be expected
    as lost once the ray cluster running job manager instance is down.
    """
    JOB_ACTOR_NAME = "_ray_internal_job_actor_{job_id}"
    # Time given to setup runtime_env for job supervisor actor.
    START_ACTOR_TIMEOUT_S = 60

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

    def submit_job(
            self,
            entrypoint: str,
            runtime_env: Optional[Dict[str, Any]] = None,
            metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Job execution happens asynchronously.

        1) Generate a new unique id for this job submission, expected to be
            executed in complete isolation.
        2) Create new detached actor with same runtime_env as job spec
        3) Get task / actor level runtime_env as env var and pass into
            subprocess
        4) asyncio.create_subprocess_shell(entrypoint)


        Args:

        Returns:

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
                # Currently we assume JobManager is created by dashboard server
                # running on headnode, same for job supervisor actors scheduled
                resources={
                    get_current_node_resource_key(): 0.001,
                },
                # For now we assume supervisor actor and driver script have
                # same runtime_env.
                runtime_env=runtime_env).remote(job_id, metadata or {})
            ray.get(
                supervisor.ready.remote(), timeout=self.START_ACTOR_TIMEOUT_S)
        except Exception as e:
            if supervisor:
                ray.kill(supervisor, no_restart=True)
            self._status_client.put_status(job_id, JobStatus.FAILED)
            raise RuntimeError(
                f"Failed to start actor for job {job_id}. This could be "
                "runtime_env configuration failure, or timed out after "
                f"{self.START_ACTOR_TIMEOUT_S} secs. "
                f"Exception message: {str(e)}")

        # Kick off the job to run in the background.
        supervisor.run.remote(entrypoint)

        return job_id

    def stop_job(self, job_id) -> bool:
        """Request job to exit."""
        job_supervisor_actor = self._get_actor_for_job(job_id)
        if job_supervisor_actor is not None:
            # Actor is still alive, signal it to stop the driver, then ensure
            # job actor exits.
            killed = ray.get(job_supervisor_actor.stop.remote())
            ray.kill(job_supervisor_actor, no_restart=True)
            return killed
        else:
            return False

    def get_job_status(self, job_id: str):
        job_supervisor_actor = self._get_actor_for_job(job_id)
        # Actor is still alive, try to get status from it.
        if job_supervisor_actor is None:
            # TODO (jiaodong): There're corner cases here where the job actor
            # died without writing job status to status client. We need to
            # ensure to do best effort recovery and don't leave its status
            # in non-terminal state forever.
            pass

        # Fall back to storage if the actor is dead.
        return self._status_client.get_status(job_id)

    def get_job_stdout(self, job_id: str) -> bytes:
        return self._log_client.get_stdout(job_id)

    def get_job_stderr(self, job_id: str) -> bytes:
        return self._log_client.get_stderr(job_id)
