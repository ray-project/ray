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

        # fire and forget call from outer job manager to this actor
        self._stop_event = asyncio.Event()

    async def ready(self):
        pass

    async def _exec_cmd(self, cmd: str, stdout_path: str, stderr_path: str):
        """
        Runs a command as a child process, streaming stderr & stdout to given
        log files.
        """
        with open(stdout_path, "a+") as stdout, open(stderr_path,
                                                     "a+") as stderr:
            parent_pid = os.getpid()
            # Create new pgid with new subprocess to execute driver command
            child_process = await asyncio.create_subprocess_shell(
                cmd, start_new_session=True, stdout=stdout, stderr=stderr)
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

            return asyncio.create_task(child_process.wait()), child_process

    async def run(
            self,
            cmd: str,
            # Signal actor used in testing to capture PENDING -> RUNNING cases
            _start_signal_actor: Optional[SignalActor] = None):
        """Run the command, then exit afterwards.
        Should update state and logs.
        """
        cur_status = self.get_status()
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

            task, child_proc = await self._exec_cmd(cmd, stdout_path,
                                                    stderr_path)

            finished, _ = await asyncio.wait(
                [task, self._stop_event.wait()], return_when=FIRST_COMPLETED)

            if self._stop_event.is_set():
                self._status_client.put_status(self._job_id, JobStatus.STOPPED)
                task.cancel()
                child_proc.kill()
            else:
                # Child process finished execution
                assert len(
                    finished) == 1, "Should have only one coroutine done"
                for task in finished:
                    return_code = task.result()
                    if return_code == 0:
                        self._status_client.put_status(self._job_id,
                                                       JobStatus.SUCCEEDED)
                    else:
                        self._status_client.put_status(self._job_id,
                                                       JobStatus.FAILED)
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            # clean up actor after tasks are finished
            ray.actor.exit_actor()

    def get_status(self) -> JobStatus:
        return self._status_client.get_status(self._job_id)

    def stop(self):
        self._stop_event.set()


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
                    self._get_current_node_resource_key(): 0.001,
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
        supervisor.run.remote(entrypoint, _start_signal_actor)

        return job_id

    def stop_job(self, job_id) -> bool:
        """Request job to exit.

        Returns:
            stopped (bool):
                True if there's running actor job we intend to stop
                False if no running actor for the job found
        """
        job_supervisor_actor = self._get_actor_for_job(job_id)
        if job_supervisor_actor is not None:
            # Actor is still alive, signal it to stop the driver, fire and
            # forget
            job_supervisor_actor.stop.remote()
            return True
        else:
            return False

    def get_job_status(self, job_id: str):
        job_supervisor_actor = self._get_actor_for_job(job_id)
        if job_supervisor_actor is None:
            # Job actor either exited or failed, we need to ensure never left
            # job in non-terminal status in case actor failed without updating
            # GCS with latest status.
            last_status = self._status_client.get_status(job_id)
            if last_status in {JobStatus.PENDING, JobStatus.RUNNING}:
                self._status_client.put_status(job_id, JobStatus.FAILED)

        return self._status_client.get_status(job_id)

    def get_job_stdout(self, job_id: str) -> bytes:
        return self._log_client.get_stdout(job_id)

    def get_job_stderr(self, job_id: str) -> bytes:
        return self._log_client.get_stderr(job_id)
