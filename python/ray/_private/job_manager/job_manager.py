import subprocess
import pickle
import os
from typing import Any, Dict, List, Optional
from enum import Enum

import ray
from ray.actor import ActorHandle
from ray.exceptions import GetTimeoutError, RayActorError
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
    _internal_kv_get,
    _internal_kv_put,
)


class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class StorageClient:
    """
    Handles formatting of status and logs storage key given job id, and
    helper functions

    Status is handled by GCS internal KV.
    Logs is handled by appending to file on local disk within session dir.
    """
    JOB_LOGS_KEY = "_ray_internal_job_logs_{job_id}"
    JOB_STATUS_KEY = "_ray_internal_job_status_{job_id}"

    def __init__(self):
        assert _internal_kv_initialized()

    def get_logs(self, job_id: str):
        logs_file_path = self.get_logs_file_path(job_id)
        with open(logs_file_path, "rb") as f:
            return f.read().rstrip()

    def put_status(self, job_id: str, status: JobStatus):
        assert isinstance(status, JobStatus)
        _internal_kv_put(
            self.JOB_STATUS_KEY.format(job_id=job_id), pickle.dumps(status))

    def get_status(self, job_id: str) -> JobStatus:
        pickled_status = _internal_kv_get(
            self.JOB_STATUS_KEY.format(job_id=job_id))
        assert pickled_status is not None, f"Status not found for {job_id}"
        return pickle.loads(pickled_status)

    def get_logs_file_path(self, job_id: str) -> str:
        session_dir = ray.worker._global_node.get_session_dir_path()
        jobs_log_dir = session_dir + "/logs/jobs"
        if not os.path.exists(jobs_log_dir):
            os.mkdir(jobs_log_dir)

        log_file_name =  f"{self.JOB_LOGS_KEY.format(job_id=job_id)}.out"
        logs_file_path = jobs_log_dir + "/" + log_file_name
        return logs_file_path


def exec_cmd_logs_to_file(cmd: str, log_file: str) -> int:
    """
    Runs a command as a child process, streaming stderr & stdout to given
    log file.
    """
    with open(log_file, "a+") as fin:
        child = subprocess.Popen(
            [cmd],
            shell=True,
            universal_newlines=True,
            stdout=fin,
            stderr=subprocess.STDOUT)

        exit_code = child.wait()
        return exit_code


class JobSupervisor:
    """
    Created for each submitted job from JobManager, runs on head node in same
    process as ray dashboard.
    """
    def __init__(self, job_id: str):
        self._job_id = job_id
        self._status = JobStatus.PENDING
        self._client = StorageClient()

    def ready(self):
        pass

    def run(self, cmd: str):
        """Run the command, then exit afterwards.

        Should update state and logs.
        """
        assert self._status == JobStatus.PENDING, "Run should only be called once."
        self._status = JobStatus.RUNNING
        self._client.put_status(self._job_id, self._status)

        # 2) Run the command until it finishes, appending logs as it goes.
        # Set JobConfig for the child process (runtime_env, metadata).
        #  - RAY_JOB_CONFIG_JSON={...}

        logs_file_path = self._client.get_logs_file_path(self._job_id)
        exit_code = exec_cmd_logs_to_file(cmd, logs_file_path)

        # 3) Once command finishes, update status to SUCCEEDED or FAILED.
        if exit_code == 0:
            self._status = JobStatus.SUCCEEDED
        else:
            self._status = JobStatus.FAILED

        self._client.put_status(self._job_id, self.get_status())
        ray.actor.exit_actor()

    def get_status(self) -> JobStatus:
        return self._status

    def stop(self):
        pass


class JobManager:
    """
    Runs on head node in same process as ray dashboard.
    """
    JOB_ACTOR_NAME = "_ray_internal_job_actor_{job_id}"
    START_ACTOR_TIMEOUT_S = 10

    def __init__(self):
        self._client = StorageClient()
        self._supervisor_actor_cls = ray.remote(JobSupervisor)
        assert _internal_kv_initialized()

    def _get_actor_for_job(self, job_id: str) -> Optional[ActorHandle]:
        try:
            return ray.get_actor(self.JOB_ACTOR_NAME.format(job_id=job_id))
        except ValueError:  # Ray returns ValueError for nonexistent actor.
            return None

    def submit_job(self,
                   job_id: str,
                   entrypoint: str,
                   runtime_env: Optional[Dict[str, Any]] = None) -> str:
        """
        1) Create new detached actor with same runtime_env as job spec
        2) Get task / actor level runtime_env as env var and pass into
            subprocess
        3) subprocess.run(entrypoint)

        Returns unique job_id.
        """
        supervisor = self._supervisor_actor_cls.options(
            lifetime="detached",
            name=self.JOB_ACTOR_NAME.format(job_id=job_id),
            runtime_env=runtime_env,
        ).remote(job_id)

        try:
            ray.get(
                supervisor.ready.remote(), timeout=self.START_ACTOR_TIMEOUT_S)
        except GetTimeoutError:
            raise RuntimeError(f"Failed to start actor for job {job_id}.")

        # Kick off the job to run in the background.
        supervisor.run.remote(entrypoint)

        return job_id

    def stop_job(self, job_id) -> bool:
        """Request job to exit."""
        a = self._get_actor_for_job(job_id)
        if a is not None:
            # Actor is still alive, signal it to stop the driver.
            a.stop.remote()

    def get_job_status(self, job_id: str):
        a = self._get_actor_for_job(job_id)
        # Actor is still alive, try to get status from it.
        try:
            return ray.get(a.get_status.remote())
        except RayActorError:
            # Actor exited, so we should fall back to internal_kv.
            pass

        # Fall back to storage if the actor is dead.
        return self._client.get_status(job_id)

    def get_job_logs(self, job_id: str):
        # a = self._get_actor_for_job(job_id)
        # if a is not None:
        #     # Actor is still alive, try to get logs from it.
        #     try:
        #         return ray.get(a.get_logs.remote())
        #     except RayActorError:
        #         # Actor exited, so we should fall back to internal_kv.
        #         pass

        # Fall back to internal_kv if the actor is dead.
        return self._client.get_logs(job_id)


# 1. get the basic job manager + supervisor working echo
#  -> no runtime_env, just check process mgmt stuff
#  -> add unit tests for this
# 2. try query job status from job manager
# 2.5 write logs to ray.worker._global_node.get_session_dir_path()
# 3. setup job http server with basic API to wire up with job manager
# 4. get logs

# 5. process fate sharing ...
# 6. get runtime_env stuff working
# ...
# 7. optimize 4. but don't just overwrite
