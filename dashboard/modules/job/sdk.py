import dataclasses
import logging
from typing import Any, Dict, Iterator, Optional

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None

from ray.dashboard.modules.job.common import (
    JobStatus,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobInfo,
    JobLogsResponse,
)
from ray.dashboard.modules.dashboard_sdk import SubmissionClient

from ray.runtime_env import RuntimeEnv

from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobSubmissionClient(SubmissionClient):
    def __init__(
        self,
        address: str,
        create_cluster_if_needed=False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        if requests is None:
            raise RuntimeError(
                "The Ray jobs CLI & SDK require the ray[default] "
                "installation: `pip install 'ray[default']``"
            )
        super().__init__(
            address=address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
        )
        self._check_connection_and_version(
            min_version="1.9",
            version_error_message="Jobs API is not supported on the Ray "
            "cluster. Please ensure the cluster is "
            "running Ray 1.9 or higher.",
        )

    @PublicAPI(stability="beta")
    def submit_job(
        self,
        *,
        entrypoint: str,
        job_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Submit and execute a job asynchronously.

        When a job is submitted, it runs once to completion or failure. Retries or
        different runs with different parameters should be handled by the
        submitter. Jobs are bound to the lifetime of a Ray cluster, so if the
        cluster goes down, all running jobs on that cluster will be terminated.

        Example:
            >>> job_submission_client.submit_job(
            >>>     entrypoint="python script.py",
            >>>     runtime_env={
            >>>         "working_dir": "./",
            >>>         "pip": ["requests==2.26.0"]
            >>>     }
            >>> )
            'raysubmit_4LamXRuQpYdSMg7J'

        Args:
            entrypoint: The shell command to run for this job.
            job_id: A unique ID for this job.
            runtime_env: The runtime environment to install and run this job in.
            metadata: Arbitrary data to store along with this job.

        Returns:
            The job ID of the submitted job.  If not specified, this is a randomly
            generated unique ID.

        Raises:
            RuntimeError: If the request to the job server fails, or if the specified
            job_id has already been used by a job on this cluster.
        """
        runtime_env = runtime_env or {}
        metadata = metadata or {}
        metadata.update(self._default_metadata)

        self._upload_working_dir_if_needed(runtime_env)
        self._upload_py_modules_if_needed(runtime_env)

        # Run the RuntimeEnv constructor to parse local pip/conda requirements files.
        runtime_env = RuntimeEnv(**runtime_env).to_dict()

        req = JobSubmitRequest(
            entrypoint=entrypoint,
            job_id=job_id,
            runtime_env=runtime_env,
            metadata=metadata,
        )

        logger.debug(f"Submitting job with job_id={job_id}.")
        r = self._do_request("POST", "/api/jobs/", json_data=dataclasses.asdict(req))

        if r.status_code == 200:
            return JobSubmitResponse(**r.json()).job_id
        else:
            self._raise_error(r)

    @PublicAPI(stability="beta")
    def stop_job(
        self,
        job_id: str,
    ) -> bool:
        """Request a job to exit asynchronously.

        Example:
            >>> job_submission_client.stop_job("raysubmit_T8zDX5W1mpRpmtWt")
            True

        Args:
            job_id: The job ID for the job to be stopped.

        Returns:
            True if the job was running, otherwise False.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        logger.debug(f"Stopping job with job_id={job_id}.")
        r = self._do_request("POST", f"/api/jobs/{job_id}/stop")

        if r.status_code == 200:
            return JobStopResponse(**r.json()).stopped
        else:
            self._raise_error(r)

    @PublicAPI(stability="beta")
    def get_job_info(
        self,
        job_id: str,
    ) -> JobInfo:
        """Get the latest status and other information associated with a job.

        Example:
            >>> job_submission_client.get_job_info("raysubmit_4LamXRuQpYdSMg7J")
            JobInfo(status='SUCCEEDED', message='Job finished successfully.',
            error_type=None, start_time=1647388711, end_time=1647388712,
            metadata={}, runtime_env={})

        Args:
            job_id: The ID of the job whose information is being requested.

        Returns:
            The information.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        r = self._do_request("GET", f"/api/jobs/{job_id}")

        if r.status_code == 200:
            return JobInfo(**r.json())
        else:
            self._raise_error(r)

    @PublicAPI(stability="beta")
    def list_jobs(self) -> Dict[str, JobInfo]:
        """List all jobs along with their status and other information.

        Lists all jobs that have ever run on the cluster, including jobs that are
        currently running and jobs that are no longer running.

        Example:
            >>> job_submission_client.list_jobs()
            {'raysubmit_4LamXRuQpYdSMg7J': JobInfo(status='SUCCEEDED',
            message='Job finished successfully.', error_type=None,
            start_time=1647388711, end_time=1647388712, metadata={}, runtime_env={}),
            'raysubmit_T8zDX5W1mpRpmtWt': JobInfo(status='STOPPED',
            message='Job was intentionally stopped.', error_type=None,
            start_time=1647389440, end_time=1647389492, metadata={}, runtime_env={})}

        Returns:
            A dictionary mapping jobs to their information.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        r = self._do_request("GET", "/api/jobs/")

        if r.status_code == 200:
            jobs_info_json = r.json()
            jobs_info = {
                job_id: JobInfo(**job_info_json)
                for job_id, job_info_json in jobs_info_json.items()
            }
            return jobs_info
        else:
            self._raise_error(r)

    @PublicAPI(stability="beta")
    def get_job_status(self, job_id: str) -> JobStatus:
        """Get the most recent status of a job.

        Example:
            >>> job_submission_client.get_job_info("raysubmit_4LamXRuQpYdSMg7J")
            'SUCCEEDED'

        Args:
            job_id: The ID of the job whose status is being requested.

        Returns:
            The status.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        return self.get_job_info(job_id).status

    @PublicAPI(stability="beta")
    def get_job_logs(self, job_id: str) -> str:
        """Get all logs produced by a job.

        Example:
            >>> job_submission_client.get_job_logs("raysubmit_4LamXRuQpYdSMg7J")
            'hello world\\n'

        Args:
            job_id: The ID of the job whose logs are being requested.

        Returns:
            A string containing the full logs of the job.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        r = self._do_request("GET", f"/api/jobs/{job_id}/logs")

        if r.status_code == 200:
            return JobLogsResponse(**r.json()).logs
        else:
            self._raise_error(r)

    @PublicAPI(stability="beta")
    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        """Get an iterator that follows the logs of a job.

        Example:
            >>> async for lines in client.tail_job_logs('raysubmit_Xe7cvjyGJCyuCvm2'):
            >>>     print(lines, end="")
            hello 1
            hello 2
            hello 3

        Args:
            job_id: The ID of the job whose logs are being requested.

        Returns:
            The iterator.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        async with aiohttp.ClientSession(cookies=self._cookies) as session:
            ws = await session.ws_connect(
                f"{self._address}/api/jobs/{job_id}/logs/tail"
            )

            while True:
                msg = await ws.receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    yield msg.data
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    pass
