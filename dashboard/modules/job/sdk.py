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
    """A local client for submitting and interacting with jobs on a remote cluster.

    Submits requests over HTTP to the job server on the cluster using the REST API.
    """

    def __init__(
        self,
        address: str,
        create_cluster_if_needed: bool = False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a JobSubmissionClient and check the connection to the cluster.

        Args:
            address: The IP address and port of the head node.
            create_cluster_if_needed: Indicates whether the cluster at the specified
                address needs to already be running. Ray doesn't start a cluster
                before interacting with jobs, but external job managers may do so.
            cookies: Cookies to use when sending requests to the HTTP job server.
            metadata: Arbitrary metadata to store along with all jobs.  New metadata
                specified per job will be merged with the global metadata provided here
                via a simple dict update.
            headers: Headers to use when sending requests to the HTTP job server, used
                for cases like authentication to a remote cluster.
        """
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
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> client.submit_job( # doctest: +SKIP
            ...     entrypoint="python script.py",
            ...     runtime_env={
            ...         "working_dir": "./",
            ...         "pip": ["requests==2.26.0"]
            ...     }
            ... )  # doctest: +SKIP
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
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> job_id = client.submit_job(entrypoint="sleep 10") # doctest: +SKIP
            >>> client.stop_job(job_id) # doctest: +SKIP
            True

        Args:
            job_id: The job ID for the job to be stopped.

        Returns:
            True if the job was running, otherwise False.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
            job server fails.
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
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> job_id = client.submit_job(entrypoint="sleep 1") # doctest: +SKIP
            >>> job_submission_client.get_job_info(job_id) # doctest: +SKIP
            JobInfo(status='SUCCEEDED', message='Job finished successfully.',
            error_type=None, start_time=1647388711, end_time=1647388712,
            metadata={}, runtime_env={})

        Args:
            job_id: The ID of the job whose information is being requested.

        Returns:
            The JobInfo for the job.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
            job server fails.
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
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.submit_job(entrypoint="sleep 2") # doctest: +SKIP
            >>> client.list_jobs() # doctest: +SKIP
            {'raysubmit_4LamXRuQpYdSMg7J': JobInfo(status='SUCCEEDED',
            message='Job finished successfully.', error_type=None,
            start_time=1647388711, end_time=1647388712, metadata={}, runtime_env={}),
            'raysubmit_1dxCeNvG1fCMVNHG': JobInfo(status='RUNNING',
            message='Job is currently running.', error_type=None,
            start_time=1647454832, end_time=None, metadata={}, runtime_env={})}

        Returns:
            A dictionary mapping job_ids to their information.

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
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.get_job_status("raysubmit_4LamXRuQpYdSMg7J") # doctest: +SKIP
            'SUCCEEDED'

        Args:
            job_id: The ID of the job whose status is being requested.

        Returns:
            The JobStatus of the job.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
            job server fails.
        """
        return self.get_job_info(job_id).status

    @PublicAPI(stability="beta")
    def get_job_logs(self, job_id: str) -> str:
        """Get all logs produced by a job.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> job_id = client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.get_job_logs(job_id) # doctest: +SKIP
            'hello\\n'

        Args:
            job_id: The ID of the job whose logs are being requested.

        Returns:
            A string containing the full logs of the job.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
            job server fails.
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
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> job_id = client.submit_job( # doctest: +SKIP
            ...     entrypoint="echo hi && sleep 5 && echo hi2")
            >>> async for lines in client.tail_job_logs( # doctest: +SKIP
            ...           'raysubmit_Xe7cvjyGJCyuCvm2'):
            ...     print(lines, end="") # doctest: +SKIP
            hi
            hi2

        Args:
            job_id: The ID of the job whose logs are being requested.

        Returns:
            The iterator.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
            job server fails.
        """
        async with aiohttp.ClientSession(
            cookies=self._cookies, headers=self._headers
        ) as session:
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
