import dataclasses
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import packaging.version

import ray
from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.dashboard.modules.job.common import (
    JobDeleteResponse,
    JobLogsResponse,
    JobStatus,
    JobStopResponse,
    JobSubmitRequest,
    JobSubmitResponse,
)
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.dashboard.modules.job.utils import strip_keys_with_value_none
from ray.dashboard.utils import get_address_for_submission_client
from ray.runtime_env import RuntimeEnv
from ray.runtime_env.runtime_env import _validate_no_local_paths
from ray.util.annotations import PublicAPI

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobSubmissionClient(SubmissionClient):
    """A local client for submitting and interacting with jobs on a remote cluster.

    Submits requests over HTTP to the job server on the cluster using the REST API.


    Args:
        address: Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>". If unspecified, will try to connect to
            a running local Ray cluster. This argument is always overridden by the
            RAY_API_SERVER_ADDRESS or RAY_ADDRESS environment variable.
        create_cluster_if_needed: Indicates whether the cluster at the specified
            address needs to already be running. Ray doesn't start a cluster
            before interacting with jobs, but third-party job managers may do so.
        cookies: Cookies to use when sending requests to the HTTP job server.
        metadata: Arbitrary metadata to store along with all jobs.  New metadata
            specified per job will be merged with the global metadata provided here
            via a simple dict update.
        headers: Headers to use when sending requests to the HTTP job server, used
            for cases like authentication to a remote cluster.
        verify: Boolean indication to verify the server's TLS certificate or a path to
            a file or directory of trusted certificates. Default: True.
    """

    def __init__(
        self,
        address: Optional[str] = None,
        create_cluster_if_needed: bool = False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        verify: Optional[Union[str, bool]] = True,
    ):
        self._client_ray_version = ray.__version__
        """Initialize a JobSubmissionClient and check the connection to the cluster."""
        if requests is None:
            raise RuntimeError(
                "The Ray jobs CLI & SDK require the ray[default] "
                "installation: `pip install 'ray[default]'`"
            )
        # Check types of arguments
        if address is not None and not isinstance(address, str):
            raise TypeError(f"address must be a string, got {type(address)}")
        if not isinstance(create_cluster_if_needed, bool):
            raise TypeError(
                f"create_cluster_if_needed must be a bool, got"
                f" {type(create_cluster_if_needed)}"
            )
        if cookies is not None and not isinstance(cookies, dict):
            raise TypeError(f"cookies must be a dict, got {type(cookies)}")
        if metadata is not None and not isinstance(metadata, dict):
            raise TypeError(f"metadata must be a dict, got {type(metadata)}")
        if headers is not None and not isinstance(headers, dict):
            raise TypeError(f"headers must be a dict, got {type(headers)}")
        if not (isinstance(verify, str) or isinstance(verify, bool)):
            raise TypeError(f"verify must be a str or bool, got {type(verify)}")

        api_server_url = get_address_for_submission_client(address)

        super().__init__(
            address=api_server_url,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
            verify=verify,
        )
        self._check_connection_and_version(
            min_version="1.9",
            version_error_message="Jobs API is not supported on the Ray "
            "cluster. Please ensure the cluster is "
            "running Ray 1.9 or higher.",
        )

        # In ray>=2.0, the client sends the new kwarg `submission_id` to the server
        # upon every job submission, which causes servers with ray<2.0 to error.
        if packaging.version.parse(self._client_ray_version) > packaging.version.parse(
            "2.0"
        ):
            self._check_connection_and_version(
                min_version="2.0",
                version_error_message=f"Client Ray version {self._client_ray_version} "
                "is not compatible with the Ray cluster. Please ensure the cluster is "
                "running Ray 2.0 or higher or downgrade the client Ray version.",
            )

    @PublicAPI(stability="stable")
    def submit_job(
        self,
        *,
        entrypoint: str,
        job_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
        submission_id: Optional[str] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_memory: Optional[int] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
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
            submission_id: A unique ID for this job.
            runtime_env: The runtime environment to install and run this job in.
            metadata: Arbitrary data to store along with this job.
            job_id: DEPRECATED. This has been renamed to submission_id
            entrypoint_num_cpus: The quantity of CPU cores to reserve for the execution
                of the entrypoint command, separately from any tasks or actors launched
                by it. Defaults to 0.
            entrypoint_num_gpus: The quantity of GPUs to reserve for the execution
                of the entrypoint command, separately from any tasks or actors launched
                by it. Defaults to 0.
            entrypoint_memory: The quantity of memory to reserve for the
                execution of the entrypoint command, separately from any tasks or
                actors launched by it. Defaults to 0.
            entrypoint_resources: The quantity of custom resources to reserve for the
                execution of the entrypoint command, separately from any tasks or
                actors launched by it.

        Returns:
            The submission ID of the submitted job.  If not specified,
            this is a randomly generated unique ID.

        Raises:
            RuntimeError: If the request to the job server fails, or if the specified
                submission_id has already been used by a job on this cluster.
        """
        if job_id:
            logger.warning(
                "job_id kwarg is deprecated. Please use submission_id instead."
            )

        if entrypoint_num_cpus or entrypoint_num_gpus or entrypoint_resources:
            self._check_connection_and_version(
                min_version="2.2",
                version_error_message="`entrypoint_num_cpus`, `entrypoint_num_gpus`, "
                "and `entrypoint_resources` kwargs "
                "are not supported on the Ray cluster. Please ensure the cluster is "
                "running Ray 2.2 or higher.",
            )

        if entrypoint_memory:
            self._check_connection_and_version(
                min_version="2.8",
                version_error_message="`entrypoint_memory` kwarg "
                "is not supported on the Ray cluster. Please ensure the cluster is "
                "running Ray 2.8 or higher.",
            )

        runtime_env = runtime_env or {}
        metadata = metadata or {}
        metadata.update(self._default_metadata)

        self._upload_working_dir_if_needed(runtime_env)
        self._upload_py_modules_if_needed(runtime_env)

        # Verify worker_process_setup_hook type.
        setup_hook = runtime_env.get("worker_process_setup_hook")
        if setup_hook and not isinstance(setup_hook, str):
            raise ValueError(
                f"Invalid type {type(setup_hook)} for `worker_process_setup_hook`. "
                "When a job submission API is used, `worker_process_setup_hook` "
                "only allows a string type (module name). "
                "Specify `worker_process_setup_hook` via "
                "ray.init within a driver to use a `Callable` type. "
            )

        # Run the RuntimeEnv constructor to parse local pip/conda requirements files.
        runtime_env = RuntimeEnv(**runtime_env)
        _validate_no_local_paths(runtime_env)
        runtime_env = runtime_env.to_dict()

        submission_id = submission_id or job_id
        req = JobSubmitRequest(
            entrypoint=entrypoint,
            submission_id=submission_id,
            runtime_env=runtime_env,
            metadata=metadata,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_memory=entrypoint_memory,
            entrypoint_resources=entrypoint_resources,
        )

        # Remove keys with value None so that new clients with new optional fields
        # are still compatible with older servers.  This is also done on the server,
        # but we do it here as well to be extra defensive.
        json_data = strip_keys_with_value_none(dataclasses.asdict(req))

        logger.debug(f"Submitting job with submission_id={submission_id}.")
        r = self._do_request("POST", "/api/jobs/", json_data=json_data)

        if r.status_code == 200:
            return JobSubmitResponse(**r.json()).submission_id
        else:
            self._raise_error(r)

    @PublicAPI(stability="stable")
    def stop_job(
        self,
        job_id: str,
    ) -> bool:
        """Request a job to exit asynchronously.

        Attempts to terminate process first, then kills process after timeout.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> sub_id = client.submit_job(entrypoint="sleep 10") # doctest: +SKIP
            >>> client.stop_job(sub_id) # doctest: +SKIP
            True

        Args:
            job_id: The job ID or submission ID for the job to be stopped.

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

    @PublicAPI(stability="stable")
    def delete_job(
        self,
        job_id: str,
    ) -> bool:
        """Delete a job in a terminal state and all of its associated data.

        If the job is not already in a terminal state, raises an error.
        This does not delete the job logs from disk.
        Submitting a job with the same submission ID as a previously
        deleted job is not supported and may lead to unexpected behavior.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient() # doctest: +SKIP
            >>> job_id = client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.delete_job(job_id) # doctest: +SKIP
            True

        Args:
            job_id: submission ID for the job to be deleted.

        Returns:
            True if the job was deleted, otherwise False.

        Raises:
            RuntimeError: If the job does not exist, if the request to the
                job server fails, or if the job is not in a terminal state.
        """
        logger.debug(f"Deleting job with job_id={job_id}.")
        r = self._do_request("DELETE", f"/api/jobs/{job_id}")

        if r.status_code == 200:
            return JobDeleteResponse(**r.json()).deleted
        else:
            self._raise_error(r)

    @PublicAPI(stability="stable")
    def get_job_info(
        self,
        job_id: str,
    ) -> JobDetails:
        """Get the latest status and other information associated with a job.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> submission_id = client.submit_job(entrypoint="sleep 1") # doctest: +SKIP
            >>> client.get_job_info(submission_id) # doctest: +SKIP
            JobDetails(status='SUCCEEDED',
            job_id='03000000', type='submission',
            submission_id='raysubmit_4LamXRuQpYdSMg7J',
            message='Job finished successfully.', error_type=None,
            start_time=1647388711, end_time=1647388712, metadata={}, runtime_env={})

        Args:
            job_id: The job ID or submission ID of the job whose information
                is being requested.

        Returns:
            The JobDetails for the job.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
                job server fails.
        """
        r = self._do_request("GET", f"/api/jobs/{job_id}")

        if r.status_code == 200:
            if JobDetails is None:
                raise RuntimeError(
                    "The Ray jobs CLI & SDK require the ray[default] "
                    "installation: `pip install 'ray[default]'`"
                )
            else:
                return JobDetails(**r.json())
        else:
            self._raise_error(r)

    @PublicAPI(stability="stable")
    def list_jobs(self) -> List[JobDetails]:
        """List all jobs along with their status and other information.

        Lists all jobs that have ever run on the cluster, including jobs that are
        currently running and jobs that are no longer running.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.submit_job(entrypoint="sleep 2") # doctest: +SKIP
            >>> client.list_jobs() # doctest: +SKIP
            [JobDetails(status='SUCCEEDED',
            job_id='03000000', type='submission',
            submission_id='raysubmit_4LamXRuQpYdSMg7J',
            message='Job finished successfully.', error_type=None,
            start_time=1647388711, end_time=1647388712, metadata={}, runtime_env={}),
            JobDetails(status='RUNNING',
            job_id='04000000', type='submission',
            submission_id='raysubmit_1dxCeNvG1fCMVNHG',
            message='Job is currently running.', error_type=None,
            start_time=1647454832, end_time=None, metadata={}, runtime_env={})]

        Returns:
            A list of JobDetails containing the job status and other information.

        Raises:
            RuntimeError: If the request to the job server fails.
        """
        r = self._do_request("GET", "/api/jobs/")

        if r.status_code == 200:
            jobs_info_json = r.json()
            jobs_info = [
                JobDetails(**job_info_json) for job_info_json in jobs_info_json
            ]
            return jobs_info
        else:
            self._raise_error(r)

    @PublicAPI(stability="stable")
    def get_job_status(self, job_id: str) -> JobStatus:
        """Get the most recent status of a job.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.get_job_status("raysubmit_4LamXRuQpYdSMg7J") # doctest: +SKIP
            'SUCCEEDED'

        Args:
            job_id: The job ID or submission ID of the job whose status is being
                requested.

        Returns:
            The JobStatus of the job.

        Raises:
            RuntimeError: If the job does not exist or if the request to the
                job server fails.
        """
        return self.get_job_info(job_id).status

    @PublicAPI(stability="stable")
    def get_job_logs(self, job_id: str) -> str:
        """Get all logs produced by a job.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> sub_id = client.submit_job(entrypoint="echo hello") # doctest: +SKIP
            >>> client.get_job_logs(sub_id) # doctest: +SKIP
            'hello\\n'

        Args:
            job_id: The job ID or submission ID of the job whose logs are being
                requested.

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

    @PublicAPI(stability="stable")
    async def tail_job_logs(self, job_id: str) -> AsyncIterator[str]:
        """Get an iterator that follows the logs of a job.

        Example:
            >>> from ray.job_submission import JobSubmissionClient
            >>> client = JobSubmissionClient("http://127.0.0.1:8265") # doctest: +SKIP
            >>> submission_id = client.submit_job( # doctest: +SKIP
            ...     entrypoint="echo hi && sleep 5 && echo hi2")
            >>> async for lines in client.tail_job_logs( # doctest: +SKIP
            ...           'raysubmit_Xe7cvjyGJCyuCvm2'):
            ...     print(lines, end="") # doctest: +SKIP
            hi
            hi2

        Args:
            job_id: The job ID or submission ID of the job whose logs are being
                requested.

        Returns:
            The iterator.

        Raises:
            RuntimeError: If the job does not exist, if the request to the
                job server fails, or if the connection closes unexpectedly
                before the job reaches a terminal state.
        """
        async with aiohttp.ClientSession(
            cookies=self._cookies, headers=self._headers
        ) as session:
            ws = await session.ws_connect(
                f"{self._address}/api/jobs/{job_id}/logs/tail", ssl=self._ssl_context
            )

            while True:
                msg = await ws.receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    yield msg.data
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.debug(
                        f"WebSocket closed for job {job_id} with close code {ws.close_code}"
                    )
                    if ws.close_code == aiohttp.WSCloseCode.ABNORMAL_CLOSURE:
                        raise RuntimeError(
                            f"WebSocket connection closed unexpectedly with close code {ws.close_code}"
                        )
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    # Old Ray versions may send ERROR on connection close
                    logger.debug(
                        f"WebSocket error for job {job_id}, treating as normal close. Err: {ws.exception()}"
                    )
                    break
