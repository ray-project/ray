import asyncio
import pickle
import time
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ray._private import ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.packaging import parse_uri
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
)

# NOTE(edoakes): these constants should be considered a public API because
# they're exposed in the snapshot API.
JOB_ID_METADATA_KEY = "job_submission_id"
JOB_NAME_METADATA_KEY = "job_name"
JOB_ACTOR_NAME_TEMPLATE = (
    f"{ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX}job_actor_" + "{job_id}"
)
# In order to get information about SupervisorActors launched by different jobs,
# they must be set to the same namespace.
SUPERVISOR_ACTOR_RAY_NAMESPACE = "SUPERVISOR_ACTOR_RAY_NAMESPACE"


class JobStatus(str, Enum):
    """An enumeration for describing the status of a job."""

    #: The job has not started yet, likely waiting for the runtime_env to be set up.
    PENDING = "PENDING"
    #: The job is currently running.
    RUNNING = "RUNNING"
    #: The job was intentionally stopped by the user.
    STOPPED = "STOPPED"
    #: The job finished successfully.
    SUCCEEDED = "SUCCEEDED"
    #: The job failed.
    FAILED = "FAILED"

    def __str__(self) -> str:
        return f"{self.value}"

    def is_terminal(self) -> bool:
        """Return whether or not this status is terminal.

        A terminal status is one that cannot transition to any other status.
        The terminal statuses are "STOPPED", "SUCCEEDED", and "FAILED".

        Returns:
            True if this status is terminal, otherwise False.
        """
        return self.value in {"STOPPED", "SUCCEEDED", "FAILED"}


# TODO(aguo): Convert to pydantic model
@dataclass
class JobInfo:
    """A class for recording information associated with a job and its execution."""

    #: The status of the job.
    status: JobStatus
    #: The entrypoint command for this job.
    entrypoint: str
    #: A message describing the status in more detail.
    message: Optional[str] = None
    # TODO(architkulkarni): Populate this field with e.g. Runtime env setup failure,
    # Internal error, user script error
    error_type: Optional[str] = None
    #: The time when the job was started.  A Unix timestamp in ms.
    start_time: Optional[int] = None
    #: The time when the job moved into a terminal state.  A Unix timestamp in ms.
    end_time: Optional[int] = None
    #: Arbitrary user-provided metadata for the job.
    metadata: Optional[Dict[str, str]] = None
    #: The runtime environment for the job.
    runtime_env: Optional[Dict[str, Any]] = None
    #: Driver agent http address
    driver_agent_http_address: Optional[str] = None
    #: The node id that driver running on. It will be None only when the job status
    # is PENDING, and this field will not be deleted or modified even if the driver dies
    driver_node_id: Optional[str] = None

    def __post_init__(self):
        if self.message is None:
            if self.status == JobStatus.PENDING:
                self.message = (
                    "Job has not started yet, likely waiting "
                    "for the runtime_env to be set up."
                )
            elif self.status == JobStatus.RUNNING:
                self.message = "Job is currently running."
            elif self.status == JobStatus.STOPPED:
                self.message = "Job was intentionally stopped."
            elif self.status == JobStatus.SUCCEEDED:
                self.message = "Job finished successfully."
            elif self.status == JobStatus.FAILED:
                self.message = "Job failed."


class JobInfoStorageClient:
    """
    Interface to put and get job data from the Internal KV store.
    """

    JOB_DATA_KEY_PREFIX = f"{ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX}job_info_"
    JOB_DATA_KEY = f"{JOB_DATA_KEY_PREFIX}{{job_id}}"

    def __init__(self, gcs_aio_client: GcsAioClient):
        self._gcs_aio_client = gcs_aio_client
        assert _internal_kv_initialized()

    async def put_info(self, job_id: str, data: JobInfo):
        await self._gcs_aio_client.internal_kv_put(
            self.JOB_DATA_KEY.format(job_id=job_id).encode(),
            pickle.dumps(data),
            True,
            namespace=ray_constants.KV_NAMESPACE_JOB,
        )

    async def get_info(self, job_id: str, timeout: int = 30) -> Optional[JobInfo]:
        pickled_info = await self._gcs_aio_client.internal_kv_get(
            self.JOB_DATA_KEY.format(job_id=job_id).encode(),
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=timeout,
        )
        if pickled_info is None:
            return None
        else:
            return pickle.loads(pickled_info)

    async def put_status(
        self,
        job_id: str,
        status: JobStatus,
        message: Optional[str] = None,
        jobinfo_replace_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Puts or updates job status.  Sets end_time if status is terminal."""

        old_info = await self.get_info(job_id)

        if jobinfo_replace_kwargs is None:
            jobinfo_replace_kwargs = dict()
        jobinfo_replace_kwargs.update(status=status, message=message)
        if old_info is not None:
            if status != old_info.status and old_info.status.is_terminal():
                assert False, "Attempted to change job status from a terminal state."
            new_info = replace(old_info, **jobinfo_replace_kwargs)
        else:
            new_info = JobInfo(
                entrypoint="Entrypoint not found.", **jobinfo_replace_kwargs
            )

        if status.is_terminal():
            new_info.end_time = int(time.time() * 1000)

        await self.put_info(job_id, new_info)

    async def get_status(self, job_id: str) -> Optional[JobStatus]:
        job_info = await self.get_info(job_id)
        if job_info is None:
            return None
        else:
            return job_info.status

    async def get_all_jobs(self, timeout: int = 30) -> Dict[str, JobInfo]:
        raw_job_ids_with_prefixes = await self._gcs_aio_client.internal_kv_keys(
            self.JOB_DATA_KEY_PREFIX.encode(),
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=timeout,
        )
        job_ids_with_prefixes = [
            job_id.decode() for job_id in raw_job_ids_with_prefixes
        ]
        job_ids = []
        for job_id_with_prefix in job_ids_with_prefixes:
            assert job_id_with_prefix.startswith(
                self.JOB_DATA_KEY_PREFIX
            ), "Unexpected format for internal_kv key for Job submission"
            job_ids.append(job_id_with_prefix[len(self.JOB_DATA_KEY_PREFIX) :])

        async def get_job_info(job_id: str):
            job_info = await self.get_info(job_id, timeout)
            return job_id, job_info

        return {
            job_id: job_info
            for job_id, job_info in await asyncio.gather(
                *[get_job_info(job_id) for job_id in job_ids]
            )
        }


def uri_to_http_components(package_uri: str) -> Tuple[str, str]:
    suffix = Path(package_uri).suffix
    if suffix not in {".zip", ".whl"}:
        raise ValueError(f"package_uri ({package_uri}) does not end in .zip or .whl")
    # We need to strip the <protocol>:// prefix to make it possible to pass
    # the package_uri over HTTP.
    protocol, package_name = parse_uri(package_uri)
    return protocol.value, package_name


def http_uri_components_to_uri(protocol: str, package_name: str) -> str:
    return f"{protocol}://{package_name}"


def validate_request_type(json_data: Dict[str, Any], request_type: dataclass) -> Any:
    return request_type(**json_data)


@dataclass
class JobSubmitRequest:
    # Command to start execution, ex: "python script.py"
    entrypoint: str
    # Optional submission_id to specify for the job. If the submission_id
    # is not specified, one will be generated. If a job with the same
    # submission_id already exists, it will be rejected.
    submission_id: Optional[str] = None
    # DEPRECATED. Use submission_id instead
    job_id: Optional[str] = None
    # Dict to setup execution environment.
    runtime_env: Optional[Dict[str, Any]] = None
    # Metadata to pass in to the JobConfig.
    metadata: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if not isinstance(self.entrypoint, str):
            raise TypeError(f"entrypoint must be a string, got {type(self.entrypoint)}")

        if self.submission_id is not None and not isinstance(self.submission_id, str):
            raise TypeError(
                "submission_id must be a string if provided, "
                f"got {type(self.submission_id)}"
            )

        if self.job_id is not None and not isinstance(self.job_id, str):
            raise TypeError(
                "job_id must be a string if provided, " f"got {type(self.job_id)}"
            )

        if self.runtime_env is not None:
            if not isinstance(self.runtime_env, dict):
                raise TypeError(
                    f"runtime_env must be a dict, got {type(self.runtime_env)}"
                )
            else:
                for k in self.runtime_env.keys():
                    if not isinstance(k, str):
                        raise TypeError(
                            f"runtime_env keys must be strings, got {type(k)}"
                        )

        if self.metadata is not None:
            if not isinstance(self.metadata, dict):
                raise TypeError(f"metadata must be a dict, got {type(self.metadata)}")
            else:
                for k in self.metadata.keys():
                    if not isinstance(k, str):
                        raise TypeError(f"metadata keys must be strings, got {type(k)}")
                for v in self.metadata.values():
                    if not isinstance(v, str):
                        raise TypeError(
                            f"metadata values must be strings, got {type(v)}"
                        )


@dataclass
class JobSubmitResponse:
    # DEPRECATED: Use submission_id instead.
    job_id: str
    submission_id: str


@dataclass
class JobStopResponse:
    stopped: bool


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    logs: str
