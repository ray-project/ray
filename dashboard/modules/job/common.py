import pickle
import time
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ray._private import ray_constants
from ray._private.runtime_env.packaging import parse_uri
from ray.experimental.internal_kv import (
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_list,
    _internal_kv_put,
)

# NOTE(edoakes): these constants should be considered a public API because
# they're exposed in the snapshot API.
JOB_ID_METADATA_KEY = "job_submission_id"
JOB_NAME_METADATA_KEY = "job_name"

# Version 0 -> 1: Added log streaming and changed behavior of job logs cli.
CURRENT_VERSION = "1"


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

    JOB_DATA_KEY_PREFIX = "_ray_internal_job_info_"
    JOB_DATA_KEY = f"{JOB_DATA_KEY_PREFIX}{{job_id}}"

    def __init__(self):
        assert _internal_kv_initialized()

    def put_info(self, job_id: str, data: JobInfo):
        _internal_kv_put(
            self.JOB_DATA_KEY.format(job_id=job_id),
            pickle.dumps(data),
            namespace=ray_constants.KV_NAMESPACE_JOB,
        )

    def get_info(self, job_id: str) -> Optional[JobInfo]:
        pickled_info = _internal_kv_get(
            self.JOB_DATA_KEY.format(job_id=job_id),
            namespace=ray_constants.KV_NAMESPACE_JOB,
        )
        if pickled_info is None:
            return None
        else:
            return pickle.loads(pickled_info)

    def put_status(self, job_id: str, status: JobStatus, message: Optional[str] = None):
        """Puts or updates job status.  Sets end_time if status is terminal."""

        old_info = self.get_info(job_id)

        if old_info is not None:
            if status != old_info.status and old_info.status.is_terminal():
                assert False, "Attempted to change job status from a terminal state."
            new_info = replace(old_info, status=status, message=message)
        else:
            new_info = JobInfo(
                entrypoint="Entrypoint not found.", status=status, message=message
            )

        if status.is_terminal():
            new_info.end_time = int(time.time() * 1000)

        self.put_info(job_id, new_info)

    def get_status(self, job_id: str) -> Optional[JobStatus]:
        job_info = self.get_info(job_id)
        if job_info is None:
            return None
        else:
            return job_info.status

    def get_all_jobs(self) -> Dict[str, JobInfo]:
        raw_job_ids_with_prefixes = _internal_kv_list(
            self.JOB_DATA_KEY_PREFIX, namespace=ray_constants.KV_NAMESPACE_JOB
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
        return {job_id: self.get_info(job_id) for job_id in job_ids}


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
class VersionResponse:
    version: str
    ray_version: str
    ray_commit: str


@dataclass
class JobSubmitRequest:
    # Command to start execution, ex: "python script.py"
    entrypoint: str
    # Optional job_id to specify for the job. If the job_id is not specified,
    # one will be generated. If a job with the same job_id already exists, it
    # will be rejected.
    job_id: Optional[str] = None
    # Dict to setup execution environment.
    runtime_env: Optional[Dict[str, Any]] = None
    # Metadata to pass in to the JobConfig.
    metadata: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if not isinstance(self.entrypoint, str):
            raise TypeError(f"entrypoint must be a string, got {type(self.entrypoint)}")

        if self.job_id is not None and not isinstance(self.job_id, str):
            raise TypeError(
                f"job_id must be a string if provided, got {type(self.job_id)}"
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
    job_id: str


@dataclass
class JobStopResponse:
    stopped: bool


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    logs: str
