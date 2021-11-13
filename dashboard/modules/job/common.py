from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional
import pickle
from ray import ray_constants
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
    _internal_kv_get,
    _internal_kv_put,
)

# NOTE(edoakes): constant should be considered a public API because it's
# exposed in the snapshot API.
JOB_ID_METADATA_KEY = "job_submission_id"

JOBS_API_PREFIX = "/api/jobs/"
JOBS_API_ROUTE_LOGS = JOBS_API_PREFIX + "logs"
JOBS_API_ROUTE_SUBMIT = JOBS_API_PREFIX + "submit"
JOBS_API_ROUTE_STOP = JOBS_API_PREFIX + "stop"
JOBS_API_ROUTE_STATUS = JOBS_API_PREFIX + "status"
JOBS_API_ROUTE_PACKAGE = JOBS_API_PREFIX + "package"


class JobStatus(str, Enum):
    def __str__(self):
        return f"{self.value}"

    DOES_NOT_EXIST = "DOES_NOT_EXIST"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


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
            self.JOB_STATUS_KEY.format(job_id=job_id),
            pickle.dumps(status),
            namespace=ray_constants.KV_NAMESPACE_JOB)

    def get_status(self, job_id: str) -> JobStatus:
        pickled_status = _internal_kv_get(
            self.JOB_STATUS_KEY.format(job_id=job_id),
            namespace=ray_constants.KV_NAMESPACE_JOB)
        if pickled_status is None:
            return JobStatus.DOES_NOT_EXIST
        else:
            return pickle.loads(pickled_status)


def validate_request_type(json_data: Dict[str, Any],
                          request_type: dataclass) -> Any:
    return request_type(**json_data)


# ==== Get Package ====


@dataclass
class GetPackageResponse:
    package_exists: bool


# ==== Job Submit ====


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
            raise TypeError(
                f"entrypoint must be a string, got {type(self.entrypoint)}")

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
                            f"runtime_env keys must be strings, got {type(k)}")

        if self.metadata is not None:
            if not isinstance(self.metadata, dict):
                raise TypeError(
                    f"metadata must be a dict, got {type(self.metadata)}")
            else:
                for k in self.metadata.keys():
                    if not isinstance(k, str):
                        raise TypeError(
                            f"metadata keys must be strings, got {type(k)}")
                for v in self.metadata.values():
                    if not isinstance(v, str):
                        raise TypeError(
                            f"metadata values must be strings, got {type(v)}")


@dataclass
class JobSubmitResponse:
    job_id: str


# ==== Job Stop ====


@dataclass
class JobStopResponse:
    stopped: bool


# ==== Job Status ====


@dataclass
class JobStatusResponse:
    job_status: JobStatus


# ==== Job Logs ====


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    logs: str
