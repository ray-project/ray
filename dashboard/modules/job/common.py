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


# ==== Get Package ====


@dataclass
class GetPackageResponse:
    package_exists: bool


# ==== Job Submit ====


@dataclass
class JobSubmitRequest:
    # Dict to setup execution environment.
    runtime_env: Dict[str, Any]
    # Command to start execution, ex: "python script.py"
    entrypoint: str
    # Optional job_id to specify for the job. If the job_id is not specified,
    # one will be generated. If a job with the same job_id already exists, it
    # will be rejected.
    job_id: Optional[str]
    # Metadata to pass in to the JobConfig.
    metadata: Dict[str, str]


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
