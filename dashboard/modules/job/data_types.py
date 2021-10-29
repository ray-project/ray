from typing import Any, Dict, Optional

from pydantic import BaseModel
from enum import Enum


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class JobSpec(BaseModel):
    # Dict to setup execution environment, better to have schema for this
    runtime_env: Dict[str, Any]
    # Command to start execution, ex: "python script.py"
    entrypoint: str
    # Metadata to pass in to configure job behavior or use as tags
    # Required by Anyscale product and already supported in Ray drivers
    metadata: Dict[str, str]
    # Likely there will be more fields needed later on for different apps
    # but we should keep it minimal and delegate policies to job manager


# ==== Job Submit ====


class JobSubmitRequest(BaseModel):
    job_spec: JobSpec
    # Globally unique job id. It’s recommended to generate this id from
    # external job manager first, then pass into this API.
    # If job server never had a job running with given id:
    #   - Start new job execution
    # Else if job server has a running job with given id:
    #   - Fail, deployment update and reconfigure should happen in job manager
    job_id: Optional[str] = None


class JobSubmitResponse(BaseModel):
    job_id: str


# ==== Job Status ====


class JobStatusRequest(BaseModel):
    job_id: str


class JobStatusResponse(BaseModel):
    job_status: JobStatus


# ==== Job Logs ====


class JobLogsRequest(BaseModel):
    job_id: str


# TODO(jiaodong): Support log streaming #19415
class JobLogsResponse(BaseModel):
    stdout: str
    stderr: str
