from enum import Enum
from typing import Any, Dict

try:
    from pydantic import BaseModel
except ImportError:
    # Lazy import without breaking class def
    BaseModel = object


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
