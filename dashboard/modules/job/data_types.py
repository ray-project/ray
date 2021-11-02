from enum import Enum
from typing import Any, Dict
from dataclasses import dataclass


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass
class JobSpec:
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


@dataclass
class JobSubmitRequest:
    job_spec: JobSpec


@dataclass
class JobSubmitResponse:
    job_id: str


# ==== Job Status ====


@dataclass
class JobStatusRequest:
    job_id: str


@dataclass
class JobStatusResponse:
    job_status: JobStatus


# ==== Job Logs ====


@dataclass
class JobLogsRequest:
    job_id: str


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    stdout: str
    stderr: str
