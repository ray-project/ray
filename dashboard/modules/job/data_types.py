from enum import Enum
from typing import Any, Dict
from dataclasses import dataclass


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


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
    # Metadata to pass in to the JobConfig.
    metadata: Dict[str, str]


@dataclass
class JobSubmitResponse:
    job_id: str


# ==== Job Status ====


@dataclass
class JobStatusResponse:
    job_status: JobStatus


# ==== Job Logs ====


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    stdout: str
    stderr: str
