from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass


class JobStatus(str, Enum):
    def __str__(self):
        return f"{self.value}"

    DOES_NOT_EXIST = "DOES_NOT_EXIST"
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
    # Optional job_id to specify for the job.
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
    stdout: str
    stderr: str
