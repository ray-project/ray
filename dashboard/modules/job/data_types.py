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
class GetPackageRequest:
    package_uri: str


@dataclass
class GetPackageResponse:
    package_exists: bool


# ==== Upload Package ====


@dataclass
class UploadPackageRequest:
    package_uri: str
    encoded_package_bytes: str


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
