from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field
from ray.dashboard.modules.job.common import JobStatus


class DriverInfo(BaseModel):
    """A class for recording information about the driver related to the job."""

    id: str = Field(..., description="The id of the driver")
    node_ip_address: str = Field(
        ..., description="The ip address of the node the driver is running on"
    )
    pid: str = Field(
        ..., description="The pid of the worker process the driver is using."
    )
    # TODO(aguo): Add node_id as a field.


class JobType(str, Enum):
    """An enumeration for describing the different job types."""

    #: A job that was initiated by the job submission apis
    SUBMISSION = "SUBMISSION"
    #: A job that was initiated by a driver script.
    DRIVER = "DRIVER"


class JobDetails(BaseModel):
    """
    Job data with extra details about its driver and its submission.
    """

    type: JobType = Field(..., description="The type of job.")
    entrypoint: Optional[str] = Field(
        None, description="The entrypoint command for this job."
    )
    job_id: Optional[str] = Field(
        None,
        description="The job id. An id that is created for every job that is "
        "launched in ray. This can be used to fetch data about jobs using ray "
        "core apis.",
    )
    submission_id: Optional[str] = Field(
        None,
        description="A submission id is an id created for every submission job. It can "
        "be used to fetch data about jobs using the job submission apis.",
    )
    driver_info: Optional[DriverInfo] = Field(
        None,
        description="The driver related to this job. For submission jobs, "
        "it is the last driver launched by that job submission, "
        "or None if there is no driver.",
    )

    # The following fields are copied from JobInfo.
    # TODO(aguo): Inherit from JobInfo once it's migrated to pydantic.
    status: JobStatus = Field(..., description="The status of the job.")
    entrypoint: str = Field(..., description="The entrypoint command for this job.")
    message: Optional[str] = Field(
        None, description="A message describing the status in more detail."
    )
    error_type: Optional[str] = Field(
        None, description="Internal error, user script error"
    )
    start_time: Optional[int] = Field(
        None,
        description="The time when the job was started. " "A Unix timestamp in ms.",
    )
    end_time: Optional[int] = Field(
        None,
        description="The time when the job moved into a terminal state. "
        "A Unix timestamp in ms.",
    )
    metadata: Optional[Dict[str, str]] = Field(
        None, description="Arbitrary user-provided metadata for the job."
    )
    runtime_env: Optional[Dict[str, Any]] = Field(
        None, description="The runtime environment for the job."
    )
    # the node info where the driver running on.
    #     - driver_agent_http_address: this node's agent http address
    #     - driver_node_id: this node's id.
    driver_agent_http_address: Optional[str] = Field(
        None, description="Driver agent http address."
    )
    driver_node_id: Optional[str] = Field(None, description="Driver node id.")
