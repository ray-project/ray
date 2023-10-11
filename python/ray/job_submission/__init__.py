from ray._private.pydantic_compat import PYDANTIC_INSTALLED
from ray.dashboard.modules.job.common import JobInfo, JobStatus
from ray.dashboard.modules.job.sdk import JobSubmissionClient

if PYDANTIC_INSTALLED:
    from ray.dashboard.modules.job.pydantic_models import (
        DriverInfo,
        JobDetails,
        JobType,
    )
else:
    DriverInfo = None
    JobDetails = None
    JobType = None


__all__ = [
    "JobSubmissionClient",
    "JobStatus",
    "JobInfo",
    "JobDetails",
    "DriverInfo",
    "JobType",
]
