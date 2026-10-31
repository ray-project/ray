---
myst:
  html_meta:
    description: "Python SDK reference for Ray Jobs: JobSubmissionClient, JobStatus, JobInfo, JobDetails, JobType, and DriverInfo."
---

(ray-job-submission-sdk-ref)=

# Python SDK API Reference

```{eval-rst}
.. currentmodule:: ray.job_submission
```

For an overview with examples see {ref}`Ray Jobs <jobs-overview>`.

For the CLI reference see {ref}`Ray Job Submission CLI Reference <ray-job-submission-cli-ref>`.

(job-submission-client-ref)=

## JobSubmissionClient

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   JobSubmissionClient
```

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   JobSubmissionClient.submit_job
   JobSubmissionClient.stop_job
   JobSubmissionClient.get_job_status
   JobSubmissionClient.get_job_info
   JobSubmissionClient.list_jobs
   JobSubmissionClient.get_job_logs
   JobSubmissionClient.tail_job_logs
   JobSubmissionClient.delete_job
```

(job-status-ref)=

## JobStatus

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

   JobStatus
```

(job-info-ref)=

## JobInfo

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   JobInfo
```

(job-details-ref)=

## JobDetails

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   JobDetails
```

(job-type-ref)=

## JobType

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

   JobType
```

(driver-info-ref)=

## DriverInfo

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   DriverInfo
```
