.. _ray-job-submission-sdk-ref:

Python SDK API Reference
========================

For an overview with examples see :ref:`Ray Jobs <jobs-overview>`.

For the CLI reference see :ref:`Ray Job Submission CLI Reference <ray-job-submission-cli-ref>`.
 
.. _job-submission-client-ref:

JobSubmissionClient
~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:

    ray.job_submission.JobSubmissionClient
    ray.job_submission.JobSubmissionClient.submit_job
    ray.job_submission.JobSubmissionClient.stop_job
    ray.job_submission.JobSubmissionClient.get_job_status
    ray.job_submission.JobSubmissionClient.get_job_info
    ray.job_submission.JobSubmissionClient.list_jobs
    ray.job_submission.JobSubmissionClient.get_job_logs
    ray.job_submission.JobSubmissionClient.tail_job_logs

.. autoclass:: ray.job_submission.JobSubmissionClient
    :members:

.. _job-status-ref:

JobStatus
~~~~~~~~~

.. autoclass:: ray.job_submission.JobStatus
    :members:

.. _job-info-ref:

JobInfo
~~~~~~~

.. autoclass:: ray.job_submission.JobInfo
    :members:
