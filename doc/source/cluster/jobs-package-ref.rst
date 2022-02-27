.. _ray-job-submission-api-ref:

Ray Job Submission API
======================

.. _ray-job-submission-cli-ref:

Job Submission CLI
------------------

.. _ray-job-submit-doc:

.. click:: ray.dashboard.modules.job.cli:job_submit
   :prog: ray job submit
   :show-nested:

.. _ray-job-status-doc:

.. click:: ray.dashboard.modules.job.cli:job_status
   :prog: ray job status
   :show-nested:

.. _ray-job-stop-doc:

.. click:: ray.dashboard.modules.job.cli:job_stop
   :prog: ray job stop
   :show-nested:

.. _ray-job-logs-doc:

.. click:: ray.dashboard.modules.job.cli:job_logs
   :prog: ray job logs
   :show-nested:

.. _ray-job-list-doc:

.. click:: ray.dashboard.modules.job.cli:job_list
   :prog: ray job list
   :show-nested:

.. _ray-job-submission-sdk-ref:

Job Submission SDK
------------------

.. _job-submission-client-ref:

JobSubmissionClient
~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.job_submission.JobSubmissionClient
    :members:

.. _job-status-ref:

JobStatus
~~~~~~~~~

.. autoclass:: ray.job_submission.JobStatus
    :members:
    :undoc-members:

.. _job-info-ref:

JobInfo
~~~~~~~

.. autoclass:: ray.job_submission.JobInfo
    :members:
    :undoc-members: