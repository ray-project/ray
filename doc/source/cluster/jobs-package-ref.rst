.. _ray-job-submission-api-ref:

Ray Job Submission API
======================

For an overview with examples see :ref:`Ray Job Submission<jobs-overview>`.

.. _ray-job-submission-cli-ref:

Job Submission CLI
------------------

.. _ray-job-submit-doc:

.. click:: ray.dashboard.modules.job.cli:submit
   :prog: ray job submit

.. _ray-job-status-doc:

.. click:: ray.dashboard.modules.job.cli:status
   :prog: ray job status
   :show-nested:

.. _ray-job-stop-doc:

.. click:: ray.dashboard.modules.job.cli:stop
   :prog: ray job stop
   :show-nested:

.. _ray-job-logs-doc:

.. click:: ray.dashboard.modules.job.cli:logs
   :prog: ray job logs
   :show-nested:

.. _ray-job-list-doc:

.. click:: ray.dashboard.modules.job.cli:list
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

.. _job-info-ref:

JobInfo
~~~~~~~

.. autoclass:: ray.job_submission.JobInfo
    :members:
