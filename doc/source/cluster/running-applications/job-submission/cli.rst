.. _ray-job-submission-cli-ref:

Ray Jobs CLI API Reference
==========================

This section contains commands for :ref:`Ray Job Submission <jobs-quickstart>`.    

.. _ray-job-submit-doc:

.. click:: ray.dashboard.modules.job.cli:submit
   :prog: ray job submit

.. warning::

    When using the CLI, do not wrap the entrypoint command in quotes.  For example, use 
    ``ray job submit --working_dir="." -- python script.py`` instead of ``ray job submit --working_dir="." -- "python script.py"``.
    Otherwise you may encounter the error ``/bin/sh: 1: python script.py: not found``.

.. warning::

   You must provide the entrypoint command, ``python script.py``, last (after the ``--``), and any other arguments to `ray job submit` (e.g., ``--working_dir="."``) must be provided before the  two hyphens (``--``).
   For example, use ``ray job submit --working_dir="." -- python script.py`` instead of ``ray job submit -- python script.py --working_dir="."``.
   This syntax supports the use of ``--`` to separate arguments to `ray job submit` from arguments to the entrypoint command.

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
