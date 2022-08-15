Ray CLI References
==================

.. _ray-cli:

Cluster Management
------------------
This section contains commands for managing Ray clusters.

.. _ray-start-doc:

.. click:: ray.scripts.scripts:start
   :prog: ray start
   :show-nested:

.. _ray-stop-doc:

.. click:: ray.scripts.scripts:stop
   :prog: ray stop
   :show-nested:

.. _ray-up-doc:

.. click:: ray.scripts.scripts:up
   :prog: ray up
   :show-nested:

.. _ray-down-doc:

.. click:: ray.scripts.scripts:down
   :prog: ray down
   :show-nested:

.. _ray-exec-doc:

.. click:: ray.scripts.scripts:exec
   :prog: ray exec
   :show-nested:

.. _ray-submit-doc:

.. click:: ray.scripts.scripts:submit
   :prog: ray submit
   :show-nested:

.. _ray-attach-doc:

.. click:: ray.scripts.scripts:attach
   :prog: ray attach
   :show-nested:

.. _ray-get_head_ip-doc:

.. click:: ray.scripts.scripts:get_head_ip
   :prog: ray get_head_ip
   :show-nested:

.. _ray-monitor-doc:

.. click:: ray.scripts.scripts:monitor
   :prog: ray monitor
   :show-nested:

Debugging applications
----------------------
This section contains commands for inspecting and debugging the current cluster.

.. _ray-stack-doc:

.. click:: ray.scripts.scripts:stack
   :prog: ray stack
   :show-nested:

.. _ray-memory-doc:

.. click:: ray.scripts.scripts:memory
   :prog: ray memory
   :show-nested:

.. _ray-timeline-doc:

.. click:: ray.scripts.scripts:timeline
   :prog: ray timeline
   :show-nested:

.. _ray-status-doc:

.. click:: ray.scripts.scripts:status
   :prog: ray status
   :show-nested:

.. click:: ray.scripts.scripts:debug
   :prog: ray debug
   :show-nested:


Usage Stats
-----------
This section contains commands to enable/disable :ref:`Ray usage stats <ref-usage-stats>`.

.. _ray-disable-usage-stats-doc:

.. click:: ray.scripts.scripts:disable_usage_stats
   :prog: ray disable-usage-stats
   :show-nested:

.. _ray-enable-usage-stats-doc:

.. click:: ray.scripts.scripts:enable_usage_stats
   :prog: ray enable-usage-stats
   :show-nested:

.. _ray-job-submission-cli-ref:

Job Submission
--------------
This section contains commands for the :ref:`Ray Job Submission <jobs-quickstart>`.

.. _ray-job-submit-doc:

.. click:: ray.dashboard.modules.job.cli:submit
   :prog: ray job submit

.. warning::

    When using the CLI, do not wrap the entrypoint command in quotes.  For example, use 
    ``ray job submit --working_dir="." -- python script.py`` instead of ``ray job submit --working_dir="." -- "python script.py"``.
    Otherwise you may encounter the error ``/bin/sh: 1: python script.py: not found``.

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

.. _state-api-cli-ref:

State
-----
This section contains commands to access the :ref:`live state of Ray resources (actor, task, object, etc.) <state-api-overview-ref>`.

.. note:: 

    APIs are :ref:`alpha <api-stability-alpha>`. This feature requires a full installation of Ray using ``pip install "ray[default]"``.

State CLI allows users to access the state of various resources (e.g., actor, task, object).

.. click:: ray.experimental.state.state_cli:task_summary
   :prog: ray summary tasks

.. click:: ray.experimental.state.state_cli:actor_summary
   :prog: ray summary actors

.. click:: ray.experimental.state.state_cli:object_summary
   :prog: ray summary objects

.. click:: ray.experimental.state.state_cli:ray_list
   :prog: ray list

.. click:: ray.experimental.state.state_cli:ray_get
   :prog: ray get

.. _ray-logs-api-cli-ref:

Log
---
This section contains commands to :ref:`access logs <state-api-log-doc>` from Ray clusters.

.. note:: 

    APIs are :ref:`alpha <api-stability-alpha>`. This feature requires a full installation of Ray using ``pip install "ray[default]"``.

Log CLI allows users to access the log from the cluster. 
Note that only the logs from alive nodes are available through this API.

.. click:: ray.scripts.scripts:ray_logs
   :prog: ray logs