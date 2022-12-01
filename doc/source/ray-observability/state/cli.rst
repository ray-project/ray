Ray State CLI
=============

.. _state-api-cli-ref:

State
-----
This section contains commands to access the :ref:`live state of Ray resources (actor, task, object, etc.) <state-api-overview-ref>`.

.. note:: 

    APIs are :ref:`alpha <api-stability-alpha>`. This feature requires a full installation of Ray using ``pip install "ray[default]"``. This feature also requires the dashboard component to be available. The dashboard component needs to be included when starting the ray cluster, which is the default behavior for ``ray start`` and ``ray.init()``. For more in-depth debugging, you could check the dashboard log at ``<RAY_LOG_DIR>/dashboard.log``, which is usually ``/tmp/ray/session_latest/logs/dashboard.log``.

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

.. click:: ray.experimental.state.state_cli:logs_state_cli_group
   :prog: ray logs