.. _state-api-ref:

State API
=========

.. note::

    APIs are :ref:`alpha <api-stability-alpha>`. This feature requires a full installation of Ray using ``pip install "ray[default]"``.

For an overview with examples see :ref:`Monitoring Ray States <state-api-overview-ref>`.

For the CLI reference see :ref:`Ray State CLI Reference <state-api-cli-ref>` or :ref:`Ray Log CLI Reference <ray-logs-api-cli-ref>`.

State Python SDK
-----------------

State APIs are also exported as functions.

Summary APIs
~~~~~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.state.summarize_actors
   ray.util.state.summarize_objects
   ray.util.state.summarize_tasks

List APIs
~~~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ray.util.state.list_actors
    ray.util.state.list_placement_groups
    ray.util.state.list_nodes
    ray.util.state.list_jobs
    ray.util.state.list_workers
    ray.util.state.list_tasks
    ray.util.state.list_objects
    ray.util.state.list_runtime_envs

Get APIs
~~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ray.util.state.get_actor
    ray.util.state.get_placement_group
    ray.util.state.get_node
    ray.util.state.get_worker
    ray.util.state.get_task
    ray.util.state.get_objects

Log APIs
~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ray.util.state.list_logs
    ray.util.state.get_log

State REST API
--------------

State APIs are also available over HTTP from the Ray dashboard. The base URL is the
dashboard address (for example, ``http://<head-node-ip>:8265``) and all endpoints
use the ``/api/v0`` prefix.

List endpoints (GET)
~~~~~~~~~~~~~~~~~~~~

- ``/api/v0/actors``
- ``/api/v0/jobs``
- ``/api/v0/nodes``
- ``/api/v0/placement_groups``
- ``/api/v0/workers``
- ``/api/v0/tasks``
- ``/api/v0/objects``
- ``/api/v0/runtime_envs``

List query parameters
~~~~~~~~~~~~~~~~~~~~~

- ``limit``: Maximum number of entries to return.
- ``timeout``: Timeout in seconds for the request.
- ``detail``: ``true`` to include detailed columns.
- ``exclude_driver``: ``true`` to exclude driver entries (where applicable).
- ``filter_keys``: Repeated parameter for filter keys.
- ``filter_predicates``: Repeated parameter for filter predicates (``=`` or ``!=``).
- ``filter_values``: Repeated parameter for filter values.

Summary endpoints (GET)
~~~~~~~~~~~~~~~~~~~~~~~

- ``/api/v0/tasks/summarize``
- ``/api/v0/actors/summarize``
- ``/api/v0/objects/summarize``

Summary query parameters
~~~~~~~~~~~~~~~~~~~~~~~~

- ``timeout``: Timeout in seconds for the request.
- ``summary_by``: Field to group by.
- ``filter_keys`` / ``filter_predicates`` / ``filter_values``: Same format as list endpoints.

Log endpoints (GET)
~~~~~~~~~~~~~~~~~~~

- ``/api/v0/logs``: List logs on a node. Requires ``node_id`` or ``node_ip``.
- ``/api/v0/logs/file``: Fetch log files.
- ``/api/v0/logs/stream``: Stream log files.

Log query parameters
~~~~~~~~~~~~~~~~~~~~

- ``node_id`` / ``node_ip``: Identify the node hosting logs.
- ``glob``: Glob filter for log filenames (list logs).
- ``filename``: Log filename (fetch logs).
- ``actor_id`` / ``task_id`` / ``submission_id`` / ``pid``: Resource identifiers.
- ``lines``: Number of lines to fetch from the end of the log file.
- ``interval``: Stream polling interval.
- ``suffix``: ``out`` or ``err``.
- ``attempt_number``: Task attempt number.
- ``timeout``: Timeout in seconds for the request.
- ``filter_ansi_code``: ``true`` to strip ANSI escape codes from streamed logs.

Authentication
~~~~~~~~~~~~~~

If your cluster requires authentication, pass HTTP headers such as an
``Authorization`` token to these endpoints. When using the State CLI, you can
use ``--headers`` for custom headers and ``--verify`` to control TLS verification.

.. _state-api-schema:

State APIs Schema
-----------------

.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

    ray.util.state.common.ActorState
    ray.util.state.common.TaskState
    ray.util.state.common.NodeState
    ray.util.state.common.PlacementGroupState
    ray.util.state.common.WorkerState
    ray.util.state.common.ObjectState
    ray.util.state.common.RuntimeEnvState
    ray.util.state.common.JobState
    ray.util.state.common.StateSummary
    ray.util.state.common.TaskSummaries
    ray.util.state.common.TaskSummaryPerFuncOrClassName
    ray.util.state.common.ActorSummaries
    ray.util.state.common.ActorSummaryPerClass
    ray.util.state.common.ObjectSummaries
    ray.util.state.common.ObjectSummaryPerKey

State APIs Exceptions
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ray.util.state.exception.RayStateApiException
