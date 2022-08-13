Ray State API
=============

.. _state-api-ref:

.. note:: 

    APIs are :ref:`alpha <api-stability-alpha>`. This feature requires a full installation of Ray using ``pip install "ray[default]"``.

State CLI
---------

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

.. _ray-logs-api-doc:

Log CLI
-------

Log CLI allows users to access the log from the cluster. 
Note that only the logs from alive nodes are available through this API.

.. click:: ray.scripts.scripts:ray_logs
    :prog: ray logs

State Python SDK
-----------------

State APIs are also exported as functions. 

Summary APIs
~~~~~~~~~~~~
.. autofunction:: ray.experimental.state.api.summarize_actors
.. autofunction:: ray.experimental.state.api.summarize_objects
.. autofunction:: ray.experimental.state.api.summarize_tasks

List APIs
~~~~~~~~~~

.. autofunction:: ray.experimental.state.api.list_actors
.. autofunction:: ray.experimental.state.api.list_placement_groups
.. autofunction:: ray.experimental.state.api.list_nodes
.. autofunction:: ray.experimental.state.api.list_jobs
.. autofunction:: ray.experimental.state.api.list_workers
.. autofunction:: ray.experimental.state.api.list_tasks
.. autofunction:: ray.experimental.state.api.list_objects
.. autofunction:: ray.experimental.state.api.list_runtime_envs

Get APIs
~~~~~~~~~

.. autofunction:: ray.experimental.state.api.get_actor
.. autofunction:: ray.experimental.state.api.get_placement_group
.. autofunction:: ray.experimental.state.api.get_node
.. autofunction:: ray.experimental.state.api.get_worker
.. autofunction:: ray.experimental.state.api.get_task
.. autofunction:: ray.experimental.state.api.get_objects

Log APIs
~~~~~~~~
.. autofunction:: ray.experimental.state.api.list_logs
.. autofunction:: ray.experimental.state.api.get_log

.. _state-api-schema:

State APIs Schema
-----------------

.. _state-api-schema-actor:

ActorState
~~~~~~~~~~

.. autoclass:: ray.experimental.state.common.ActorState
    :members:

.. _state-api-schema-task:

TaskState
~~~~~~~~~

.. autoclass:: ray.experimental.state.common.TaskState
    :members:

.. _state-api-schema-node:

NodeState
~~~~~~~~~

.. autoclass:: ray.experimental.state.common.NodeState
    :members:

.. _state-api-schema-pg:

PlacementGroupState
~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.experimental.state.common.PlacementGroupState
    :members:

.. _state-api-schema-worker:

WorkerState
~~~~~~~~~~~

.. autoclass:: ray.experimental.state.common.WorkerState
    :members:

.. _state-api-schema-obj:

ObjectState
~~~~~~~~~~~

.. autoclass:: ray.experimental.state.common.ObjectState
    :members:

.. _state-api-schema-runtime-env:

RuntimeEnvState
~~~~~~~~~~~~~~~

.. autoclass:: ray.experimental.state.common.RuntimeEnvState
    :members:

.. _state-api-schema-job:

JobState
~~~~~~~~

.. autoclass:: ray.experimental.state.common.JobState
    :members:

.. _state-api-schema-summary:

StateSummary
~~~~~~~~~~~~

.. autoclass:: ray.experimental.state.common.StateSummary
    :members:

.. _state-api-schema-task-summary:

TaskSummary
~~~~~~~~~~~

.. _state-api-schema-task-summaries:

.. autoclass:: ray.experimental.state.common.TaskSummaries
    :members:

.. _state-api-schema-task-summary-per-key:

.. autoclass:: ray.experimental.state.common.TaskSummaryPerFuncOrClassName
    :members:

.. _state-api-schema-actor-summary:

ActorSummary
~~~~~~~~~~~~

.. _state-api-schema-actor-summaries:

.. autoclass:: ray.experimental.state.common.ActorSummaries
    :members:

.. _state-api-schema-actor-summary-per-key:

.. autoclass:: ray.experimental.state.common.ActorSummaryPerClass
    :members:

.. _state-api-schema-object-summary:

ObjectSummary
~~~~~~~~~~~~~

.. _state-api-schema-object-summaries:

.. autoclass:: ray.experimental.state.common.ObjectSummaries
    :members:

.. _state-api-schema-object-summary-per-key:

.. autoclass:: ray.experimental.state.common.ObjectSummaryPerKey
    :members:

State APIs Exceptions
---------------------

.. _state-api-exceptions:

.. autoclass:: ray.experimental.state.exception.RayStateApiException
    :members: