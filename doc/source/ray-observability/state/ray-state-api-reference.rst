Ray State API
=============

.. _state-api-ref:

.. tip:: APIs are pre-alpha and under active development. APIs are subject to change and not stable across versions.

State CLI
---------

State CLI allows users to access the state of various resources (e.g., actor, task, object).

.. click:: ray.experimental.state.state_cli:task_summary
   :prog: ray summary tasks

.. click:: ray.experimental.state.state_cli:actor_summary
   :prog: ray summary actors

.. click:: ray.experimental.state.state_cli:object_summary
   :prog: ray summary objects

.. click:: ray.experimental.state.state_cli:list
   :prog: ray list

.. click:: ray.experimental.state.state_cli:get
   :prog: ray get

.. _ray-logs-api-doc:

Log CLI
-------

Log CLI allows users to access the log from the cluster. 
Note that only the logs from alive nodes are available through this API.

.. click:: ray.scripts.scripts:ray_logs
    :prog: ray logs

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