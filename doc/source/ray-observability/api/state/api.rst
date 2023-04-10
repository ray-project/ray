State API
=========

.. _state-api-ref:

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
   :toctree: doc/

    ray.experimental.state.api.summarize_actors
    ray.experimental.state.api.summarize_objects
    ray.experimental.state.api.summarize_tasks

List APIs
~~~~~~~~~~

.. autosummary::
   :toctree: doc/

    ray.experimental.state.api.list_actors
    ray.experimental.state.api.list_placement_groups
    ray.experimental.state.api.list_nodes
    ray.experimental.state.api.list_jobs
    ray.experimental.state.api.list_workers
    ray.experimental.state.api.list_tasks
    ray.experimental.state.api.list_objects
    ray.experimental.state.api.list_runtime_envs

Get APIs
~~~~~~~~~

.. autosummary::
   :toctree: doc/

    ray.experimental.state.api.get_actor
    ray.experimental.state.api.get_placement_group
    ray.experimental.state.api.get_node
    ray.experimental.state.api.get_worker
    ray.experimental.state.api.get_task
    ray.experimental.state.api.get_objects

Log APIs
~~~~~~~~

.. autosummary::
   :toctree: doc/

    ray.experimental.state.api.list_logs
    ray.experimental.state.api.get_log

.. _state-api-schema:

State APIs Schema
-----------------

.. autosummary::
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

    ray.experimental.state.common.ActorState
    ray.experimental.state.common.TaskState
    ray.experimental.state.common.NodeState
    ray.experimental.state.common.PlacementGroupState
    ray.experimental.state.common.WorkerState
    ray.experimental.state.common.ObjectState
    ray.experimental.state.common.RuntimeEnvState
    ray.experimental.state.common.JobState
    ray.experimental.state.common.StateSummary
    ray.experimental.state.common.TaskSummaries
    ray.experimental.state.common.TaskSummaryPerFuncOrClassName
    ray.experimental.state.common.ActorSummaries
    ray.experimental.state.common.ActorSummaryPerClass
    ray.experimental.state.common.ObjectSummaries
    ray.experimental.state.common.ObjectSummaryPerKey

State APIs Exceptions
---------------------

.. autosummary::
   :toctree: doc/

    ray.experimental.state.exception.RayStateApiException
