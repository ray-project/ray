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
