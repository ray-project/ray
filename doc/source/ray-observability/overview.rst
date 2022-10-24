Overview
========

This section covers a list of available monitoring and debugging tools and features in Ray.

This documentation only covers the high-level description of available tools and features. For more details, see :ref:`Ray Observability <observability>`.

Application Logging
-------------------
By default, all stdout and stderr of tasks and actors are streamed to the Ray driver (the entrypoint script that calls ``ray.init``).

.. literalinclude:: doc_code/app_logging.py
  :language: python

All stdout emitted from the ``print`` method is printed to the driver with a ``(the task or actor repr, the process ID, IP address)`` prefix.

.. code-block:: bash

    (pid=45601) task
    (Actor pid=480956) actor

See :ref:`Logging <ray-logging>` for more details.

Exceptions
----------
Creating a new task or submitting an actor task generates an object reference. When ``ray.get`` is called on the object reference,
the API raises an exception if anything goes wrong with a related task, actor or object. For example,

- :ref:`RayTaskError <ray-core-exceptions-ray-task-error>` is raised when there's an error from user code that throws an exception.
- :ref:`RayActorError <ray-core-exceptions-ray-actor-error>` is raised when an actor is dead (by a system failure such as node failure or user-level failure such as an exception from ``__init__`` method). 
- :ref:`RuntimeEnvSetupError <ray-core-exceptions-runtime-env-setup-error>` is raised when the actor or task couldn't be started because :ref:`a runtime environment <runtime-environments>` failed to be created.

See :ref:`Exceptions Reference <ray-core-exceptions>` for more details.

Accessing Ray States
--------------------
Starting from Ray 2.0, it supports CLI / Python APIs to query the state of resources (e.g., actor, task, object, etc.).

For example, the following command will summarize the task state of the cluster.

.. code-block:: bash

    ray summary tasks

.. code-block:: text

    ======== Tasks Summary: 2022-07-22 08:54:38.332537 ========
    Stats:
    ------------------------------------
    total_actor_scheduled: 2
    total_actor_tasks: 0
    total_tasks: 2


    Table (group by func_name):
    ------------------------------------
        FUNC_OR_CLASS_NAME        STATE_COUNTS    TYPE
    0   task_running_300_seconds  RUNNING: 2      NORMAL_TASK
    1   Actor.__init__            FINISHED: 2     ACTOR_CREATION_TASK

The following command will list all the actors from the cluster.

.. code-block:: bash

    ray list actors

.. code-block:: text

    ======== List: 2022-07-23 21:29:39.323925 ========
    Stats:
    ------------------------------
    Total: 2

    Table:
    ------------------------------
        ACTOR_ID                          CLASS_NAME    NAME      PID  STATE
    0  31405554844820381c2f0f8501000000  Actor                 96956  ALIVE
    1  f36758a9f8871a9ca993b1d201000000  Actor                 96955  ALIVE

See :ref:`Ray State API <state-api-overview-ref>` for more details.

Dashboard (Web UI)
------------------
Ray supports the web-based dashboard to help users monitor the cluster. When a new cluster is started, the dashboard is available
through the default address `localhost:8265` (port can be automatically incremented if port 8265 is already occupied).

See :ref:`Ray Dashboard <ray-dashboard>` for more details.

Debugger
--------
Ray has a built-in debugger that allows you to debug your distributed applications.
It allows you to set breakpoints in your Ray tasks and actors, and when hitting the breakpoint, you can
drop into a PDB session that you can then use to:

- Inspect variables in that context
- Step within that task or actor
- Move up or down the stack

See :ref:`Ray Debugger <ray-debugger>` for more details.

Monitoring Cluster State and Resource Demands
---------------------------------------------
You can monitor cluster usage and auto-scaling status by running (on the head node) a CLI command ``ray status``. It displays

- **Cluster State**: Nodes that are up and running. Addresses of running nodes. Information about pending nodes and failed nodes.
- **Autoscaling Status**: The number of nodes that are autoscaling up and down.
- **Cluster Usage**: The resource usage of the cluster. E.g., requested CPUs from all Ray tasks and actors. Number of GPUs that are used.

Here's an example output.

.. code-block:: shell

   $ ray status
   ======== Autoscaler status: 2021-10-12 13:10:21.035674 ========
   Node status
   ---------------------------------------------------------------
   Healthy:
    1 ray.head.default
    2 ray.worker.cpu
   Pending:
    (no pending nodes)
   Recent failures:
    (no failures)

   Resources
   ---------------------------------------------------------------
   Usage:
    0.0/10.0 CPU
    0.00/70.437 GiB memory
    0.00/10.306 GiB object_store_memory

   Demands:
    (no resource demands)

Metrics
-------
Ray collects and exposes the physical stats (e.g., CPU, memory, GRAM, disk, and network usage of each node), 
internal stats (e.g., number of actors in the cluster, number of worker failures of the cluster), 
and custom metrics (e.g., metrics defined by users). All stats can be exported as time series data (to Prometheus by default) and used
to monitor the cluster over time. 

See :ref:`Ray Metrics <ray-metrics>` for more details.

Profiling
---------
Ray is compatible with Python profiling tools such as ``CProfile``. It also supports its built-in profiling tool such as :ref:```ray timeline`` <ray-timeline-doc>`. 

See :ref:`Profiling <ray-core-profiling>` for more details.

Tracing
-------
To help debug and monitor Ray applications, Ray supports distributed tracing (integration with OpenTelemetry) across tasks and actors.

See :ref:`Ray Tracing <ray-tracing>` for more details.