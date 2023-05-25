.. _observability-key-concepts:

Key Concepts
============

This section covers a list of key concepts for monitoring and debugging tools and features in Ray.

Dashboard (Web UI)
------------------
Ray supports the web-based dashboard to help users monitor the cluster. When a new cluster is started, the dashboard is available
through the default address `localhost:8265` (port can be automatically incremented if port 8265 is already occupied).

See :ref:`Getting Started <observability-getting-started>` for more details about the dashboard.

Accessing Ray States
--------------------
Ray 2.0 and later versions support CLI and Python APIs for querying the state of resources (e.g., actor, task, object, etc.)

For example, the following command summarizes the task state of the cluster:

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

The following command lists all the actors from the cluster:

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

Metrics
-------
Ray collects and exposes the physical stats (e.g., CPU, memory, GRAM, disk, and network usage of each node),
internal stats (e.g., number of actors in the cluster, number of worker failures of the cluster),
and custom metrics (e.g., metrics defined by users). All stats can be exported as time series data (to Prometheus by default) and used
to monitor the cluster over time.

See :ref:`Ray Metrics <dash-metrics-view>` for more details.

Exceptions
----------
Creating a new task or submitting an actor task generates an object reference. When ``ray.get`` is called on the object reference,
the API raises an exception if anything goes wrong with a related task, actor or object. For example,

- :class:`RayTaskError <ray.exceptions.RayTaskError>` is raised when there's an error from user code that throws an exception.
- :class:`RayActorError <ray.exceptions.RayActorError>` is raised when an actor is dead (by a system failure such as node failure or user-level failure such as an exception from ``__init__`` method).
- :class:`RuntimeEnvSetupError <ray.exceptions.RuntimeEnvSetupError>` is raised when the actor or task couldn't be started because :ref:`a runtime environment <runtime-environments>` failed to be created.

See :ref:`Exceptions Reference <ray-core-exceptions>` for more details.

Debugger
--------
Ray has a built-in debugger that allows you to debug your distributed applications.
It allows you to set breakpoints in your Ray tasks and actors, and when hitting the breakpoint, you can
drop into a PDB session that you can then use to:

- Inspect variables in that context
- Step within that task or actor
- Move up or down the stack

See :ref:`Ray Debugger <ray-debugger>` for more details.

Profiling
---------
Ray is compatible with Python profiling tools such as ``CProfile``. It also supports its built-in profiling tool such as :ref:`ray timeline <ray-timeline-doc>`.

See :ref:`Profiling <dashboard-cprofile>` for more details.

Tracing
-------
To help debug and monitor Ray applications, Ray supports distributed tracing (integration with OpenTelemetry) across tasks and actors.

See :ref:`Ray Tracing <ray-tracing>` for more details.

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

Driver logs
~~~~~~~~~~~
An entry point of Ray applications that calls ``ray.init()`` is called a driver.
All the driver logs are handled in the same way as normal Python programs.

Job logs
~~~~~~~~
Logs for jobs submitted via the :ref:`Ray Jobs API <jobs-overview>` can be retrieved using the ``ray job logs`` :ref:`CLI command <ray-job-logs-doc>` or using ``JobSubmissionClient.get_logs()`` or ``JobSubmissionClient.tail_job_logs()`` via the :ref:`Python SDK <ray-job-submission-sdk-ref>`.
The log file consists of the stdout of the entrypoint command of the job.  For the location of the log file on disk, see :ref:`Logging directory structure <logging-directory-structure>`.

.. _ray-worker-logs:

Worker stdout and stderr
~~~~~~~~~~~~~~~~~~~~~~~~
Ray's tasks or actors are executed remotely within Ray's worker processes. Ray has special support to improve the visibility of stdout and stderr produced by workers.

- By default, stdout and stderr from all tasks and actors are redirected to the worker log files, including any log messages generated by the worker. See :ref:`Logging directory structure <logging-directory-structure>` to understand the structure of the Ray logging directory.
- By default, the driver reads the worker log files to which the stdout and stderr for all tasks and actors are redirected. Drivers display all stdout and stderr generated from their tasks or actors to their own stdout and stderr.

Let's look at a code example to see how this works.

.. code-block:: python

    import ray
    # Initiate a driver.
    ray.init()

    @ray.remote
    def task():
        print("task")

    ray.get(task.remote())

You should be able to see the string `task` from your driver stdout.

When logs are printed, the process id (pid) and an IP address of the node that executes tasks/actors are printed together. Check out the output below.

.. code-block:: bash

    (pid=45601) task

Actor log messages look like the following by default.

.. code-block:: bash

    (MyActor pid=480956) actor log message

.. _logging-directory-structure:

Logging directory structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray logs are stored in a ``/tmp/ray/session_*/logs`` directory.

.. note::

    The default temp directory is ``/tmp/ray`` (for Linux and MacOS). To change the temp directory, specify it when you call ``ray start`` or ``ray.init()``. 

A new Ray instance creates a new session ID to the temp directory. The latest session ID is symlinked to ``/tmp/ray/session_latest``.

Here's a Ray log directory structure. Note that ``.out`` is logs from stdout/stderr and ``.err`` is logs from stderr. The backward compatibility of log directories is not maintained.

- ``dashboard.[log|err]``: A log file of a Ray dashboard. ``log.`` file contains logs generated from the dashboard's logger. ``.err`` file contains stdout and stderr printed from the dashboard. They are usually empty except when the dashboard crashes unexpectedly.
- ``dashboard_agent.log``: Every Ray node has one dashboard agent. This is a log file of the agent.
- ``gcs_server.[out|err]``: The GCS server is a stateless server that manages Ray cluster metadata. It exists only in the head node.
- ``io-worker-[worker_id]-[pid].[out|err]``: Ray creates IO workers to spill/restore objects to external storage by default from Ray 1.3+. This is a log file of IO workers.
- ``job-driver-[submission_id].log``: The stdout of a job submitted via the :ref:`Ray Jobs API <jobs-overview>`.
- ``log_monitor.[log|err]``: The log monitor is in charge of streaming logs to the driver. ``log.`` file contains logs generated from the log monitor's logger. ``.err`` file contains the stdout and stderr printed from the log monitor. They are usually empty except when the log monitor crashes unexpectedly.
- ``monitor.[out|err]``: Stdout and stderr of a cluster launcher.
- ``monitor.log``: Ray's cluster launcher is operated with a monitor process. It also manages the autoscaler.
- ``plasma_store.[out|err]``: Deprecated.
- ``python-core-driver-[worker_id]_[pid].log``: Ray drivers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- ``python-core-worker-[worker_id]_[pid].log``: Ray workers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- ``raylet.[out|err]``: A log file of raylets.
- ``redis-shard_[shard_index].[out|err]``: Redis shard log files.
- ``redis.[out|err]``: Redis log files.
- ``runtime_env_agent.log``: Every Ray node has one agent that manages :ref:`runtime environment <runtime-environments>` creation, deletion and caching.
  This is the log file of the agent containing logs of create/delete requests and cache hits and misses.
  For the logs of the actual installations (including e.g. ``pip install`` logs), see the ``runtime_env_setup-[job_id].log`` file (see below).
- ``runtime_env_setup-[job_id].log``: Logs from installing :ref:`runtime environments <runtime-environments>` for a task, actor or job.  This file will only be present if a runtime environment is installed.
- ``runtime_env_setup-ray_client_server_[port].log``: Logs from installing :ref:`runtime environments <runtime-environments>` for a job when connecting via :ref:`Ray Client <ray-client-ref>`.
- ``worker-[worker_id]-[job_id]-[pid].[out|err]``: Python or Java part of Ray drivers and workers. All of stdout and stderr from tasks or actors are streamed here. Note that job_id is an id of the driver.- 

