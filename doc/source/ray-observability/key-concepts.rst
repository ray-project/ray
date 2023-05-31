.. _observability-key-concepts:

Key Concepts
============

This section covers key concepts for monitoring and debugging tools and features in Ray.

Dashboard (Web UI)
------------------
Ray supports a web-based dashboard to help users monitor the Cluster. When a new Cluster is started, the Dashboard is available
through the default address `localhost:8265` (port can be automatically incremented if port 8265 is already occupied).

See :ref:`Getting Started <observability-getting-started>` for more details about the Dashboard.

Accessing Ray states
--------------------
Ray 2.0 and later versions support CLI and Python APIs for querying the state of resources (e.g., Actor, Task, Object, etc.)

For example, the following command summarizes the task state of the Cluster:

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

The following command lists all the Actors from the Cluster:

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
internal stats (e.g., number of Actors in the Cluster, number of worker failures of the Cluster),
and custom metrics (e.g., metrics defined by users). All stats can be exported as time series data (to Prometheus by default) and used
to monitor the Cluster over time.

See :ref:`Ray Metrics <dash-metrics-view>` for more details.

Exceptions
----------
Creating a new Task or submitting an Actor task generates an object reference. When ``ray.get`` is called on the object reference,
the API raises an exception if anything goes wrong with a related Task, Actor or Object. For example,

- :class:`RayTaskError <ray.exceptions.RayTaskError>` is raised when an error from user code throws an exception.
- :class:`RayActorError <ray.exceptions.RayActorError>` is raised when an Actor is dead (by a system failure, such as a node failure, or a user-level failure, such as an exception from ``__init__`` method).
- :class:`RuntimeEnvSetupError <ray.exceptions.RuntimeEnvSetupError>` is raised when the Actor or Task can't be started because :ref:`a runtime environment <runtime-environments>` failed to be created.

See :ref:`Exceptions Reference <ray-core-exceptions>` for more details.

Debugger
--------
Ray has a built-in debugger for debugging your distributed applications.
Set breakpoints in Ray Tasks and Actors, and when hitting the breakpoint,
drop into a PDB session to:

- Inspect variables in that context
- Step within a Task or Actor
- Move up or down the stack

See :ref:`Ray Debugger <ray-debugger>` for more details.

Profiling
---------
Ray is compatible with Python profiling tools, such as ``CProfile``. It also supports its built-in profiling tool, such as :ref:`ray timeline <ray-timeline-doc>`.

See :ref:`Profiling <dashboard-profiling>` for more details.

Tracing
-------
To help debug and monitor Ray applications, Ray supports distributed tracing (integration with OpenTelemetry) across Tasks and Actors.

See :ref:`Ray Tracing <ray-tracing>` for more details.

Application logging
-------------------
By default, all stdout and stderr of Tasks and Actors are streamed to the Ray driver (the entrypoint script that calls ``ray.init``).

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
All the driver logs are handled like normal Python programs.

Job logs
~~~~~~~~
Retrieve logs for Jobs submitted with the :ref:`Ray Jobs API <jobs-overview>` using the ``ray job logs`` :ref:`CLI command <ray-job-logs-doc>`, ``JobSubmissionClient.get_logs()``, or ``JobSubmissionClient.tail_job_logs()`` via the :ref:`Python SDK <ray-job-submission-sdk-ref>`.
The log file consists of the stdout of the entrypoint command of the Job.  For the location of the log file on disk, see :ref:`Logging directory structure <logging-directory-structure>`.

.. _ray-worker-logs:

Worker stdout and stderr
~~~~~~~~~~~~~~~~~~~~~~~~
Ray executes Tasks or Actors are remotely within Ray's worker processes. Ray has special support to improve the visibility of stdout and stderr produced by workers.

- By default, stdout and stderr from all Tasks and Actors are redirected to the worker log files, including any log messages generated by the worker. See :ref:`Logging directory structure <logging-directory-structure>` to understand the structure of the Ray logging directory.
- By default, the driver reads the worker log files to which the stdout and stderr for all tasks and actors are redirected. Drivers display all stdout and stderr generated from their Tasks or Actors to their own stdout and stderr.

For the following code:

.. code-block:: python

    import ray
    # Initiate a driver.
    ray.init()

    @ray.remote
    def task():
        print("task")

    ray.get(task.remote())

You should be able to see the string `task` from your driver stdout.

When logs are printed, the process id (pid) and an IP address of the node that executes Tasks or Actors are printed together. See the output below.

.. code-block:: bash

    (pid=45601) task

Actor log messages look like the following by default:

.. code-block:: bash

    (MyActor pid=480956) actor log message

.. _logging-directory-structure:

Logging directory structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray logs are stored in a ``/tmp/ray/session_*/logs`` directory.

.. note::

    The default temp directory is ``/tmp/ray`` (for Linux and MacOS). To change the temp directory, specify it when you call ``ray start`` or ``ray.init()``. 

A new Ray instance creates a new session ID to the temp directory. The latest session ID is symlinked to ``/tmp/ray/session_latest``.

The Ray log directory structure follows. Note that files with the ``.out`` suffix are logs from stdout/stderr and files with the ``.err`` suffix are logs from stderr. The backward compatibility of log directories is not maintained.

- ``dashboard.[log|err]``: A log file of a Ray Dashboard. ``log.`` file contains logs generated from the Dashboard's logger. ``.err`` file contains stdout and stderr printed from the Dashboard. These files are usually empty except when the Dashboard crashes unexpectedly.
- ``dashboard_agent.log``: Every Ray node has one Dashboard agent. This file is a log file of the agent.
- ``gcs_server.[out|err]``: The GCS server is a stateless server that manages Ray Cluster metadata. It exists only in the head node.
- ``io-worker-[worker_id]-[pid].[out|err]``: Ray creates IO workers to spill or restore objects to external storage by default from Ray 1.3+. This file is a log file of IO workers.
- ``job-driver-[submission_id].log``: The stdout of a Job submitted with the :ref:`Ray Jobs API <jobs-overview>`.
- ``log_monitor.[log|err]``: The log monitor is in charge of streaming logs to the driver. A ``log.`` file contains logs generated from the log monitor's logger. The ``.err`` file contains the stdout and stderr printed from the log monitor. These files are usually empty except when the log monitor crashes unexpectedly.
- ``monitor.[out|err]``: Stdout and stderr of a Cluster Launcher.
- ``monitor.log``: Ray's Cluster Launcher is operated with a monitor process. It also manages the Autoscaler.
- ``plasma_store.[out|err]``: Deprecated.
- ``python-core-driver-[worker_id]_[pid].log``: Ray drivers consist of CPP core and Python or Java frontend. This file is a log file generated from CPP code.
- ``python-core-worker-[worker_id]_[pid].log``: Ray workers consist of CPP core and Python or Java frontend. This file is a log file generated from CPP code.
- ``raylet.[out|err]``: A log file of raylets.
- ``redis-shard_[shard_index].[out|err]``: Redis shard log files.
- ``redis.[out|err]``: Redis log files.
- ``runtime_env_agent.log``: Every Ray node has one agent that manages :ref:`runtime environment <runtime-environments>` creation, deletion, and caching.
  This file is the log file of the agent containing logs of create or delete requests and cache hits and misses.
  For the logs of the actual installations (including e.g., ``pip install`` logs), see the ``runtime_env_setup-[job_id].log`` file (see below).
- ``runtime_env_setup-[job_id].log``: Logs from installing :ref:`runtime environments <runtime-environments>` for a Task, Actor or Job.  This file is only present if a runtime environment is installed.
- ``runtime_env_setup-ray_client_server_[port].log``: Logs from installing :ref:`runtime environments <runtime-environments>` for a Job when connecting with :ref:`Ray Client <ray-client-ref>`.
- ``worker-[worker_id]-[job_id]-[pid].[out|err]``: Python or Java part of Ray drivers and workers. All of stdout and stderr from Tasks or Actors are streamed here. Note that job_id is an id of the driver. 

