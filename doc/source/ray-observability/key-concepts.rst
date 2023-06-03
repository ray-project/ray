.. _observability-key-concepts:

Key Concepts
============

This section covers key concepts for monitoring and debugging tools and features in Ray.

Dashboard (Web UI)
------------------
Ray provides a web-based dashboard to help users monitor and debug Ray applications and clusters.

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


Ray States
----------
Ray States refer to the state of various Ray entities (e.g., Actor, Task, Object, etc.). Ray 2.0 and later versions support :ref:`querying the states of entities with the CLI and Python APIs <observability-programmatic>`

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

View :ref:`Monitoring with the CLI or SDK <state-api-overview-ref>` for more details.

Metrics
-------
Ray collects and exposes the physical stats (e.g., CPU, memory, GRAM, disk, and network usage of each node),
internal stats (e.g., number of actors in the cluster, number of worker failures of the cluster),
and custom application metrics (e.g., metrics defined by users). All stats can be exported as time series data (to Prometheus by default) and used
to monitor the cluster over time.

View :ref:`Metrics View <dash-metrics-view>` for where to view the metrics in Ray Dashboard. View :ref:`collecting metrics <collect-metrics>` for how to collect metrics from Ray clusters.

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

View :ref:`Ray Debugger <ray-debugger>` for more details.

.. _profiling-concept:

Profiling
---------
Profiling is way of analyzing the performance of an application by sampling the resource usage of it. Ray supports various profiling tools:

- CPU profiling for worker processes, including integration with :ref:`py-spy <dashboard-profiling>` and :ref:`cProfile <dashboard-cprofile>`
- Memory profiling for worker processes with :ref:`memray <memray-profiling>`
- Built in task/actor profiling tool called :ref:`ray timeline <ray-core-timeline>`

Ray doesn't provide native integration with GPU profiling tools. Try running GPU profilers like PyTorch Profiler without Ray to identify the issues.

Tracing
-------
To help debug and monitor Ray applications, Ray supports distributed tracing (integration with OpenTelemetry) across Tasks and Actors.

See :ref:`Ray Tracing <ray-tracing>` for more details.

Application logs
----------------
Logs are important for general monitoring and debugging. For distributed Ray applications, logs are even more important but more complicated at the same time. A Ray application runs both on Driver and Worker processes (or even across multiple machines) and the logs of these processes are the main sources of application logs.

.. image:: ./images/application-logging.png
    :alt: Application logging

Driver logs
~~~~~~~~~~~
An entry point of Ray applications that calls ``ray.init()`` is called a **Driver**.
All the driver logs are handled in the same way as normal Python programs.

.. _ray-worker-logs:

Worker logs (stdout and stderr)
~~~~~~~~~~~~~~~~~~~~~~~~

Ray executes Tasks or Actors remotely within worker processes. Ray has special support to improve the visibility of stdout and stderr produced by workers.

- By default, Ray redirects stdout and stderr from all Tasks and Actors to the worker log files, including any log messages generated by the worker. See :ref:`Logging directory structure <logging-directory-structure>` to understand the structure of the Ray logging directory.
- By default, the driver reads the worker log files to which the stdout and stderr for all Tasks and Actors are redirected. Drivers display all stdout and stderr generated from their Tasks or Actors, to their own stdout and stderr.

For the following code:

.. code-block:: python

    import ray
    # Initiate a driver.
    ray.init()

    @ray.remote
    def task-foo():
        print("task!")

    ray.get(task.remote())

#. Ray task ``task-foo`` runs on a Ray worker process. String ``task!`` is saved into the corresponding worker ``stdout`` log file.
#. The Driver reads the worker log file and sends it to its ``stdout`` (terminal) where you should be able to see the string ``task!``.

When logs are printed, the process id (pid) and an IP address of the node that executes Tasks or Actors are printed together. Here is the output:

.. code-block:: bash

    (pid=45601) task!

Actor log messages look like the following by default:

.. code-block:: bash

    (MyActor pid=480956) actor log message


By default, all stdout and stderr of tasks and actors are redirected to the driver output. View :ref:`Configuring Logging <log-redirection-to-driverg>` for more details.



Job logs
~~~~~~~~
Ray applications are usually run as Ray Jobs. Worker logs of Ray Jobs are always captured in the Ray logging directory while Driver logs are not.

Driver logs are captured only for Ray Jobs submitted via :ref:`Ray Jobs API <jobs-quickstart>`. The driver logs are captured in the logging direcotry and available from the dashboard, CLI (using the ``ray job logs`` :ref:`CLI command <ray-job-logs-doc>`), or the :ref:`Python SDK <ray-job-submission-sdk-ref>` (``JobSubmissionClient.get_logs()`` or ``JobSubmissionClient.tail_job_logs()``).

