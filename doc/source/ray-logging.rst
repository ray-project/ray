Logging
=======
This document will explain Ray's logging system and its best practices.

Driver logs
~~~~~~~~~~~
An entry point of Ray applications that calls `ray.init(address='auto')` or `ray.init()` is called a driver.
All the driver logs are handled in the same way as normal Python programs. 

Worker logs
~~~~~~~~~~~
Ray's tasks or actors are executed remotely within Ray's worker processes. Ray has special support to improve the visibility of logs produced by workers.

- By default, all of the tasks/actors stdout and stderr are redirected to the worker log files. Check out :ref:`Logging directory structure <logging-directory-structure>` to learn how Ray's logging directory is structured.
- By default, all of the tasks/actors stdout and stderr that is redirected to worker log files are published to the driver. Drivers display logs generated from its tasks/actors to its stdout and stderr.

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

How to set up loggers
~~~~~~~~~~~~~~~~~~~~~
When using ray, all of the tasks and actors are executed remotely in Ray's worker processes. 
Since Python logger module creates a singleton logger per process, loggers should be configured on per task/actor basis. 

.. note::

    To stream logs to a driver, they should be flushed to stdout and stderr.

.. code-block:: python

    import ray
    import logging
    # Initiate a driver.
    ray.init()

    @ray.remote
    class Actor:
        def __init__(self):
            # Basic config automatically configures logs to
            # be streamed to stdout and stderr.
            # Set the severity to INFO so that info logs are printed to stdout.
            logging.basicConfig(level=logging.INFO)
        
        def log(self, msg):
            logging.info(msg)
    
    actor = Actor.remote()
    ray.get(actor.log.remote("A log message for an actor."))

    @ray.remote
    def f(msg):
        logging.basicConfig(level=logging.INFO)
        logging.info(msg)
    
    ray.get(f.remote("A log message for a task"))

.. code-block:: bash

    (pid=95193) INFO:root:A log message for a task
    (pid=95192) INFO:root:A log message for an actor.

How to use structured logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The metadata of tasks or actors may be obtained by Ray's :ref:`runtime_context APIs <runtime-context-apis>`.
Runtime context APIs help you to add metadata to your logging messages, making your logs more structured.

.. code-block:: python

    import ray
    # Initiate a driver.
    ray.init()

    @ray.remote
    def task():
        print(f"task_id: {ray.get_runtime_context().task_id}")
    
    ray.get(task.remote())

.. code-block:: bash

    (pid=47411) task_id: TaskID(a67dc375e60ddd1affffffffffffffffffffffff01000000)

Logging directory structure
---------------------------
.. _logging-directory-structure:

By default, Ray logs are stored in a `/tmp/ray/session_*/logs` directory. 

.. note::

    The default temp directory is `/tmp/ray` (for Linux and Mac OS). If you'd like to change the temp directory, you can specify it when `ray start` or `ray.init()` is called. 

A new Ray instance creates a new session ID to the temp directory. The latest session ID is symlinked to `/tmp/ray/session_latest`.

Here's a Ray log directory structure. Note that `.out` is logs from stdout/stderr and `.err` is logs from stderr. The backward compatibility of log directories is not maintained.

- `dashboard.log`: A log file of a Ray dashboard.
- `dashboard_agent.log`: Every Ray node has one dashboard agent. This is a log file of the agent.
- `gcs_server.[out|err]`: The GCS server is a stateless server that manages business logic that needs to be performed on GCS (Redis). It exists only in the head node.
- `log_monitor.log`: The log monitor is in charge of streaming logs to the driver.
- `monitor.log`: Ray's cluster launcher is operated with a monitor process. It also manages the autoscaler.
- `monitor.[out|err]`: Stdout and stderr of a cluster launcher.
- `plasma_store.[out|err]`: Deprecated.
- `python-core-driver-[worker_id]_[pid].log`: Ray drivers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- `python-core-worker-[worker_id]_[pid].log`: Ray workers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- `raylet.[out|err]`: A log file of raylets.
- `redis-shard_[shard_index].[out|err]`: A log file of GCS (Redis by default) shards.
- `redis.[out|err]`: A log file of GCS (Redis by default).
- `worker-[worker_id]-[job_id]-[pid].[out|err]`: Python/Java part of Ray drivers and workers. All of stdout and stderr from tasks/actors are streamed here. Note that job_id is an id of the driver.
- `io-worker-[worker_id]-[pid].[out|err]`: Ray creates IO workers to spill/restore objects to external storage by default from Ray 1.3+. This is a log file of IO workers.

Log rotation
------------
Ray supports log rotation of log files. Note that not all components are currently supporting log rotation. (Raylet, Python/Java worker, and Redis logs are not rotating).

By default, logs are rotating when it reaches to 512MB (maxBytes), and there could be up to 5 backup files (backupCount). Indexes are appended to all backup files (e.g., `raylet.out.1`)
If you'd like to change the log rotation configuration, you can do it by specifying environment variables. For example,

.. code-block:: bash

    RAY_ROTATION_MAX_BYTES=1024; ray start --head # Start a ray instance with maxBytes 1KB.
    RAY_ROTATION_BACKUP_COUNT=1; ray start --head # Start a ray instance with backupCount 1.

