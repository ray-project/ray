.. _ray-logging:

Logging
=======
This document will explain Ray's logging system and its best practices.

Driver logs
~~~~~~~~~~~
An entry point of Ray applications that calls ``ray.init()`` is called a driver.
All the driver logs are handled in the same way as normal Python programs.

Job logs
~~~~~~~~
Logs for jobs submitted via the :ref:`Ray Jobs API <jobs-overview>` can be retrieved using the ``ray job logs`` :ref:`CLI command <ray-job-logs-doc>` or using ``JobSubmissionClient.get_logs()`` or ``JobSubmissionClient.tail_job_logs()`` via the :ref:`Python SDK <ray-job-submission-sdk-ref>`.
The log file consists of the stdout of the entrypoint command of the job.  For the location of the log file on disk, see :ref:`Logging directory structure <logging-directory-structure>`.

.. _ray-worker-logs:

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

Actor log messages look like the following by default.

.. code-block:: bash

    (MyActor pid=480956) actor log message

Log deduplication
~~~~~~~~~~~~~~~~~

By default, Ray will deduplicate logs that appear redundantly across multiple processes. The first instance of each log message will always be immediately printed. However, subsequent log messages of the same pattern (ignoring words with numeric components) will be buffered for up to five seconds and printed in batch. For example, for the following code snippet:

.. code-block:: python

    import ray
    import random

    @ray.remote
    def task():
        print("Hello there, I am a task", random.random())

    ray.get([task.remote() for _ in range(100)])

The output will be as follows:

.. code-block:: bash

    2023-03-27 15:08:34,195	INFO worker.py:1603 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265 
    (task pid=534172) Hello there, I am a task 0.20583517821231412
    (task pid=534174) Hello there, I am a task 0.17536720316370757 [repeated 99x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication)

This feature is especially useful when importing libraries such as `tensorflow` or `numpy`, which may emit many verbose warning messages when imported. You can configure this feature as follows:

1. Set ``RAY_DEDUP_LOGS=0`` to disable this feature entirely.
2. Set ``RAY_DEDUP_LOGS_AGG_WINDOW_S=<int>`` to change the agggregation window.
3. Set ``RAY_DEDUP_LOGS_ALLOW_REGEX=<string>`` to specify log messages to never deduplicate.
4. Set ``RAY_DEDUP_LOGS_SKIP_REGEX=<string>`` to specify log messages to skip printing.


Disabling logging to the driver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In large scale runs, it may be undesirable to route all worker logs to the driver. You can disable this feature by setting ``log_to_driver=False`` in Ray init:

.. code-block:: python

    import ray

    # Task and actor logs will not be copied to the driver stdout.
    ray.init(log_to_driver=False)

Customizing Actor logs prefixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is often useful to distinguish between log messages from different actors. For example, suppose you have a large number of worker actors. In this case, you may want to be able to easily see the index of the actor that logged a particular message. This can be achieved by defining the `__repr__ <https://docs.python.org/3/library/functions.html#repr>`__ method for an actor class. When defined, the actor repr will be used in place of the actor name. For example:

.. literalinclude:: /ray-core/doc_code/actor-repr.py

This produces the following output:

.. code-block:: bash

    (MyActor(index=2) pid=482120) hello there
    (MyActor(index=1) pid=482119) hello there

Coloring Actor log prefixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
By default Ray prints Actor logs prefixes in light blue:
Users may instead activate multi-color prefixes by setting the environment variable ``RAY_COLOR_PREFIX=1``.
This will index into an array of colors modulo the PID of each process.

.. image:: images/images/coloring-actor-log-prefixes.png
    :align: center

Distributed progress bars (tqdm)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using `tqdm <https://tqdm.github.io>`__ in Ray remote tasks or actors, you may notice that the progress bar output is corrupted. To avoid this problem, you can use the Ray distributed tqdm implementation at ``ray.experimental.tqdm_ray``:

.. literalinclude:: /ray-core/doc_code/tqdm.py

This tqdm implementation works as follows:

1. The ``tqdm_ray`` module translates TQDM calls into special json log messages written to worker stdout.
2. The Ray log monitor, instead of copying these log messages directly to the driver stdout, routes these messages to a tqdm singleton.
3. The tqdm singleton determines the positions of progress bars from various Ray tasks / actors, ensuring they don't collide or conflict with each other.

Limitations:

- Only a subset of tqdm functionality is supported. Refer to the ray_tqdm `implementation <https://github.com/ray-project/ray/blob/master/python/ray/experimental/tqdm_ray.py>`__ for more details.
- Performance may be poor if there are more than a couple thousand updates per second (updates are not batched).

By default, the builtin print will also be patched to use `ray.experimental.tqdm_ray.safe_print` when `tqdm_ray` is used.
This avoids progress bar corruption on driver print statements. To disable this, set `RAY_TQDM_PATCH_PRINT=0`.

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

By default, Ray logs are stored in a ``/tmp/ray/session_*/logs`` directory. 

.. note::

    The default temp directory is ``/tmp/ray`` (for Linux and Mac OS). If you'd like to change the temp directory, you can specify it when ``ray start`` or ``ray.init()`` is called. 

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
- ``worker-[worker_id]-[job_id]-[pid].[out|err]``: Python/Java part of Ray drivers and workers. All of stdout and stderr from tasks/actors are streamed here. Note that job_id is an id of the driver.- 

.. _ray-log-rotation:

Log rotation
------------

Ray supports log rotation of log files. Note that not all components are currently supporting log rotation. (Raylet and Python/Java worker logs are not rotating).

By default, logs are rotating when it reaches to 512MB (maxBytes), and there could be up to 5 backup files (backupCount). Indexes are appended to all backup files (e.g., `raylet.out.1`)
If you'd like to change the log rotation configuration, you can do it by specifying environment variables. For example,

.. code-block:: bash

    RAY_ROTATION_MAX_BYTES=1024; ray start --head # Start a ray instance with maxBytes 1KB.
    RAY_ROTATION_BACKUP_COUNT=1; ray start --head # Start a ray instance with backupCount 1.

Redirecting Ray logs to stderr
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
By default, Ray logs are written to files under the ``/tmp/ray/session_*/logs`` directory. If you wish to redirect all internal Ray logging and your own logging within tasks/actors to stderr of the host nodes, you can do so by ensuring that the ``RAY_LOG_TO_STDERR=1`` environment variable is set on the driver and on all Ray nodes. This is very useful if you are using a log aggregator that needs log records to be written to stderr in order for them to be captured.

Redirecting logging to stderr will also cause a ``({component})`` prefix, e.g. ``(raylet)``, to be added to each of the log record messages.

.. code-block:: bash

    [2022-01-24 19:42:02,978 I 1829336 1829336] (gcs_server) grpc_server.cc:103: GcsServer server started, listening on port 50009.
    [2022-01-24 19:42:06,696 I 1829415 1829415] (raylet) grpc_server.cc:103: ObjectManager server started, listening on port 40545.
    2022-01-24 19:42:05,087 INFO (dashboard) dashboard.py:95 -- Setup static dir for dashboard: /mnt/data/workspace/ray/python/ray/dashboard/client/build
    2022-01-24 19:42:07,500 INFO (dashboard_agent) agent.py:105 -- Dashboard agent grpc address: 0.0.0.0:49228

This should make it easier to filter the stderr stream of logs down to the component of interest. Note that multi-line log records will **not** have this component marker at the beginning of each line.

When running a local Ray cluster, this environment variable should be set before starting the local cluster:

.. code-block:: python

    os.environ["RAY_LOG_TO_STDERR"] = "1"
    ray.init()

When starting a local cluster via the CLI or when starting nodes in a multi-node Ray cluster, this environment variable should be set before starting up each node:

.. code-block:: bash

    env RAY_LOG_TO_STDERR=1 ray start

If using the Ray cluster launcher, you would specify this environment variable in the Ray start commands:

.. code-block:: bash

    head_start_ray_commands:
        - ray stop
        - env RAY_LOG_TO_STDERR=1 ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml

    worker_start_ray_commands:
        - ray stop
        - env RAY_LOG_TO_STDERR=1 ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076

When connecting to the cluster, be sure to set the environment variable before connecting:

.. code-block:: python

    os.environ["RAY_LOG_TO_STDERR"] = "1"
    ray.init(address="auto")
