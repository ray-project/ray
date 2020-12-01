Debugging
=========

Starting processes in a debugger
--------------------------------
When processes are crashing, it is often useful to start them in a debugger.
Ray currently allows processes to be started in the following:

- valgrind
- the valgrind profiler
- the perftools profiler
- gdb
- tmux

To use any of these tools, please make sure that you have them installed on
your machine first (``gdb`` and ``valgrind`` on MacOS are known to have issues).
Then, you can launch a subset of ray processes by adding the environment
variable ``RAY_{PROCESS_NAME}_{DEBUGGER}=1``. For instance, if you wanted to
start the raylet in ``valgrind``, then you simply need to set the environment
variable ``RAY_RAYLET_VALGRIND=1``.

To start a process inside of ``gdb``, the process must also be started inside of
``tmux``. So if you want to start the raylet in ``gdb``, you would start your
Python script with the following:

.. code-block:: bash

 RAY_RAYLET_GDB=1 RAY_RAYLET_TMUX=1 python

You can then list the ``tmux`` sessions with ``tmux ls`` and attach to the
appropriate one.

You can also get a core dump of the ``raylet`` process, which is especially
useful when filing `issues`_. The process to obtain a core dump is OS-specific,
but usually involves running ``ulimit -c unlimited`` before starting Ray to
allow core dump files to be written.

Inspecting Redis shards
-----------------------
To inspect Redis, you can use the global state API. The easiest way to do this
is to start or connect to a Ray cluster with ``ray.init()``, then query the API
like so:

.. code-block:: python

 ray.init()
 ray.nodes()
 # Returns current information about the nodes in the cluster, such as:
 # [{'ClientID': '2a9d2b34ad24a37ed54e4fcd32bf19f915742f5b',
 #   'IsInsertion': True,
 #   'NodeManagerAddress': '1.2.3.4',
 #   'NodeManagerPort': 43280,
 #   'ObjectManagerPort': 38062,
 #   'ObjectStoreSocketName': '/tmp/ray/session_2019-01-21_16-28-05_4216/sockets/plasma_store',
 #   'RayletSocketName': '/tmp/ray/session_2019-01-21_16-28-05_4216/sockets/raylet',
 #   'Resources': {'CPU': 8.0, 'GPU': 1.0}}]

To inspect the primary Redis shard manually, you can also query with commands
like the following.

.. code-block:: python

 r_primary = ray.worker.global_worker.redis_client
 r_primary.keys("*")

To inspect other Redis shards, you will need to create a new Redis client.
For example (assuming the relevant IP address is ``127.0.0.1`` and the
relevant port is ``1234``), you can do this as follows.

.. code-block:: python

 import redis
 r = redis.StrictRedis(host='127.0.0.1', port=1234)

You can find a list of the relevant IP addresses and ports by running

.. code-block:: python

 r_primary.lrange('RedisShards', 0, -1)

.. _backend-logging:

Backend logging
---------------
The ``raylet`` process logs detailed information about events like task
execution and object transfers between nodes. To set the logging level at
runtime, you can set the ``RAY_BACKEND_LOG_LEVEL`` environment variable before
starting Ray. For example, you can do:

.. code-block:: shell

 export RAY_BACKEND_LOG_LEVEL=debug
 ray start

This will print any ``RAY_LOG(DEBUG)`` lines in the source code to the
``raylet.err`` file, which you can find in :ref:`temp-dir-log-files`.
If it worked, you should see as the first line in ``raylet.err``:

.. code-block:: shell

  logging.cc:270: Set ray log level from environment variable RAY_BACKEND_LOG_LEVEL to -1

(-1 is defined as RayLogLevel::DEBUG in logging.h.)

.. literalinclude:: /../../src/ray/util/logging.h
  :language: C
  :lines: 52,54


.. _`issues`: https://github.com/ray-project/ray/issues