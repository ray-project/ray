.. _configuring-ray:

Configuring Ray
===============

.. note:: For running Java applications, please see `Java Applications`_.

This page discusses the various way to configure Ray, both from the Python API
and from the command line. Take a look at the ``ray.init`` `documentation
<package-ref.html#ray.init>`__ for a complete overview of the configurations.

.. important:: For the multi-node setting, you must first run ``ray start`` on the command line to start the Ray cluster services on the machine before ``ray.init`` in Python to connect to the cluster services. On a single machine, you can run ``ray.init()`` without ``ray start``, which will both start the Ray cluster services and connect to them.


Cluster Resources
-----------------

Ray by default detects available resources.

.. code-block:: python

  # This automatically detects available resources in the single machine.
  ray.init()

If not running cluster mode, you can specify cluster resources overrides through ``ray.init`` as follows.

.. code-block:: python

  # If not connecting to an existing cluster, you can specify resources overrides:
  ray.init(num_cpus=8, num_gpus=1)

  # Specifying custom resources
  ray.init(num_gpus=1, resources={'Resource1': 4, 'Resource2': 16})

When starting Ray from the command line, pass the ``--num-cpus`` and ``--num-gpus`` flags into ``ray start``. You can also specify custom resources.

.. code-block:: bash

  # To start a head node.
  $ ray start --head --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

  # To start a non-head node.
  $ ray start --address=<address> --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

  # Specifying custom resources
  ray start [--head] --num-cpus=<NUM_CPUS> --resources='{"Resource1": 4, "Resource2": 16}'

If using the command line, connect to the Ray cluster as follow:

.. code-block:: python

  # Connect to ray. Notice if connected to existing cluster, you don't specify resources.
  ray.init(address=<address>)

.. _omp-num-thread-note:

.. note::
    Ray sets the environment variable ``OMP_NUM_THREADS=1`` by default. This is done
    to avoid performance degradation with many workers (issue #6998). You can
    override this by explicitly setting ``OMP_NUM_THREADS``. ``OMP_NUM_THREADS`` is commonly
    used in numpy, PyTorch, and Tensorflow to perform multit-threaded linear algebra.
    In multi-worker setting, we want one thread per worker instead of many threads
    per worker to avoid contention.


Logging and Debugging
---------------------

Each Ray session will have a unique name. By default, the name is
``session_{timestamp}_{pid}``. The format of ``timestamp`` is
``%Y-%m-%d_%H-%M-%S_%f`` (See `Python time format <strftime.org>`__ for details);
the pid belongs to the startup process (the process calling ``ray.init()`` or
the Ray process executed by a shell in ``ray start``).

For each session, Ray will place all its temporary files under the
*session directory*. A *session directory* is a subdirectory of the
*root temporary path* (``/tmp/ray`` by default),
so the default session directory is ``/tmp/ray/{ray_session_name}``.
You can sort by their names to find the latest session.

Change the *root temporary directory* in one of these ways:

* Pass ``--temp-dir={your temp path}`` to ``ray start``
* Specify ``temp_dir`` when call ``ray.init()``

You can also use ``default_worker.py --temp-dir={your temp path}`` to
start a new worker with the given *root temporary directory*.

**Layout of logs**:

.. code-block:: text

  /tmp
  └── ray
      └── session_{datetime}_{pid}
          ├── logs  # for logging
          │   ├── log_monitor.err
          │   ├── log_monitor.out
          │   ├── monitor.err
          │   ├── monitor.out
          │   ├── plasma_store.err  # outputs of the plasma store
          │   ├── plasma_store.out
          │   ├── raylet.err  # outputs of the raylet process
          │   ├── raylet.out
          │   ├── redis-shard_0.err   # outputs of redis shards
          │   ├── redis-shard_0.out
          │   ├── redis.err  # redis
          │   ├── redis.out
          │   ├── webui.err  # ipython notebook web ui
          │   ├── webui.out
          │   ├── worker-{worker_id}.err  # redirected output of workers
          │   ├── worker-{worker_id}.out
          │   └── {other workers}
          └── sockets  # for sockets
              ├── plasma_store
              └── raylet  # this could be deleted by Ray's shutdown cleanup.

.. _ray-ports:

Ports configurations
--------------------
Ray requires bi-directional communication among its nodes in a cluster. Each of node is supposed to open specific ports to receive incoming network requests.

All Nodes
~~~~~~~~~
- ``--node-manager-port``: Raylet port for node manager. Default: Random value.
- ``--object-manager-port``: Raylet port for object manager. Default: Random value.

The following options specify the range of ports used by worker processes across machines. All ports in the range should be open.

- ``--min-worker-port``: Minimum port number worker can be bound to. Default: 10000.
- ``--max-worker-port``: Maximum port number worker can be bound to. Default: 10999.

Head Node
~~~~~~~~~
In addition to ports specified above, the head node needs to open several more ports.

- ``--port``: Port of GCS. Default: 6379.
- ``--dashboard-port``: Port for accessing the dashboard. Default: 8265
- ``--gcs-server-port``: GCS Server port. GCS server is a stateless service that is in charge of communicating with the GCS. Default: Random value.

Redis Port Authentication
-------------------------

Ray instances should run on a secure network without public facing ports.
The most common threat for Ray instances is unauthorized access to Redis,
which can be exploited to gain shell access and run arbitrary code.
The best fix is to run Ray instances on a secure, trusted network.

Running Ray on a secured network is not always feasible.
To prevent exploits via unauthorized Redis access, Ray provides the option to
password-protect Redis ports. While this is not a replacement for running Ray
behind a firewall, this feature is useful for instances exposed to the internet
where configuring a firewall is not possible. Because Redis is
very fast at serving queries, the chosen password should be long.


.. note:: The Redis passwords provided below may not contain spaces.

Redis authentication is only supported on the raylet code path.

To add authentication via the Python API, start Ray using:

.. code-block:: python

  ray.init(_redis_password="password")

To add authentication via the CLI or to connect to an existing Ray instance with
password-protected Redis ports:

.. code-block:: bash

  ray start [--head] --redis-password="password"

While Redis port authentication may protect against external attackers,
Ray does not encrypt traffic between nodes so man-in-the-middle attacks are
possible for clusters on untrusted networks.

One of most common attack with Redis is port-scanning attack. Attacker scans
open port with unprotected redis instance and execute arbitrary code. Ray
enables a default password for redis. Even though this does not prevent brute
force password cracking, the default password should alleviate most of the
port-scanning attack. Furthermore, redis and other ray services are bind
to localhost when the ray is started using ``ray.init``.

See the `Redis security documentation <https://redis.io/topics/security>`__
for more information.

Java Applications
-----------------

.. important:: For the multi-node setting, you must first run ``ray start`` on the command line to start the Ray cluster services on the machine before ``Ray.init()`` in Java to connect to the cluster services. On a single machine, you can run ``Ray.init()`` without ``ray start``, which will both start the Ray cluster services and connect to them.

.. _code_search_path:

Code Search Path
~~~~~~~~~~~~~~~~

If you want to run a Java application in cluster mode, you must first run ``ray start`` to start the Ray cluster. In addition to any ``ray start`` parameters mentioned above, you must add ``--code-search-path`` to tell Ray where to load jars when starting Java workers. Your jar files must be distributed to all nodes of the Ray cluster before running your code, and this parameter must be set on both the head node and non-head nodes.

.. code-block:: bash

  $ ray start ... --code-search-path=/path/to/jars

The ``/path/to/jars`` here points to a directory which contains jars. All jars in the directory will be loaded by workers. You can also provide multiple directories for this parameter.

.. code-block:: bash

  $ ray start ... --code-search-path=/path/to/jars1:/path/to/jars2:/path/to/pys1:/path/to/pys2

Code search path is also used for loading Python code if it's specified. This is required for :ref:`cross_language`. If code search path is specified, you can only run Python remote functions which can be found in the code search path.

You don't need to configure code search path if you run a Java application in single machine mode.

.. note:: Currently we don't provide a way to configure Ray when running a Java application in single machine mode. If you need to configure Ray, run ``ray start`` to start the Ray cluster first.

Driver Options
~~~~~~~~~~~~~~

There is a limited set of options for Java drivers. They are not for configuring the Ray cluster, but only for configuring the driver.

Ray uses `Typesafe Config <https://lightbend.github.io/config/>`__ to read options. There are several ways to set options:

- System properties. You can configure system properties either by adding options in the format of ``-Dkey=value`` in the driver command line, or by invoking ``System.setProperty("key", "value");`` before ``Ray.init()``.
- A `HOCON format <https://github.com/lightbend/config/blob/master/HOCON.md>`__ configuration file. By default, Ray will try to read the file named ``ray.conf`` in the root of the classpath. You can customize the location of the file by setting system property ``ray.config-file`` to the path of the file.

.. note:: Options configured by system properties have higher priority than options configured in the configuration file.

The list of available driver options:

- ``ray.address``

  - The cluster address if the driver connects to an existing Ray cluster. If it is empty, a new Ray cluster will be created.
  - Type: ``String``
  - Default: empty string.

- ``ray.local-mode``

  - If it's set to ``true``, the driver will run in :ref:`local_mode`.
  - Type: ``Boolean``
  - Default: ``false``

.. _`Apache Arrow`: https://arrow.apache.org/
