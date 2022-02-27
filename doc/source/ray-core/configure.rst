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
    used in numpy, PyTorch, and Tensorflow to perform multi-threaded linear algebra.
    In multi-worker setting, we want one thread per worker instead of many threads
    per worker to avoid contention. Some other libraries may have their own way to configure
    parallelism. For example, if you're using OpenCV, you should manually set the number of
    threads using cv2.setNumThreads(num_threads) (set to 0 to disable multi-threading).


.. _temp-dir-log-files:

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

Change the *root temporary directory* by passing ``--temp-dir={your temp path}`` to ``ray start``.

(There is not currently a stable way to change the root temporary directory when calling ``ray.init()``, but if you need to, you can provide the ``_temp_dir`` argument to ``ray.init()``.)

Look :ref:`Logging Directory Structure <logging-directory-structure>` for more details.

.. _ray-ports:

Ports configurations
--------------------
Ray requires bi-directional communication among its nodes in a cluster. Each of node is supposed to open specific ports to receive incoming network requests.

All Nodes
~~~~~~~~~
- ``--node-manager-port``: Raylet port for node manager. Default: Random value.
- ``--object-manager-port``: Raylet port for object manager. Default: Random value.

The node manager and object manager run as separate processes with their own ports for communication.

The following options specify the range of ports used by worker processes across machines. All ports in the range should be open.

- ``--min-worker-port``: Minimum port number worker can be bound to. Default: 10002.
- ``--max-worker-port``: Maximum port number worker can be bound to. Default: 19999.

Port numbers are how Ray disambiguates input and output to and from multiple workers on a single node. Each worker will take input and give output on a single port number. Thus, for example, by default, there is a maximum of 10,000 workers on each node, irrespective of number of CPUs.

In general, it is recommended to give Ray a wide range of possible worker ports, in case any of those ports happen to be in use by some other program on your machine. However, when debugging it is useful to explicitly specify a short list of worker ports such as ``--worker-port-list=10000,10001,10002,10003,10004`` (note that this will limit the number of workers, just like specifying a narrow range).

Head Node
~~~~~~~~~
In addition to ports specified above, the head node needs to open several more ports.

- ``--port``: Port of Ray (GCS server). The head node will start a GCS server listening on this port. Default: 6379.
- ``--ray-client-server-port``: Listening port for Ray Client Server. Default: 10001.
- ``--redis-shard-ports``: Comma-separated list of ports for non-primary Redis shards. Default: Random values.

- If ``--include-dashboard`` is true (the default), then the head node must open ``--dashboard-port``. Default: 8265.

If ``--include-dashboard`` is true but the ``--dashboard-port`` is not open on
the head node, you will repeatedly get

.. code-block:: bash

  WARNING worker.py:1114 -- The agent on node <hostname of node that tried to run a task> failed with the following error:
  Traceback (most recent call last):
    File "/usr/local/lib/python3.8/dist-packages/grpc/aio/_call.py", line 285, in __await__
      raise _create_rpc_error(self._cython_call._initial_metadata,
  grpc.aio._call.AioRpcError: <AioRpcError of RPC that terminated with:
    status = StatusCode.UNAVAILABLE
    details = "failed to connect to all addresses"
    debug_error_string = "{"description":"Failed to pick subchannel","file":"src/core/ext/filters/client_channel/client_channel.cc","file_line":4165,"referenced_errors":[{"description":"failed to connect to all addresses","file":"src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc","file_line":397,"grpc_status":14}]}"

(Also, you will not be able to access the dashboard.)

If you see that error, check whether the ``--dashboard-port`` is accessible
with ``nc`` or ``nmap`` (or your browser).

.. code-block:: bash

  $ nmap -sV --reason -p 8265 $HEAD_ADDRESS
  Nmap scan report for compute04.berkeley.edu (123.456.78.910)
  Host is up, received reset ttl 60 (0.00065s latency).
  rDNS record for 123.456.78.910: compute04.berkeley.edu
  PORT     STATE SERVICE REASON         VERSION
  8265/tcp open  http    syn-ack ttl 60 aiohttp 3.7.2 (Python 3.8)
  Service detection performed. Please report any incorrect results at https://nmap.org/submit/ .

Note that the dashboard runs as a separate subprocess which can crash invisibly
in the background, so even if you checked port 8265 earlier, the port might be
closed *now* (for the prosaic reason that there is no longer a service running
on it). This also means that if that port is unreachable, if you ``ray stop``
and ``ray start``, it may become reachable again due to the dashboard
restarting.

If you don't want the dashboard, set ``--include-dashboard=false``.

TLS Authentication
------------------

Ray can be configured to use TLS on it's gRPC channels.
This means that connecting to the Ray client on the head node will
require an appropriate set of credentials and also that data exchanged between
various processes (client, head, workers) will be encrypted.

Enabling TLS will cause a performance hit due to the extra overhead of mutual
authentication and encryption.
Testing has shown that this overhead is large for small workloads and becomes
relatively smaller for large workloads.
The exact overhead will depend on the nature of your workload.

TLS is enabled by setting environment variables.

- ``RAY_USE_TLS``: Either 1 or 0 to use/not-use TLS. If this is set to 1 then all of the environment variables below must be set. Default: 0.
- ``RAY_TLS_SERVER_CERT``: Location of a `certificate file` which is presented to other endpoints so as to achieve mutual authentication.
- ``RAY_TLS_SERVER_KEY``: Location of a `private key file` which is the cryptographic means to prove to other endpoints that you are the authorized user of a given certificate.
- ``RAY_TLS_CA_CERT``: Location of a `CA certificate file` which allows TLS to decide whether an endpoint's certificate has been signed by the correct authority.


Java Applications
-----------------

.. important:: For the multi-node setting, you must first run ``ray start`` on the command line to start the Ray cluster services on the machine before ``Ray.init()`` in Java to connect to the cluster services. On a single machine, you can run ``Ray.init()`` without ``ray start``, which will both start the Ray cluster services and connect to them.

.. _code_search_path:

Code Search Path
~~~~~~~~~~~~~~~~

If you want to run a Java application in a multi-node cluster, you must specify the code search path in your driver. The code search path is to tell Ray where to load jars when starting Java workers. Your jar files must be distributed to the same path(s) on all nodes of the Ray cluster before running your code.

.. code-block:: bash

  $ java -classpath <classpath> \
      -Dray.address=<address> \
      -Dray.job.code-search-path=/path/to/jars/ \
      <classname> <args>

The ``/path/to/jars/`` here points to a directory which contains jars. All jars in the directory will be loaded by workers. You can also provide multiple directories for this parameter.

.. code-block:: bash

  $ java -classpath <classpath> \
      -Dray.address=<address> \
      -Dray.job.code-search-path=/path/to/jars1:/path/to/jars2:/path/to/pys1:/path/to/pys2 \
      <classname> <args>

You don't need to configure code search path if you run a Java application in a single-node cluster.

See ``ray.job.code-search-path`` under :ref:`Driver Options <java-driver-options>` for more information.

.. note:: Currently we don't provide a way to configure Ray when running a Java application in single machine mode. If you need to configure Ray, run ``ray start`` to start the Ray cluster first.

.. _java-driver-options:

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

- ``ray.job.code-search-path``

  - The paths for Java workers to load code from. Currently only directories are supported. You can specify one or more directories split by a ``:``. You don't need to configure code search path if you run a Java application in single machine mode or local mode. Code search path is also used for loading Python code if it's specified. This is required for :ref:`cross_language`. If code search path is specified, you can only run Python remote functions which can be found in the code search path.
  - Type: ``String``
  - Default: empty string.
  - Example: ``/path/to/jars1:/path/to/jars2:/path/to/pys1:/path/to/pys2``

- ``ray.job.namespace``

  - The namespace of this job. It's used for isolation between jobs. Jobs in different namespaces cannot access each other. If it's not specified, a randomized value will be used instead.
  - Type: ``String``
  - Default: A random UUID string value.

.. _`Apache Arrow`: https://arrow.apache.org/
