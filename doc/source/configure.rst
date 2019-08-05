Configuring Ray
===============

This page discusses the various way to configure Ray, both from the Python API
and from the command line. Take a look at the ``ray.init`` `documentation
<package-ref.html#ray.init>`__ for a complete overview of the configurations.

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

When starting Ray from the command line, pass the ``--num-cpus`` and ``--num-cpus`` flags into ``ray start``. You can also specify custom resources.

.. code-block:: bash

  # To start a head node.
  $ ray start --head --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

  # To start a non-head node.
  $ ray start --redis-address=<redis-address> --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

  # Specifying custom resources
  ray start [--head] --num-cpus=<NUM_CPUS> --resources='{"Resource1": 4, "Resource2": 16}'

If using the command line, connect to the Ray cluster as follow:

.. code-block:: python

  # Connect to ray. Notice if connected to existing cluster, you don't specify resources.
  ray.init(redis_address=<redis-address>)


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

Redis authentication is only supported on the raylet code path.

To add authentication via the Python API, start Ray using:

.. code-block:: python

  ray.init(redis_password="password")

To add authentication via the CLI or to connect to an existing Ray instance with
password-protected Redis ports:

.. code-block:: bash

  ray start [--head] --redis-password="password"

While Redis port authentication may protect against external attackers,
Ray does not encrypt traffic between nodes so man-in-the-middle attacks are
possible for clusters on untrusted networks.

See the `Redis security documentation <https://redis.io/topics/security>`__
for more information.


Using the Object Store with Huge Pages
--------------------------------------

Plasma is a high-performance shared memory object store originally developed in
Ray and now being developed in `Apache Arrow`_. See the `relevant
documentation`_.


On Linux, it is possible to increase the write throughput of the Plasma object
store by using huge pages. You first need to create a file system and activate
huge pages as follows.

.. code-block:: shell

  sudo mkdir -p /mnt/hugepages
  gid=`id -g`
  uid=`id -u`
  sudo mount -t hugetlbfs -o uid=$uid -o gid=$gid none /mnt/hugepages
  sudo bash -c "echo $gid > /proc/sys/vm/hugetlb_shm_group"
  # This typically corresponds to 20000 2MB pages (about 40GB), but this
  # depends on the platform.
  sudo bash -c "echo 20000 > /proc/sys/vm/nr_hugepages"

**Note:** Once you create the huge pages, they will take up memory which will
never be freed unless you remove the huge pages. If you run into memory issues,
that may be the issue.

You need root access to create the file system, but not for running the object
store.

You can then start Ray with huge pages on a single machine as follows.

.. code-block:: python

  ray.init(huge_pages=True, plasma_directory="/mnt/hugepages")

In the cluster case, you can do it by passing ``--huge-pages`` and
``--plasma-directory=/mnt/hugepages`` into ``ray start`` on any machines where
huge pages should be enabled.

See the relevant `Arrow documentation for huge pages`_.

.. _`Apache Arrow`: https://arrow.apache.org/
.. _`relevant documentation`: https://arrow.apache.org/docs/python/plasma.html#the-plasma-in-memory-object-store
.. _`Arrow documentation for huge pages`: https://arrow.apache.org/docs/python/plasma.html#using-plasma-with-huge-pages
