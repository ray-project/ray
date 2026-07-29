.. _fault-tolerance-gcs:

GCS Fault Tolerance
===================

The Global Control Service, or GCS, manages cluster-level metadata.
It also provides a handful of cluster-level operations including :ref:`actor <ray-remote-classes>`, :ref:`placement groups <ray-placement-group-doc-ref>` and node management.
By default, the GCS isn't fault tolerant because it stores all data in memory. If it fails, the entire Ray cluster fails.
To enable GCS fault tolerance, back the GCS with durable storage so it can reload cluster metadata after a restart. Ray offers two backends:

- **External Redis** (officially supported): the GCS persists its state to a highly available Redis instance, known as HA Redis.
- **Embedded RocksDB** (alpha): the GCS persists its state to a local `RocksDB <https://rocksdb.org/>`__ database on a persistent volume, with no external datastore to run. See :ref:`fault-tolerance-gcs-rocksdb`.

Either way, when the GCS restarts, it loads all the data back from the backing store and resumes regular functions.

During the recovery period, the following functions aren't available:

- Actor creation, deletion and reconstruction.
- Placement group creation, deletion and reconstruction.
- Resource management.
- Worker node registration.
- Worker process creation.

However, running Ray tasks and actors remain alive, and any existing objects stay available.

Setting up Redis
----------------

.. tab-set::

    .. tab-item:: KubeRay (officially supported)

        If you are using :ref:`KubeRay <kuberay-index>`, refer to :ref:`KubeRay docs on GCS Fault Tolerance <kuberay-gcs-ft>`.

    .. tab-item:: ray start

        If you are using :ref:`ray start <ray-start-doc>` to start the Ray head node,
        set the OS environment ``RAY_REDIS_ADDRESS`` to
        the Redis address, and supply the ``--redis-password`` flag with the password when calling ``ray start``:

        .. code-block:: shell

          RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD --redis-username default

    .. tab-item:: ray up

        If you are using :ref:`ray up <ray-up-doc>` to start the Ray cluster, change :ref:`head_start_ray_commands <cluster-configuration-head-start-ray-commands>` field to add ``RAY_REDIS_ADDRESS`` and ``--redis-password`` to the ``ray start`` command:

        .. code-block:: yaml

          head_start_ray_commands:
            - ray stop
            - ulimit -n 65536; RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD --redis-username default --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --dashboard-host=0.0.0.0


After you back the GCS with Redis, it recovers its state from Redis when it restarts.
While the GCS recovers, each raylet tries to reconnect to it.
If a raylet can't reconnect for more than 60 seconds, that raylet exits and the corresponding node fails.
Set this timeout threshold with the OS environment variable ``RAY_gcs_rpc_server_reconnect_timeout_s``.

If the GCS IP address might change after restarts, use a qualified domain name
and pass it to all raylets at start time. Each raylet resolves the domain name and connects to
the correct GCS. You need to ensure that at any time, only one GCS is alive.

.. note::

  GCS fault tolerance with external Redis is officially supported
  only if you are using :ref:`KubeRay <kuberay-index>` for :ref:`Ray serve fault tolerance <serve-e2e-ft>`.
  For other cases, you can use it at your own risk and
  you need to implement additional mechanisms to detect the failure of GCS or the head node
  and restart it.

.. note::

  You can also enable GCS fault tolerance when running Ray on `Anyscale <https://www.anyscale.com/>`_. See the Anyscale `documentation <https://docs.anyscale.com/platform/services/head-node-ft/>`_ for instructions.

.. _fault-tolerance-gcs-rocksdb:

Embedded RocksDB backend (alpha)
--------------------------------

.. note::

  The embedded RocksDB backend is in alpha and may change before becoming stable.
  We're actively looking for feedback: please share your experience on
  `GitHub <https://github.com/ray-project/ray/issues>`_.

The Redis-backed setup above makes the GCS fault tolerant, but it also adds an external,
highly available Redis instance that you have to deploy, secure, and operate. The *embedded
RocksDB backend* removes that dependency: the GCS persists its state to a local
`RocksDB <https://rocksdb.org/>`__ database on a persistent volume instead of to Redis.
There's no separate datastore to run, just a directory on durable storage.

The recovery model is identical to Redis-backed fault tolerance. When the GCS restarts, it
reads its state back from disk and resumes, and each raylet reconnects while it recovers.
Only the *location* of the persisted state differs: a local RocksDB database instead of an
external Redis instance.

Redis or RocksDB?
~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 34 33 33

   * -
     - External Redis
     - Embedded RocksDB
   * - Extra process to operate
     - Yes (HA Redis)
     - No
   * - Where state lives
     - External Redis instance
     - Local RocksDB database on a persistent volume
   * - Survives head node or Pod loss
     - Yes, if Redis survives
     - Yes, if the persistent volume survives and reattaches to the new head
   * - Platform support
     - All platforms
     - Linux only
   * - Maturity
     - Officially supported (with KubeRay for Ray Serve)
     - Alpha

Choose the embedded RocksDB backend when you want GCS fault tolerance without running Redis,
and you can attach a durable, reattachable volume, for example a Kubernetes ``PersistentVolume``,
to whichever node runs the GCS.

Enabling it
~~~~~~~~~~~

Set two environment variables before you start the head node:

- ``RAY_gcs_storage=rocksdb`` selects the backend.
- ``RAY_gcs_storage_path=<dir>`` points at a directory on a persistent volume where RocksDB
  stores its files. This is required; Ray fails fast at startup if it's unset.

.. code-block:: shell

  RAY_gcs_storage=rocksdb RAY_gcs_storage_path=/mnt/ray-gcs ray start --head

The directory must live on storage that survives a GCS (head) restart and that can be
reattached to the node running the recovered GCS: the same durability requirement that HA
Redis satisfies for the Redis backend.

.. note::

  The RocksDB database is embedded in the GCS process and is single-writer: exactly one GCS
  may open the storage path at a time. Point every restart of a given cluster's head at the
  *same* path, and never share a path between clusters.

For a step-by-step Kubernetes walkthrough, see :ref:`kuberay-gcs-rocksdb-ft`.

Advanced tuning
~~~~~~~~~~~~~~~

RocksDB I/O, including the write-ahead-log fsync that dominates write latency, runs on a
dedicated thread pool so it never stalls the GCS event loop. Two environment variables tune
it. The defaults suit the GCS metadata workload, and most users never change them:

- ``RAY_gcs_rocksdb_io_pool_size`` (default ``4``): worker threads in the RocksDB I/O
  offload pool.
- ``RAY_gcs_rocksdb_strand_buckets`` (default ``64``): per-key ordering buckets. Single-key
  operations are hashed into a bucket and serialized within it, while different buckets run
  concurrently.
