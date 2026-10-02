---
myst:
  html_meta:
    description: "Make the GCS fault tolerant by backing cluster metadata with Redis or the alpha embedded RocksDB, with tuning guidance."
---

(fault-tolerance-gcs)=

# GCS fault tolerance

The Global Control Service, or GCS, manages cluster-level metadata. It also handles cluster-level operations, including {ref}`actor <ray-remote-classes>`, {ref}`placement group <ray-placement-group-doc-ref>`, and node management. By default, the GCS isn't fault tolerant because it stores all data in memory. If it fails, the entire Ray cluster fails. To enable GCS fault tolerance, back the GCS with durable storage so it can reload cluster metadata after a restart. Ray offers two backends:

- **External Redis**: The GCS persists its state to a highly available Redis instance, known as HA Redis. This backend is officially supported.
- **Embedded RocksDB**: The GCS persists its state to a local [RocksDB](https://rocksdb.org/) database on a persistent volume, so you don't run an external data store. This backend is alpha. See {ref}`fault-tolerance-gcs-rocksdb`.

With either backend, when the GCS restarts, it reloads all its data from the backing store and resumes its regular functions.

During the recovery period, the following functions aren't available:

- Actor creation, deletion, and reconstruction
- Placement group creation, deletion, and reconstruction
- Resource management
- Worker node registration
- Worker process creation

However, running Ray tasks and actors remain alive, and any existing objects stay available.

## Setting up Redis

::::{tab-set}
:::{tab-item} KubeRay (officially supported)
If you use {ref}`KubeRay <kuberay-index>`, see the {ref}`KubeRay docs on GCS fault tolerance <kuberay-gcs-ft>`.
:::

:::{tab-item} ray start
If you use {ref}`ray start <ray-start-doc>` to start the Ray head node, set the `RAY_REDIS_ADDRESS` OS environment variable to the Redis address, and pass the password with the `--redis-password` flag when you call `ray start`:

```shell
RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD --redis-username default
```
:::

:::{tab-item} ray up
If you use {ref}`ray up <ray-up-doc>` to start the Ray cluster, add `RAY_REDIS_ADDRESS` and `--redis-password` to the `ray start` command in the {ref}`head_start_ray_commands <cluster-configuration-head-start-ray-commands>` field:

```yaml
head_start_ray_commands:
  - ray stop
  - ulimit -n 65536; RAY_REDIS_ADDRESS=redis_ip:port ray start --head --redis-password PASSWORD --redis-username default --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --dashboard-host=0.0.0.0
```
:::
::::


After you back the GCS with Redis, it recovers its state from Redis when it restarts. While the GCS recovers, each raylet tries to reconnect to it. If a raylet can't reconnect for more than 60 seconds, that raylet exits and the corresponding node fails. Set this timeout with the `RAY_gcs_rpc_server_reconnect_timeout_s` OS environment variable.

If the GCS IP address might change after restarts, use a qualified domain name and pass it to all raylets at start time. Each raylet resolves the domain name and connects to the correct GCS. Make sure that only one GCS is alive at any time.

:::{note}
GCS fault tolerance with external Redis is officially supported only if you use {ref}`KubeRay <kuberay-index>` for {ref}`Ray Serve fault tolerance <serve-e2e-ft>`. In other cases, you can use it at your own risk, but you need to implement additional mechanisms that detect a failure of the GCS or the head node and restart it.
:::

:::{note}
You can also enable GCS fault tolerance when you run Ray on [Anyscale](https://www.anyscale.com/). For instructions, see the [Anyscale documentation](https://docs.anyscale.com/platform/services/head-node-ft/).
:::

(fault-tolerance-gcs-rocksdb)=

## Embedded RocksDB backend (alpha)

:::{note}
The embedded RocksDB backend is alpha and might change before it becomes stable. The Ray project welcomes feedback. Share your experience in a [GitHub issue](https://github.com/ray-project/ray/issues).
:::

The Redis backend makes the GCS fault tolerant, but it also adds an external, highly available Redis instance that you have to deploy, secure, and operate. The *embedded RocksDB backend* removes that dependency. The GCS persists its state to a local [RocksDB](https://rocksdb.org/) database on a persistent volume instead of to Redis. Instead of running a separate data store, you provide a directory on durable storage.

The recovery model is identical to Redis-backed fault tolerance. When the GCS restarts, it reads its state back from disk and resumes, and each raylet reconnects while the GCS recovers. The only difference is where the state persists, in a local RocksDB database instead of an external Redis instance.

### Redis or RocksDB?

```{list-table}
:header-rows: 1
:widths: 34 33 33

* -
  - External Redis
  - Embedded RocksDB
* - Extra process to operate
  - Yes, HA Redis
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
  - Officially supported with KubeRay for Ray Serve
  - Alpha
```

Choose the embedded RocksDB backend when you want GCS fault tolerance without running Redis, and you can attach a durable volume that you can reattach to whichever node runs the GCS, such as a Kubernetes `PersistentVolume`.

### Enabling it

Set two environment variables before you start the head node:

- `RAY_gcs_storage=rocksdb` selects the backend.
- `RAY_gcs_storage_path=<dir>` points at a directory on a persistent volume where RocksDB stores its files. This variable is required. If it's unset, Ray fails fast at startup.

```shell
RAY_gcs_storage=rocksdb RAY_gcs_storage_path=/mnt/ray-gcs ray start --head
```

The directory must live on storage that survives a restart of the GCS or the head node, and that you can reattach to the node running the recovered GCS. HA Redis satisfies the same durability requirement for the Redis backend.

:::{note}
The GCS process embeds the RocksDB database, which is single-writer. Only one GCS can open the storage path at a time. Point every restart of a cluster's head node at the same path, and never share a path between clusters.
:::

For a step-by-step Kubernetes walkthrough, see {ref}`kuberay-gcs-rocksdb-ft`.

### Advanced tuning

RocksDB I/O runs on a dedicated thread pool so it never stalls the GCS event loop. This I/O includes the write-ahead-log fsync, which dominates write latency. The defaults suit the GCS metadata workload, so you usually don't need to change them. Tune the pool with the following two environment variables:

- `RAY_gcs_rocksdb_io_pool_size`: The number of worker threads in the RocksDB I/O offload pool. Defaults to `4`.
- `RAY_gcs_rocksdb_strand_buckets`: The number of per-key ordering buckets. Single-key operations are hashed into a bucket and serialized within it, while different buckets run concurrently. Defaults to `64`.
