(kuberay-gcs-rocksdb-ft)=
# Embedded RocksDB backend for GCS fault tolerance

The Global Control Service (GCS) supports an embedded RocksDB backend
([REP-64](https://github.com/ray-project/enhancements/blob/main/reps/2026-02-23-gcs-embedded-storage.md)) as an
alternative to external Redis for fault tolerance. Instead of writing state
over the network to a Redis instance, GCS writes to a local RocksDB
database on a persistent volume; on head pod restart, GCS reads the state
back from disk and resumes.

The recovery model is identical to Redis-backed FT — GCS goes down,
restarts, reads state, workers reconnect. The difference is purely in
where state is persisted.

## When to use RocksDB vs Redis

| Situation | Suggested backend |
|---|---|
| New KubeRay deployment, want fewer moving parts and no external dependency | Embedded RocksDB |
| Latency-sensitive writes; want to avoid network RTT to Redis | Embedded RocksDB |
| Already run managed Redis at high quality with monitoring and backup | Redis |
| Need external inspection of GCS state (`redis-cli`) or cross-cluster state sharing | Redis |
| Cross-zone failover required without a regional / `ReadWriteMany` volume | Redis |

## Prerequisites

* Ray version with REP-64 support (the version that introduced this page).
* A `StorageClass` available in the cluster that backs the PVC. Any
  `ReadWriteOnce` block storage class (e.g. `gp3` on EKS, `standard-rwo`
  on GKE, `managed-premium` on AKS) works.

## Quickstart

This section describes the manual setup. It works on any current
KubeRay version because it only uses standard `RayCluster` spec fields
(PVC, `volumeMounts`, env vars). A follow-up KubeRay change will offer
a first-class `gcsFaultToleranceOptions.backend: rocksdb` field that
generates the equivalent spec automatically.

### Step 1: Create a PersistentVolumeClaim for GCS state

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-ray-gcs-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
  # storageClassName: <your-storage-class>   # uses default SC if omitted
```

GCS metadata is small (typically 10–100 MB across all column families
in steady state); 1 GiB is ample headroom.

:::{note}
On most cloud block-storage classes, baseline IOPS scale with the
provisioned volume size, so sizing for capacity alone can throttle
RocksDB's per-write fsync. For example, AWS `gp2` provides 3 IOPS per
GiB (so a 1 GiB volume gets the 100 IOPS floor); GCE `pd-balanced`
provides 6 read + 12 write IOPS per GiB; Azure Premium SSD v1 buckets
IOPS by tier (P1 = 120 IOPS at 4 GiB). AWS `gp3` and Azure Premium SSD
v2 let you provision IOPS independently of size and are the better fit
when the volume only needs to hold a small amount of state. Size — or
provision IOPS — for the write rate your cluster generates, not just
the metadata footprint.
:::

### Step 2: Mount the PVC and set the env vars on the head pod

In the `RayCluster` (or `RayService` / `RayJob`) spec, configure the
head group template to mount the PVC and tell Ray to use RocksDB:

```yaml
spec:
  headGroupSpec:
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:latest
          env:
          - name: RAY_gcs_storage
            value: "rocksdb"
          - name: RAY_gcs_storage_path
            value: "/data/gcs-state"
          volumeMounts:
          - name: gcs-storage
            mountPath: /data/gcs-state
        volumes:
        - name: gcs-storage
          persistentVolumeClaim:
            claimName: my-ray-gcs-pvc
```

When the head pod restarts, Kubernetes re-attaches the same PVC and
GCS recovers from the existing RocksDB state.

### Step 3: (Optional) Tune the offload path

By default, RocksDB I/O — including the per-write WAL fsync — runs
inline on the GCS event loop. For clusters with non-trivial write
rates, set `RAY_gcs_rocksdb_async_offload=true` to run RocksDB I/O on a
background thread pool. RocksDB's group-commit then aggregates
concurrent in-flight writers into one fsync, raising aggregate
throughput without changing per-call latency.

```yaml
env:
- name: RAY_gcs_rocksdb_async_offload
  value: "true"
- name: RAY_gcs_rocksdb_io_pool_size      # optional; default 4
  value: "4"
- name: RAY_gcs_rocksdb_strand_buckets    # optional; default 64
  value: "64"
```

## Durability and the death-notification tables

By default RocksDB persists every GCS write with a synchronous WAL
`fsync` (`WriteOptions::sync = true`) before acknowledging it, so a write
that GCS has acked is guaranteed to survive a crash. There is one
deliberate exception, controlled by `RAY_gcs_rocksdb_soft_durability_tables`
(default `"NODE,ACTOR"`), and it is worth understanding because it differs
from the strict per-write durability you might assume.

### Why an exception exists: publish-after-persist

When a node or actor dies, GCS does two things: it **persists** the new
death state, and it **publishes** a notification so the rest of the
cluster (drivers, raylets) learns about the death and can react — for
example, by reconstructing objects that lived on the dead node.

Crucially, GCS publishes the notification *from inside the storage
write's completion callback* — i.e. **publish-after-persist**. The
notification is only sent once the write has been durably committed:

```text
node/actor dies
      │
      ▼
  GCS writes death record ──► [ WAL fsync ] ──► on_done callback ──► publish notification
                               ~3–15 ms                              (cluster reacts)
```

With an in-memory or `appendfsync everysec` Redis backend the "persist"
step is effectively instant, so the notification goes out immediately.
Under RocksDB with a per-write fsync, the notification is gated behind
that fsync. The fsync itself is fast (single-digit to low-tens of
milliseconds), but it is enough to widen a pre-existing, timing-sensitive
race in Ray Core's object/generator-reconstruction path: a driver
reconstructing a `num_returns=None` generator whose task depended on the
dead actor could stall indefinitely waiting for the death notification
that the fsync delayed. (The race is in Core reconstruction, not in the
storage backend — RocksDB returns correct data; only the notification
timing differs. Per-table isolation testing pinned the trigger to the
`ACTOR`-table write for the actor-death case.)

### The fix: relax fsync on the death-notification tables

Tables listed in `gcs_rocksdb_soft_durability_tables` are written with
`sync = false`, so death notifications are no longer gated on the fsync.
The default carries the two death-notification tables, `NODE` and
`ACTOR`.

This does **not** make RocksDB FT less durable than the production
baseline Ray already ships:

* Ray's recommended Redis GCS runs with `appendfsync everysec` — Redis
  fsyncs its append-only log **once per second**, not per write (see
  {ref}`kuberay-gcs-persistent-ft`). Per-write fsync (`appendfsync
  always`) is documented there as an *optional* stronger setting.
  RocksDB with `sync = false` on these tables still flushes to the OS
  and is recovered on a clean process restart; only an OS/host crash in
  the sub-second window before the next background sync can lose the
  last write — the **same** exposure `appendfsync everysec` already
  accepts.
* The affected state is **re-derivable** after a GCS restart regardless:
  node liveness is re-established by health checks, and actor state is
  reconciled from the running cluster. These are exactly the tables for
  which losing the last unsynced record is recoverable.

Every other table keeps the strict per-write fsync. Writes with no table
name (the cluster-ID marker and the job-ID counter) always fsync.

### Tuning the knob

The default is appropriate for essentially all deployments. The knob
exists for two reasons: to make the durability decision explicit and
auditable, and to let an operator who wants strict per-write durability
on *every* table opt back in:

```yaml
env:
# Keep the strict per-write fsync on all tables (reverts the relaxation;
# may re-expose the reconstruction stall described above).
- name: RAY_gcs_rocksdb_soft_durability_tables
  value: ""
```

The value is a comma-separated list of GCS table names (for example
`"NODE,ACTOR"`). The proper long-term fix is in Ray Core — decoupling
the death-notification publish from the storage persist so notifications
are never gated on fsync regardless of backend — and is tracked as a
REP-64 follow-up; this knob is the contained, backend-local mitigation.

## Caveats

* **Volume topology determines failover scope.** With a `ReadWriteOnce`
  cloud block volume (the default), the head pod survives node-level
  failures within a zone. Cross-zone head pod failover requires a
  regional disk or a `ReadWriteMany` filesystem (EFS, CephFS, Azure
  Files); see the REP for the full topology discussion.
* **Distributed training still breaks.** RocksDB-backed FT preserves
  cluster metadata across head restarts; it does not save in-flight
  training. NCCL training groups still break on head failure and
  training must resume from the last application checkpoint.
* **PVC lifecycle is currently manual.** With the spec above, the PVC
  outlives the `RayCluster`. Clean it up explicitly when you delete
  the cluster, or use `ownerReferences` to tie its lifecycle to the
  `RayCluster`. The forthcoming KubeRay operator change will manage
  this automatically.

## See also

* {ref}`fault-tolerance-gcs` — Ray Core overview of GCS fault tolerance.
* {ref}`kuberay-gcs-ft` — the Redis-backed FT setup guide.
* [REP-64: Embedded Storage Backend for GCS Fault Tolerance](https://github.com/ray-project/enhancements/blob/main/reps/2026-02-23-gcs-embedded-storage.md) — full design and rationale.
