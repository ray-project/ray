(kuberay-gcs-rocksdb-ft)=
# Embedded RocksDB backend for GCS fault tolerance

The Global Control Service (GCS) supports an embedded RocksDB backend
([REP-64](https://github.com/ray-project/enhancements/pull/64)) as an
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
* [REP-64: Embedded Storage Backend for GCS Fault Tolerance](https://github.com/ray-project/enhancements/pull/64) — full design and rationale.
