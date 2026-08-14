(kuberay-gcs-rocksdb-ft)=
# GCS fault tolerance with embedded RocksDB (alpha)

```{admonition} Alpha
:class: warning
The embedded RocksDB GCS backend is in alpha and may change before becoming stable.
If you try it, please share your experience on [GitHub](https://github.com/ray-project/ray/issues).
```

The Global Control Service (GCS) manages cluster-level metadata. By default it keeps that
metadata in memory, so a GCS restart takes down the whole Ray cluster. {ref}`GCS fault
tolerance <fault-tolerance-gcs>` fixes this by persisting the metadata to durable storage.

The {ref}`Redis-backed setup <kuberay-gcs-ft>` does this with an external, highly available
Redis instance that you deploy and operate alongside Ray. The **embedded RocksDB backend**
persists GCS state to a local [RocksDB](https://rocksdb.org/) database on a Kubernetes
`PersistentVolume` instead, so there's no Redis to run. You opt in with
`gcsFaultToleranceOptions.backend: rocksdb`, and KubeRay provisions the volume, mounts it on
the head Pod, sets the required environment variables, and garbage-collects the volume with
the cluster. When the head Pod restarts, it reattaches the same volume, reads the metadata
back from disk, and workers reconnect while the GCS recovers.

For the concepts, the Redis-vs-RocksDB trade-offs, and non-Kubernetes usage, see
{ref}`fault-tolerance-gcs-rocksdb`.

```{seealso}
For the officially supported, Redis-backed setup, see
{ref}`GCS fault tolerance in KubeRay <kuberay-gcs-ft>`.
```

## Prerequisites

* KubeRay v1.7 or later, which is the first release that supports the embedded RocksDB backend.
* Ray 2.57.0 or later, which is the first release that contains the embedded RocksDB backend.
* Linux worker nodes (the RocksDB backend is Linux only).
* A `StorageClass` that provisions a durable volume which can reattach to the node that runs
  the recovered head Pod.

## Enable the operator feature gate

The embedded backend is alpha and gated behind the KubeRay `GCSFaultToleranceEmbeddedStorage`
feature gate, which is **off by default**. Start the KubeRay operator with the gate enabled:

```sh
--feature-gates=GCSFaultToleranceEmbeddedStorage=true
```

Set this on the operator Deployment (for example through the Helm chart's `featureGates`
value). Without it, KubeRay rejects any RayCluster that sets `backend: rocksdb` during
validation.

## How it works

* You set `gcsFaultToleranceOptions.backend: rocksdb` on the RayCluster. KubeRay provisions a
  `PersistentVolumeClaim` named `{cluster}-gcs-pvc`, mounts it on the head Pod at `/data/gcs`,
  and sets `RAY_gcs_storage=rocksdb` and `RAY_gcs_storage_path` automatically. You don't set
  those environment variables yourself.
* The GCS writes its state to a RocksDB database on that volume, syncing every mutating write
  to disk.
* KubeRay injects `RAY_gcs_rpc_server_reconnect_timeout_s=600` into the worker Pods, exactly
  as it does for the Redis backend, so workers wait for the head Pod to come back instead of
  exiting during recovery.
* If the head Pod dies and Kubernetes reschedules it, the new Pod reattaches the *same*
  volume and the GCS recovers from the on-disk database.
* The operator-managed PVC is owned by the RayCluster, so by default Kubernetes
  garbage-collects it when you delete the cluster. Set `deletionPolicy: Retain` to keep the
  volume and its data after the cluster is gone.

```{admonition} Single writer
:class: note
The RocksDB database is embedded in the GCS process and is single-writer: at most one GCS
process may have the database open at any time, and two concurrent writers corrupt it. The
default `ReadWriteOnce` volume with a single head replica guarantees this. Any other setup
must still enforce a single active writer, so a storage path must never be opened by more than
one Pod at a time, and must never be shared between clusters.
```

## Deploy a RayCluster with the RocksDB backend

Apply the following manifest. Setting `gcsFaultToleranceOptions.backend: rocksdb` is all it
takes to enable the backend; KubeRay handles the PVC, the mount, and the environment
variables.

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-rocksdb-ft
spec:
  gcsFaultToleranceOptions:
    # Select the embedded RocksDB backend. KubeRay provisions a PVC, mounts it on
    # the head Pod at /data/gcs, and sets RAY_gcs_storage / RAY_gcs_storage_path.
    backend: rocksdb
    storage:
      # Operator-managed PVC. KubeRay creates `{cluster}-gcs-pvc` and garbage-collects
      # it with the RayCluster. It's created once and not reconfigured in place; to
      # change size, class, or access modes, delete the PVC and let KubeRay recreate it.
      size: 10Gi
      # storageClassName: ssd              # optional; defaults to the cluster's default StorageClass
      # accessModes: [ReadWriteOnce]       # optional; ReadWriteOnce is the default and suits a
      #                                    # single-head cluster (RocksDB is single-writer)
      # subPath: clusters/my-ray/gcs       # optional; mount a subdirectory of the volume
      # deletionPolicy: DeleteWithCluster  # optional; default. Set to Retain to keep the PVC
      #                                    # (and its data) after the RayCluster is deleted.
      # claimName: my-gcs-pvc              # optional; bring your own PVC instead of an
      #                                    # operator-managed one (mutually exclusive with
      #                                    # size / storageClassName / accessModes).
  headGroupSpec:
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.57.0
  workerGroupSpecs:
  - groupName: small-group
    replicas: 1
    minReplicas: 1
    maxReplicas: 1
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.57.0
```

```{admonition} Storage options
:class: tip
The fields under `storage` mirror the KubeRay API:

* `size`, `storageClassName`, and `accessModes` describe the PVC that KubeRay provisions and
  owns. It's created once; to change any of them, delete the PVC so KubeRay recreates it.
* `subPath` mounts a subdirectory of the volume instead of its root.
* `deletionPolicy` is `DeleteWithCluster` (default) or `Retain`. `Retain` omits the owner
  reference so the PVC and its data outlive the cluster; a later cluster can recover the state
  through `claimName` (or by reusing the same cluster name, which adopts the retained PVC).
* `claimName` brings your own PVC and is mutually exclusive with `size`, `storageClassName`,
  and `accessModes`. It's also how you persist GCS state across a RayService zero-downtime
  upgrade: point every generation at the same claim.
```

```{admonition} Size the volume for throughput, not just capacity
:class: warning
On most cloud providers, a volume's IOPS and throughput scale with its provisioned size (for
example, AWS `gp3` and GCP `pd-balanced` grant more baseline throughput to larger disks).
Because the GCS syncs every mutating write to disk, an undersized volume can throttle GCS
write latency even when it has plenty of free capacity. Provision the volume for the disk
throughput your workload needs rather than for the metadata footprint alone, and consult your
`StorageClass` and provider documentation for the size-to-throughput relationship.
```

## Verify recovery

Confirm the head Pod is running, then delete it to simulate a GCS crash and watch KubeRay
recreate it against the same volume:

```sh
# Confirm KubeRay provisioned the operator-managed PVC.
kubectl get pvc raycluster-rocksdb-ft-gcs-pvc

# Wait for the head Pod to be ready.
kubectl get pods -l ray.io/node-type=head

# Delete the head Pod to simulate a GCS/head failure.
kubectl delete pod -l ray.io/node-type=head

# KubeRay recreates the head Pod. It reattaches the same PersistentVolumeClaim
# and the GCS recovers its state from the on-disk RocksDB database.
kubectl get pods -l ray.io/node-type=head -w
```

Because the GCS metadata persisted to the volume, the recovered cluster keeps its state
instead of starting fresh. During recovery, cluster-level operations such as actor and
placement group creation are briefly unavailable, exactly as with the Redis backend.

## Clean up

```sh
kubectl delete raycluster raycluster-rocksdb-ft
```

Under the default `deletionPolicy: DeleteWithCluster`, KubeRay garbage-collects the
operator-managed `raycluster-rocksdb-ft-gcs-pvc` PVC with the cluster, so there's no separate
volume to delete. If you set `deletionPolicy: Retain` or brought your own PVC with
`claimName`, delete the PVC manually when you no longer need the data:

```sh
kubectl delete pvc raycluster-rocksdb-ft-gcs-pvc
```

## Next steps

* {ref}`GCS fault tolerance concepts and tuning <fault-tolerance-gcs>`
* {ref}`Redis-backed GCS fault tolerance <kuberay-gcs-ft>`
* {ref}`Tuning Redis for a persistent fault tolerant GCS <kuberay-gcs-persistent-ft>`
