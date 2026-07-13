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
`PersistentVolume` instead, so there's no Redis to run. When the head Pod restarts, it
reattaches the same volume, reads the metadata back from disk, and workers reconnect while
the GCS recovers.

For the concepts, the Redis-vs-RocksDB trade-offs, and non-Kubernetes usage, see
{ref}`fault-tolerance-gcs-rocksdb`.

```{seealso}
For the officially supported, Redis-backed setup, see
{ref}`GCS fault tolerance in KubeRay <kuberay-gcs-ft>`.
```

## Prerequisites

* KubeRay 1.3.0+
* Linux worker nodes (the RocksDB backend is Linux only)
* A `StorageClass` that provisions a durable volume which can reattach to the node that runs
  the recovered head Pod

## How it works

* You mount a `PersistentVolume` on the head Pod and point the GCS at it with two environment
  variables.
* The GCS writes its state to a RocksDB database on that volume, syncing every mutating write
  to disk.
* If the head Pod dies and Kubernetes reschedules it, the new Pod reattaches the *same*
  volume and the GCS recovers from the on-disk database.

```{admonition} Single writer
:class: note
The RocksDB database is embedded in the GCS process and is single-writer: at most one GCS
process may have the database open at any time, and two concurrent writers corrupt it. The
simplest way to guarantee this on Kubernetes, and what this guide uses, is a `ReadWriteOnce`
volume with a single head replica. Any other setup must still enforce a single active writer,
so a storage path must never be opened by more than one Pod at a time, and must never be
shared between clusters.
```

## Deploy a RayCluster with the RocksDB backend

Apply the following manifest. It creates a `PersistentVolumeClaim` for the GCS database and a
RayCluster whose head mounts that claim and sets the two environment variables that enable the
backend.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ray-gcs-rocksdb
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-rocksdb-ft
spec:
  headGroupSpec:
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:latest
          env:
          # Select the embedded RocksDB backend.
          - name: RAY_gcs_storage
            value: rocksdb
          # Store the RocksDB database on the mounted persistent volume.
          - name: RAY_gcs_storage_path
            value: /mnt/ray-gcs
          volumeMounts:
          - name: gcs-rocksdb
            mountPath: /mnt/ray-gcs
        volumes:
        - name: gcs-rocksdb
          persistentVolumeClaim:
            claimName: ray-gcs-rocksdb
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
          image: rayproject/ray:latest
          env:
          # Keep workers alive while the head Pod restarts and its volume
          # reattaches. Because this setup configures GCS fault tolerance
          # manually (not through gcsFaultToleranceOptions), KubeRay doesn't
          # inject this timeout automatically, and the 60s default is usually
          # too short for a PersistentVolume to detach and reattach.
          - name: RAY_gcs_rpc_server_reconnect_timeout_s
            value: "600"
```

```{admonition} Keep the mount path and the storage path in sync
:class: tip
`RAY_gcs_storage_path` must match the container `mountPath` (`/mnt/ray-gcs` above). Ray fails
fast at startup if `RAY_gcs_storage_path` is unset or points at a path that isn't writable.
```

## Verify recovery

Confirm the head Pod is running, then delete it to simulate a GCS crash and watch KubeRay
recreate it against the same volume:

```sh
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
kubectl delete pvc ray-gcs-rocksdb
```

## Next steps

* {ref}`GCS fault tolerance concepts and tuning <fault-tolerance-gcs>`
* {ref}`Redis-backed GCS fault tolerance <kuberay-gcs-ft>`
* {ref}`Tuning Redis for a persistent fault tolerant GCS <kuberay-gcs-persistent-ft>`
