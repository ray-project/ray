(kuberay-gcs-persistent-ft)=
# Tuning Redis for a Persistent Fault Tolerant GCS

Using Redis to back up the Global Control Store (GCS) with KubeRay provides
fault tolerance in the event that the Ray Head is lost, allowing the new Ray
Head to rebuild its state from reading Redis.

However, if Redis also has data loss, the Ray Head state will be lost.

You may want further protection in the event that your Redis cluster experiences
partial or total failure. This guide documents how to configure and tune Redis
for a highly available Ray Cluster with KubeRay.

Tuning your ray cluster to be highly available can safeguard long-running jobs
against unexpected failures or allow you to run on commodity
hardware/pre-emptible machines.

## Solution overview

KubeRay supports using Redis to persist the GCS, which allows us to move the
point of failure (for data loss) externally. We still have to configure Redis
itself to be resilient to failures.

Our solution will provision a [Persistent
Volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) backed
by hardware storage, which Redis will use to write regular snapshots. If Redis
(or its host node) is lost, the Redis deployment can be restored from the
snapshot.

While Redis supports clustering, KubeRay only supports standalone (single
replica) Redis, so clustering is omitted.

## Persistent storage

Specialty storage volumes (like Google Cloud Storage FUSE or S3) do not support
append operations, which Redis uses to efficiently write its Append Only File
(AOF) log. When using using these options, it is recommended to disable AOF.

With GCP GKE and Azure AKS, the default storage classes are [persistent
disks](https://cloud.google.com/kubernetes-engine/docs/concepts/persistent-volumes)
and [SSD Azure
disks](https://learn.microsoft.com/en-us/azure/aks/azure-csi-disk-storage-provision)
respectively, and the only configuration needed to provision a disk is as
follows:

```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: redis-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 8Gi
  storageClassName: standard-rwo
```

On AWS, you must [Create a storage
class](https://docs.aws.amazon.com/eks/latest/userguide/create-storage-class.html)
yourself as well.

## Tuning backups

Redis supports database dumps at set intervals, which is good for fast recovery
and high performance during normal operation.

Redis also supports journaling at frequent intervals (or continuously), which
can provide stronger durabililty at the cost of more disk writes (i.e., slower
performance).

A good starting point for backups is to enable both, which can be done like so:

```
# Dump a backup every 60s, if there are 1000 writes since the prev. backup.
save 60 1000
dbfilename dump.rdb

# Enable the append-only log file.
appendonly yes
appendfilename "appendonly.aof"

```

In this recommended configuration, full backups are created every 60s while the
append-only log is updated every second, which is a reasonable balance for disk
space, latency, and data safety.

There are more options to configure the AOF, defaults shown here:

```
# Sync the log to disk every second.
# Alternatives are "no" and "always" (every write).
appendfsync everysec
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
```

You can view the full reference
[here](https://raw.githubusercontent.com/redis/redis/refs/tags/7.4.0/redis.conf).


If your job is generally idempotent and can resume from several minutes of state
loss, you may prefer to disable the append-only log.

If you prefer your job to lose as little state as possible, then you may prefer
to set `appendfsync` to `always`, such that all write are stored immediately.

## Putting it together

Edit [the full
YAML](https://github.com/ray-project/kuberay/blob/master/config/samples/ray-cluster.persistent-redis.yaml)
to your satisfaction and apply it:

```
kubectl apply -f config/samples/ray-cluster.persistent-redis.yaml
```

Verify that a disk has been provisioned and redis is running:

```
kubectl get persistentvolumes
kubectl get pods
# Should see redis-0 running.
```

After running a job with some state in GCS, you will be able to delete the ray
head pod as well as the redis pod without data loss.

## Verifying

Forward connections to the ray cluster you just created with the Ray Kubectl
plugin:

```
$ kubectl ray session raycluster-external-redis
```

Then submit any Ray job of your choosing and let it run. When finished, delete
all your pods:

```
$ kubectl delete pods --all
```

Wait for the ray head to be provisioned again and enter ready state. Then
restart your port forwarding and view the ray dashboard. You should find the
job's metadata has been persisted, despite the loss of the ray head as well as
the redis replica.
