(resource-isolation-with-writable-cgroups)=

# Resource Isolation with Writable Cgroups on Google Kubernetes Engine (GKE)

This guide covers how to enable Ray resource isolation on GKE using [writable cgroups](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/writable-cgroups).
Ray resource isolation (introduced in v2.51.0) significantly improves Ray's reliability by using cgroups v2 to reserve dedicated CPU and memory
resources for critical system processes.

Historically, enabling resource isolation required privileged containers capable of writing to the `/sys/fs/cgroup` file system.
This approach was not recommended due to the security risks associated with privileged containers. In newer versions of GKE,
you can enable writable cgroups, granting containers read-write access to the cgroups API without requiring privileged mode.

## Prerequisites

* `kubectl` installed and configured to interact with your cluster.
* `gcloud` CLI installed and configured.
* [Helm](https://helm.sh/) installed.
* Ray 2.51.0 or newer.

## Create a GKE Cluster with writable cgroups enabled

To use Ray resource isolation on Kubernetes without privileged containers, you must use a platform that supports cgroups v2 and writable cgroups.

On GKE, create a cluster with writable cgroups enabled as follows:
```bash
$ cat > containerd_config.yaml << EOF
writableCgroups:
  enabled: true
EOF

$ gcloud container clusters create ray-resource-isolation \
    --cluster-version=1.34 \
    --machine-type=e2-standard-16 \
    --num-nodes=3 \
    --containerd-config-from-file=containerd_config.yaml
```

## Install the KubeRay Operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Create a RayCluster with resource isolation enabled

Create a RayCluster with writable cgroups enabled:
```bash
$ kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster-resource-isolation.gke.yaml
```

The applied manifest enables Ray resource isolation by setting `--enable-resource-isolation` in the `ray start` command. It also includes annotations on the Head and Worker Pods
to enable writable cgroups, allowing for hierarchical cgroup management within the container:
```yaml
metadata:
  annotations:
    node.gke.io/enable-writable-cgroups.ray-head: "true"
```

It also applies a node selector to ensure Ray pods are scheduled only on GKE nodes with this capability enabled:
```yaml
nodeSelector:
  node.gke.io/enable-writable-cgroups: "true"
```

## System reserved resources

When resource isolation is enabled, Ray manages a cgroup hierarchy within the container:

```text
       base_cgroup_path (e.g. /sys/fs/cgroup)
              |
      ray-node_<node_id>
     |                 |
   system             user
     |               |    |
    leaf        workers  non-ray
```

The cgroup hierarchy enables Ray to reserve resources for both system and user-level processes based on the container's total reserved resources.

*   **System-reserved CPU:** By default, Ray reserves between 1 and 3 cores.
    *   Formula: `min(3.0, max(1.0, 0.05 * num_cores_on_the_system))`
*   **System-reserved Memory:** By default, Ray reserves between 500MB and 10GB.
    *   Formula: `min(10GB, max(500MB, 0.10 * memory_available_on_the_system))`

All remaining resources are reserved for user processes (e.g. Ray workers).

## Verify resource isolation for Ray processes

Verify that resource isolation is enabled by inspecting the cgroup filesystem within a Ray container.
```bash
$ HEAD_POD=$(kubectl get po -l ray.io/cluster=raycluster-resource-isolation,ray.io/node-type=head -o custom-columns=NAME:.metadata.name --no-headers)

$ kubectl exec -ti $HEAD_POD -- bash
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ # check system cgroup folder
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ ls /sys/fs/cgroup/ray-node*/system
cgroup.controllers      cgroup.stat             cpu.stat         memory.events.local  memory.pressure
cgroup.events           cgroup.subtree_control  cpu.stat.local   memory.high          memory.reclaim
cgroup.freeze           cgroup.threads          cpu.weight       memory.low           memory.stat
cgroup.kill             cgroup.type             cpu.weight.nice  memory.max           memory.swap.current
cgroup.max.depth        cpu.idle                io.pressure      memory.min           memory.swap.events
cgroup.max.descendants  cpu.max                 leaf             memory.numa_stat     memory.swap.high
cgroup.pressure         cpu.max.burst           memory.current   memory.oom.group     memory.swap.max
cgroup.procs            cpu.pressure            memory.events    memory.peak          memory.swap.peak

(base) ray@raycluster-resource-isolation-head-p2xqx:~$ # check user cgroup folder
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ ls /sys/fs/cgroup/ray-node*/user
cgroup.controllers      cgroup.subtree_control  cpu.weight           memory.min           memory.swap.high
cgroup.events           cgroup.threads          cpu.weight.nice      memory.numa_stat     memory.swap.max
cgroup.freeze           cgroup.type             io.pressure          memory.oom.group     memory.swap.peak
cgroup.kill             cpu.idle                memory.current       memory.peak          non-ray
cgroup.max.depth        cpu.max                 memory.events        memory.pressure      workers
cgroup.max.descendants  cpu.max.burst           memory.events.local  memory.reclaim
cgroup.pressure         cpu.pressure            memory.high          memory.stat
cgroup.procs            cpu.stat                memory.low           memory.swap.current
cgroup.stat             cpu.stat.local          memory.max           memory.swap.events
```

You can inspect specific files to confirm the reserved CPU and memory for system and user processes.
The RayCluster created in an earlier step creates containers requesting a total of 2 CPUs.
Based on Ray's default calculation of system resources (`min(3.0, max(1.0, 0.05 * num_cores_on_the_system))`),
we should expect 1 CPU for system processes. However, since CPU is a compressible resource, cgroups v2 expresses
CPU resources using weights rather than core units, with a total weight of 10000. If Ray has
2 CPUs and reserves 1 CPU for system processes, expect a CPU weight of 5000 for the system processes.

```bash
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ cat /sys/fs/cgroup/ray-node*/system/cpu.weight
5000
```

## Verify cgroup hierarchy for system processes

Verify the list of processes under the `system` cgroup hierarchy by inspecting the `cgroup.procs` file.
The example below shows that the `gcs_server` process is correctly placed in the system cgroup:

```bash
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ cat /sys/fs/cgroup/ray-node*/system/leaf/cgroup.procs
26
99
100
101
686
214
215
216
217
218
219
220
221
222
223
687
729
731

$ ps 26
PID TTY      STAT   TIME COMMAND
    26 ?        Sl     1:11 /home/ray/anaconda3/lib/python3.10/site-packages/ray/core/src/ray/gcs/gcs_server
```


## Verify cgroup hierarchy for user processes

Verify no user processes:
```bash
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ cat /sys/fs/cgroup/ray-node*/user/workers/cgroup.procs
```

Run a simple Ray job on your Ray cluster:
```bash
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ ray job submit --address http://localhost:8265 --no-wait -- python -c "import ray; import time; ray.init(); time.sleep(100)"
Job submission server address: http://10.108.2.10:8265

-------------------------------------------------------
Job 'raysubmit_zuuc7Uq6KEymnR9P' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_zuuc7Uq6KEymnR9P
  Query the status of the job:
    ray job status raysubmit_zuuc7Uq6KEymnR9P
  Request the job to be stopped:
    ray job stop raysubmit_zuuc7Uq6KEymnR9P
```

Observe the new processes:
```bash
(base) ray@raycluster-resource-isolation-head-p2xqx:~$ cat /sys/fs/cgroup/ray-node*/user/workers/cgroup.procs
95794
95795
96093

(base) ray@raycluster-resource-isolation-head-p2xqx:~$ ps 95795
    PID TTY      STAT   TIME COMMAND
  95795 ?        Sl     0:00 python -c import ray; import time; ray.init(); time.sleep(100)
```

## Configuring system reserved CPU and memory

You can configure system reserved resources for CPU and memory by setting flags `--system-reserved-cpu` and `--system-reserved-memory` respectively.
See [this KubeRay example](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster-resource-isolation-with-overrides.gke.yaml) for how to configure these flags in RayCluster.
