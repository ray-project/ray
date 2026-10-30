---
myst:
  html_meta:
    description: "Learn how to bind a Ray GPU actor's CPU threads and host memory to its GPU's NUMA node, since Ray assigns GPUs but doesn't place actors NUMA-aware by default."
---

(numa-aware-placement)=
# NUMA-aware placement for GPU actors

Ray assigns GPUs to actors by setting `CUDA_VISIBLE_DEVICES` in the worker process, as described in {ref}`accelerator support <accelerator-support>`, but doesn't automatically bind their CPU threads or host memory to the GPU's NUMA node. This guide shows three ways to set that placement yourself.

```{note}
These are workarounds until Ray supports NUMA-aware placement natively.
```

## Prerequisites

- Linux hosts with NVIDIA GPUs and multiple NUMA nodes
- `nvidia-smi`, access to PCI and NUMA information in `/sys`, and `numactl` for the approaches that use it

Check your host's topology before starting:

```bash
numactl --hardware
nvidia-smi topo -m
```

Use the reported NUMA node IDs and GPU affinity throughout this guide. A socket can contain several NUMA nodes, so use NUMA node IDs rather than socket numbers.

## Choose an approach

| Approach | Use it when | What you change |
| --- | --- | --- |
| Pin each actor's CPU threads | Each actor uses one GPU | Actor initialization |
| Bind each worker before it starts | You want to choose a local GPU set and bind memory allocation | `ray start` flags and actor options |
| Run one Ray node per NUMA domain | You want every actor placed without changing code, or actors often pass large objects through the object store | Cluster configuration |

Approaches 1 and 2 bind each actor individually inside a Ray node that spans the whole host, so two things stay outside the actor's control. Approach 3 avoids both by giving each NUMA domain its own Ray node.

- **Object store.** Actors still share the Ray node's object store, so an actor may access objects stored in memory on another NUMA node. Binding the actor doesn't move those shared objects. With approach 3, each Ray node's object store stays within its NUMA domain. Objects passed between Ray nodes require a transfer.
- **Thread affinity.** Both approaches place threads by setting the worker's CPU affinity mask, which a process can change again. Libraries such as OpenMP and MKL runtimes may override thread affinity during initialization or execution. Disable conflicting settings such as `OMP_PROC_BIND`, `OMP_PLACES`, `GOMP_CPU_AFFINITY`, and `KMP_AFFINITY`, and verify thread placement after initialization. Approach 3 restricts CPUs with a cgroup, which the kernel enforces, so libraries can't escape it.

## Pin each actor's CPU threads

Call the helper below at the start of your actor's `__init__`, before initializing thread pools or allocating large buffers. It finds the assigned GPU's NUMA node and pins the actor's threads to that node's CPUs. In containers, confirm that the GPU ID returned by Ray identifies the same physical GPU in `nvidia-smi`.

```python
import os
import subprocess
from pathlib import Path
import ray

def pin_to_gpu_numa_node():
    """Pin this actor's existing threads to its GPU's NUMA node."""
    gpu_ids = ray.get_gpu_ids()
    if len(gpu_ids) != 1:
        raise RuntimeError("Expected one assigned GPU.")
    gpu = gpu_ids[0]
    bus = subprocess.check_output(
        ["nvidia-smi", f"--id={gpu}",
         "--query-gpu=pci.bus_id", "--format=csv,noheader"],
        text=True,
    ).strip().lower()[-12:]

    node = int(Path(f"/sys/bus/pci/devices/{bus}/numa_node").read_text())
    if node < 0:
        raise RuntimeError("GPU NUMA affinity is unavailable.")

    cpus = set()
    cpu_list = Path(f"/sys/devices/system/node/node{node}/cpulist").read_text()
    for span in cpu_list.strip().split(","):
        bounds = span.split("-")
        cpus.update(range(int(bounds[0]), int(bounds[-1]) + 1))

    cpus &= os.sched_getaffinity(0)
    if not cpus:
        raise RuntimeError("No allowed CPUs are local to the GPU.")

    for tid in os.listdir("/proc/self/task"):
        try:
            os.sched_setaffinity(int(tid), cpus)
        except ProcessLookupError:
            pass  # The thread exited before it could be pinned.
```

For example, the call belongs before your actor's existing initialization:

```python
@ray.remote(num_gpus=1)
class MyActor:
    def __init__(self):
        pin_to_gpu_numa_node()
        self.model = load_model()  # Allocate after pinning.
```

CPU pinning helps new allocations stay local under the default memory policy. It doesn't bind memory or move existing allocations. Use the next approach if you need memory binding.

Use this helper for actors. Reused task workers need additional handling to restore their affinity between tasks.

## Bind each worker before it starts

Choose the NUMA node and GPUs each actor should use. Then configure Ray to start the actor's worker under `numactl`, binding its CPU threads and new memory allocations to that node. For illustration, the following examples assume GPUs 0-3 belong to NUMA node 0 and GPUs 4-7 to NUMA node 1.

1. Advertise GPU capacity as a custom resource for each NUMA node, and set `--num-gpus=0`.

   ```bash
   ray start --head --num-gpus=0 --resources='{"numa0_gpu": 4, "numa1_gpu": 4}'
   ```

1. Request the matching resource and configure the actor's `runtime_env` as shown below.

   ```python
   def numa_env(numa_node, gpu_ids):
       return {
           "py_executable": (
               f"numactl --cpunodebind={numa_node} "
               f"--membind={numa_node} python"
           ),
           "env_vars": {
               "CUDA_VISIBLE_DEVICES": ",".join(map(str, gpu_ids)),
               "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES": "1",
           },
       }

   @ray.remote(num_gpus=0)
   class MyActor:
       ...

   # Example: GPUs 0-3 are local to NUMA node 0;
   # GPUs 4-7 are local to NUMA node 1.

   # Two actors on NUMA node 0, two GPUs each.
   a = MyActor.options(
       resources={"numa0_gpu": 2},
       runtime_env=numa_env(0, [0, 1]),
   ).remote()
   b = MyActor.options(
       resources={"numa0_gpu": 2},
       runtime_env=numa_env(0, [2, 3]),
   ).remote()

   # One actor on NUMA node 1, four GPUs.
   c = MyActor.options(
       resources={"numa1_gpu": 4},
       runtime_env=numa_env(1, [4, 5, 6, 7]),
   ).remote()
   ```

```{warning}
Ray counts the per-NUMA-node resource but doesn't track which GPU IDs are in use. Make sure no two running actors receive the same IDs, and don't run actors that request `num_gpus` on the same node. These assignments aren't recorded as Ray GPU resource allocations. Configure autoscaling around the custom resources.
```

## Run one Ray node per NUMA domain

Run one Ray node per NUMA node on each host. Restrict each Ray node to that NUMA node's CPUs, memory, and GPUs.

### Kubernetes with KubeRay

1. In the kubelet configuration (`KubeletConfiguration`) of each node that runs Ray GPU worker pods, set the following so that the kubelet gives each pod the CPUs, memory, and GPUs of a NUMA node:

   ```yaml
   cpuManagerPolicy: static
   memoryManagerPolicy: Static
   topologyManagerScope: pod
   topologyManagerPolicy: single-numa-node
   ```

   Configure CPU reservations and `reservedMemory` as required by CPU Manager and Memory Manager. The GPU device plugin must report NUMA topology.

1. Update your existing worker groups so each worker pod fits one NUMA node. Set matching CPU and memory requests and limits for every container so the pod has Guaranteed QoS. Request whole physical cores according to the host's SMT configuration.

   For illustration, the fragment below shows where these resource settings go in a RayCluster.

   ```yaml
   workerGroupSpecs:
     - groupName: numa-workers
       template:
         spec:
           containers:
             - name: ray-worker
               resources:
                 requests: { cpu: "94", memory: "900Gi", nvidia.com/gpu: "4" }
                 limits:   { cpu: "94", memory: "900Gi", nvidia.com/gpu: "4" }
   ```

```{note}
Kubernetes 1.37 introduces the standard DRA device attribute `resource.kubernetes.io/numaNode`. With compatible drivers, you can use it to request devices on the same NUMA node. CPU and memory alignment still require the appropriate kubelet policies. See [Dynamic Resource Allocation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/) for details.
```

### VMs or bare metal

Start each Ray node with its own CPU, memory, and GPU allocation. Set its resource capacities and memory sizes to match that allocation.

For example, on a p5.48xlarge instance, which has two sockets and eight GPUs:

```bash
sudo systemd-run --scope --uid=$USER -p AllowedCPUs=0-47,96-143 -p AllowedMemoryNodes=0 \
  env CUDA_VISIBLE_DEVICES=0,1,2,3 PATH=$PATH HOME=$HOME \
  ray start --head --num-cpus=96 --num-gpus=4 \
  --object-store-memory=200000000000 --memory=800000000000

sudo systemd-run --scope --uid=$USER -p AllowedCPUs=48-95,144-191 -p AllowedMemoryNodes=1 \
  env CUDA_VISIBLE_DEVICES=4,5,6,7 PATH=$PATH HOME=$HOME \
  ray start --address=127.0.0.1:6379 --num-cpus=96 --num-gpus=4 \
  --object-store-memory=200000000000 --memory=800000000000
```

To adapt this to your host, replace:

- `AllowedCPUs` and `AllowedMemoryNodes` with each NUMA node's CPU list and node ID from `numactl -H`, and `--num-cpus` to that list's count.
- `CUDA_VISIBLE_DEVICES` and `--num-gpus` with that NUMA node's GPUs from `nvidia-smi topo -m`.
- `--object-store-memory` with 30% of the NUMA node's memory, up to 200 GB, and `--memory` with the rest, both in bytes. If you leave them out, each node sizes itself as if it had the whole host.

### Scheduling and data-transfer implications

- An actor can't request more GPUs than a single Ray node exposes.
- Objects in separate Ray nodes' object stores require transfer rather than access through one shared object store, even when those Ray nodes share a host.

## Verify placement

Find an actor's process ID with `ray list actors --detail`, then check its threads and memory. On Kubernetes, run these commands inside the worker pod.

Each thread's allowed CPUs should fall within the intended NUMA node:

```text
$ grep Cpus_allowed_list /proc/<pid>/task/*/status
/proc/29025/task/29025/status:Cpus_allowed_list:  48-95,144-191
/proc/29025/task/29483/status:Cpus_allowed_list:  48-95,144-191
...
```

After your workload allocates and uses its buffers, check that most of the new memory is on the intended NUMA node. Existing allocations and shared objects may remain elsewhere:

```text
$ numastat -p <pid>
                    Node 0      Node 1       Total
Private               8.52     4210.77     4219.29
Total                10.63     4623.21     4633.84
```

A few MB on the other node is normal.

## Benchmark

This benchmark measured CPU-to-GPU copy throughput on a p5.48xlarge with eight H100 GPUs and two CPU sockets, each with 96 vCPUs. GPUs 0-3 were local to socket 0, and GPUs 4-7 to socket 1.

Eight actors, one per GPU, each copied a 2 GiB pinned host buffer to its GPU, then copied the data back. All actors copied concurrently in each direction, with 10 copies per direction per run. The table reports mean aggregate throughput across all eight actors over five runs, in decimal GB/s.

| Configuration | Host to device (GB/s) | Device to host (GB/s) |
| --- | --- | --- |
| Baseline: no explicit NUMA placement | 114.7 | 23.7 |
| Approach 1: pin actor CPU threads | 114.7 | 49.2 |
| Approach 2: bind workers at launch | 115.0 | 49.4 |
| Approach 3: one Ray node per NUMA domain | 114.9 | 49.4 |
| Worst case: buffers on the opposite socket | 91.7 | 14.5 |

NUMA-aware placement increased device-to-host throughput by about 2.1× over the baseline, while host-to-device throughput remained similar. Application-level gains depend on the workload.
