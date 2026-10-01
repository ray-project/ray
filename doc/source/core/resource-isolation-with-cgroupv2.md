---
myst:
  html_meta:
    description: "Isolate Ray system and application resources with cgroup v2 across containers, Kubernetes, GKE, and bare-metal deployments."
---

(resource-isolation)=

# Resource isolation with cgroup v2

This page describes how to use Ray's native resource isolation, which uses cgroup v2, to improve the reliability of a Ray cluster.

:::{note}
This feature is available only on Linux in Ray 2.51.0 and later. The complete memory monitoring system that uses cgroup v2 to improve system stability is available in Ray 2.56.0 and later. For more details, see {ref}`Out-of-memory prevention <ray-oom-prevention>`.
:::

:::{note}
To use resource isolation to debug out-of-memory issues, see {ref}`Debugging out of memory <troubleshooting-out-of-memory>`.
:::

## Background

A Ray cluster consists of Ray nodes, which run two types of processes:

1. System processes internal to Ray that are critical to node health.
1. Worker processes that run your code inside remote tasks and actors.

Without resource isolation, user processes can starve system processes of CPU and memory, which leads to node failure. Node failure can make your workload unstable and, in extreme cases, cause the job to fail.

As of Ray 2.51.0, Ray uses [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html) to reserve CPU and memory for its system processes, which protects them from out-of-memory (OOM) errors and CPU starvation.

## Requirements

Configuring resource isolation can take some work, depending on how you deploy and run Ray. The following requirements apply to all environments:

- Ray 2.51.0 or later.
- Linux operating system running kernel version 5.8 or later.
- An environment with cgroup v1 disabled.
- An environment with cgroup v2 enabled with read and write permissions. For more information, see {ref}`How to enable cgroup v2 <enable-cgroupv2>`.

(resource-isolation-containers)=

### Running Ray in a container

If you run Ray in a container, such as on Kubernetes, the container must have read and write access to the cgroup mount point. The following sections cover the most common ways to run Ray in a container.

#### Running in Kubernetes with privileged security context

To run privileged Pods in Kubernetes, [set the `securityContext`](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-container) in your Pod spec to privileged:

```{code-block} yaml
:emphasize-lines: 11-12

apiVersion: v1
kind: Pod
metadata:
  name: ubuntu-privileged
spec:
  containers:
  - name: ubuntu
    image: ubuntu:22.04
    command: ["/bin/bash", "-c", "--"]
    args: ["while true; do sleep 30; done;"]
    securityContext:
      privileged: true
    resources:
      requests:
        cpu: "32"
        memory: "128Gi"
```

#### Running in Google Kubernetes Engine (GKE) with writable cgroups

If running Pods in a privileged security context isn't acceptable for your use case, use writable cgroups on GKE instead. For step-by-step instructions, see the {ref}`Resource isolation with writable cgroups on GKE <resource-isolation-with-writable-cgroups>` guide. For more details, see the [GKE documentation on writable cgroups](https://cloud.google.com/kubernetes-engine/docs/how-to/writable-cgroups).

#### Running in a bare container

If you run Ray in a bare container, such as with Docker, [use a privileged container](https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities).

(resource-isolation-vm)=

(running-ray-outside-of-a-container-vm-or-baremetal)=

### Running Ray outside a container on a VM or bare metal

Running Ray directly on Linux takes more setup. Complete the following steps:

1. Create a cgroup for Ray.
1. Give the user that starts Ray read and write permissions on the cgroup.
1. Move the process that starts Ray into the cgroup.
1. Start Ray with the cgroup path.

:::{note}
The following example script is for running Ray on a single node for testing. It isn't the recommended way to run a Ray cluster in production.
:::

The following script performs these steps:

```bash
# Create the cgroup that will be managed by Ray.
sudo mkdir -p /sys/fs/cgroup/ray

# Make the current user the owner of the managed cgroup.
sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/ray

# Make the cgroup subtree writable.
sudo chmod -R u+rwx /sys/fs/cgroup/ray

# Add the current process to the managed cgroup so ray will start
# inside that cgroup.
echo $$ | sudo tee /sys/fs/cgroup/ray/cgroup.procs

# Start ray with resource isolation enabled passing the cgroup path to Ray.
ray start --enable-resource-isolation --cgroup-path=/sys/fs/cgroup/ray
```

## Usage

You can enable and configure resource isolation when you start a Ray cluster with `ray start` or when you run Ray locally with `ray.init`.

### Enable resource isolation on a Ray cluster

```bash
# Example of enabling resource isolation with default values.
ray start --enable-resource-isolation

# Example of enabling resource isolation overriding reserved resources:
# - /sys/fs/cgroup/ray is used as the base cgroup.
# - 1.5 CPU cores reserved for system processes.
# - 5GB memory reserved for system processes.
ray start --enable-resource-isolation \
    --cgroup-path=/sys/fs/cgroup/ray \
    --system-reserved-cpu=1.5 \
    --system-reserved-memory=5368709120
```


If you use the {doc}`Ray Cluster Launcher </cluster/vms/user-guides/launching-clusters/on-premises>`, add the resource isolation flags to `head_start_ray_commands` and `worker_start_ray_commands`.


### Enable resource isolation with the SDK

```python
import ray

# Example of enabling resource isolation overriding reserved resources:
# - /sys/fs/cgroup/ray is used as the base cgroup.
# - 1.5 CPU cores reserved for system processes.
# - 5GB memory reserved for system processes.
ray.init(
    enable_resource_isolation=True,
    cgroup_path="/sys/fs/cgroup/ray",
    system_reserved_cpu=1.5,
    system_reserved_memory=5368709120,
)
```

### API reference

```{list-table}
:header-rows: 1
:widths: 20 10 15 55

* - Option
  - Type
  - Default
  - Description
* - `enable-resource-isolation`
  - boolean
  - `false`
  - Enables resource isolation.
* - `cgroup-path`
  - string
  - `"/sys/fs/cgroup"`
  - The cgroup that Ray uses as its base cgroup. Setting it without `enable-resource-isolation` raises `ValueError`.
* - `system-reserved-cpu`
  - float
  - See {ref}`defaults <resource-isolation-defaults>`
  - CPU cores reserved for system processes. Setting it without `enable-resource-isolation` raises `ValueError`.
* - `system-reserved-memory`
  - integer
  - See {ref}`defaults <resource-isolation-defaults>`
  - Bytes of memory reserved for system processes. Setting it without `enable-resource-isolation` raises `ValueError`. Doesn't include `object_store_memory`. Ray guarantees that system processes have `system-reserved-memory + object-store-memory` for the system cgroup.
```

:::{note}
If you specify only some of the options, Ray uses default values for the rest. For example, you can specify only `--system-reserved-memory`.
:::

(resource-isolation-defaults)=

## Default values for CPU and memory reservations

If you enable resource isolation but don't specify `system-reserved-cpu` or `system-reserved-memory`, Ray assigns default values. Ray calculates the defaults from the following parameters:

```python
# CPU
RAY_DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION = 0.05
RAY_DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES = 1.0
RAY_DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES = 3.0

# Memory
RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION = 0.10
RAY_DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES = 0.5 * 1024**3 #500MiB
RAY_DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES = 10 * 1024**3 #10GiB
```

You can override these default parameters with environment variables.

### Calculation logic

To keep default reservations reasonable for clusters of all sizes, Ray calculates each default as follows:

1. Calculate the value as a proportion of available resources, such as `RAY_DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION * total_cpu_cores`.
1. If the value is less than the minimum, use the minimum.
1. If the value is greater than the maximum, use the maximum.

### Example

The following example shows the default values for a worker node with 32 CPU cores and 64 GB of RAM:

```bash
# Calculated default values:
#   object_store_memory = MIN(0.3 * 64GB, 200GB) = 19.2GB
#   system_reserved_memory = MIN(10GB, MAX(0.10 * 64GB, 0.5GB)) = 6.4GB
#   system_reserved_cpu = MIN(3.0, MAX(0.05 * 32, 1.0)) = 1.6
#
# The total system_reserved_memory (including object_store_memory) will be 19.2GB + 6.4GB = 25.6GB

ray start --enable-resource-isolation
```

(enable-cgroupv2)=

## How to enable cgroup v2 for resource isolation

Resource isolation requires cgroup v2 enabled and cgroup v1 disabled. Most modern Linux distributions use this configuration by default.

The most reliable way to check this configuration is to inspect the `mount` output, which should look like the following:

```console
$ mount | grep cgroup
cgroup2 on /sys/fs/cgroup type cgroup2 (rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot)
```

:::{important}
If you don't see cgroup v2, or you see both cgroup v1 and cgroup v2, disable cgroup v1 and enable cgroup v2.
:::

The recommended approach is to use a distribution that enables cgroup v2 by default. Otherwise, if your distribution uses GRUB, add `systemd.unified_cgroup_hierarchy=1` to `GRUB_CMDLINE_LINUX` in `/etc/default/grub`, then run `sudo update-grub`.

## Troubleshooting

To check that you enabled resource isolation correctly, look at the `raylet.out` log file. If the setup works, the log should contain a line with details about the cgroups that Ray created and the cgroup constraints it enabled.

The following example shows that log line:

```json
{
  "asctime": "2026-01-14 13:53:13,853",
  "levelname": "I",
  "message": "Initializing CgroupManager at base cgroup at '/sys/fs/cgroup'. Ray's cgroup hierarchy will under the node cgroup at '/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04' with [memory, cpu] controllers enabled. The system cgroup at '/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/system' will have [memory] controllers enabled with [cpu.weight=666, memory.min=25482231398] constraints. The user cgroup '/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/user' will have no controllers enabled with [cpu.weight=9334] constraints. The user cgroup will contain the [/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/user/workers, /sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/user/non-ray] cgroups.",
  "component": "raylet",
  "filename": "cgroup_manager.cc",
  "lineno": 212
}
```
