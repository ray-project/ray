---
myst:
  html_meta:
    description: "Guide to TPU subslicing and SubslicePlacementGroups with Ray on GKE."
---

(kuberay-tpu-subslicing)=

# TPU subslicing on GKE

TPU slice topology dictates the kind of workload you can run on your cluster. Use subslicing to change your topology on the fly for more flexibility when you schedule, parallelize, and isolate your workloads.

This guide covers two primary methods for managing TPU topologies with Ray on Google Kubernetes Engine (GKE):

1. **Ray {class}`~ray.util.tpu.SubslicePlacementGroup`**: A Ray Core API for configuring a Ray job to run on a subslice of a provisioned worker group within a TPU-enabled Ray cluster.
2. **RayCluster Subslicing**: A KubeRay feature for provisioning multiple isolated RayClusters on a single, pre-deployed TPU nodepool.

---

## Comparison: Which option is right for me?

Use the following table to choose the best option for your workload and infrastructure setup.

| Feature / Dimension | `SubslicePlacementGroup` (Ray Core) | RayCluster Subslicing (KubeRay) |
| :--- | :--- | :--- |
| **Target Audience** | Machine Learning Engineers / Researchers | Platform Admins / Users |
| **Control Interface** | Python API | KubeRay YAML (`RayCluster` CR) |
| **Provisioning** | **Static**: Uses already-provisioned TPU nodes in a Ray cluster. | **Static**: Uses a pre-provisioned GKE nodepool. |
| **TPU Generations** | All TPU generations (v4, v5e, v5p, v6e, etc.) | TPU v6e and earlier |
| **Minimum version** | Ray 2.57 or later | KubeRay TPU webhook 1.4.0 or later, or GKE 1.36.3-gke.1519000 or later |
| **Failure Domain** | **Shared**: Jobs on the same Ray cluster share a failure domain. | **Shared**: RayClusters on the same nodepool share a failure domain. |
| **Setup Complexity** | Low (Standard Ray Placement Group API) | Low (Requires adding annotations to `RayCluster` YAML) |
| **Topology Support** | Any valid subslice topology permitted by the parent slice. | Limited to shapes selectable via GCE topology node labels. |

---

## Ray SubslicePlacementGroup

The Ray {class}`~ray.util.tpu.SubslicePlacementGroup` is a runtime API for partitioning TPU resources *within* a single, running Ray cluster.

This is the most flexible option for sharing and reusing TPU resources, queuing jobs in Ray, and adjusting resource usage without administrative intervention.

### Benefits
* Simple, pythonic configuration at runtime.
* Shares and queues TPU resources dynamically within a Ray cluster.
* Supported on all TPU generations (v4, v5e, v5p, v6e).
* Supports all subslice shapes permitted by the parent slice.

### Drawbacks
* **No Provisioning Integration**: Does not trigger GKE node provisioning. The Ray cluster must already have the physical TPU nodes running.
* **Shared Failure Domain**: All subsliced jobs share the failure domain of their parent GKE nodepool.

### Note on TPU workers and hosts
In TPU generations such as v6e, each VM host contains 4 TPU chips. A 16-chip slice with a `4x4` topology consists of 4 VM hosts. In KubeRay, this is represented as 4 hosts, each running one Ray worker Pod. When using `SubslicePlacementGroup`, you reserve subsets of these hosts.

### The API

```python
from ray.util.tpu import subslice_placement_group, SubslicePlacementGroup

sg: SubslicePlacementGroup = subslice_placement_group(
    subslice_topology="2x4",      # The shape you want to run on
    accelerator_version="v6e",    # TPU generation (e.g., "v4", "v5e", "v6e")
    chips_per_vm=4,               # Optional; helps resolve ambiguous topologies
    subslice_index=None,          # None = auto-select an idle subslice; or specify index (0, 1, etc.)
)
```

#### Key properties of `SubslicePlacementGroup`

| Property | Description | Example Value |
| :--- | :--- | :--- |
| `sg.placement_group` | The Ray `PlacementGroup` handle to pass to tasks/actors. | `PlacementGroup(id=...)` |
| `sg.num_hosts` | Number of VM hosts (Ray workers) in this subslice. | `2` |
| `sg.subslice_index` | The index of the allocated subslice within the parent slice. | `0` or `1` |

For the full list of properties, see the {class}`~ray.util.tpu.SubslicePlacementGroup` API reference.

### Sample usage

Below is a complete example of reserving a `2x4` subslice (2 hosts, 8 chips) out of a `4x4` v6e slice (4 hosts, 16 chips) and running a JAX task.

```python
import ray
from ray.util.tpu import subslice_placement_group, run_on_slice

ray.init()

# Reserve 2 hosts (8 chips) out of a 4-host (16 chips) 4x4 v6e slice.
# The two hosts are guaranteed to be physically adjacent on the ICI mesh.
sg = subslice_placement_group(
    subslice_topology="2x4",
    accelerator_version="v6e",
    chips_per_vm=4,
)

@ray.remote
def train():
    import jax
    # Each task runs on a full host and sees 4 chips
    return jax.device_count()

# Launch tasks on all hosts in the subslice using run_on_slice.
# run_on_slice automatically waits for the placement group to be ready.
results = ray.get(run_on_slice(train, tpu_slice=sg))
print(results)  # Output: [4, 4]

sg.shutdown()
ray.shutdown()
```

### Running multiple jobs simultaneously

To request two independent subslices on the same physical slice simultaneously, you can request specific subslices by setting `subslice_index`:

```python
# Reserve the first half of the slice
sg0 = subslice_placement_group("2x4", "v6e", chips_per_vm=4, subslice_index=0)

# Reserve the second half of the slice
sg1 = subslice_placement_group("2x4", "v6e", chips_per_vm=4, subslice_index=1)

ray.get([sg0.placement_group.ready(), sg1.placement_group.ready()])

# sg0 and sg1 are now running on disjoint, ICI-adjacent sets of workers.
# You can now schedule separate Ray tasks/jobs on sg0.placement_group and sg1.placement_group.
```

### How SubslicePlacementGroup works

TPU slices are connected by high-bandwidth ICI links. To ensure optimal performance, Ray must allocate physically contiguous workers.

On the **first call** for a given topology, Ray temporarily reserves the parent slice to query the physical coordinates of the TPU chips (via `libtpu`). It computes which workers are physically adjacent to form valid subslices and caches this mapping in Ray's internal KV store.

On **subsequent calls**, Ray reuses the cached mapping to immediately create the placement group for the requested subslice without querying coordinates again.

---

## RayCluster subslicing

With *RayCluster subslicing*, you create multiple independent RayClusters (or RayJobs) on a subset of a pre-provisioned TPU slice by adding an annotation to the TPU worker group.

### Benefits
* Simple YAML configuration.
* Works on existing TPU slices without re-provisioning the underlying GKE nodepool.

### Drawbacks
* Requires a pre-provisioned nodepool.
* Only supported on TPU v6e and below.
* Requires GKE 1.36 or later.
* Only subslices selectable with GCE topology node labels are supported.
* Subsliced RayClusters share a failure domain (if one node in the parent pool fails, it may affect multiple clusters).

### Example configuration

To subslice, add the `cloud.google.com/gke-tpu-slice-topology` annotation to your worker group spec, setting it to the desired subslice shape. The `numOfHosts` parameter must match the size of this smaller shape.

In the following example, you run a `2x4` subslice (8 chips, 2 hosts) on a pre-provisioned `4x4` parent slice (16 chips, 4 hosts):

```yaml
workerGroupSpecs:
- groupName: tpu-group
  replicas: 1
  maxReplicas: 1
  # numOfHosts must match the subslice shape (2x4 v6e has 8 chips / 4 chips per host = 2 hosts)
  numOfHosts: 2
  rayStartParams: {}
  template:
    metadata:
      annotations:
        cloud.google.com/gke-tpu-slice-topology: 2x4  # Request the smaller subslice shape
    spec:
      nodeSelector:
        cloud.google.com/gke-tpu-accelerator: tpu-v6e-slice
        cloud.google.com/gke-tpu-topology: 4x4  # Pin to the pre-provisioned parent slice topology
      containers:
      - name: ray-worker
        image: rayproject/ray:nightly-tpu
        resources:
          limits:
            google.com/tpu: 4
          requests:
            google.com/tpu: 4
```

Within this RayCluster, only the `2x4` subslice will be visible and usable. You can deploy another RayCluster with similar config to consume the remaining `2x4` subslice on the same nodepool.
