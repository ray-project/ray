---
myst:
  html_meta:
    description: "Guide to TPU subslicing, dynamic slicing, and SubslicePlacementGroups with Ray on GKE."
---

(kuberay-tpu-subslicing)=

# TPU subslicing and dynamic slicing on GKE

TPU slice topology dictates the kind of workload you can run on your cluster. Use subslicing, dynamic slicing, and superslicing to change your topology on the fly for more flexibility when you schedule, parallelize, and isolate your workloads.

This guide covers three primary methods for managing TPU topologies with Ray on Google Kubernetes Engine (GKE):

1. **Dynamic Slicing**: A GKE provisioning mode that couples TPU provisioning with RayCluster creation, replacing the default "all-or-nothing" provisioning.
2. **RayCluster Subslicing**: A KubeRay feature for provisioning multiple isolated RayClusters on a single, pre-deployed TPU nodepool.
3. **Ray's {class}`~ray.util.tpu.SubslicePlacementGroup`**: A Ray Core API for configuring a Ray job to run on a subslice of a provisioned worker group within a TPU-enabled Ray cluster.

---

## Comparison: Which option is right for me?

Use the following table to choose the best option for your workload and infrastructure setup.

| Feature / Dimension | Dynamic Slicing (GKE) | RayCluster Subslicing (KubeRay) | `SubslicePlacementGroup` (Ray Core) |
| :--- | :--- | :--- | :--- |
| **Target Audience** | Platform Admins | Platform Admins / Users | Machine Learning Engineers (MLEs) |
| **Control Interface** | Kubernetes (Kueue/Provisioning Requests) | KubeRay YAML (`RayCluster` CR) | Python API |
| **Provisioning** | **Dynamic**: Provisions GKE nodes on-demand. | **Static**: Uses a pre-provisioned GKE nodepool. | **Static**: Uses already-provisioned TPU nodes in a Ray cluster. |
| **TPU Generations** | TPU v7x and later | TPU v6e and earlier | All TPU generations (v4, v5e, v5p, v6e, etc.) |
| **Minimum version** | GKE 1.36.0-gke.3712000 or later | KubeRay TPU webhook 1.4.0 or later, or GKE 1.36.3-gke.1519000 or later | Ray 2.57 or later |
| **Failure Domain** | **Isolated**: Each dynamically provisioned slice is isolated. | **Shared**: RayClusters on the same nodepool share a failure domain. | **Shared**: Jobs on the same Ray cluster share a failure domain. |
| **Setup Complexity** | High (Requires Kueue and Provisioning Requests setup) | Low (Requires adding annotations to `RayCluster` YAML) | Low (Standard Ray Placement Group API) |
| **Topology Support** | Any valid topology supported by reservation. | Limited to shapes selectable via GCE topology node labels. | Any valid subslice topology permitted by the parent slice. |

---

## Dynamic slicing for RayClusters

*Dynamic slicing* is GKE's most flexible option to provision TPUs.

### Why use dynamic slicing?

* **Avoids "All-or-Nothing" Blocking**: Without dynamic slicing, nodepool creation is blocked if not all hosts/machines in the requested slice are healthy. With dynamic slicing, you can create nodepools for your entire capacity and let the system reconcile unhealthy nodes.
* **Flexible Topology**: You do not need to create rigid nodepools that exactly match your workload topologies. Slices are created dynamically during workload placement.
* **Failure Isolation**: Each dynamic slice has an isolated failure domain.

### Constraints
* Requires using Kueue, the Slice Controller, and Provisioning Requests to deploy your RayCluster.
* Requires [TPU All Capacity Reservations](https://docs.cloud.google.com/tpu/docs/view-all-capacity-topology-tpus).
* Supported on TPU v7x (or later).

### Example configuration

To use dynamic slicing, follow the [GKE Dynamic Slicing Guide](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/use-gke-dynamic-slicing#kueue-dynamic-slicing) to set up Kueue and TAS. Instead of a `JobSet`, deploy a `RayCluster` configured to target the Kueue local queue.

Here is an example `RayCluster` targeting a `4x12x16` TPU topology (768 chips):

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-jax-tpu
  labels:
    # Target the Kueue LocalQueue
    kueue.x-k8s.io/queue-name: lq
spec:
  rayVersion: '2.56.0'
  # Suspend the cluster initially. Kueue will unsuspend it once capacity is provisioned.
  suspend: true
  # Kueue manages the lifecycle; do not let KubeRay autoscale workers.
  enableInTreeAutoscaling: false
  
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:nightly
          resources:
            requests:
              cpu: "2"
              memory: "8Gi"
              
  workerGroupSpecs:
  - groupName: jax-tpu-workers
    # Multi-Host TPU Slice Representation:
    # A 4x12x16 topology has 768 chips. Since tpu7x has 4 chips per VM host,
    # we need 192 VMs (Hosts). In KubeRay, this is 1 replica of 192 hosts.
    replicas: 1
    numOfHosts: 192
    minReplicas: 1
    maxReplicas: 1
    rayStartParams: {}
    template:
      metadata:
        annotations:
          # Request the specific GKE TPU topology
          cloud.google.com/gke-tpu-slice-topology: 4x12x16
        labels:
          ray.io/node-type: worker
      spec:
        tolerations:
        - key: "google.com/tpu"
          operator: "Equal"
          value: "present"
          effect: "NoSchedule"
        nodeSelector:
          cloud.google.com/gke-tpu-accelerator: tpu7x
        containers:
        - name: ray-worker
          image: rayproject/ray:nightly-tpu
          resources:
            limits:
              google.com/tpu: 4
            requests:
              google.com/tpu: 4
```

Once provisioned, you can connect to the RayCluster and run jobs using `RayTrain` or JAX.

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

---

## Ray's SubslicePlacementGroup

Ray's {class}`~ray.util.tpu.SubslicePlacementGroup` is a runtime API for partitioning TPU resources *within* a single, running Ray cluster.

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
from ray.util.tpu import subslice_placement_group, dispatch

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

# Launch tasks on all hosts in the subslice using dispatch.
# dispatch automatically waits for the placement group to be ready.
results = ray.get(dispatch(train, tpu_slice=sg))
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
