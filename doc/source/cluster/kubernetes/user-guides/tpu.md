(kuberay-tpu)=

# Use TPUs with KubeRay

This document provides tips on TPU usage with KubeRay.

TPUs are available on Google Kubernetes Engine (GKE). To use TPUs with Kubernetes, configure
both the Kubernetes setup and add additional values to the RayCluster CR configuration.
Configure TPUs on GKE by referencing the {ref}`kuberay-gke-tpu-cluster-setup`.

## About TPUs

TPUs are custom-designed AI accelerators, which are optimized for training and inference of large AI models. A TPU host is a VM that runs on a physical computer connected to TPU hardware. TPU workloads can run on one or multiple hosts. A TPU Pod slice is a collection of chips all physically colocated and connected by high-speed inter chip interconnects (ICI). Single-host TPU Pod slices contain independent TPU VM hosts and communicate over the Data Center Network (DCN) rather than ICI interconnects. Multi-host TPU Pod slices contain two or more interconnected TPU VM hosts. In GKE, multi-host TPU Pod slices run on their own node pools and GKE scales them atomically by node pools, rather than individual nodes. Ray enables single-host and multi-host TPU Pod slices to be scaled seamlessly to multiple slices, enabling greater parallelism to support larger workloads.

## Quickstart: Serve a Stable Diffusion model on GKE with TPUs

After setting up a GKE cluster with TPUs and the Ray TPU initialization webhook, run a workload on Ray with TPUs. {ref}`Serve a Stable Diffusion model on GKE with TPUs <kuberay-tpu-stable-diffusion-example>` shows how to serve a model with KubeRay on single-host TPUs.

## Configuring Ray Pods for TPU usage

Using any TPU accelerator requires specifying `google.com/tpu` resource `limits` and `requests` in the container fields of your `RayCluster`'s
`workerGroupSpecs`. This resource specifies the number of TPU chips for GKE to allocate each Pod. KubeRay v1.1.0 adds a `numOfHosts`
field to the RayCluster custom resource, specifying the number of TPU hosts to create per worker group replica. For multi-host worker groups,
Ray treats replicas as Pod slices rather than individual workers, and creates `numOfHosts` worker nodes per replica.
Additionally, GKE uses `gke-tpu` node selectors to schedule TPU Pods on the node matching the desired TPU accelerator and topology.

Below is a config snippet for a RayCluster worker group with 2 Ray TPU worker Pods. Ray schedules each worker on its own GKE v4 TPU node belonging to the same TPU Pod slice.

```
   groupName: tpu-group
   replicas: 1
   minReplicas: 0
   maxReplicas: 1
   numOfHosts: 2
   ...
   template:
       spec:
        ...
        containers:
         - name: ray-worker
           image: rayproject/ray:2.9.0-py310
           ...
           resources:
             google.com/tpu: "4" # Required to use TPUs.
             ...
           limits:
             google.com/tpu: "4" # The resources and limits value is expected to be equal.
             ...
        nodeSelector:
            cloud.google.com/gke-tpu-accelerator: tpu-v4-podslice
            cloud.google.com/gke-tpu-topology: 2x2x2
            ...
```

## TPU workload scheduling

After Ray deploys a Ray Pod with TPU resources, the Ray Pod can execute tasks and actors annotated with TPU requests.
Ray supports TPUs as a [custom resource](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html#custom-resources).
Tasks or actors request TPUs using the decorator `@ray.remote(resources={"TPU": NUM_TPUS})`.

### TPU default labels

When running on Google Cloud TPUs with KubeRay, Ray automatically detects and adds the following labels to describe the underlying compute. These are critical for scheduling distributed workloads that must span an entire TPU "slice" (a set of interconnected hosts).

* `ray.io/accelerator-type`: The type of TPU accelerator, such as TPU-V6E.
* `ray.io/tpu-slice-name`: The name of the TPU Pod or slice. Ray uses this to ensure all workers of a job land on the *same* slice.
* `ray.io/tpu-worker-id`: The integer worker ID within the slice.
* `ray.io/tpu-topology`: The physical topology of the slice.
* `ray.io/tpu-pod-type`: The TPU pod type, which defines the size and TPU generation such as `v4-8` or `v5p-16`.

You can use these labels to schedule a `placement_group` that requests an entire TPU slice. For example, to request all TPU devices on a `v6e-16` slice:

```py
# Request 4 bundles, one for each TPU VM in the v6e-16 slice.
pg = placement_group(
    [{"TPU": 4}] * 4,
    strategy="SPREAD",
    bundle_label_selector=[{
        "ray.io/tpu-pod-type": "v6e-16"
    }] * 4
)
ray.get(pg.ready())
```

### TPU scheduling utility library

The `ray.util.tpu` package introduces a number of TPU utilities related to scheduling that streamline the process of utilizing TPUs in multi-host and/or multi-slice configurations. These utilities utilize default Ray node labels that are set when running on Google Kubernetes Engine (GKE) with KubeRay.

Previously, when simply requesting TPU using something like `resources={"TPU": 4}` over multiple tasks or actors, it was never guaranteed that the Ray nodes scheduled were part of the same slice or even the same TPU generation. To address the latter, Ray introduced the {ref}`label selector API <kuberay-label-scheduling>` and default labels (like `ray.io/accelerator-type`) to describe the underlying compute.

Going even further, the new TPU utility library leverages the default node labels and the label selector API to abstract away the complexities of TPU scheduling, particularly for multi-host slices. The core abstraction is the `SlicePlacementGroup`.

#### `SlicePlacementGroup`

The `SlicePlacementGroup` class provides a high-level interface to reserve one or more complete, available TPU slices and create a Ray [Placement Group](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) constrained to those slices. This guarantees that all bundles within the placement group (and thus the tasks/actors scheduled on them) run on workers belonging to the reserved physical TPU slices.

**How it works:**

1.  **Reservation:** When you create a `SlicePlacementGroup`, it first interacts with the Ray scheduler to find an available TPU slice matching your specified `topology` and `accelerator_version`. It does this by temporarily reserving the "head" TPU node (worker ID 0) of a slice using a small, internal placement group.
2.  **Slice Identification:** From the reserved head node, it retrieves the unique slice name (using the `ray.io/tpu-slice-name` default label). This label is set using an environment variable injected by a GKE webhook. The GKE webhook also ensures that KubeRay pods with `numOfHosts > 1` are scheduled with affinity on the same GKE nodepool, which is 1:1 with a TPU multi-host slice.
3.  **Main Placement Group Creation:** It then creates the main placement group you requested. This group contains bundles representing each host (VM) in the slice(s). For each slice, it uses `bundle_label_selector` to target the specific `ray.io/tpu-slice-name` identified in the previous step. This ensures all bundles for a given slice land on workers within that exact slice.
4.  **Handle:** It returns a `SlicePlacementGroup` handle which exposes the underlying Ray `PlacementGroup` object (`.placement_group`) along with useful properties like the number of workers (`.num_workers`) and chips per host (`.chips_per_host`).

**Usage:**

You typically create a `SlicePlacementGroup` using the `slice_placement_group` function:

```python
import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tpu import slice_placement_group

# Reserve two v6e TPU slices, each with a 4x4 topology (16 chips each).
# This topology typically has 4 VM workers, each with 4 chips.
slice_handle = slice_placement_group(topology="4x4", accelerator_version="v6e", num_slices=2)
slice_pg = slice_handle.placement_group

print("Waiting for placement group to be ready...")
ray.get(slice_pg.ready(), timeout=600) # Increased timeout for potential scaling
print("Placement group ready.")

@ray.remote(num_cpus=0, resources={"TPU": 4})
def spmd_task(world_size, rank):
    pod_name = ray.util.tpu.get_current_pod_name()
    chips_on_node = ray.util.tpu.get_num_tpu_chips_on_node()
    print(f"Worker Rank {rank}/{world_size}: Running on slice '{pod_name}' with {chips_on_node} chips.")
    return rank

# Launch one task per VM in the reserved slices. The num_workers field describes the total
# number of VMs across all slices in the SlicePlacementGroup.
tasks = [
    spmd_task.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=slice_pg,
        )
    ).remote(world_size=slice_handle.num_workers, rank=i)
    for i in range(slice_handle.num_workers)
]

results = ray.get(tasks)
print(f"Task results: {results}")
```

#### TPU Pod Information Utilities

These functions provide information about the TPU pod that a given worker is a part of. They return None if the worker is not running on a TPU.


* `ray.util.tpu.get_current_pod_name() -> Optional[str]`

Returns the name of the TPU pod that the worker is a part of.

* `ray.util.tpu.get_current_pod_worker_count() -> Optional[int]`

Counts the number of workers associated with the TPU pod that the worker belongs to.

* `ray.util.tpu.get_num_tpu_chips_on_node() -> int`

Returns the total number of TPU chips on the current node. Returns 0 if none are found.


## Multi-Host TPU autoscaling

Multi-host TPU autoscaling is supported in Kuberay versions 1.1.0 or later and Ray versions 2.32.0 or later. Ray multi-host TPU worker groups are worker groups, which specify "google.com/tpu" Kubernetes container limits or requests and have `numOfHosts` greater than 1. Ray treats each replica of a Ray multi-host TPU worker group as a TPU Pod slice and scales them atomically. When scaling up, multi-host worker groups create `numOfHosts` Ray workers per replica. Likewise, Ray scales down multi-host worker group replicas by `numOfHosts` workers. When Ray schedules a deletion of a single Ray worker in a multi-host TPU worker group, it terminates the entire replica to which the worker belongs. When scheduling TPU workloads on multi-host worker groups, ensure that Ray tasks or actors run on every TPU VM host in a worker group replica to avoid Ray from scaling down idle TPU workers.

Further reference and discussion
--------------------------------
* See [TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus) for more details on using TPUs.
* [TPU availability](https://cloud.google.com/tpu/docs/regions-zones)
* [TPU System Architecture](https://cloud.google.com/tpu/docs/system-architecture-tpu-vm)
