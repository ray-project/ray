---
myst:
  html_meta:
    description: "Use TPUs with KubeRay: configure Kubernetes Pods for TPUs, TPU workload scheduling and default labels, and multi-host TPU autoscaling."
---

(kuberay-tpu)=

# Use TPUs with KubeRay

This guide explains how to configure Kubernetes Pods for Cloud TPUs, schedule single-slice and multi-slice TPU workloads across SPMD and MPMD execution models, and orchestrate training, inference, and reinforcement learning (RL) on Google Kubernetes Engine (GKE).

To create a GKE cluster with TPU node pools and the KubeRay TPU initialization webhook, see {ref}`kuberay-gke-tpu-cluster-setup`.

## About TPUs and execution models

[Google Cloud TPUs](https://cloud.google.com/kubernetes-engine/docs/concepts/tpus) are custom AI accelerators for large-scale training, inference, and post-training workloads. A *TPU slice* is a group of TPU chips connected by high-speed inter-chip interconnects (ICI). Chips connect in a 2D torus on [TPU v5e](https://cloud.google.com/tpu/docs/v5e) and [v6e (Trillium)](https://cloud.google.com/tpu/docs/v6e), or a 3D torus on [TPU v4](https://cloud.google.com/tpu/docs/v4), [v5p](https://cloud.google.com/tpu/docs/v5p), and [Ironwood (`TPU7x`)](https://cloud.google.com/kubernetes-engine/docs/concepts/tpu-ironwood). A TPU slice spans one or more Kubernetes nodes (TPU VMs), and GKE provisions and scales slices as node pools:

- **Single-host TPU slices** fit on one TPU node (such as a four-chip `v6e` `2x2` slice or an eight-chip `2x4` slice). A single-host GKE node pool scales from zero to many independent TPU nodes that communicate over the data center network (DCN).
- **Multi-host TPU slices** span two or more ICI-connected TPU nodes (such as a four-node `v6e` `4x4` slice or a two-node `TPU7x` `2x2x2` slice). GKE provisions each multi-host slice as a dedicated node pool that scales atomically.
- **Multi-slice configurations** combine multiple TPU slices over DCN using MegaScale collectives to scale beyond a single ICI domain.

Ray supports both **SPMD (single program, multiple data)** workloads—where all workers run the same program across data or model shards—and **MPMD (multiple program, multiple data)** workloads—where distinct actor groups (such as reinforcement learning rollout and training actors) run different programs concurrently across devices, subslices, or slices. Depending on your framework, you can schedule workers at two granularities:

- **One process per TPU host (`{"TPU": 4}`):** One Ray task or actor runs on each TPU node and manages all local TPU devices on that host. Frameworks such as JAX, {class}`~ray.train.v2.jax.JaxTrainer`, MaxText, and the JAX `tpu-inference` backend for vLLM use this model.
- **One process per TPU device (`{"TPU": 1}`):** One Ray task or actor runs on each logical TPU device, matching the one-process-per-GPU model. Frameworks such as PyTorch with `torch_tpu`, {class}`~ray.train.torch.TorchTrainer` with the `tpu_dist` backend, the PyTorch `vllm-torchtpu` backend for vLLM, and per-device MPMD actor groups use this model.

:::{note}
On `v4`, `v5e`, `v5p`, and `v6e`, each physical TPU chip exposes one logical TPU device (`{"TPU": 4}` on a four-chip node). On [Ironwood (`TPU7x`)](https://cloud.google.com/tpu/docs/tpu7x#dual-chiplet-architecture), each physical chip contains two compute chiplets that expose two separate logical TPU devices (`8` logical devices on a four-chip `tpu7x-standard-4t` node). For host-level SPMD workloads (one process per host, such as JAX), use the default `{"TPU": 4}` resource count—`RAY_TPU_RESOURCE_PER_CHIP` isn't required. Only set `RAY_TPU_RESOURCE_PER_CHIP=2` and advertise `{"TPU": 8}` in `rayStartParams.resources` on `TPU7x` when running one process per logical TPU device (`{"TPU": 1}`).
:::

## Quickstart: Serve a Stable Diffusion model on GKE with TPUs

After setting up a GKE cluster with TPUs and the KubeRay TPU initialization webhook, run a workload on Ray with TPUs. {ref}`Serve a Stable Diffusion model on GKE with TPUs <kuberay-tpu-stable-diffusion-example>` shows how to serve a model with KubeRay on single-host TPUs. For multi-host LLM serving with vLLM on TPUs, see {doc}`TPU serving </serve/llm/user-guides/tpu>`.

## Configure Kubernetes Pods for TPU usage

To use TPUs in KubeRay, specify `google.com/tpu` resource `requests` and `limits` in the container spec of your `RayCluster` `workerGroupSpecs`. Set `numOfHosts` to the number of TPU nodes in each slice replica; KubeRay creates `numOfHosts` worker Pods per replica. GKE uses the `cloud.google.com/gke-tpu-accelerator` and `cloud.google.com/gke-tpu-topology` node selectors to schedule the Pods onto matching TPU node pools.

The following snippets show the `workerGroupSpecs` configuration for `v6e` and `TPU7x` slices. For complete `RayCluster` manifests, see [`ray-cluster.tpu-v6e-singlehost.yaml`](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.tpu-v6e-singlehost.yaml) and [`ray-cluster.tpu-v6e-16-multihost.yaml`](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.tpu-v6e-16-multihost.yaml) in the KubeRay repository:

::::{tab-set}

:::{tab-item} TPU v6e (4x4, 4 hosts)

```yaml
workerGroupSpecs:
  - groupName: tpu-v6e-group
    replicas: 1
    minReplicas: 0
    maxReplicas: 1
    numOfHosts: 4
    rayStartParams: {}
    template:
      spec:
        containers:
          - name: ray-worker
            image: rayproject/ray:2.54.0
            resources:
              requests:
                cpu: "24"
                memory: "100G"
                google.com/tpu: "4"
              limits:
                cpu: "24"
                memory: "100G"
                google.com/tpu: "4"
        nodeSelector:
          cloud.google.com/gke-tpu-accelerator: tpu-v6e-slice
          cloud.google.com/gke-tpu-topology: 4x4
```

:::

:::{tab-item} TPU7x (2x2x2, 2 hosts)

```yaml
workerGroupSpecs:
  - groupName: tpu-7x-group
    replicas: 1
    minReplicas: 0
    maxReplicas: 1
    numOfHosts: 2
    rayStartParams:
      # Optional: only required for 1-process-per-device workloads (such as TorchTPU).
      # Omit for host-level SPMD workloads (such as JAX) to use the default {"TPU": 4}.
      resources: '"{\"TPU\": 8}"'
    template:
      spec:
        containers:
          - name: ray-worker
            image: rayproject/ray:2.54.0
            env:
              # Optional: only required for 1-process-per-device workloads (such as TorchTPU).
              - name: RAY_TPU_RESOURCE_PER_CHIP
                value: "2"
            resources:
              requests:
                cpu: "24"
                memory: "200G"
                google.com/tpu: "4"
              limits:
                cpu: "24"
                memory: "200G"
                google.com/tpu: "4"
        nodeSelector:
          cloud.google.com/gke-tpu-accelerator: tpu7x
          cloud.google.com/gke-tpu-topology: 2x2x2
```

:::

::::

### What the KubeRay TPU webhook configures

When you deploy a `RayCluster` on GKE (with KubeRay `v1.6.0+` and [`kuberay-tpu-webhook`](https://github.com/ai-on-gke/kuberay-tpu-webhook) `v1.4.0+`), the webhook validates that `numOfHosts` matches `cloud.google.com/gke-tpu-topology` and injects the following configuration into TPU worker Pods:

- **Slice identity and affinity:** Sets `TPU_WORKER_ID`, `TPU_NAME`, headless service DNS fields (`hostname` and `subdomain`), and `podAffinity` rules so all workers in a replica schedule on the same TPU slice.
- **Multi-host coordination:** Sets `TPU_WORKER_HOSTNAMES` on `v4` through `v6e` slices, and `TPU_PROCESS_ADDRESSES` and `TPU_PROCESS_PORT` on `TPU7x` slices.
- **PyTorch and TorchTPU (`v1.4.0+`):** Sets `TORCH_TPU_TOPOLOGY` and `TORCH_TPU_SLICEBUILDER_ADDRESSES` so `torch_tpu` can initialize across hosts.
- **MegaScale multi-slice (`v1.3.0+`):** Sets `MEGASCALE_SLICE_ID`, `MEGASCALE_COORDINATOR_ADDRESS`, and `MEGASCALE_PORT` when `MEGASCALE_NUM_SLICES` is present.
- **Ray Dashboard metrics:** Sets `TPU_DEVICE_PLUGIN_HOST_IP` and `TPU_DEVICE_PLUGIN_ADDR` (`<host-ip>:2112`) so the Raylet collects TPU utilization and HBM metrics (see {ref}`System Metrics <system-metrics>`).

## TPU workload scheduling

Tasks and actors request TPUs using `@ray.remote(resources={"TPU": <num-tpus>})`. When a task or actor requests a subset of a host's TPUs (such as `resources={"TPU": 1}`), Ray assigns specific TPU IDs and sets `TPU_VISIBLE_CHIPS`, `TPU_CHIPS_PER_HOST_BOUNDS`, and `TPU_HOST_BOUNDS`. When a task or actor requests all TPUs on the host, Ray clears those sub-host bounds so the framework uses the full host. See {ref}`Accelerator support <accelerator-support>`.

### TPU default labels

On Google Cloud TPUs with KubeRay, Ray attaches the following default node labels (see {ref}`Default node labels <defaults>`):

- `ray.io/accelerator-type`: The TPU generation, such as `TPU-V6E` or `TPU-V7X`.
- `ray.io/tpu-slice-name`: The unique name of the TPU slice.
- `ray.io/tpu-worker-id`: The zero-based host index (`"0"`, `"1"`, ...) within the slice.
- `ray.io/tpu-topology`: The physical interconnect topology, such as `4x4` or `2x2x2`.
- `ray.io/tpu-pod-type`: The TPU Pod type combining generation and logical device count, such as `v6e-16` or `tpu7x-16`.

You can pass `bundle_label_selector` to {func}`~ray.util.placement_group` to target a specific TPU Pod type:

```python
import ray
from ray.util.placement_group import placement_group

# Request four host bundles on a v6e-16 slice.
pg = placement_group(
    [{"TPU": 4}] * 4,
    strategy="SPREAD",
    bundle_label_selector=[{"ray.io/tpu-pod-type": "v6e-16"}] * 4,
)
ray.get(pg.ready())
```

### TPU scheduling utility library

In a cluster with multiple slices of the same TPU Pod type, `bundle_label_selector={"ray.io/tpu-pod-type": "v6e-16"}` alone can spread bundles across different physical slices. The `ray.util.tpu` package builds on Ray's {ref}`label selector API <labels>` to guarantee atomic slice and subslice placement for both SPMD and MPMD workloads.

#### `SlicePlacementGroup`

{func}`ray.util.tpu.slice_placement_group` reserves worker `0` (`ray.io/tpu-worker-id: "0"`) of an idle slice matching `topology` and `accelerator_version`, reads its `ray.io/tpu-slice-name` label, and returns a {class}`~ray.util.tpu.SlicePlacementGroup` whose bundles are pinned to that slice:

::::{tab-set}

:::{tab-item} One process per host (JAX)

By default, `slice_placement_group` creates one bundle per TPU host (for example, four `{"TPU": 4}` bundles on a `v6e` `4x4` slice). Use {func}`ray.util.tpu.run_on_slice` or `PlacementGroupSchedulingStrategy` to run one task per host:

```python
import ray
from ray.util.tpu import run_on_slice, slice_placement_group

@ray.remote(resources={"TPU": 4})
def jax_spmd_task():
    import jax
    return {
        "slice": ray.util.tpu.get_current_pod_name(),
        "global_devices": jax.device_count(),
    }

slice_handle = slice_placement_group(topology="4x4", accelerator_version="v6e")
ray.get(slice_handle.placement_group.ready(), timeout=600)

results = ray.get(run_on_slice(jax_spmd_task, tpu_slice=slice_handle))
print(results)
slice_handle.shutdown()
```

:::

:::{tab-item} One process per device (PyTorch / TorchTPU)

Pass `resources_per_bundle={"TPU": 1}` to create one bundle per logical TPU device (for example, 16 bundles on a `v6e` `4x4` or `TPU7x` `2x2x2` slice):

```python
import os
import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tpu import slice_placement_group

slice_handle = slice_placement_group(
    topology="4x4",
    accelerator_version="v6e",
    resources_per_bundle={"TPU": 1},
)
slice_pg = slice_handle.placement_group
ray.get(slice_pg.ready(), timeout=600)

@ray.remote(num_cpus=1, resources={"TPU": 1})
def device_worker(rank: int):
    return {
        "rank": rank,
        "tpu_ids": ray.get_runtime_context().get_accelerator_ids()["TPU"],
        "tpu_visible_chips": os.environ.get("TPU_VISIBLE_CHIPS"),
    }

tasks = [
    device_worker.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=slice_pg,
            placement_group_bundle_index=i,
        )
    ).remote(rank=i)
    for i in range(slice_handle.num_bundles)
]
print(ray.get(tasks))
slice_handle.shutdown()
```

:::

::::

:::{tip}
In multi-process collective runtimes such as `torch.distributed` with `torch_tpu`, all worker processes on a host open the full local `/dev/vfio/*` mesh and select their device via `LOCAL_RANK`. When launching custom per-device actors outside of {class}`~ray.train.torch.TorchTrainer` or `vllm-torchtpu`, set `RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS=1` in the actor `runtime_env` (or clear `TPU_VISIBLE_CHIPS`, `TPU_CHIPS_PER_HOST_BOUNDS`, and `TPU_HOST_BOUNDS`) before initializing `torch_tpu`.
:::

#### `SubslicePlacementGroup`

Use {func}`ray.util.tpu.subslice_placement_group` to partition a larger multi-host TPU slice into smaller, ICI-connected subslices (for example, two `2x4` subslices inside a `4x4` `v6e` slice, or two `2x2x2` subslices inside a `2x2x4` `TPU7x` slice). Ray discovers the physical chip coordinates on each host via `libtpu`, caches valid rectangular subslice mappings, and returns a {class}`~ray.util.tpu.SubslicePlacementGroup` pinned to an idle subslice:

```python
import ray
from ray.util.tpu import subslice_placement_group

# Reserve a 2x4 subslice (2 hosts, 8 chips) inside a parent 4x4 v6e slice.
subslice_handle = subslice_placement_group(
    subslice_topology="2x4",
    accelerator_version="v6e",
)
ray.get(subslice_handle.placement_group.ready(), timeout=600)
subslice_handle.shutdown()
```

### Reinforcement learning (RL) on TPUs

Reinforcement learning post-training workloads (using frameworks such as [VeRL](https://github.com/volcengine/verl), [SkyRL](https://github.com/NovaSky-AI/SkyRL), or [Tunix](https://github.com/google/tunix)) alternate between rollout generation and policy training. Because each physical TPU chip locks `/dev/vfio` exclusively to a single process and routes ICI collectives across fixed torus meshes, Ray supports two placement architectures for RL on TPUs:

- **Colocated RL (shared slice):** Rollout and training run in the same worker process on a single {func}`~ray.util.tpu.slice_placement_group`. Use this when the model, optimizer states, and KV cache fit in slice memory (or with host DRAM offloading) to avoid transferring weights over the network.
- **Disaggregated RL (separate slices or subslices):** Rollout actors (such as [vLLM](https://docs.vllm.ai/)) and learner actors (such as PyTorch `torch_tpu` or JAX) run on separate slices via {func}`~ray.util.tpu.slice_placement_group` or on subslices of a larger slice via {func}`~ray.util.tpu.subslice_placement_group`. Use this to dedicate full slice HBM to rollout KV caches and transfer updated weights over the data center network (DCN) between steps (see {ref}`Recommended GKE node pool options <kuberay-gke-tpu-cluster-setup>` for multi-NIC DRANET setup).

The following example partitions a `4x4` `v6e` slice into two `2x4` subslices—one for rollout actors and one for learner actors:

```python
import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tpu import subslice_placement_group

# Partition a 4x4 v6e slice into two 2x4 subslices (2 hosts, 8 TPUs each).
rollout_pg = subslice_placement_group(
    subslice_topology="2x4", accelerator_version="v6e", subslice_index=0
)
learner_pg = subslice_placement_group(
    subslice_topology="2x4", accelerator_version="v6e", subslice_index=1
)
ray.get(
    [rollout_pg.placement_group.ready(), learner_pg.placement_group.ready()],
    timeout=600,
)

@ray.remote(resources={"TPU": 4})
class RolloutWorker:
    def generate(self, prompts):
        return [f"response for {p}" for p in prompts]

    def update_weights(self, step: int):
        return step

@ray.remote(resources={"TPU": 4})
class LearnerWorker:
    def train_step(self, rollouts):
        return {"loss": 0.1, "num_batches": len(rollouts)}

rollout_workers = [
    RolloutWorker.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=rollout_pg.placement_group,
            placement_group_bundle_index=i,
        )
    ).remote()
    for i in range(rollout_pg.num_hosts)
]

learner_workers = [
    LearnerWorker.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=learner_pg.placement_group,
            placement_group_bundle_index=i,
        )
    ).remote()
    for i in range(learner_pg.num_hosts)
]

# Run one RL step: generate rollouts -> train policy -> sync weights.
rollouts = ray.get([w.generate.remote(["prompt-1", "prompt-2"]) for w in rollout_workers])
metrics = ray.get([w.train_step.remote(rollouts) for w in learner_workers])
ray.get([w.update_weights.remote(step=1) for w in rollout_workers])

rollout_pg.shutdown()
learner_pg.shutdown()
```

For per-device MPMD RL actors (such as `torch_tpu`), pass `resources_per_bundle={"CPU": 4, "TPU": 4}` to `subslice_placement_group` and schedule `chips_per_host` actors (`@ray.remote(resources={"TPU": 1})`) onto each host bundle.

#### TPU Pod information utilities

The `ray.util.tpu` module also provides helper functions to inspect TPU slice metadata:

- {func}`ray.util.tpu.get_current_pod_name`: Returns the TPU slice name (`TPU_NAME`) of the current node, or `None` if not on a TPU node.
- {func}`ray.util.tpu.get_current_pod_worker_count`: Returns the number of TPU hosts in the current TPU slice.
- {func}`ray.util.tpu.get_num_tpu_chips_on_node`: Returns the number of logical TPU devices detected on the current node.
- {func}`ray.util.tpu.get_num_ready_tpu_slices`: Returns the number of intact, idle TPU slices matching `topology` and `accelerator_type`.
- {func}`ray.util.tpu.get_tpu_coordinator_env_vars`: Returns the `MEGASCALE_*` environment variables for multi-slice coordination over DCN.

## Multi-host TPU autoscaling

KubeRay `v1.1.0+` and Ray `2.32.0+` scale multi-host TPU worker groups (`numOfHosts > 1`) atomically by slice replica. When scaling up, the Ray autoscaler and KubeRay provision all `numOfHosts` worker Pods in the replica together; when scaling down, they terminate the entire replica as a unit. Schedule tasks or actors across every TPU node in a multi-host replica so the autoscaler does not treat partially unused hosts as idle.

## Further reference

- [TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus)
- [TPU regions and zones](https://cloud.google.com/tpu/docs/regions-zones)
- [Cloud TPU system architecture](https://cloud.google.com/tpu/docs/system-architecture-tpu-vm)
- [Train a model on multi-host TPUs with Ray Train and TorchTPU](https://docs.cloud.google.com/kubernetes-engine/docs/tutorials/train-models-tpu-ray-torchtpu)
- [Serve an LLM on multi-host TPUs with Ray Serve and TorchTPU](https://docs.cloud.google.com/kubernetes-engine/docs/tutorials/serve-llm-tpu-ray-torchtpu)
