---
myst:
  html_meta:
    description: "Use TPUs with KubeRay: configure Kubernetes Pods for TPUs, TPU workload scheduling and default labels, and multi-host TPU autoscaling."
---

(kuberay-tpu)=

# Use TPUs with KubeRay

This document provides tips on TPU usage with KubeRay.

TPUs are available on Google Kubernetes Engine (GKE). To use TPUs with Kubernetes, configure both the Kubernetes setup and add additional values to the RayCluster CR configuration. Configure TPUs on GKE by referencing the {ref}`kuberay-gke-tpu-cluster-setup`.

## About TPUs

TPUs are custom-designed AI accelerators, which are optimized for training and inference of large AI models. A TPU host is a VM that runs on a physical computer connected to TPU hardware. TPU workloads can run on one or multiple hosts. A TPU Pod slice is a collection of chips all physically colocated and connected by high-speed inter chip interconnects (ICI). Single-host TPU Pod slices contain independent TPU VM hosts and communicate over the Data Center Network (DCN) rather than ICI interconnects. Multi-host TPU Pod slices contain two or more interconnected TPU VM hosts. In GKE, multi-host TPU Pod slices run on their own node pools and GKE scales them atomically by node pools, rather than individual nodes. Ray enables single-host and multi-host TPU Pod slices to be scaled seamlessly to multiple slices, enabling greater parallelism to support larger workloads.

Ray supports both **SPMD** and **MPMD** workloads on TPUs. In SPMD workloads, all workers run the same program across data or model shards. In MPMD workloads, distinct actor groups such as reinforcement learning rollout and learner actors run different programs concurrently across devices or slices. Depending on your framework, you can schedule workers at two granularities:

- **One process per TPU host (`{"TPU": 4}`):** One Ray task or actor runs on each TPU node and manages all local TPU devices on that host. Frameworks such as JAX, MaxText, and [vLLM-TPU](https://github.com/vllm-project/tpu-inference) use this model.
- **One process per TPU device (`{"TPU": 1}`):** One Ray task or actor runs on each logical TPU device, matching the one-process-per-GPU model. Frameworks such as TorchTPU, vLLM-TorchTPU, and per-device MPMD actor groups use this model.

(kuberay-tpu7x-dual-chiplet)=

On TPU v4, v5e, v5p, and v6e, each physical TPU chip exposes one logical TPU device, which corresponds to `{"TPU": 4}` on a four-chip node. On [TPU7x](https://cloud.google.com/tpu/docs/tpu7x#dual-chiplet-architecture), each physical chip contains two compute chiplets that expose two separate logical TPU devices, providing eight logical devices on a four-chip `tpu7x-standard-4t` node. For host-level SPMD workloads such as JAX, use the default `{"TPU": 4}` resource count. To run one process per logical TPU device with `{"TPU": 1}` on TPU7x, set `RAY_TPU_RESOURCE_PER_CHIP=2` on both the head and worker containers and set `resources: '"{\"TPU\": 8}"'` in `rayStartParams` on the worker group.

## Quickstart: Serve a Stable Diffusion model on GKE with TPUs

After setting up a GKE cluster with TPUs and the Ray TPU initialization webhook, run a workload on Ray with TPUs. {ref}`Serve a Stable Diffusion model on GKE with TPUs <kuberay-tpu-stable-diffusion-example>` is a full example of how to serve a Stable Diffusion model with KubeRay on single-host TPUs.

## Configuring Kubernetes Pods for TPU usage

Using any TPU accelerator requires specifying `google.com/tpu` resource `limits` and `requests` in the container fields of your `RayCluster`'s `workerGroupSpecs`. This resource specifies the number of TPU chips for GKE to allocate each Pod. KubeRay v1.1.0 adds a `numOfHosts` field to the RayCluster custom resource, specifying the number of TPU hosts to create per worker group replica. For multi-host worker groups, Ray treats replicas as Pod slices rather than individual workers, and creates `numOfHosts` worker nodes per replica. Additionally, GKE uses `gke-tpu` node selectors to schedule TPU Pods on the node matching the desired TPU accelerator and topology.

The following snippets show the `workerGroupSpecs` configuration for TPU v6e and TPU7x slices. For complete `RayCluster` manifests, see [`ray-cluster.tpu-v6e-singlehost.yaml`](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.tpu-v6e-singlehost.yaml) and [`ray-cluster.tpu-v6e-16-multihost.yaml`](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.tpu-v6e-16-multihost.yaml) in the KubeRay repository:

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
            image: rayproject/ray:2.59.0
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
      # Set only for 1-process-per-device workloads on TPU7x.
      resources: '"{\"TPU\": 8}"'
    template:
      spec:
        containers:
          - name: ray-worker
            image: rayproject/ray:2.59.0
            env:
              # Set on both ray-head and ray-worker for 1-process-per-device workloads on TPU7x.
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

(kuberay-tpu-webhook-config)=

### What the KubeRay TPU webhook configures

When you deploy a `RayCluster` on GKE with [`kuberay-tpu-webhook`](https://github.com/ai-on-gke/kuberay-tpu-webhook), the webhook validates that `numOfHosts` matches `cloud.google.com/gke-tpu-topology` and injects the following configuration into TPU worker Pods:

- **Slice identity and affinity:** Sets `TPU_WORKER_ID`, `TPU_NAME`, headless service DNS `hostname` and `subdomain` fields, and `podAffinity` rules so all workers in a replica schedule on the same TPU slice.
- **Multi-host coordination:** Sets `TPU_WORKER_HOSTNAMES` on TPU v4 through TPU v6e slices, and `TPU_PROCESS_ADDRESSES` and `TPU_PROCESS_PORT` on TPU7x slices.
- **TorchTPU:** Sets `TORCH_TPU_TOPOLOGY` and `TORCH_TPU_SLICEBUILDER_ADDRESSES` in webhook v1.4.0 and later so TorchTPU can initialize across hosts.
- **MegaScale multi-slice:** Sets `MEGASCALE_SLICE_ID`, `MEGASCALE_COORDINATOR_ADDRESS`, and `MEGASCALE_PORT` in webhook v1.3.0 and later when `MEGASCALE_NUM_SLICES` is present.
- **Ray Dashboard metrics:** Sets `TPU_DEVICE_PLUGIN_HOST_IP` and `TPU_DEVICE_PLUGIN_ADDR` so the Ray Dashboard agent collects TPU utilization and HBM metrics. For the exported metric names, see {ref}`System Metrics <system-metrics>`.

## TPU workload scheduling

After Ray deploys a Pod with TPU resources, the Pod can execute tasks and actors annotated with TPU requests. Ray supports TPUs as a {ref}`custom resource <custom-resources>`. Tasks or actors request TPUs using the decorator `@ray.remote(resources={"TPU": NUM_TPUS})`. When a worker requests a subset of a host's TPUs, Ray isolates the assigned chips using `TPU_VISIBLE_CHIPS` and sub-host bounds. For details, see {ref}`Accelerator support <accelerator-support>`.

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

The `ray.util.tpu` package uses Ray's default TPU node labels and {ref}`label selectors <kuberay-label-scheduling>` to schedule workloads on intact single-slice or multi-slice TPU topologies.

#### `SlicePlacementGroup`

{class}`~ray.util.tpu.SlicePlacementGroup` reserves one or more available TPU slices matching your `topology` and `accelerator_version` and creates a Ray {ref}`placement group <ray-placement-group-doc-ref>` pinned to those physical slices using the `ray.io/tpu-slice-name` node label:

- **Bundle granularity:** Creates one bundle per TPU host (`{"TPU": 4}`) by default, or one bundle per logical TPU device when you pass `resources_per_bundle={"TPU": 1}`. For topologies such as TPU v6e `2x4` that can run on either one 8-chip VM or two 4-chip VMs, set `chips_per_vm` to match your node pool.
- **Execution:** Call {func}`ray.util.tpu.slice_placement_group` to reserve slices and {func}`ray.util.tpu.run_on_slice` to launch tasks or actors across the bundles.
- **Ray Train:** {class}`~ray.train.ScalingConfig` automatically creates a `SlicePlacementGroup` when you set `use_tpu=True` and `topology`. See {doc}`Configure scale and accelerators </train/user-guides/using-accelerators>`.
- **Ray Serve LLM:** {class}`~ray.serve.llm.LLMConfig` automatically creates a `SlicePlacementGroup` when you set `accelerator_config={"kind": "tpu", "topology": ...}`. See {doc}`TPU serving </serve/llm/user-guides/tpu>`.

::::{tab-set}

:::{tab-item} One process per host (JAX)

```python
import ray
from ray.util.tpu import run_on_slice, slice_placement_group

# Reserve two v6e 4x4 TPU slices, each with 4 hosts and 16 chips.
slice_handle = slice_placement_group(
    topology="4x4",
    accelerator_version="v6e",
    num_slices=2,
)

@ray.remote
def spmd_task():
    pod_name = ray.util.tpu.get_current_pod_name()
    chips_on_node = ray.util.tpu.get_num_tpu_chips_on_node()
    return f"Running on slice '{pod_name}' with {chips_on_node} chips."

# Launch one task per TPU host across the reserved slices.
results = ray.get(run_on_slice(spmd_task, tpu_slice=slice_handle))
print(f"Task results: {results}")
slice_handle.shutdown()
```

:::

:::{tab-item} One process per device (TorchTPU)

```python
import os
import ray
from ray.util.tpu import run_on_slice, slice_placement_group

# Reserve one v6e 4x4 slice with 16 per-device bundles.
slice_handle = slice_placement_group(
    topology="4x4",
    accelerator_version="v6e",
    resources_per_bundle={"TPU": 1},
)

@ray.remote
def device_worker():
    return {
        "host_id": os.environ.get("TPU_WORKER_ID"),
        "tpu_ids": ray.get_runtime_context().get_accelerator_ids()["TPU"],
        "tpu_visible_chips": os.environ.get("TPU_VISIBLE_CHIPS"),
    }

# Launch one task per logical TPU device across the slice.
results = ray.get(run_on_slice(device_worker, tpu_slice=slice_handle))
print(results)
slice_handle.shutdown()
```

:::

::::

#### TPU Pod Information Utilities

The following helper functions in `ray.util.tpu` query TPU slice metadata and configure multi-slice coordination:

* {func}`ray.util.tpu.get_current_pod_name`: Returns the name of the TPU Pod that the worker is a part of, or `None` if not running on a TPU.
* {func}`ray.util.tpu.get_current_pod_worker_count`: Returns the number of workers in the TPU Pod that the worker belongs to, or `None` if not running on a TPU.
* {func}`ray.util.tpu.get_num_tpu_chips_on_node`: Returns the number of TPU chips on the current node, or `0` if none are found.
* {func}`ray.util.tpu.get_num_ready_tpu_slices`: Returns the number of ready, idle TPU slices matching `topology` and `accelerator_type`.
* {func}`ray.util.tpu.get_tpu_coordinator_env_vars`: Returns the `MEGASCALE_*` environment variables required for multi-slice coordination.

### Reinforcement learning on TPUs

Reinforcement learning algorithms such as GRPO and PPO alternate between rollout generation and policy updates. Because each logical TPU device can only be acquired by one process at a time, separate inference and training processes cannot share the same TPU device simultaneously.

RL workloads on TPUs typically use one of two placement patterns:

- **Disaggregated RL:** Rollout workers and training workers run on separate TPU slices, while rollout samples and updated weights transfer across slices over DCN.
- **Colocated RL:** A single worker group runs on one TPU slice and executes both rollout generation and training inside the same process on each TPU host or device, avoiding cross-slice weight transfers.

The following disaggregated RL example deploys vLLM on one TPU v6e `4x4` slice with `LLMConfig` and runs policy training on a second TPU v6e `4x4` slice with {class}`~ray.train.torch.TorchTrainer`:

```python
from openai import OpenAI
import torch
import ray.train
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer, get_device, prepare_model

MODEL_ID = "Qwen/Qwen3-8B"

# 1. Start vLLM rollout workers on a TPU v6e 4x4 slice.
llm_config = LLMConfig(
    model_loading_config=dict(model_id=MODEL_ID, model_source=MODEL_ID),
    accelerator_type="TPU-V6E",
    accelerator_config={"kind": "tpu", "topology": "4x4"},
    placement_group_config={"bundle_per_worker": {"TPU": 1}},
    runtime_env={"env_vars": {"TPU_MULTIHOST_BACKEND": "ray"}},
    engine_kwargs=dict(
        tensor_parallel_size=16,
        max_model_len=4096,
        max_num_batched_tokens=4096,
        distributed_executor_backend="ray",
    ),
)
serve.run(build_openai_app({"llm_configs": [llm_config]}))

# Generate rollouts against the OpenAI-compatible Serve endpoint.
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")
response = client.chat.completions.create(
    model=MODEL_ID,
    messages=[{"role": "user", "content": "Solve: 2x + 5 = 17"}],
    max_tokens=128,
)


# 2. Run distributed policy training on a second TPU v6e 4x4 slice.
def train_func(config):
    device = get_device()
    model = torch.nn.Linear(128, 128).to(device)
    model = prepare_model(model)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4)

    for step in range(config["num_steps"]):
        optimizer.zero_grad()
        inputs = torch.randn(8, 128, device=device)
        loss = model(inputs).sum()
        loss.backward()
        optimizer.step()
        ray.train.report({"step": step, "loss": loss.item()})


trainer = TorchTrainer(
    train_func,
    train_loop_config={"num_steps": 10},
    scaling_config=ScalingConfig(
        num_workers=16,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
        resources_per_worker={"TPU": 1},
    ),
)
trainer.fit()
```


## Multi-Host TPU autoscaling

Multi-host TPU autoscaling is supported in KubeRay versions 1.1.0 or later and Ray versions 2.32.0 or later. Ray multi-host TPU worker groups are worker groups, which specify "google.com/tpu" Kubernetes container limits or requests and have `numOfHosts` greater than 1. Ray treats each replica of a Ray multi-host TPU worker group as a TPU Pod slice and scales them atomically. When scaling up, multi-host worker groups create `numOfHosts` Ray workers per replica. Likewise, Ray scales down multi-host worker group replicas by `numOfHosts` workers. When Ray schedules a deletion of a single Ray worker in a multi-host TPU worker group, it terminates the entire replica to which the worker belongs. When scheduling TPU workloads on multi-host worker groups, ensure that Ray tasks or actors run on every TPU VM host in a worker group replica to avoid Ray from scaling down idle TPU workers.

Further reference and discussion
--------------------------------
* See [TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus) for more details on using TPUs.
* [TPU availability](https://cloud.google.com/tpu/docs/regions-zones)
* [TPU System Architecture](https://cloud.google.com/tpu/docs/system-architecture-tpu-vm)
* [Train a model on multi-host TPUs with Ray Train and TorchTPU](https://cloud.google.com/kubernetes-engine/docs/tutorials/train-models-tpu-ray-torchtpu)
* [Serve an LLM on multi-host TPUs with Ray Serve and TorchTPU](https://cloud.google.com/kubernetes-engine/docs/tutorials/serve-llm-tpu-ray-torchtpu)
