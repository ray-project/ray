---
myst:
  html_meta:
    description: "Run Ray on Google Cloud TPUs with KubeRay: TPU slices, SPMD and MPMD process layouts, getting-started examples for Ray Core, TorchTrainer, and JaxTrainer, TPU-aware scheduling, autoscaling, and Pod configuration."
---

(kuberay-tpu)=

# Use TPUs with KubeRay

This page explains how to schedule, run, and autoscale Ray workloads on Google Cloud TPUs with KubeRay. Before you start, create a Google Kubernetes Engine (GKE) cluster with TPU node pools and the Ray TPU webhook by following {ref}`kuberay-gke-tpu-cluster-setup`.

## About TPUs

Tensor Processing Units (TPUs) are Google's custom-developed, application-specific integrated circuits (ASICs) for accelerating machine learning workloads using frameworks such as PyTorch and JAX. You can access [Cloud TPUs](https://cloud.google.com/tpu/docs/intro-to-tpu) on Google Cloud through Google Kubernetes Engine (GKE), Compute Engine, and Vertex AI. Each TPU chip pairs matrix multiplication units with on-chip high-bandwidth memory (HBM), and a high-speed inter-chip interconnect (ICI) links chips into slices that scale up your workloads. For hardware details, see [TPU system architecture](https://cloud.google.com/tpu/docs/system-architecture-tpu-vm), and for regional availability, see [TPU regions and zones](https://cloud.google.com/tpu/docs/regions-zones).

## Running Ray on TPUs

A TPU host is a virtual machine (VM) attached to a set of TPU chips. A TPU Pod slice is a group of physically colocated chips connected over ICI. A single-host slice has one host, and single-host slices communicate with each other over the data center network (DCN) rather than ICI. A multi-host slice spans two or more interconnected hosts. On GKE, each multi-host slice runs in its own node pool, and GKE scales that node pool as a unit rather than node by node.

KubeRay runs each TPU host as a Pod, so one Ray node maps to one TPU host. Ray schedules tasks and actors onto those nodes with the `TPU` resource and the TPU node labels described in {ref}`kuberay-tpu-labels`, scales single-host and multi-host slices out to many slices for larger workloads, and scales the hosts of a multi-host slice down together.

Ray supports both *single program, multiple data* (SPMD) and *multiple program, multiple data* (MPMD) workloads on TPUs. In an SPMD workload, every worker runs the same program over data or model shards. In an MPMD workload, distinct actor groups, such as reinforcement learning rollout actors and learner actors, run different programs at the same time across devices or slices. Depending on your framework, schedule workers at one of two granularities:

- **One process per TPU host**: One Ray task or actor requests all TPUs on a host, such as `{"TPU": 4}` on a four-chip host, and drives all of the host's TPU devices. JAX, MaxText, and [vLLM-TPU](https://github.com/vllm-project/tpu-inference) use this model.
- **One process per TPU device**: One Ray task or actor requests `{"TPU": 1}` and runs on a single logical TPU device. [TorchTPU](https://torchtpu.readthedocs.io/en/latest/), [vLLM-TorchTPU](https://vllm-torchtpu.readthedocs.io/en/latest/), and per-device MPMD actor groups use this model.

(kuberay-tpu7x-dual-chiplet)=

### TPU7x logical devices

On TPU v4, v5e, v5p, and v6e, each physical chip exposes one logical TPU device, so a four-chip host registers `{"TPU": 4}`. On [TPU7x](https://cloud.google.com/tpu/docs/tpu7x#dual-chiplet-architecture), each physical chip contains two compute chiplets that expose two logical TPU devices, so a four-chip `tpu7x-standard-4t` host has eight logical devices. For host-level SPMD workloads such as JAX, keep the default `{"TPU": 4}`. To run one process per logical device with `{"TPU": 1}` on TPU7x, set `RAY_TPU_RESOURCE_PER_CHIP=2` on both the head and worker containers and set `resources: '"{\"TPU\": 8}"'` in `rayStartParams` on the worker group. {ref}`kuberay-tpu-pod-config` shows the full worker group configuration.

(kuberay-tpu-get-started)=

## Get started

The following examples show the three most common ways to run Ray on TPUs. Each example runs on one TPU v6e `4x4` slice, which has four hosts and 16 chips, on a RayCluster configured as described in {ref}`kuberay-tpu-pod-config`.

::::{tab-set}

:::{tab-item} Ray Core with TorchTPU

Reserve a slice with one bundle per logical TPU device, then run one task per device. Each task binds to its device as `torch.device("tpu")` after importing `torch_tpu`:

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
    import torch
    import torch_tpu  # Register the "tpu" device with PyTorch.

    x = torch.ones(1024, 1024, device="tpu")
    return {
        "host_id": os.environ.get("TPU_WORKER_ID"),
        "tpu_ids": ray.get_runtime_context().get_accelerator_ids()["TPU"],
        "checksum": (x @ x).sum().item(),
    }


# Launch one task per logical TPU device across the slice.
results = ray.get(run_on_slice(device_worker, tpu_slice=slice_handle))
print(results)
slice_handle.shutdown()
```

:::

:::{tab-item} Ray Train with TorchTrainer

{class}`~ray.train.torch.TorchTrainer` launches one worker per logical TPU device when you set `use_tpu=True` and `resources_per_worker={"TPU": 1}`, initializes the `tpu_dist` backend for `torch.distributed`, and binds each worker to its device through {func}`ray.train.torch.get_device`:

```python
import torch

import ray.train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer, get_device, prepare_model


def train_func():
    device = get_device()
    model = torch.nn.Linear(128, 10).to(device)
    model = prepare_model(model)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)

    inputs = torch.randn(32, 128, device=device)
    targets = torch.randint(0, 10, (32,), device=device)

    optimizer.zero_grad()
    loss = torch.nn.functional.cross_entropy(model(inputs), targets)
    loss.backward()
    optimizer.step()

    ray.train.report({"loss": loss.item()})


trainer = TorchTrainer(
    train_func,
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

:::

:::{tab-item} Ray Train with JaxTrainer

{class}`~ray.train.v2.jax.JaxTrainer` launches one worker per TPU host when you set `use_tpu=True` and initializes JAX's distributed runtime across the slice, so `jax.devices()` returns every chip in the slice from each worker:

```python
import ray.train
from ray.train import ScalingConfig
from ray.train.v2.jax import JaxTrainer


def train_func():
    import jax

    devices = jax.devices()
    ray.train.report({"num_devices": len(devices)})


trainer = JaxTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=4,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
    ),
)
trainer.fit()
```

:::

::::

For more trainer options, see {doc}`Configure scale and accelerators </train/user-guides/using-accelerators>`. To serve a model with vLLM on TPUs, see {doc}`TPU serving </serve/llm/user-guides/tpu>`.

(kuberay-tpu-scheduling)=

## Schedule TPU workloads

After KubeRay starts a Pod with TPU resources, the Pod can run tasks and actors that request TPUs. Ray exposes TPUs as a {ref}`custom resource <custom-resources>`, so a task or actor requests them with a decorator such as `@ray.remote(resources={"TPU": 1})`. When a worker requests a subset of a host's TPUs, Ray isolates the assigned chips with `TPU_VISIBLE_CHIPS` and sub-host bounds. For details, see {ref}`Accelerator support <accelerator-support>`.

(kuberay-tpu-labels)=

### Default TPU node labels

When Ray starts on a Google Cloud TPU host with KubeRay, it detects the TPU and adds the following node labels. Distributed workloads that must span an entire slice, which is a set of interconnected hosts, depend on these labels:

* `ray.io/accelerator-type`: The TPU generation, such as `TPU-V6E`.
* `ray.io/tpu-slice-name`: The name of the TPU Pod slice. Ray uses this label to keep all workers of a job on the same slice.
* `ray.io/tpu-worker-id`: The integer worker ID of the host within the slice.
* `ray.io/tpu-topology`: The physical topology of the slice, such as `4x4`.
* `ray.io/tpu-pod-type`: The TPU Pod type, which encodes the generation and chip count, such as `v4-8` or `v5p-16`.

The following example uses these labels to schedule a placement group that requests all TPU devices on a `v6e-16` slice:

```python
import ray
from ray.util.placement_group import placement_group

# Request four bundles, one for each TPU host in the v6e-16 slice.
pg = placement_group(
    [{"TPU": 4}] * 4,
    strategy="SPREAD",
    bundle_label_selector=[{"ray.io/tpu-pod-type": "v6e-16"}] * 4,
)
ray.get(pg.ready())
```

### TPU scheduling utilities

The `ray.util.tpu` package uses the default TPU node labels and {ref}`label selectors <kuberay-label-scheduling>` to schedule workloads on intact single-slice or multi-slice TPU topologies.

#### `SlicePlacementGroup`

{class}`~ray.util.tpu.SlicePlacementGroup` reserves one or more available TPU slices that match your `topology` and `accelerator_version`, and creates a Ray {ref}`placement group <ray-placement-group-doc-ref>` pinned to those physical slices through the `ray.io/tpu-slice-name` node label:

- **Bundle granularity**: Creates one bundle per TPU host by default, such as `{"TPU": 4}` on a four-chip host, or one bundle per logical TPU device when you pass `resources_per_bundle={"TPU": 1}`. For topologies such as TPU v6e `2x4` that can run on either one eight-chip VM or two four-chip VMs, set `chips_per_vm` to match your node pool.
- **Execution**: Call {func}`ray.util.tpu.slice_placement_group` to reserve slices and {func}`ray.util.tpu.run_on_slice` to launch tasks or actors across the bundles.
- **Ray Train**: {class}`~ray.train.ScalingConfig` creates a `SlicePlacementGroup` when you set `use_tpu=True` and `topology`. See {doc}`Configure scale and accelerators </train/user-guides/using-accelerators>`.
- **Ray Serve LLM**: {class}`~ray.serve.llm.LLMConfig` creates a `SlicePlacementGroup` when you set `accelerator_config={"kind": "tpu", "topology": ...}`. See {doc}`TPU serving </serve/llm/user-guides/tpu>`.

The following example reserves two multi-host slices and runs one host-level JAX task on every host:

```python
import ray
from ray.util.tpu import run_on_slice, slice_placement_group

# Reserve two v6e 4x4 TPU slices, each with four hosts and 16 chips.
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

For one task per logical TPU device instead, pass `resources_per_bundle={"TPU": 1}` as in the {ref}`Ray Core example <kuberay-tpu-get-started>`.

#### TPU Pod information utilities

The following helper functions in `ray.util.tpu` query TPU slice metadata and configure multi-slice coordination:

* {func}`ray.util.tpu.get_current_pod_name`: Returns the name of the TPU Pod slice that the worker belongs to, or `None` when the worker isn't on a TPU.
* {func}`ray.util.tpu.get_current_pod_worker_count`: Returns the number of hosts in the TPU Pod slice that the worker belongs to, or `None` when the worker isn't on a TPU.
* {func}`ray.util.tpu.get_num_tpu_chips_on_node`: Returns the number of TPU chips on the current node, or `0` when it finds none.
* {func}`ray.util.tpu.get_num_ready_tpu_slices`: Returns the number of ready, idle TPU slices that match `topology` and `accelerator_type`.
* {func}`ray.util.tpu.get_tpu_coordinator_env_vars`: Returns the `MEGASCALE_*` environment variables that multi-slice coordination requires.

## Reinforcement learning on TPUs

Reinforcement learning (RL) algorithms such as GRPO and PPO alternate between rollout generation and policy updates. Because only one process can hold a logical TPU device at a time, separate inference and training processes can't share a TPU device. RL workloads on TPUs use one of two placement patterns:

- **Disaggregated RL**: Rollout workers and training workers run on separate TPU slices, and rollout samples and updated weights transfer between the slices over DCN.
- **Colocated RL**: A single worker group runs on one TPU slice and executes both rollout generation and training inside the same process on each TPU host or device, which avoids cross-slice weight transfers.

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

# Start vLLM rollout workers on a TPU v6e 4x4 slice.
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


# Run distributed policy training on a second TPU v6e 4x4 slice.
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

(kuberay-tpu-autoscaling)=

## TPU autoscaling

The Ray autoscaler scales TPU worker groups the same way it scales any KubeRay worker group. When pending tasks, actors, or placement groups request `TPU` resources or TPU node labels that no running node can satisfy, the autoscaler adds replicas up to `maxReplicas`. When the nodes of a replica stay idle for longer than `idleTimeoutSeconds`, it removes the replica, down to `minReplicas`. Single-host TPU worker groups scale one Pod at a time. `SlicePlacementGroup`, `ScalingConfig(use_tpu=True)`, and `LLMConfig(accelerator_config=...)` all express their demand as placement groups that request TPUs on matching nodes, so a pending TPU workload can scale the matching worker group up from zero replicas.

### Multi-host autoscaling

Multi-host TPU autoscaling requires KubeRay 1.1.0 or later and Ray 2.32.0 or later. A multi-host TPU worker group sets `google.com/tpu` container limits or requests and `numOfHosts` greater than `1`. Ray treats each replica of the worker group as one TPU Pod slice and scales it atomically. Scaling up creates `numOfHosts` Ray nodes per replica, and scaling down removes `numOfHosts` Ray nodes per replica. When Ray schedules the deletion of a single node in a multi-host worker group, KubeRay terminates the whole replica that the node belongs to.

:::{caution}
Run tasks or actors on every host of a multi-host replica. Ray considers a host with no running work idle, and removing an idle host takes the rest of its slice with it.
:::

(kuberay-tpu-pod-config)=

## Configure Kubernetes Pods for TPUs

To use TPUs, set `google.com/tpu` in the `limits` and `requests` of the container in each TPU `workerGroupSpecs` entry of your RayCluster. This resource tells GKE how many TPU chips to attach to each Pod. KubeRay 1.1.0 and later also accept a `numOfHosts` field on the worker group, which sets the number of TPU hosts per replica. For multi-host worker groups, KubeRay treats each replica as a TPU Pod slice rather than a single worker and creates `numOfHosts` worker Pods per replica. GKE schedules the Pods onto the right machines with the `cloud.google.com/gke-tpu-accelerator` and `cloud.google.com/gke-tpu-topology` node selectors.

The following snippets show the `workerGroupSpecs` configuration for TPU v6e and TPU7x slices. For complete RayCluster manifests, see [`ray-cluster.tpu-v6e-singlehost.yaml`](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.tpu-v6e-singlehost.yaml) and [`ray-cluster.tpu-v6e-16-multihost.yaml`](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.tpu-v6e-16-multihost.yaml) in the KubeRay repository:

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
      # Set only for one-process-per-device workloads on TPU7x.
      resources: '"{\"TPU\": 8}"'
    template:
      spec:
        containers:
          - name: ray-worker
            image: rayproject/ray:2.59.0
            env:
              # Set on both ray-head and ray-worker for one-process-per-device workloads on TPU7x.
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

When you deploy a RayCluster on GKE with [`kuberay-tpu-webhook`](https://github.com/ai-on-gke/kuberay-tpu-webhook), the webhook validates that `numOfHosts` matches `cloud.google.com/gke-tpu-topology` and injects the following configuration into the TPU worker Pods:

- **Slice identity and affinity**: Sets `TPU_WORKER_ID`, `TPU_NAME`, the headless Service DNS `hostname` and `subdomain` fields, and `podAffinity` rules so that all workers of a replica schedule onto the same TPU slice.
- **Multi-host coordination**: Sets `TPU_WORKER_HOSTNAMES` on TPU v4 through TPU v6e slices, and `TPU_PROCESS_ADDRESSES` and `TPU_PROCESS_PORT` on TPU7x slices.
- **TorchTPU**: Sets `TORCH_TPU_TOPOLOGY` and `TORCH_TPU_SLICEBUILDER_ADDRESSES` in webhook v1.4.0 and later so that TorchTPU can initialize across hosts.
- **MegaScale multi-slice**: Sets `MEGASCALE_SLICE_ID`, `MEGASCALE_COORDINATOR_ADDRESS`, and `MEGASCALE_PORT` in webhook v1.3.0 and later when `MEGASCALE_NUM_SLICES` is present.
- **Ray dashboard metrics**: Sets `TPU_DEVICE_PLUGIN_HOST_IP` and `TPU_DEVICE_PLUGIN_ADDR` so that the Ray dashboard agent collects TPU utilization and HBM metrics. For the exported metric names, see {ref}`System metrics <system-metrics>`.

## Examples and guides

- {ref}`Serve a Stable Diffusion model on GKE with TPUs <kuberay-tpu-stable-diffusion-example>`: An end-to-end KubeRay example on single-host TPUs.
- [Train a model on multi-host TPUs with Ray Train and TorchTPU](https://cloud.google.com/kubernetes-engine/docs/tutorials/train-models-tpu-ray-torchtpu): A GKE tutorial for `TorchTrainer` on a multi-host slice.
- [Serve an LLM on multi-host TPUs with Ray Serve and TorchTPU](https://cloud.google.com/kubernetes-engine/docs/tutorials/serve-llm-tpu-ray-torchtpu): A GKE tutorial for Ray Serve LLM with vLLM-TorchTPU.
- {doc}`TPU serving </serve/llm/user-guides/tpu>`: How Ray Serve LLM places vLLM workers on TPU slices.
- {doc}`Configure scale and accelerators </train/user-guides/using-accelerators>`: Ray Train scaling configuration for TPUs.
- [PyTorch on Cloud TPU](https://cloud.google.com/tpu/docs/pytorch-on-tpu): Overview of the native PyTorch stack on Cloud TPU.
- [TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus): GKE documentation for TPU node pools and workloads.
