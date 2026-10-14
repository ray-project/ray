---
myst:
  html_meta:
    description: "Serve LLMs on single-host and multi-host TPU slices with Ray Serve LLM, including topology-aware placement groups and a multi-host vLLM TPU example."
---

(serve-llm-tpu)=
# TPU serving

Ray Serve LLM can run a vLLM TPU engine on single-host and multi-host TPU slices, where a TPU slice is a group of interconnected TPU chips. Use this when your Ray cluster already exposes TPU resources and TPU node labels, and your container image includes either the JAX TPU variant of vLLM (`tpu-inference`) or the PyTorch `torch_tpu` variant (`vllm-torchtpu`). For Kubernetes setup, see {doc}`Use TPUs with KubeRay </kuberay/user-guides/tpu>`.

## Topology and placement

A TPU topology describes the chip grid in one physical TPU slice. For example, a [TPU v6e (Trillium)](https://cloud.google.com/tpu/docs/v6e) `4x4` slice has 16 chips. Multi-host slices spread those chips across multiple TPU hosts connected by the TPU interconnect. For v6e `4x4`, that usually means four hosts with four chips each. For visualization, check out [TPU Topology Visualizer](https://tpu-visualizer.uc.r.appspot.com/).

Ray Serve LLM uses `tensor_parallel_size * pipeline_parallel_size` as the number of TPU chips that one model replica requests. When you also set a TPU topology, Ray Serve LLM computes the number of chips per host and creates one placement group bundle per TPU host:

```python
LLMConfig(
    accelerator_type="TPU-V6E",
    accelerator_config={"kind": "tpu", "topology": "4x4"},
    engine_kwargs={"tensor_parallel_size": 16},
)
```

For a v6e `4x4` slice, this produces four host-level bundles:

```python
[
    {"TPU": 4, "accelerator_type:TPU-V6E": 0.001},
    {"TPU": 4, "accelerator_type:TPU-V6E": 0.001},
    {"TPU": 4, "accelerator_type:TPU-V6E": 0.001},
    {"TPU": 4, "accelerator_type:TPU-V6E": 0.001},
]
```

The model still spans all 16 chips because `tensor_parallel_size=16`. The bundle shape only tells Ray how to reserve the hosts that own those chips. If you need per-chip bundles, set `placement_group_config={"bundle_per_worker": {"TPU": 1}}`.

```{figure} ../images/ray_serve_llm_tpu.png
---
width: 100%
name: ray-serve-llm-tpu-placement
alt: One Ray Serve LLM replica spanning a v6e 4x4 TPU slice, with one placement group bundle per TPU host and four TPU chips reserved in each bundle.
---
Topology-aware TPU placement for one Ray Serve LLM replica.
```

:::{note}
TPU support in Ray Serve LLM is topology-aware when you set `accelerator_config={"kind": "tpu", "topology": ...}`. Without a topology, Ray Serve LLM falls back to a regular placement group with per-chip `{"TPU": 1}` bundles.
:::

## `SlicePlacementGroup`

For topology-aware TPU configs, Ray Serve LLM creates a {class}`~ray.util.tpu.SlicePlacementGroup` instead of a plain placement group. `SlicePlacementGroup` reserves a matching TPU slice, reads its `ray.io/tpu-slice-name` label, and creates the worker placement group with a per-bundle label selector that pins all bundles to that same physical slice.

This makes the TPU slice an atomic scheduling unit. A replica reserves the complete slice it needs, and the placement group doesn't span unrelated slices.

## How the TPU vLLM executor uses the bundles

Ray Serve LLM supports two vLLM TPU execution backends:

- **JAX backend (`tpu-inference`):** The `tpu-inference` [Ray executor](https://github.com/vllm-project/tpu-inference/blob/main/tpu_inference/executors/ray_distributed_executor_v2.py) checks `parallel_config.placement_group`. When Ray Serve LLM provides one, the executor reuses it, selects the TPU bundles, and starts one Ray worker actor per bundle with `PlacementGroupSchedulingStrategy`. With the default topology-aware bundles, one vLLM worker actor maps to one TPU host (`{"TPU": 4}`) and manages all local TPU devices on that host.
- **PyTorch backend (`vllm-torchtpu`):** When serving with `vllm-torchtpu` (`torch_tpu`), vLLM runs one `RayWorkerWrapper` actor per logical TPU device (`{"TPU": 1}`). Set `placement_group_config={"bundle_per_worker": {"TPU": 1}}` and `"TPU_MULTIHOST_BACKEND": "ray"` in `runtime_env` on `LLMConfig` so that `SlicePlacementGroup` creates one `{"TPU": 1}` bundle per logical TPU device across the reserved slice.

:::{note}
On [Ironwood (`TPU-V7X`)](https://cloud.google.com/kubernetes-engine/docs/concepts/tpu-ironwood), each dual-chiplet physical chip exposes two logical TPU devices (`8` logical devices per four-chip host). For the JAX backend (`tpu-inference`), which runs one worker per host, use the default `{"TPU": 4}` host bundles (`RAY_TPU_RESOURCE_PER_CHIP` isn't required). For `vllm-torchtpu`, which runs one worker per logical TPU device (`{"TPU": 1}`), configure your Ray worker Pods with `RAY_TPU_RESOURCE_PER_CHIP=2` and `resources: {TPU: "8"}` so `SlicePlacementGroup` creates 16 per-device bundles across the two hosts in a `2x2x2` slice.
:::

## Example

Build the container image from the corresponding vLLM TPU base image and install a Ray version that includes TPU topology support:

::::{tab-set}

:::{tab-item} Dockerfile (JAX / tpu-inference)

```dockerfile
FROM vllm/vllm-tpu:v0.21.0

ENV VLLM_TARGET_DEVICE=tpu
ENV VLLM_XLA_CACHE_PATH=/tmp/vllm_xla_cache
ENV JAX_PLATFORMS=tpu,cpu
ENV TPU_MULTIHOST_BACKEND=ray
ENV TPU_BACKEND_TYPE=jax
ENV ENABLE_PJRT_COMPATIBILITY=true

USER root

# Use a released Ray version or wheel URL that contains TPU topology support.
ARG RAY_PACKAGE="ray"
RUN pip install --no-cache-dir -U "${RAY_PACKAGE}" && \
    pip install --no-cache-dir --no-deps "ray[llm]"

COPY serve_tpu_multihost.py /home/ray/serve_tpu_multihost.py
```
:::

:::{tab-item} Python (JAX / tpu-inference)

```{literalinclude} ../../../llm/doc_code/serve/tpu/serve_tpu_multihost.py
:language: python
:start-after: __serve_tpu_multihost_start__
:end-before: __serve_tpu_multihost_end__
```
:::

:::{tab-item} Dockerfile (PyTorch / vllm-torchtpu)

```dockerfile
FROM us-docker.pkg.dev/cloud-tpu-images/torch-tpu/vllm-tpu:0.1.0

ARG RAY_PACKAGE="ray[default,llm]"
RUN pip install --no-cache-dir -U "${RAY_PACKAGE}"

COPY serve_tpu_multihost.py /home/ray/serve_tpu_multihost.py
```
:::

:::{tab-item} Python (PyTorch / vllm-torchtpu)

```{literalinclude} ../../../llm/doc_code/serve/tpu/serve_tpu_multihost.py
:language: python
:start-after: __serve_torchtpu_multihost_start__
:end-before: __serve_torchtpu_multihost_end__
```
:::

::::

These examples serve a model across one `v6e` `4x4` slice (16 TPU devices across four hosts) with `tensor_parallel_size=16`. Set `model_source` to a Hugging Face model ID or a storage path that all TPU hosts can read.

## See also

- {doc}`Use TPUs with KubeRay </kuberay/user-guides/tpu>`
- [Serve Gemma open models using multi-host TPUs on GKE with Ray](https://docs.cloud.google.com/kubernetes-engine/docs/tutorials/serve-multi-host-tpu-llm)
- [Serve an LLM on multi-host TPUs with Ray Serve and TorchTPU](https://docs.cloud.google.com/kubernetes-engine/docs/tutorials/serve-llm-tpu-ray-torchtpu)
