---
myst:
  html_meta:
    description: "Serve LLMs on single-host and multi-host TPU slices with Ray Serve LLM, including topology-aware placement groups and a multi-host vLLM TPU example."
---

(serve-llm-tpu)=
# TPU serving

Ray Serve LLM can run a vLLM TPU engine on single-host and multi-host TPU slices, where a TPU slice is a group of interconnected TPU chips. Use this when your Ray cluster already exposes TPU resources and TPU node labels, and your container image includes the TPU variant of vLLM from `tpu-inference`. For Kubernetes setup, see {doc}`Use TPUs with KubeRay </cluster/kubernetes/user-guides/tpu>`.

## Topology and placement

A TPU topology describes the chip grid in one physical TPU slice. For example, a v6e `4x4` slice has 16 chips. Multi-host slices spread those chips across multiple TPU hosts connected by the TPU interconnect. For v6e `4x4`, that usually means four hosts with four chips each.

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

The `tpu-inference` [Ray executor](https://github.com/vllm-project/tpu-inference/blob/main/tpu_inference/executors/ray_distributed_executor_v2.py) checks `parallel_config.placement_group`. When Ray Serve LLM already provided one, the executor reuses it instead of creating its own.

The executor then does the following:

1. Selects the TPU bundles from the placement group.
1. Reads the TPU count from each bundle.
1. Starts one Ray worker actor per bundle with `PlacementGroupSchedulingStrategy`, pinning each actor to its bundle.

With the default topology-aware bundles, one vLLM worker actor maps to one TPU host and consumes all local TPU chips on that host. The worker process uses the chips across the multi-host slice. Otherwise, manually setting `{"TPU": 1}` results in the creation of one worker per chip.

## Example

Build the image from the vLLM TPU base image and install a Ray wheel that includes TPU topology support.

::::{tab-set}

:::{tab-item} Dockerfile

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

:::{tab-item} Python

```{literalinclude} ../../../llm/doc_code/serve/tpu/serve_tpu_multihost.py
:language: python
:start-after: __serve_tpu_multihost_start__
:end-before: __serve_tpu_multihost_end__
```
:::

::::

This example serves `google/gemma-4-31B-it` on one v6e `4x4` slice with `tensor_parallel_size=16`. Set `model_source` to a local or mounted model path, or another path that all TPU hosts can read.

## See also

- {doc}`Use TPUs with KubeRay </cluster/kubernetes/user-guides/tpu>`
- [Serve Gemma open models using multi-host TPUs on GKE with Ray](https://docs.cloud.google.com/kubernetes-engine/docs/tutorials/serve-multi-host-tpu-llm)
