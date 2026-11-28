---
myst:
  html_meta:
    description: "Serve LLMs on single-host and multi-host TPU slices with Ray Serve LLM, including topology-aware placement groups and a multi-host vLLM TPU example."
---

(serve-llm-tpu)=
# TPU serving

Ray Serve LLM can run a vLLM TPU engine on single-host and multi-host TPU slices. A TPU slice is a group of interconnected TPU chips. Use TPU serving when your Ray cluster already exposes TPU resources and TPU node labels, and your container image includes either [vLLM-TPU](https://github.com/vllm-project/tpu-inference) or [vLLM-TorchTPU](https://github.com/vllm-project/vllm-torchtpu). For Kubernetes setup, see {doc}`Use TPUs with KubeRay </kuberay/user-guides/tpu>`.

## Topology and placement

A TPU topology describes the chip grid in one physical TPU slice. For example, a v6e `4x4` slice has 16 chips. Multi-host slices spread those chips across multiple TPU hosts connected by the TPU interconnect. For v6e `4x4`, that usually means four hosts with four chips each. For visualization, check out [TPU Topology Visualizer](https://tpu-visualizer.uc.r.appspot.com/).

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

The model still spans all 16 chips because `tensor_parallel_size=16`. The bundle shape only tells Ray how to reserve the hosts that own those chips. Set `placement_group_config={"bundle_per_worker": {"TPU": 1}}` only for per-device backends such as vLLM-TorchTPU. Host-level JAX vLLM-TPU expects one host-sized bundle per TPU VM, such as `{"TPU": 4}` on a four-chip host, and fails TPU initialization when given per-chip `{"TPU": 1}` bundles.

```{figure} ../images/ray_serve_llm_tpu.png
---
width: 100%
name: ray-serve-llm-tpu-placement
alt: One Ray Serve LLM replica spanning a v6e 4x4 TPU slice, with one placement group bundle per TPU host and four TPU chips reserved in each bundle.
---
Topology-aware TPU placement for one Ray Serve LLM replica.
```

:::{note}
TPU support in Ray Serve LLM is topology-aware when you set `accelerator_config={"kind": "tpu", "topology": ...}`. Without a topology, Ray Serve LLM creates a plain placement group that defaults to per-chip `{"TPU": 1}` bundles, which work only for per-device backends such as vLLM-TorchTPU and fail TPU initialization on host-level JAX vLLM-TPU.

A placement group created without a topology also omits `ray.io/tpu-slice-name` bundle label selectors. On clusters with more than one slice, even manually configured host-sized bundles in `placement_group_config` can split across slices depending on when nodes join the cluster, and multi-host TPU collective initialization then hangs and times out.
:::

## `SlicePlacementGroup`

For topology-aware TPU configs, Ray Serve LLM creates a {class}`~ray.util.tpu.SlicePlacementGroup` instead of a plain placement group. `SlicePlacementGroup` reserves a matching TPU slice, reads its `ray.io/tpu-slice-name` label, and creates the worker placement group with a per-bundle label selector that pins all bundles to that same physical slice.

This makes the TPU slice an atomic scheduling unit. A replica reserves the complete slice it needs, and the placement group doesn't span unrelated slices.

### Verify slice placement

On a running cluster, inspect active placement groups from the Ray head node to confirm that a replica's worker bundles are pinned to one slice:

```bash
ray list placement-groups --filter "state!=REMOVED" --detail
```

When `topology` is set, each replica creates a single-bundle `TPU-<pod-type>-head` reservation group and a worker placement group in which every TPU bundle carries the same `ray.io/tpu-slice-name` selector:

```yaml
bundles:
-   bundle_id:
        placement_group_id: 9d44a6f12de5cc35682133dc51b601000000
        bundle_index: 0
    unit_resources:
        CPU: 1.0
        TPU: 4.0
        accelerator_type:TPU-V6E: 0.001
    node_id: 334c90f45be1512aef1a971eb4abf2a021a9025f26814a2f5a379702
    label_selector:
        ray.io/tpu-slice-name: tpu-group-1
```

If `label_selector` is `{}` on the TPU bundles, `topology` wasn't applied and the replica isn't pinned to a single slice.

## How the TPU vLLM executor uses the bundles

The [vLLM-TPU Ray executor](https://github.com/vllm-project/tpu-inference/blob/main/tpu_inference/executors/ray_distributed_executor_v2.py) checks `parallel_config.placement_group`. If Ray Serve LLM already provides one, the executor reuses it instead of creating its own.

The executor then does the following:

1. Selects the TPU bundles from the placement group.
1. Reads the TPU count from each bundle.
1. Starts one Ray worker actor per bundle with `PlacementGroupSchedulingStrategy`, pinning each actor to its bundle.

With the default topology-aware bundles, one vLLM-TPU worker actor maps to one TPU host and consumes all local TPU chips on that host, such as `{"TPU": 4}` on a four-chip VM. Do not pass per-chip `{"TPU": 1}` bundles to vLLM-TPU; each worker actor then sees only one chip and TPU initialization fails.

Per-chip bundles apply only to per-device backends such as vLLM-TorchTPU, which runs one worker actor per logical TPU device. On `LLMConfig`, set `placement_group_config={"bundle_per_worker": {"TPU": 1}}`, and set `"TPU_MULTIHOST_BACKEND": "ray"` in `runtime_env`. For TPU7x, see {ref}`TPU7x logical devices <kuberay-tpu7x-dual-chiplet>`.

## Example

Build the container image from a vLLM-TPU or vLLM-TorchTPU base image that matches the vLLM version your Ray release pins. Ray 2.60.0 pins vLLM 0.29.0, so the Dockerfiles below pair `RAY_VERSION=2.60.0` with the `v0.29.0` base image tags. Update both versions together when upgrading.

Each Dockerfile installs `ray[serve]` with `grpcio-reflection` matched to the base image's `grpcio` version, then installs `ray[llm]` with `--no-deps` so pip keeps the base image's TPU vLLM and PyTorch or JAX packages instead of pulling PyPI CUDA wheels.

::::{tab-set}

:::{tab-item} Dockerfile (vLLM-TPU)

```dockerfile
FROM vllm/vllm-tpu:v0.29.0

ENV VLLM_TARGET_DEVICE=tpu
ENV VLLM_XLA_CACHE_PATH=/tmp/vllm_xla_cache
ENV JAX_PLATFORMS=tpu,cpu
ENV TPU_MULTIHOST_BACKEND=ray
ENV TPU_BACKEND_TYPE=jax
ENV ENABLE_PJRT_COMPATIBILITY=true

USER root

# Ray 2.60.0 pins vLLM 0.29.0. Update RAY_VERSION and the base image tag together.
ARG RAY_VERSION=2.60.0
RUN pip install --no-cache-dir \
        "ray[serve]==${RAY_VERSION}" \
        "grpcio-reflection==$(python -c 'import grpc; print(grpc.__version__)')" && \
    pip install --no-cache-dir --no-deps "ray[llm]==${RAY_VERSION}"

COPY serve_tpu_multihost.py /home/ray/serve_tpu_multihost.py
```
:::

:::{tab-item} Python (vLLM-TPU)

```{literalinclude} ../../../llm/doc_code/serve/tpu/serve_tpu_multihost.py
:language: python
:start-after: __serve_tpu_multihost_start__
:end-before: __serve_tpu_multihost_end__
```
:::

:::{tab-item} Dockerfile (vLLM-TorchTPU)

```dockerfile
FROM vllm/vllm-torchtpu:v0.29.0

USER root

# Ray 2.60.0 pins vLLM 0.29.0. Update RAY_VERSION and the base image tag together.
ARG RAY_VERSION=2.60.0
RUN pip install --no-cache-dir \
        "ray[serve]==${RAY_VERSION}" \
        "grpcio-reflection==$(python -c 'import grpc; print(grpc.__version__)')" && \
    pip install --no-cache-dir --no-deps "ray[llm]==${RAY_VERSION}"

COPY serve_tpu_multihost.py /home/ray/serve_tpu_multihost.py
```
:::

:::{tab-item} Python (vLLM-TorchTPU)

```{literalinclude} ../../../llm/doc_code/serve/tpu/serve_tpu_multihost.py
:language: python
:start-after: __serve_torchtpu_multihost_start__
:end-before: __serve_torchtpu_multihost_end__
```
:::

::::

The vLLM-TPU example serves `google/gemma-4-31B-it` on one v6e `4x4` slice with `tensor_parallel_size=16`. Set `model_source` to a local or mounted model path, or another path that all TPU hosts can read. The vLLM-TorchTPU example serves `Qwen/Qwen3-8B` across one v6e `4x4` slice with one worker per logical TPU device.

## See also

- {doc}`Use TPUs with KubeRay </kuberay/user-guides/tpu>`
- [Serve Gemma open models using multi-host TPUs on GKE with Ray](https://cloud.google.com/kubernetes-engine/docs/tutorials/serve-multi-host-tpu-llm)
- [Serve an LLM on multi-host TPUs with Ray Serve and TorchTPU](https://cloud.google.com/kubernetes-engine/docs/tutorials/serve-llm-tpu-ray-torchtpu)
