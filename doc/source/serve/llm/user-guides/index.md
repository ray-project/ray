# User guides

How-to guides for deploying, scaling, and operating Ray Serve LLM. If you are new, start with the {doc}`Quickstart <../quick-start>`, then come back here to go deeper.

## Configure and deploy

- {doc}`Configuration reference <configuration>`: every `LLMConfig` field, from model loading and engine kwargs to accelerators, placement, and deployment options.
- {doc}`Deployment initialization <deployment-initialization>`: speed up model loading and replica startup with caching, streaming load formats, and initialization callbacks.
- {doc}`Multi-LoRA deployment <multi-lora>`: serve many LoRA adapters on a shared base model with runtime switching and an LRU cache.

## Scale across GPUs and nodes

- {doc}`Cross-node parallelism <cross-node-parallelism>`: distribute a model across GPUs and nodes with tensor and pipeline parallelism and placement groups.
- {doc}`Data parallel attention <data-parallel-attention>`: replicate the model into coordinated data-parallel groups to raise throughput, especially for MoE models.
- {doc}`Fractional GPU serving <fractional-gpu>`: pack multiple small-model replicas onto a single GPU.

## Optimize latency and throughput

- {doc}`Prefill/decode disaggregation <prefill-decode>`: split prompt processing and token generation onto separate replicas to tune each independently.
- {doc}`KV cache offloading <kv-cache-offloading>`: extend KV cache capacity with LMCache and tiered storage backends.
- {doc}`Prefix-aware routing <prefix-aware-routing>`: route requests to replicas that already hold a matching prefix to maximize cache hits.
- {doc}`Direct streaming <direct-streaming>`: bypass the ingress when streaming tokens to cut per-token latency.

## Choose an engine

- {doc}`vLLM compatibility <vllm-compatibility>`: use vLLM features such as embeddings, structured outputs, vision, and reasoning through Ray Serve LLM.
- {doc}`SGLang integration <sglang>`: run SGLang as the inference engine instead of vLLM.

## Operate in production

- {doc}`Observability and monitoring <observability>`: engine and request metrics, Grafana dashboards, and Prometheus integration.

```{toctree}
:hidden:
:maxdepth: 1

Configuration reference <configuration>
Deployment initialization <deployment-initialization>
Multi-LoRA deployment <multi-lora>
Cross-node parallelism <cross-node-parallelism>
Data parallel attention <data-parallel-attention>
Fractional GPU serving <fractional-gpu>
Prefill/decode disaggregation <prefill-decode>
KV cache offloading <kv-cache-offloading>
Prefix-aware routing <prefix-aware-routing>
Direct streaming <direct-streaming>
vLLM compatibility <vllm-compatibility>
SGLang integration <sglang>
Observability and monitoring <observability>
```
