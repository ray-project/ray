---
myst:
  html_meta:
    description: "Route LLM requests on measured KV cache overlap and token load with KVAwareRouter: installation, configuration, router comparison, and architecture."
---

(kv-aware-routing-guide)=
# KV-aware routing

Route each request to the replica that gives the best balance of KV cache reuse and current token load.

:::{warning}
`KVAwareRouter` is in alpha and may change before becoming stable.
:::

Each replica reports KV block creation and eviction through vLLM-native KV events. The router uses these events to maintain a global view of KV cache state across replicas, allowing it to identify which replicas already hold KV blocks that overlap with a request’s prefix.

`KVAwareRouter` uses this overlap to estimate the request’s remaining prefill work for each replica. It then combines this remaining prefill work with the replica’s active prefill and decode work. We call this combined estimate the replica’s token load. The request is routed to the replica with the lowest estimated token load.

## When to use KV-aware routing

The best policy depends on your workload. You can configure different routers through the same `request_router_config`.

| Router | Routes on | Use it when | Cost |
| --- | --- | --- | --- |
| `RoundRobinRouter` | Request order | Prompts share little beyond the system prompt, and you want the simplest even spread. | No KV cache awareness. |
| `ConsistentHashRouter` | Hash of the `x-session-id` header | Clients can provide a session ID and each session’s history forms the reusable prefix. | A session with more turns or longer input or output sequences can saturate its replica. |
| `PrefixCacheAffinityRouter` | Prompt text matched against a router-maintained prefix tree | Requests frequently share a long textual prefixes. | Approximates engine KV cache state from prompt text and falls back to power of two choices when queue lengths become imbalanced. |
| `KVAwareRouter` | Token load: the request's remaining prefill work after accounting for KV cache overlap, plus the replica's active prefill and decode work | Replicas carry uneven token load, GPU memory is under pressure, or requests share prefixes beyond the system prompt. | Requires direct streaming, and its extra overhead may not pay off on simple, uniform workloads. |

## Installation

The router scores replicas with the selection service from [NVIDIA Dynamo](https://github.com/ai-dynamo/dynamo), which Ray Serve LLM runs in-process inside the ingress replica. Scoring is all it does: Ray Serve manages replica discovery, request orchestration, and request lifecycle. Install it in your cluster environment, for example in the image:

```bash
pip install "ai-dynamo>=1.4.0"
```

## Configuration

`KVAwareRouter` runs inside the `LLMRouter` ingress request router, so it needs the {ref}`direct streaming <direct-streaming-guide>` path. It scores on prompt tokens, so the ingress also needs the request body. Export all three environment variables before you start Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
```

Then select the router through `request_router_config`:

::::{tab-set}
:::{tab-item} Python
```python
from ray import serve
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.request_router import KVAwareRouter

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen3-0.6b",
        "model_source": "Qwen/Qwen3-0.6B",
    },
    deployment_config={
        "autoscaling_config": {"min_replicas": 2, "max_replicas": 2},
        "request_router_config": RequestRouterConfig(
            request_router_class=KVAwareRouter,
        ),
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: llm-kv-router
    route_prefix: /
    import_path: ray.serve.llm:build_openai_app
    args:
      llm_configs:
        - model_loading_config:
            model_id: qwen3-0.6b
            model_source: Qwen/Qwen3-0.6B
          deployment_config:
            autoscaling_config:
              min_replicas: 2
              max_replicas: 2
            request_router_config:
              request_router_class: ray.serve.llm.request_router.KVAwareRouter
```

Run `serve run config.yaml`.
:::
::::

### Tuning

Set these in the cluster environment:

| Environment variable | Default | Effect |
| --- | --- | --- |
| `RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE` | 1 | Ingress replicas per proxy node. Raise it when ingress tokenization and request scoring bound throughput. |
| `RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS` | 0 | Report decode progress as the engine generates tokens, for more accurate load tracking. Because each engine replica sends updates to every ingress replica, this can add network overhead at high concurrency. |
| `RAY_SERVE_LLM_KV_TOKEN_STAGING_TTL_S` | 60 | How long a replica holds a staged prompt-token payload before eviction. |
| `RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_ENTRIES` | 8192 | Staged payloads retained per engine replica. |
| `RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_BYTES` | 1 GiB | Memory allocated for staged payloads per engine replica. |

Set these as `experimental_configs` keys on the `LLMConfig`:

| `experimental_configs` key | Default | Effect |
| --- | --- | --- |
| `KV_INDEXER_THREADS` | 4 | Rust threads the router uses to ingest KV-cache events. |
| `KV_EVENTS_PORT_BASE` | 5557 | Base port for the engine's KV-cache event socket. Each replica takes the base plus its node-local rank. |
| `KV_TOKEN_PORT_BASE` | 7557 | Base port for the channel that carries prompt tokens to replicas. |

The router's scoring weights are also configurable. See [Tune the scoring weights](#tune-the-scoring-weights).

## How it works

Replica selection determines which `LLMServer` engine replica processes each request. This selection logic runs inside the `LLMRouter` ingress replica, with Dynamo’s selection service handling the scoring. The selection service maintains a global KV cache index and a view of the load on each replica, then uses this information to select the best candidate for each request. Ray Serve manages everything around this process:

- **Replica discovery.** Ray Serve tracks which `LLMServer` replicas are running and which are eligible to receive traffic. The router only scores the available replicas.
- **Request orchestration.** The ingress receives and tokenizes each request, selects a replica, and dispatches the request to it.
- **Request lifecycle.** Engine replicas report prefill completion, decode progress, and request completion back to the ingress, which uses these updates to maintain its view of replica token load.
- **Event transport and synchronization.** KV cache events are broadcast to all ingress replicas, while the ingress replicas synchronize token load updates with one another. This gives each ingress replica a consistent view of KV cache state and token load, enabling them to make consistent routing decisions.

```{figure} ../images/kv_aware_routing_flow.png
---
width: 800px
name: kv-aware-routing-flow
---
How a request flows through a KV-aware deployment.
```

1. The client sends a request to HAProxy.
1. HAProxy forwards the prompt to an `LLMRouter` ingress replica. The router tokenizes it and asks the selection service to score the running `LLMServer` replicas on KV cache overlap and token load.
1. The router returns the replica it selected.
1. HAProxy sends the request to that replica.
1. The replica streams the response back to the client with direct streaming.

### How a replica is chosen

Token load estimates how much work remains on each engine replica. It captures the two main phases of LLM inference: compute-bound prefill and memory-bound decode. For each candidate replica, the selection service estimates token load in KV blocks, and the router sends the request to the replica with the lowest estimated load.

- **Prefill load**, the compute-bound term. KV cache overlap tells the router how much of the incoming request’s prefill can be skipped. The remaining uncached tokens are combined with the replica’s active prefill work to estimate its total prefill load. GPU-resident KV blocks receive full cache credit, while CPU-offloaded blocks receive less because they must first be loaded back to the GPU.
- **Decode load**, the memory-bound term. During decoding, each step accesses the KV cache accumulated by active requests. This term captures the KV blocks associated with ongoing decode work, weighted by the estimated remaining output based on `max_tokens`.

### Tune the scoring weights

The selection service exposes several scoring weights that you can tune to match the characteristics of your workload. These weights are configured through DYN_* environment variables in the `LLMConfig` `runtime_env`. Ray Serve LLM passes them to the ingress replicas where scoring runs:

```python
llm_config = LLMConfig(
    # ...
    runtime_env={"env_vars": {"DYN_ROUTER_PREFILL_LOAD_SCALE": "2.0"}},
)
```

| Variable | Default | Effect |
| --- | --- | --- |
| `DYN_ROUTER_PREFILL_LOAD_SCALE` | 1.0 | Weight of the whole prefill term against decode load. Raise it for prefill-heavy traffic, and lower it for decode-heavy traffic. |
| `DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT` | 1.0 | Controls how much GPU-resident KV cache overlap reduces the prefill cost. Increase it when KV cache reuse is more important, or set it to 0.0 to ignore KV cache overlap and route based on load alone. |
| `DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT_DECAY` | 0.0, off | Reduces the benefit of KV cache overlap as a replica’s prefill backlog grows relative to the least-loaded candidate. Increase it when cache affinity repeatedly favors a busy replica while others remain underutilized. |
| `DYN_ROUTER_DECODE_ACTIVE_REQUEST_WEIGHT` | 0.0, off | Adds a cost for each request a replica is already serving. Increase it when many small requests tend to concentrate on the same replica. |

For the full set of selection-service settings, see NVIDIA's [standalone selection service](https://docs.nvidia.com/dynamo/knowledge-base/modular-components/router/standalone-selection) documentation.

### Tokenization at the ingress

The router scores requests using token IDs, so each request is tokenized at the ingress using the same renderer and chat template as the engine. This has two implications:

- **Tokenization adds CPU overhead at the ingress.** This is the primary reason to scale the number of ingress replicas.

- **Tokenization is not repeated at the engine.** The ingress sends the tokenized prompt to the selected engine replica, avoiding duplicate tokenization. The tokens are sent over a separate channel and may arrive before the corresponding HTTP request, so the engine replica temporarily **stages** them until the request arrives. Delivery is best effort: if the token payload is missing or has expired, the engine simply tokenizes the prompt again. The `RAY_SERVE_LLM_KV_TOKEN_STAGING_*` variables control how long and how much token data each engine replica can stage.


### Scaling the ingress tier

Serve runs one ingress replica per proxy node by default. Raise `RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE` when tokenizing and scoring at the ingress bound throughput. Two per node is usually enough, though the right number depends on your traffic.

Ray Serve LLM keeps the KV cache and token load views synchronized across ingress replicas. These views are **eventually consistent**: token load and engine updates are propagated in the background, so an ingress replica may briefly make routing decisions based on a slightly stale KV cache or token load view.

## Limitations

- **Direct streaming only.** `KVAwareRouter` inherits direct streaming's constraints, including one model per application and no LoRA- or multiplex-aware routing. See {ref}`direct-streaming-limitations`.
- **No data-parallel deployments.** The router doesn't yet score individual data-parallel ranks. Support is planned.
- **No prefill-decode disaggregation.** The router doesn't yet support disaggregated prefill and decode. Support is planned.

## See also

- {doc}`Direct streaming <direct-streaming>` - The ingress path KV-aware routing runs on
- {doc}`KV cache offloading <kv-cache-offloading>` - Extend the cache to host memory and let the router route to it
- {doc}`Prefix-aware routing <prefix-aware-routing>` - Text-based cache affinity without the extra dependency
- {ref}`routing-policies-guide` - Request routing concepts and available policies
- [Dynamo standalone selection service](https://docs.nvidia.com/dynamo/knowledge-base/modular-components/router/standalone-selection) - The selection service that scores replicas, and its full settings
