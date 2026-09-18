---
myst:
  html_meta:
    description: "Route LLM requests on measured KV cache overlap and token load with KVAwareRouter: installation, configuration, and tuning."
---

(kv-aware-routing-guide)=
# KV-aware routing

Route each request to the replica that gives the best balance of KV cache reuse and current token load.

:::{warning}
`KVAwareRouter` is in alpha and may change before becoming stable.
:::

The router accounts for cached prompt prefixes across replicas, so requests can reuse work from earlier requests.

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

Install the [NVIDIA Dynamo](https://github.com/ai-dynamo/dynamo) dependency in your cluster environment, for example in the image:

```bash
pip install "ai-dynamo>=1.4.0"
```

## Configuration

`KVAwareRouter` requires {ref}`direct streaming <direct-streaming-guide>` and request-body forwarding. Export all three environment variables before you start Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
```

:::{note}
With body forwarding enabled, `RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE` sets HAProxy's request-buffer cap (256 KiB by default). HAProxy sends only a prefix of larger bodies to the router. Raise it when `serve_haproxy_ingress_router_truncations_total` shows that this truncation is affecting body-aware routing; a larger buffer increases HAProxy memory use and can delay routing and TTFT.
:::

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

### Prefill/decode disaggregation

Configure `KVAwareRouter` on **both** `prefill_config` and `decode_config` when using
`build_pd_openai_app`. Use the same model and tokenizer configuration for both pools.
For example, adapt the configuration above:

```python
from ray.serve.llm import build_pd_openai_app

llm_config.engine_kwargs["kv_transfer_config"] = {
    "kv_connector": "NixlConnector",
    "kv_role": "kv_both",
}
app = build_pd_openai_app({
    "prefill_config": llm_config.model_copy(deep=True),
    "decode_config": llm_config.model_copy(deep=True),
})
serve.run(app)
```

Prefill routing balances prefix-cache reuse against load. Decode routing balances
load after prefill finishes. Generated tokens stream directly from the decode
server through HAProxy to the client.

Decode-progress reporting is off by default. Enable
`RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS=1` for load estimates that account for
generation progress, including load decay when the request specifies an output
token limit. This adds reporting overhead.

The HAProxy routing timeout includes prefill. Set
`RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S` high enough for your longest
expected prefill, including queueing. Requests must fit within HAProxy's routing
body limit; truncated requests are rejected.

This configuration supports only chat and completion requests with single text
prompts and data-parallel size 1. Other API endpoints, including model listing,
are not supported on this P/D routing path.
`MoRIIOConnector`, including inside `MultiConnector`, isn't supported with
`KVAwareRouter`.

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

For P/D deployments, cache-reuse and prefill weights apply to prefill selection.
Decode selection balances decode load.

For the full set of selection-service settings, see NVIDIA's [standalone selection service](https://docs.nvidia.com/dynamo/knowledge-base/modular-components/router/standalone-selection) documentation.

### Tokenization

Use the same model and tokenizer configuration across ingress and engine replicas.
Ingress tokenization adds work before generation; measure time to first token and
throughput with representative prompts before enabling KV-aware routing.

## Limitations

- **Direct streaming only.** `KVAwareRouter` inherits direct streaming's constraints, including one model per application and no LoRA- or multiplex-aware routing. See {ref}`direct-streaming-limitations`.
- **No data-parallel deployments.** The router doesn't yet score individual data-parallel ranks. Support is planned.
- **P/D constraints.** Disaggregated deployments require single text prompts. `MoRIIOConnector` is unsupported.

## See also

- {doc}`Direct streaming <direct-streaming>` - The ingress path KV-aware routing runs on
- {doc}`KV cache offloading <kv-cache-offloading>` - Extend the cache to host memory and let the router route to it
- {doc}`Prefix-aware routing <prefix-aware-routing>` - Text-based cache affinity without the extra dependency
- {ref}`routing-policies-guide` - Request routing concepts and available policies
- [Dynamo standalone selection service](https://docs.nvidia.com/dynamo/knowledge-base/modular-components/router/standalone-selection) - The selection service that scores replicas, and its full settings
