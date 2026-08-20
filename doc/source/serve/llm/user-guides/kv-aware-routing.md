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

Every replica reports the KV blocks it caches and evicts through vLLM-native KV events. The router keeps a global KV index of those reports, so each routing decision knows which replicas already hold the request's prefix KV caches.

`KVAwareRouter` turns that overlap into a number: how much prefill each candidate remains. It adds the replica's current decode work and routes to the lowest total. This guide calls that total the replica's **token load**.

## When to use KV-aware routing

The best policy depends on the workload. You can configure different routers through the same `request_router_config`.

| Router | Routes on | Use it when | Cost |
| --- | --- | --- | --- |
| `RoundRobinRouter` | Request order | Prompts share little beyond the system prompt, and you want the simplest even spread. | No KV cache awareness. |
| `ConsistentHashRouter` | Hash of the `x-session-id` header | Clients can supply a session ID, and each session's history is the reusable prefix. | A session with more turns or longer input or output sequences than the rest can saturate its replica. |
| `PrefixCacheAffinityRouter` | Prompt text, matched against a prefix tree the router maintains | Requests share a long textual prefix. | Approximates engine KV cache state from text, and falls back to power of two choices when queue lengths diverge. |
| `KVAwareRouter` | Token load: remaining prefill after KV cache overlap, plus decode work | Replicas carry uneven token load, GPU memory is under pressure, or requests share prefixes beyond the system prompt. | Requires direct streaming, and its extra overhead may not pay off on simple, uniform workloads. |

## Installation

The router scores replicas with the selection service from [NVIDIA Dynamo](https://github.com/ai-dynamo/dynamo), which Ray Serve LLM runs in-process inside the ingress replica. Scoring is all it does: Ray Serve manages replica discovery, request orchestration, and request lifecycle. Install it in your cluster environment, either in the image or through the deployment's `runtime_env`:

```bash
pip install "ai-dynamo"
```

## Configuration

`KVAwareRouter` runs inside the `LLMRouter` ingress request router, so it needs the {ref}`direct streaming <direct-streaming-guide>` path. It scores on prompt tokens, so the ingress also needs the request body. Export all three variables before you start Serve:

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

Keep three things in mind:

- **Ray Serve LLM handles the rest of the wiring.** It sets up the engine's KV-cache event stream, the per-replica ports those events use, and the process settings that keep KV block hashes consistent across replicas.
- **Leave `enable_prefix_caching` on.** It's the vLLM default. The engine emits KV-cache events only for the blocks it caches, so with prefix caching off the router has nothing to score.
- **Offloaded KV cache counts.** Turn on {ref}`KV cache offloading <native-kv-cache-offloading>` and the router tracks the KV cache blocks that spilled to external memory alongside those still resident on GPU.

### Tuning

Set these in the cluster environment or the deployment's `runtime_env`:

| Environment variable | Default | Effect |
| --- | --- | --- |
| `RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE` | 1 | Ingress replicas per proxy node. Raise it when ingress tokenization and scoring bound throughput. |
| `RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS` | 0 | Report decode progress as the engine generates tokens, for more accurate load tracking. Because each replica sends updates to every ingress replica, this can add network overhead at high concurrency. Enable it only when you need it. |
| `RAY_SERVE_LLM_KV_TOKEN_STAGING_TTL_S` | 60 | How long a replica holds a staged prompt-token payload before dropping it. |
| `RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_ENTRIES` | 8192 | Staged payloads retained per replica. |
| `RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_BYTES` | 1 GiB | Memory a replica devotes to staged payloads. |

Set these as `experimental_configs` keys on the `LLMConfig`:

| `experimental_configs` key | Default | Effect |
| --- | --- | --- |
| `KV_INDEXER_THREADS` | 4 | Threads the router uses to ingest KV-cache events. |
| `KV_EVENTS_PORT_BASE` | 5557 | Base port for the engine's KV-cache event socket. Each replica takes the base plus its node-local rank. |
| `KV_TOKEN_PORT_BASE` | 7557 | Base port for the channel that carries prompt tokens to replicas. |

The router's scoring weights are tunable too. See [Tune the scoring weights](#tune-the-scoring-weights) once you've read how scoring works.

## How it works

Replica selection runs inside the `LLMRouter` ingress replica. Scoring is the one part Dynamo's selection service handles. It runs in-process in the `LLMRouter` replica, holds the global KV cache index and the per-replica load view, and determines which candidate replica should serve the request. Ray Serve manages everything around that:

- **Replica discovery.** Ray Serve tracks which `LLMServer` replicas are running and which are eligible to receive traffic, through the same deployment, autoscaling, and health machinery every Serve app uses. The router scores only the replicas Ray Serve offers it.
- **Request orchestration.** The ingress receives the request, tokenizes it, asks for a replica, and dispatches the request there.
- **Request lifecycle.** Engine replicas report prefill completion, decode progress, and completion back to the ingress, which applies them to the load view.
- **Event transport.** Ray Serve LLM configures each engine's KV cache event stream and its ports, and keeps the ingress replicas in sync with one another.

```{figure} ../images/kv_aware_routing_flow.png
---
width: 800px
name: kv-aware-routing-flow
---
How a request flows through a KV-aware deployment.
```

1. The client sends a request to HAProxy.
2. HAProxy forwards the prompt to an `LLMRouter` ingress replica. The router tokenizes it and asks the selection service to score the running `LLMServer` replicas on KV cache overlap and token load.
3. The router returns the replica it selected.
4. HAProxy sends the request to that replica.
5. The replica streams the response back to the client with direct streaming.

### What the router tracks

Each engine replica reports every KV block it caches and evicts. The selection service folds those reports into one global index, so at any moment it knows which replicas hold which KV caches, including KV caches that spilled to the CPU tier when you enable KV cache offloading.

The index follows the replica set. A new replica becomes scorable once it reports in, and a replica that leaves has its blocks and its in-flight requests dropped. An ingress replica that restarts rebuilds its index from the engines rather than routing against an empty one.

### How a replica is chosen

Token load estimates how much work each engine is carrying. It combines the two phases of LLM inference: compute-bound prefill and memory-bound decode. The selection service estimates this load for each candidate replica in KV blocks, and the router sends the request to the replica with the lowest load:

- **Remaining prefill tokens**, the compute-bound term. The engine reuses any matching KV cache and computes only the remaining input tokens. This term includes those remaining tokens and the prefill already queued on the replica, with credit for cached blocks. GPU-resident blocks receive full credit, while CPU-offloaded blocks receive less because they must be reloaded.
- **Decode tokens**, the memory-bound term. After prefill, each decode step reads the KV cache accumulated so far. This term captures the KV blocks used by active decoding requests, weighted by their estimated remaining output based on `max_tokens`.

The router is more likely to select a replica with a long KV cache match, but cache locality isn't the only factor. A CPU-resident match provides less benefit than a GPU-resident one, and a busy replica can lose to a less loaded replica with a shorter match.

### Tune the scoring weights

The selection service reads its weights from `DYN_*` environment variables, which you set in the `LLMConfig`'s `runtime_env`. Ray Serve LLM passes them to the ingress replica where scoring runs:

```python
llm_config = LLMConfig(
    # ...
    runtime_env={"env_vars": {"DYN_ROUTER_PREFILL_LOAD_SCALE": "2.0"}},
)
```

| Variable | Default | Effect |
| --- | --- | --- |
| `DYN_ROUTER_PREFILL_LOAD_SCALE` | 1.0 | Weight of the whole prefill term against decode load. Raise it for prefill-heavy traffic, and lower it for decode-heavy traffic. |
| `DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT` | 1.0 | Credit a GPU-resident cache hit earns against the prefill term. Raise it above 1.0 when cache hit rate matters more than even load. Set it to 0.0 to ignore KV cache overlap entirely and route on load alone. |
| `DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT_DECAY` | 0.0, off | Fades cache credit as a replica's prefill backlog grows past the least-loaded candidate. Raise it when one replica keeps winning on cache affinity while others sit idle. |
| `DYN_ROUTER_DECODE_ACTIVE_REQUEST_WEIGHT` | 0.0, off | Charge per request a replica is already serving. Raise it when many small requests concentrate on one replica. |

For the full set of selection-service settings, see NVIDIA's [standalone selection service](https://docs.nvidia.com/dynamo/knowledge-base/modular-components/router/standalone-selection) documentation.

### Tokenization at the ingress

The router scores on token ids, so it tokenizes every request at the ingress, using the same renderer and chat template as the engine. That has two consequences worth planning for:

- Tokenizing is real CPU work on the ingress, and it's the main reason to add ingress replicas.
- The ingress hands the tokens it produced to the chosen replica, so the engine doesn't tokenize the same prompt twice. The tokens travel on their own channel and arrive before the HTTP request does, so the replica holds them, or *stages* them, until the matching request shows up. Delivery is best effort: if a payload never arrives or has already expired, the engine tokenizes the prompt itself. The `RAY_SERVE_LLM_KV_TOKEN_STAGING_*` variables bound how long and how much each replica stages.

### Scaling the ingress tier

Serve runs one ingress replica per proxy node by default. Raise `RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE` when tokenizing and scoring at the ingress bound throughput. Two per node is usually enough, though the right number depends on your traffic.

Ray Serve LLM keeps every ingress replica's view of cache and load in sync, so adding replicas doesn't cost routing quality. Those views are eventually consistent: replicas broadcast their bookings and the engines' reports in the background, so one can score against a slightly stale view for a moment before it catches up.

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
