---
myst:
  html_meta:
    description: "Extend KV cache capacity by offloading to CPU memory or local disk with native vLLM offloading or LMCache, and compose backends with MultiConnector."
---

(kv-cache-offloading-guide)=
# KV cache offloading

Extend KV cache capacity by offloading to CPU memory or local disk for larger batch sizes and reduced GPU memory pressure.

:::{note}
KV cache offloading is a vLLM feature. Ray Serve LLM builds on it by optionally enabling KV-aware routing across GPU and offloaded KV cache tiers. This guide covers vLLM’s native CPU backend, the LMCache integration, and composing multiple backends with MultiConnector.
:::


Benefits of KV cache offloading:

- **Increased capacity**: Store more KV caches by using CPU RAM or local storage instead of relying solely on GPU memory.
- **Cache reuse across requests**: Save and reuse previously computed KV caches for repeated or similar prompts, reducing prefill computation and improve TTFT.
- **Flexible storage backends**: Choose from multiple storage options including local CPU, disk, or distributed systems.

KV cache offloading matters most when there is GPU memory pressure:

- **Long-running services**: multi-turn conversations and agent sessions return after a pause, by which point the GPU has evicted their history and the engine recomputes the entire prefill. A CPU tier carries that history across the gap.
- **Long context at high concurrency**: a handful of long prompts fills the GPU block pool on its own, so even prefixes that concurrent requests share get evicted before the next request can reuse them.

## Choose a backend

All three backends extend KV cache capacity, but their integration with Ray Serve LLM differs:

| Backend | Configure with | KV-aware routing sees the offloaded tier | Grafana KV Offload panels |
| --- | --- | --- | --- |
| Native CPU | `kv_offloading_backend` and `kv_offloading_size` | Yes | All |
| LMCache | `kv_transfer_config`, plus LMCache environment variables | No | Hit-rate panels only |
| MultiConnector | `kv_transfer_config` | No | Hit-rate panels only |

`KVAwareRouter` indexes the GPU KV cache events that Ray Serve LLM enables on every KV-aware deployment. Tracking blocks in the offloaded tier additionally needs CPU-tier events, which Ray Serve LLM enables only for the native backend. With LMCache or MultiConnector the router still routes, but scores an offloaded prefix as though it weren't cached.

(native-kv-cache-offloading)=
## Offload to CPU memory with native vLLM offloading

vLLM's native backend moves evicted KV blocks to CPU memory instead of discarding them, allowing them to be reloaded on a later cache hit.

Enable it with two `engine_kwargs`:

- **`kv_offloading_size`**: CPU KV cache capacity per replica, in GiB. With tensor parallelism, this is the total across all TP ranks. Offloading is disabled unless this value is set.
- **`kv_offloading_backend`**: Set to `"native"` for vLLM's built-in CPU offloading. Setting it to `"lmcache"` uses `LMCacheMPConnector` instead, where LMCache manages the capacity and `kv_offloading_size` doesn't apply. See {ref}`lmcache-kv-offloading` for direct LMCache configuration.

Native offloading builds on Automatic Prefix Caching, so keep `enable_prefix_caching` set to `True`. For the connector's full option set, including multi-tier setups that add disk or object storage behind the CPU tier, see the vLLM [KV offloading usage guide](https://docs.vllm.ai/en/stable/features/kv_offloading_usage/).

::::{tab-set}
:::{tab-item} Python
```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen3-0.6b",
        "model_source": "Qwen/Qwen3-0.6B",
    },
    deployment_config={
        "autoscaling_config": {"min_replicas": 2, "max_replicas": 2},
    },
    engine_kwargs={
        "enable_prefix_caching": True,
        "kv_offloading_backend": "native",
        "kv_offloading_size": 8,  # GiB of CPU KV cache per replica
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: llm-with-native-offload
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
          engine_kwargs:
            enable_prefix_caching: true
            kv_offloading_backend: native
            kv_offloading_size: 8  # GiB of CPU KV cache per replica
```

Deploy with:

```bash
serve run config.yaml
```
:::
::::

### Combine offloading with a request router

Offloading pays off under any request router. Wherever a request lands, that replica can reload a matching prefix from its CPU tier instead of recomputing the prefill, so a router that already sends related requests to the same replica simply gets more reuse out of the larger cache.

`KVAwareRouter` is the only router with visibility into the offloaded tier. It tracks which replica holds a prefix in CPU memory and credits that match less than a GPU-resident one, so the reload's transfer cost is part of the routing decision. Every other router scores replicas without knowing the prefix exists off-GPU.

For router configuration, see {ref}`kv-aware-routing-guide` and {ref}`routing-policies-guide`.

### Monitor offloading and reloading

The Serve LLM Grafana dashboard includes a **KV Cache Offload / Reload** row, collapsed by default.

```{figure} ../images/kv_offload_dashboard.png
---
width: 800px
name: kv-offload-dashboard
---
The KV Cache Offload / Reload row expanded. Every panel breaks down by `LLMServer` replica.
```

| Panel | Metric | What to look for |
| --- | --- | --- |
| Store / Reload Throughput | `ray_vllm_kv_offload_store_bytes_total`, `ray_vllm_kv_offload_load_bytes_total` | GPU-to-CPU and CPU-to-GPU data rates. Store traffic without reloads suggests offloaded blocks aren't being reused. |
| Store and Reload Operations/s | `ray_vllm_kv_offload_store_size_count`, `ray_vllm_kv_offload_load_size_count` | Transfer operations per second. Compare with throughput to identify many small transfers. |
| Store / Reload Bandwidth | Bytes over transfer time | Effective transfer bandwidth. Helps distinguish low traffic from slow transfers. |
| CPU Capacity Pinned by Transfers | `ray_vllm_kv_offload_cpu_cache_usage_perc` and its read and write splits | CPU cache capacity pinned by in-flight transfers. Sustained values near 100% may cause dropped transfers; consider increasing `kv_offloading_size`. |
| External Prefix Hit Rate | `ray_vllm_external_prefix_cache_hits_total` over `..._queries_total` | Share of prefix lookups served from the offloaded tier. |
| Overall Prefix Hit Rate | GPU and external hits over `ray_vllm_prefix_cache_queries_total` | Combined hit rate versus GPU-only. The gap shows the additional reuse from offloading. |
| Lookup Delay P90 | `ray_vllm_kv_offload_lookup_sync_delay_seconds` | P90 lookup latency before prefill. Monitor alongside TTFT when tuning the offloading buffer. |

(lmcache-kv-offloading)=
## Deploy with LMCache

LMCache provides KV cache offloading with support for multiple storage backends.

### Prerequisites

Install LMCache:

```bash
uv pip install lmcache
```

### Basic deployment

The following example shows how to deploy with LMCache for local CPU offloading:

::::{tab-set}
:::{tab-item} Python
```python
from ray.serve.llm import LLMConfig, build_openai_app
import ray.serve as serve

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct"
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "kv_transfer_config": {
            "kv_connector": "LMCacheConnectorV1",
            "kv_role": "kv_both",
        }
    },
    runtime_env={
        "env_vars": {
            "LMCACHE_LOCAL_CPU": "True",
            "LMCACHE_CHUNK_SIZE": "256",
            "LMCACHE_MAX_LOCAL_CPU_SIZE": "100",  # 100GB
        }
    }
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: llm-with-lmcache
    route_prefix: /
    import_path: ray.serve.llm:build_openai_app
    runtime_env:
      env_vars:
        LMCACHE_LOCAL_CPU: "True"
        LMCACHE_CHUNK_SIZE: "256"
        LMCACHE_MAX_LOCAL_CPU_SIZE: "100"
    args:
      llm_configs:
        - model_loading_config:
            model_id: qwen-0.5b
            model_source: Qwen/Qwen2-0.5B-Instruct
          engine_kwargs:
            tensor_parallel_size: 1
            kv_transfer_config:
              kv_connector: LMCacheConnectorV1
              kv_role: kv_both
```

Deploy with:

```bash
serve run config.yaml
```
:::
::::

## Compose multiple KV transfer backends with MultiConnector

You can combine multiple KV transfer backends using `MultiConnector`. This is useful when you want both local offloading and cross-instance transfer in disaggregated deployments.

### When to use MultiConnector

Use `MultiConnector` to combine multiple backends when you're using prefill/decode disaggregation and want both cross-instance transfer (NIXL) and local offloading.


The following example shows how to combine NIXL (for cross-instance transfer) with LMCache (for local offloading) in a prefill/decode deployment:

:::{note}
The order of connectors matters. Since you want to prioritize local KV cache lookup through LMCache, it appears first in the list before the NIXL connector.
:::

::::{tab-set}
:::{tab-item} Python
```python
from ray.serve.llm import LLMConfig, build_pd_openai_app
import ray.serve as serve

# Shared KV transfer config combining NIXL and LMCache
kv_config = {
    "kv_connector": "MultiConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "connectors": [
            {
                "kv_connector": "LMCacheConnectorV1",
                "kv_role": "kv_both",
            },
            {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
                "backends": ["UCX"],
            }
        ]
    }
}

prefill_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct"
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "kv_transfer_config": kv_config,
    },
    runtime_env={
        "env_vars": {
            "LMCACHE_LOCAL_CPU": "True",
            "LMCACHE_CHUNK_SIZE": "256",
            "UCX_TLS": "all",
        }
    }
)

decode_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct"
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "kv_transfer_config": kv_config,
    },
    runtime_env={
        "env_vars": {
            "LMCACHE_LOCAL_CPU": "True",
            "LMCACHE_CHUNK_SIZE": "256",
            "UCX_TLS": "all",
        }
    }
)

pd_config = {
    "prefill_config": prefill_config,
    "decode_config": decode_config,
}

app = build_pd_openai_app(pd_config)
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: pd-multiconnector
    route_prefix: /
    import_path: ray.serve.llm:build_pd_openai_app
    runtime_env:
      env_vars:
        LMCACHE_LOCAL_CPU: "True"
        LMCACHE_CHUNK_SIZE: "256"
        UCX_TLS: "all"
    args:
      prefill_config:
        model_loading_config:
          model_id: qwen-0.5b
          model_source: Qwen/Qwen2-0.5B-Instruct
        engine_kwargs:
          tensor_parallel_size: 1
          kv_transfer_config:
            kv_connector: MultiConnector
            kv_role: kv_both
            kv_connector_extra_config:
              connectors:
                - kv_connector: LMCacheConnectorV1
                  kv_role: kv_both
                - kv_connector: NixlConnector
                  kv_role: kv_both
                  backends: ["UCX"]
      decode_config:
        model_loading_config:
          model_id: qwen-0.5b
          model_source: Qwen/Qwen2-0.5B-Instruct
        engine_kwargs:
          tensor_parallel_size: 1
          kv_transfer_config:
            kv_connector: MultiConnector
            kv_role: kv_both
            kv_connector_extra_config:
              connectors:
                - kv_connector: LMCacheConnectorV1
                  kv_role: kv_both
                - kv_connector: NixlConnector
                  kv_role: kv_both
                  backends: ["UCX"]
```

Deploy with:

```bash
serve run config.yaml
```
:::
::::

## Configuration parameters

### LMCache environment variables

- `LMCACHE_LOCAL_CPU`: Set to `"True"` to enable local CPU offloading
- `LMCACHE_CHUNK_SIZE`: Size of KV cache chunks, in terms of tokens (default: 256)
- `LMCACHE_MAX_LOCAL_CPU_SIZE`: Maximum CPU storage size in GB
- `LMCACHE_PD_BUFFER_DEVICE`: Buffer device for prefill/decode scenarios (default: "cpu")

For the full list of LMCache configuration options, see the [LMCache configuration reference](https://docs.lmcache.ai/api_reference/configurations.html).

### MultiConnector configuration

- `kv_connector`: Set to `"MultiConnector"` to compose multiple backends
- `kv_connector_extra_config.connectors`: List of connector configurations to compose. Order matters—connectors earlier in the list take priority.
- Each connector in the list uses the same configuration format as standalone connectors

## Performance considerations

Extending KV cache beyond local GPU memory introduces overhead for managing and looking up caches across different memory hierarchies. This creates a tradeoff: you gain larger cache capacity but may experience increased latency. Consider these factors:

**Overhead in cache-miss scenarios**: When there are no cache hits, offloading adds modest overhead (~10-15%) compared to pure GPU caching, based on our internal experiments. This overhead comes from the additional hashing, data movement, and management operations.

**Benefits with cache hits**: When caches can be reused, offloading significantly reduces prefill computation. For example, in multi-turn conversations where users return after minutes of inactivity, LMCache retrieves the conversation history from CPU rather than recomputing it, significantly reducing time to first token for follow-up requests.

**Network transfer costs**: When combining MultiConnector with cross-instance transfer (such as NIXL), ensure that the benefits of disaggregation outweigh the network transfer costs.


## See also

- {doc}`Prefill/decode disaggregation <prefill-decode>` - Deploy LLMs with separated prefill and decode phases
- [vLLM KV offloading usage guide](https://docs.vllm.ai/en/stable/features/kv_offloading_usage/) - `OffloadingConnector` options, tiering specs, and tuning
- {ref}`kv-aware-routing-guide` - Route on measured KV cache overlap and token load
- {ref}`routing-policies-guide` - Request routing concepts and the available routing policies
- {doc}`Observability and monitoring <observability>` - Engine metrics, Grafana dashboards, and Prometheus integration
- [LMCache documentation](https://docs.lmcache.ai/) - LMCache configuration and features
