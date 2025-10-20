(serve-llm-architecture-deepseek-v3)=
# DeepSeek-V3 pattern

The DeepSeek-V3 pattern demonstrates how to compose multiple serving patterns for efficient deployment of large sparse Mixture-of-Experts (MoE) models with Multi-head Latent Attention (MLA). This pattern combines prefill-decode disaggregation, data parallelism, and custom request routing to maximize throughput and resource utilization.

For more details on the DeepSeek-V3 inference system design, see the [DeepSeek-V3 R1 Inference System Overview](https://github.com/deepseek-ai/open-infra-index/blob/main/202502OpenSourceWeek/day_6_one_more_thing_deepseekV3R1_inference_system_overview.md).

## Why combine patterns?

Large sparse MoE models like DeepSeek-V3 present unique serving challenges:

### Model characteristics

- **Sparse MoE architecture**: Only a subset of experts are activated per token, creating opportunities for parallel processing.
- **Multi-head Latent Attention (MLA)**: Reduces KV cache memory requirements, allowing larger batch sizes.
- **Massive scale**: 671B total parameters with 37B activated per token requires multi-node deployment.

### Pattern synergy

Each pattern addresses a specific challenge:

1. **Prefill-decode disaggregation**: Separates compute-intensive prefill from memory-bound decode, allowing different optimization strategies.
2. **Data parallelism with expert parallelism**: Distributes experts across replicas while replicating attention layers, enabling higher batch sizes to saturate experts.
3. **Custom request routing**: Routes requests with cache affinity to improve prefix cache hit rates and reduce redundant computation.

## Architecture overview

The following diagram shows how the three patterns compose:

```
┌─────────────────────────────────────────────────────┐
│                  Ingress                            │
│               (32 replicas)                         │
└─────────────┬───────────────────────────────────────┘
              │
              ▼
        ┌─────────────────┐
        │  PDProxyServer  │
        │  (32 replicas)  │
        └──┬──────────┬───┘
           │          │
     ┌─────┘          └──────────┐
     ▼                           ▼
┌────────────────┐       ┌────────────────┐
│ Prefill DP-32  │       │ Decode DP-96   │
│ (Expert Par.)  │ ─KV─→ │ (Expert Par.)  │
│                │       │                │
│ Rank 0-31      │       │ Rank 0-95      │
│ Work together  │       │ Work together  │
└────────────────┘       └────────────────┘
```

Key aspects:

- **Top layer**: Ingress (32 replicas) receives requests and routes to PDProxyServer replicas.
- **Middle layer**: PDProxyServer (32 replicas) coordinates prefill and decode phases with KV cache transfer. We roughly allocate 2 replicas per node. Custom request routing logic determines which prefill/decode replicas to use.
- **Bottom layer**: Data parallel replicas (32 prefill, 96 decode) with expert parallelism work together to serve each request.

## Pattern composition

### PD + DP architecture

The following example shows how to deploy DeepSeek-V3 with prefill-decode disaggregation and data parallelism:

```python
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_pd_openai_app
from ray.llm._internal.serve.deployments.data_parallel.dp_server import build_dp_deployment
from ray.serve.config import RequestRouterConfig
from ray import serve

# Configuration
P_DP_SIZE, D_DP_SIZE = 32, 96
DP_SIZE_PER_NODE = 8
NUM_PDPROXY_REPLICAS = 32

DS_V3_PATH = "/path/to/DeepSeek-V3"
MODEL_ID = "dsv3"

# Prefill configuration
p_config = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id=MODEL_ID,
        model_source=DS_V3_PATH,
    ),
    engine_kwargs=dict(
        data_parallel_size=P_DP_SIZE,
        enable_expert_parallel=True,
        enforce_eager=True,  # Required for deepep_high_throughput backend
        kv_transfer_config=dict(
            kv_connector="NixlConnector",
            kv_role="kv_both",
        ),
    ),
    deployment_config=dict(
        request_router_config=RequestRouterConfig(
            request_router_class="ray.llm._internal.serve.request_router.prefix_aware.PrefixCacheAffinityRouter",
            request_routing_stats_period_s=2,
            request_routing_stats_timeout_s=1,
        )
    ),
    experimental_configs={
        "NIXL_SIDE_CHANNEL_PORT_BASE": 10500,
        "dp_size_per_node": DP_SIZE_PER_NODE,
    },
    runtime_env={
        "VLLM_USE_DEEP_GEMM": "1",
        "VLLM_ALL2ALL_BACKEND": "deepep_high_throughput",
    },
)

# Decode configuration
d_config = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id=MODEL_ID,
        model_source=DS_V3_PATH,
    ),
    engine_kwargs=dict(
        data_parallel_size=D_DP_SIZE,
        enable_expert_parallel=True,
        enforce_eager=True,
        kv_transfer_config=dict(
            kv_connector="NixlConnector",
            kv_role="kv_both",
        ),
    ),
    deployment_config=dict(
        request_router_config=RequestRouterConfig(
            request_router_class="ray.llm._internal.serve.request_router.prefix_aware.PrefixCacheAffinityRouter",
            request_routing_stats_period_s=2,
            request_routing_stats_timeout_s=1,
        )
    ),
    experimental_configs={
        "NIXL_SIDE_CHANNEL_PORT_BASE": 9500,
        "dp_size_per_node": DP_SIZE_PER_NODE,
    },
    runtime_env={
        "VLLM_USE_DEEP_GEMM": "1",
        "VLLM_ALL2ALL_BACKEND": "deepep_low_latency",
    },
)

# Build DP deployments
p_deployment = build_dp_deployment(
    p_config, 
    name_prefix=f"Prefill-DP{P_DP_SIZE}:", 
)
d_deployment = build_dp_deployment(
    d_config, 
    name_prefix=f"Decode-DP{D_DP_SIZE}:", 
)

app = build_pd_openai_app({"prefill": p_deployment, "decode": d_deployment})
serve.run(app, blocking=True)
```

Key configuration details:

- **Data parallelism**: `data_parallel_size=32` for prefill and `96` for decode, with `enable_expert_parallel=True` distributing experts across replicas.
- **KV cache transfer**: `NixlConnector` for low-latency transfer between prefill and decode.
- **Expert communication**: Different `VLLM_ALL2ALL_BACKEND` settings optimize for prefill (high throughput) versus decode (low latency).
- **NIXL ports**: Separate port ranges for prefill and decode to avoid conflicts.
- **Request routing**: Prefix-aware routing at PDProxyServer level routes requests to appropriate prefill/decode replicas.

## See also

- {doc}`../overview` - High-level architecture overview
- {doc}`data-parallel` - Data parallelism architecture
- {doc}`prefill-decode` - Prefill-decode disaggregation architecture
- {doc}`../routing-policies` - Request routing architecture
- [DeepSeek-V3 R1 Inference System Overview](https://github.com/deepseek-ai/open-infra-index/blob/main/202502OpenSourceWeek/day_6_one_more_thing_deepseekV3R1_inference_system_overview.md)

