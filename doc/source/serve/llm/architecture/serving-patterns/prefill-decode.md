(serve-llm-architecture-prefill-decode)=
# Prefill-decode disaggregation

Prefill-decode (PD) disaggregation is a serving pattern that separates the prefill phase (processing input prompts) from the decode phase (generating tokens). This pattern optimizes resource utilization by scaling each phase independently based on its specific requirements.

## Architecture overview

```{figure} ../../images/pd.png
---
width: 700px
name: pd-architecture
---
Prefill-decode disaggregation architecture with PDProxyServer coordinating prefill and decode deployments.
```

In prefill-decode disaggregation:
- **Prefill deployment**: Processes input prompts and generates initial KV cache.
- **Decode deployment**: Uses transferred KV cache to generate output tokens.
- **Independent scaling**: Each phase scales based on its own load.
- **Resource optimization**: Different hardware configurations for different phases.

## Why disaggregate?

### Resource characteristics

Prefill and decode have different computational patterns:

| Phase | Characteristics | Resource Needs |
|-------|----------------|----------------|
| Prefill | Processes entire prompt at once | High compute, lower memory |
| | Parallel token processing | Benefits from high FLOPS |
| | Short duration per request | Can use fewer replicas |
| Decode | Generates one token at a time | Lower compute, high memory |
| | Sequential generation | Benefits from large KV cache |
| | Long duration (many tokens) | Needs more replicas |

### Scaling benefits

Disaggregation enables:
- **Cost optimization**: Use expensive GPUs for prefill, cheaper for decode.
- **Throughput**: Scale decode independently for long generations.
- **Efficiency**: Prefill serves multiple requests while decode generates.
- **Flexibility**: Different autoscaling policies for each phase.

## Components

### PDProxyServer

`PDProxyServer` orchestrates the disaggregated serving:

```python
class PDProxyServer:
    """Proxy server for prefill-decode disaggregation."""
    
    def __init__(
        self,
        prefill_handle: DeploymentHandle,
        decode_handle: DeploymentHandle,
    ):
        """Initialize PD proxy.
        
        Args:
            prefill_handle: Handle to prefill LLMServer
            decode_handle: Handle to decode LLMServer
        """
        self.prefill_handle = prefill_handle
        self.decode_handle = decode_handle
    
    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request: Optional[Request] = None
    ) -> AsyncGenerator[str, None]:
        """Handle chat completion with PD flow.
        
        Flow:
        1. Send request to prefill deployment
        2. Prefill processes prompt, transfers KV to decode
        3. Decode generates tokens, streams to client
        """
        # Prefill phase
        prefill_result = await self.prefill_handle.chat.remote(
            request, raw_request
        )
        
        # Extract KV cache metadata
        kv_metadata = prefill_result["kv_metadata"]
        
        # Decode phase with KV reference
        async for chunk in self.decode_handle.chat.remote(
            request, 
            kv_metadata=kv_metadata
        ):
            yield chunk
```

Key responsibilities:
- Route requests between prefill and decode.
- Handle KV cache metadata transfer.
- Stream responses from decode to client.
- Manage errors in either phase.

### Prefill LLMServer

Standard `LLMServer` configured for prefill:

```python
prefill_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct"
    ),
    engine_kwargs=dict(
        # Prefill-specific configuration
        kv_transfer_config={
            "kv_connector": "NixlConnector",
            "kv_role": "kv_producer",  # Produces KV cache
            "engine_id": "prefill-engine"
        },
        # Optimize for prefill
        max_num_seqs=128,  # More concurrent prefills
    ),
    runtime_env=dict(env_vars={"VLLM_USE_V1": "1"}),
)
```

### Decode LLMServer

Standard `LLMServer` configured for decode:

```python
decode_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct"
    ),
    engine_kwargs=dict(
        # Decode-specific configuration
        kv_transfer_config={
            "kv_connector": "NixlConnector",
            "kv_role": "kv_consumer",  # Consumes KV cache
            "engine_id": "decode-engine"
        },
        # Optimize for decode
        max_model_len=8192,  # Large output space
    ),
    runtime_env=dict(env_vars={"VLLM_USE_V1": "1"}),
)
```

## KV cache transfer

### Transfer backends

vLLM v1 supports multiple KV cache transfer mechanisms:

#### NIXLConnector

Network-based transfer using NIXL (Network Interface for eXtended LLM):

**Advantages**:
- Simple setup, no external dependencies.
- Automatic network configuration.
- Good for single-cluster deployments.

**Configuration**:
```python
kv_transfer_config = {
    "kv_connector": "NixlConnector",
    "kv_role": "kv_both",  # or "kv_producer", "kv_consumer"
    "engine_id": "unique-engine-id"
}
```

#### LMCacheConnectorV1

Advanced caching with multiple storage backends:

**Advantages**:
- Supports persistent caching.
- Multiple storage backends (Redis, S3, local).
- Better for multi-cluster or long-lived caches.

**Requirements**:
- etcd server for metadata coordination.
- LMCache library installed.

**Configuration**:
```python
kv_transfer_config = {
    "kv_connector": "LMCacheConnectorV1",
    "kv_role": "kv_both",
    "engine_id": "unique-engine-id",
    "lmcache_config_file": "/path/to/lmcache.yaml"
}
```

### Request flow

```{figure} ../../images/dp_flow.png
---
width: 700px
name: pd-flow
---
Prefill-decode request flow showing KV cache transfer between phases.
```

Detailed request flow:

1. **Client request**: HTTP POST to `/v1/chat/completions`.
2. **Ingress**: Routes to `PDProxyServer`.
3. **Proxy → Prefill**: `PDProxyServer` calls prefill deployment.
   - Prefill server processes prompt.
   - Generates KV cache.
   - Transfers KV to storage backend.
   - Returns KV metadata (location, size, etc.).
4. **Proxy → Decode**: `PDProxyServer` calls decode deployment with KV metadata.
   - Decode server loads KV cache from storage.
   - Begins token generation.
   - Streams tokens back through proxy.
5. **Response streaming**: Client receives generated tokens.

:::{note}
The KV cache transfer is transparent to the client. From the client's perspective, it's a standard OpenAI API call.
:::

## Combining with data parallelism

You can combine PD with DP for maximum scalability:

```python
from ray.serve.llm.builders import (
    build_dp_deployment,
    build_pd_openai_app
)

# Build DP prefill (2 parallel instances)
prefill_dp = build_dp_deployment(
    llm_config=prefill_config,
    dp_size=2,
    deployment_name="prefill-dp"
)

# Build DP decode (4 parallel instances)
decode_dp = build_dp_deployment(
    llm_config=decode_config,
    dp_size=4,
    deployment_name="decode-dp"
)

# Build PD application with DP deployments
app = build_pd_openai_app({
    "prefill_deployment": prefill_dp,
    "decode_deployment": decode_dp,
})

serve.run(app)
```

This gives you:
- 2-way DP on prefill (2 replicas).
- 4-way DP on decode (4 replicas).
- Independent scaling of each.
- KV cache transfer between all prefill and decode replicas.

## Performance characteristics

### When to use PD disaggregation

Prefill-decode disaggregation works best when:

- **Long generations**: Decode phase dominates total latency.
- **Imbalanced phases**: Prefill and decode need different resources.
- **Cost optimization**: Want to use different GPU types for each phase.
- **High decode load**: Many requests in decode phase simultaneously.
- **Batch efficiency**: Prefill can batch multiple requests efficiently.

### When not to use PD

Consider alternatives when:

- **Short outputs**: Decode latency minimal, overhead not worth it.
- **Simple workload**: Single-phase serving sufficient.
- **Network limitations**: KV transfer overhead too high.
- **Small models**: Both phases fit comfortably on same resources.

### Resource allocation strategies

#### Prefill optimization

High compute, moderate memory:

```python
prefill_config = LLMConfig(
    accelerator_type="H100",  # High TFLOPS
    engine_kwargs=dict(
        tensor_parallel_size=2,
        max_num_seqs=64,  # Batch multiple prefills
        max_num_batched_tokens=4096,
    ),
)
```

#### Decode optimization

Moderate compute, high memory:

```python
decode_config = LLMConfig(
    accelerator_type="L4",  # Cost-effective
    engine_kwargs=dict(
        tensor_parallel_size=1,
        max_num_seqs=256,  # Many concurrent generations
        max_model_len=8192,  # Large context window
    ),
)
```

### Autoscaling strategies

Set different scaling targets:

```python
# Prefill: Scale based on input tokens
prefill_config = LLMConfig(
    deployment_config=dict(
        autoscaling_config=dict(
            target_ongoing_requests=16,  # Fewer concurrent
            upscale_delay_s=10,  # Quick scale-up
            downscale_delay_s=60,
        )
    ),
)

# Decode: Scale based on output tokens
decode_config = LLMConfig(
    deployment_config=dict(
        autoscaling_config=dict(
            target_ongoing_requests=32,  # More concurrent
            upscale_delay_s=5,  # Very quick scale-up
            downscale_delay_s=120,  # Slower scale-down
        )
    ),
)
```

## Design considerations

### KV cache transfer latency

The latency of KV cache transfer between prefill and decode affects overall request latency. Choose the appropriate connector based on your deployment:

- **NIXLConnector**: Lower latency for single-cluster deployments.
- **LMCacheConnectorV1**: Higher latency but more flexible for multi-cluster or persistent caching.

### Phase load balancing

The system must balance load between prefill and decode phases. Mismatched scaling can lead to:
- **Prefill bottleneck**: Requests queue at prefill, decode replicas idle.
- **Decode bottleneck**: Prefill completes quickly, decode can't keep up.

Monitor both phases and adjust replica counts and autoscaling policies accordingly.

### Error handling

The proxy must handle errors in either phase:
- **Prefill failure**: Return error to client immediately.
- **Decode failure**: Attempt retry or return partial results.
- **Transfer failure**: Retry transfer or fall back to single-phase.

## See also

- {doc}`../overview` - High-level architecture overview
- {doc}`../core` - Core components and protocols
- {doc}`data-parallel` - Data parallelism architecture
- {doc}`../../user-guides/prefill-decode` - Practical deployment guide

