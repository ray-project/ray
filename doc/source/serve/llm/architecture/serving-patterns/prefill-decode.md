(serve-llm-architecture-prefill-decode)=
# Prefill-decode disaggregation

Prefill-decode (PD) disaggregation is a serving pattern that separates the prefill phase (processing input prompts) from the decode phase (generating tokens). This pattern was first pioneered in [DistServe](https://hao-ai-lab.github.io/blogs/distserve/) and optimizes resource utilization by scaling each phase independently based on its specific requirements.

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
- **Resource optimization**: Different engine configurations for different phases.

## Why disaggregate?

### Resource characteristics

Prefill and decode have different computational patterns:

| Phase | Characteristics | Resource Needs |
|-------|----------------|----------------|
| Prefill | Processes the entire prompt at once | High compute, lower memory |
| | Parallel token processing | Benefits from high FLOPS |
| | Short duration per request | Can use fewer replicas when decode-limited |
| Decode | Generates one token at a time | Lower compute, high memory |
| | Auto-regressive generation | Benefits from large batch sizes |
| | Long duration (many tokens) | Needs more replicas |

### Scaling benefits

Disaggregation enables:

- **Cost optimization**: The correct ratio of prefill to decode instances improves overall throughput per node.
- **Dynamic traffic adjustment**: Scale prefill and decode independently depending on workloads (prefill-heavy versus decode-heavy) and traffic volume.
- **Efficiency**: Prefill serves multiple requests while decode generates, allowing one prefill instance to feed multiple decode instances.

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
        self.prefill_handle = prefill_handle
        self.decode_handle = decode_handle
    
    async def chat(
        self,
        request: ChatCompletionRequest,
    ) -> AsyncGenerator[str, None]:
        """Handle chat completion with PD flow.
        
        Flow:
        1. Send request to prefill deployment
        2. Prefill processes prompt, transfers KV to decode
        3. Decode generates tokens, streams to client
        """
        # Prefill phase
        prefill_result = await self.prefill_handle.chat.remote(request)
        
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
- Manage errors in either phase per request.

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
            "kv_role": "kv_both",
        },
    ),
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
            "kv_role": "kv_both",
        },
    ),
)
```


### Request flow

```{figure} ../../images/pd.png
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

## Performance characteristics

### When to use PD disaggregation

Prefill-decode disaggregation works best when:

- **Long generations**: Decode phase dominates total end-to-end latency.
- **Imbalanced phases**: Prefill and decode need different resources.
- **Cost optimization**: Use different GPU types for each phase.
- **High decode load**: Many requests are in decode phase simultaneously.
- **Batch efficiency**: Prefill can batch multiple requests efficiently.

### When not to use PD

Consider alternatives when:

- **Short outputs**: Decode latency minimal, overhead not worth it.
- **Network limitations**: KV transfer overhead too high.
- **Small models**: Both phases fit comfortably on same resources.


## Design considerations

### KV cache transfer latency

The latency of KV cache transfer between prefill and decode affects overall request latency and it's mostly determined by network bandwidth. NIXL has different backend plugins, but its performance on different network stacks isn't mature yet. You should inspect your deployment to verify NIXL uses the right network backend for your environment.

### Phase load balancing

The system must balance load between prefill and decode phases. Mismatched scaling can lead to:

- **Prefill bottleneck**: Requests queue at prefill, decode replicas idle.
- **Decode bottleneck**: Prefill completes quickly, decode can't keep up.

Monitor both phases and adjust replica counts and autoscaling policies accordingly.

## See also

- {doc}`../overview` - High-level architecture overview
- {doc}`../core` - Core components and protocols
- {doc}`data-parallel` - Data parallel attention architecture
- {doc}`../../user-guides/prefill-decode` - Practical deployment guide

