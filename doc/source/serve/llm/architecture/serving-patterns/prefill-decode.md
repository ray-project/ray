(serve-llm-architecture-prefill-decode)=
# Prefill-decode disaggregation

Prefill-decode (PD) disaggregation is a serving pattern that separates the prefill phase (processing input prompts) from the decode phase (generating tokens). This pattern was first pioneered in [DistServe](https://hao-ai-lab.github.io/blogs/distserve/) and optimizes resource utilization by scaling each phase independently based on its specific requirements.

## Architecture overview

```{figure} ../../images/pd_arch.svg
---
width: 700px
name: pd-architecture
---
Prefill-decode disaggregation architecture with PDDecodeServer orchestrating remote prefill and local decode.
```

In prefill-decode disaggregation:

- **Prefill deployment** (`PDPrefillServer`): Processes input prompts and generates initial KV cache.
- **Decode deployment** (`PDDecodeServer`): Orchestrates the flow. Initiates prefill remotely, then runs decode locally on its own engine using the transferred KV cache.
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

### PDDecodeServer

`PDDecodeServer` is the decode-side LLM server that orchestrates the disaggregated flow. It owns a real engine and holds a handle to the prefill deployment:

```python
class PDDecodeServer(PDOrchestratorMixin, LLMServer):
    """Decode-side server with orchestration."""

    def __init__(
        self,
        llm_config: LLMConfig,
        prefill_server: DeploymentHandle,
    ):
        self._prefill_handle = prefill_server
        # Initialize real decode engine
        super().__init__(llm_config)

    async def chat(
        self,
        request: ChatCompletionRequest,
    ) -> AsyncGenerator[str, None]:
        """Handle chat completion with PD flow.

        Flow:
        1. Send request to prefill deployment (remote)
        2. Prefill processes prompt, returns KV metadata
        3. Run decode locally on own engine with KV metadata
        4. Stream tokens to client
        """
        ...
```

Key responsibilities:

- Orchestrate remote prefill then local decode.
- Handle KV cache metadata transfer.
- Stream responses from local decode to client.
- Manage errors in either phase per request.

### PDPrefillServer

`PDPrefillServer` extends `LLMServer` for the prefill side. It is a standard LLM server with an additional `prewarm_prefill` method for optional connector warm-up.

#### Pre-warming the connector

KV transfer connectors (such as NIXL) require a handshake between each prefill and decode replica that happens eagerly upon the first request. This can cause queing when traffic is high. Pre-warming allows to mitigate this cold-start problem by sending a tiny dummy request through the full prefill-to-decode path for every prefill replica so that the connector establishes its connections eagerly at startup before marking the replica as healthy. Enable it by setting `experimental_configs={"_prewarm_prefill_decode": True}` in the **decode** `LLMConfig`.

```python
prefill_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct"
    ),
    engine_kwargs=dict(
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
        kv_transfer_config={
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        },
    ),
)
```


### Request flow

```{figure} ../../images/pd.svg
---
width: 700px
name: pd-flow
---
Prefill-decode request flow showing KV cache transfer between phases.
```

Detailed request flow:

1. **Client request**: HTTP POST to `/v1/chat/completions`.
2. **Ingress**: Routes to `PDDecodeServer`.
3. **Decode → Prefill (remote)**: `PDDecodeServer` calls prefill deployment.
   - Prefill server processes prompt.
   - Generates KV cache.
   - Transfers KV to storage backend.
   - Returns KV metadata (location, size, etc.).
4. **Decode (local)**: `PDDecodeServer` runs decode on its own engine with the KV metadata.
   - Decode engine loads KV cache from storage.
   - Begins token generation.
   - Streams tokens back through ingress.
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
